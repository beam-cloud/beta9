package clients

import (
	"context"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"golang.org/x/time/rate"
)

// One budget per process, shared by all workspace clients and multipart parts.
// Disabled unless explicitly configured for the worker's available uplink.
var storageUploadBudget = storageUploadRate(os.Getenv("BETA9_STORAGE_UPLOAD_MBIT_PER_SEC"))

func storageUploadRate(value string) *rate.Limiter {
	mbit, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(mbit) || mbit < 1 || mbit > 100000 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(mbit*1000000/8), 256*1024)
}

type storageUploadHTTPClient struct {
	base   aws.HTTPClient
	budget *rate.Limiter
}

func (c storageUploadHTTPClient) Do(request *http.Request) (*http.Response, error) {
	if request.Body == nil || request.Body == http.NoBody || request.Method != http.MethodPut {
		return c.base.Do(request)
	}
	request = request.Clone(request.Context())
	wrap := func(body io.ReadCloser) io.ReadCloser {
		ctx, cancel := context.WithCancel(request.Context())
		return &storageUploadBody{ReadCloser: body, ctx: ctx, cancel: cancel, budget: c.budget}
	}
	request.Body = wrap(request.Body)
	if original := request.GetBody; original != nil {
		request.GetBody = func() (io.ReadCloser, error) {
			body, err := original()
			if err != nil {
				return nil, err
			}
			return wrap(body), nil
		}
	}
	return c.base.Do(request)
}

type storageUploadBody struct {
	io.ReadCloser
	ctx    context.Context
	cancel context.CancelFunc
	budget *rate.Limiter
}

func (b *storageUploadBody) Close() error {
	if b.cancel != nil {
		b.cancel()
	}
	return b.ReadCloser.Close()
}

func (b *storageUploadBody) Read(data []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	if len(data) > b.budget.Burst() {
		data = data[:b.budget.Burst()]
	}
	n, err := b.ReadCloser.Read(data)
	if n > 0 {
		if waitErr := b.budget.WaitN(b.ctx, n); waitErr != nil {
			return 0, waitErr
		}
	}
	return n, err
}
