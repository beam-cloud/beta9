package clients

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	// storageObjectParts is how many parts the store lets an object have.
	storageObjectParts = 10000

	storageUploadAbortTimeout = 10 * time.Second
)

// GrowingUpload sends a file to the store while the file is being written. A
// part goes as soon as the file has all of it, so the upload is under way
// long before the writing ends, and takes as long as the slower of the two.
//
// The object is in the store once Finish has succeeded, and not before. An
// upload that fails is aborted, which leaves the key as it was: the file can
// still be uploaded whole. The requests are those of the SDK's uploader.
type GrowingUpload struct {
	client      *s3.Client
	bucket, key string
	file        io.ReaderAt
	partSize    int64
	sent        func(int64)

	ctx     context.Context
	cancel  context.CancelFunc
	workers chan struct{}
	sending sync.WaitGroup
	create  sync.Once

	mu       sync.Mutex
	id       string // of the multipart upload, once the store has one
	queued   int64  // how much of the file parts have been made of
	uploaded int64
	parts    []s3types.CompletedPart
	err      error
}

// UploadGrowing starts sending file to key, in parts of partSize. Nothing is
// asked of the store until the file has a whole part. sent, when not nil, is
// told how much of the file the store has after each part.
func (c *WorkspaceStorageClient) UploadGrowing(ctx context.Context, key string, file io.ReaderAt, partSize int64, sent func(int64)) *GrowingUpload {
	ctx, cancel := context.WithCancel(ctx)
	return &GrowingUpload{
		client:   c.StorageClient.s3Client,
		bucket:   *c.WorkspaceStorage.BucketName,
		key:      key,
		file:     file,
		partSize: partSize,
		sent:     sent,
		ctx:      ctx,
		cancel:   cancel,
		workers:  make(chan struct{}, storageMultipartUploadWorkers),
	}
}

// Wrote says that the file now holds its first total bytes, which do not
// change again.
func (u *GrowingUpload) Wrote(total int64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.queue(total)
}

// queue makes parts of what the file holds that no part has. u.mu is held.
func (u *GrowingUpload) queue(total int64) {
	for u.err == nil && total-u.queued >= u.partSize {
		u.send(u.partSize)
	}
}

// send starts uploading the file's next size bytes. u.mu is held.
func (u *GrowingUpload) send(size int64) {
	number := u.queued/u.partSize + 1
	if number > storageObjectParts {
		u.err = fmt.Errorf("the file has more than the %d parts of %d bytes an object may have", storageObjectParts, u.partSize)
		u.cancel()
		return
	}
	offset := u.queued
	u.queued += size
	u.sending.Add(1)
	go func() {
		defer u.sending.Done()
		if err := u.part(int32(number), offset, size); err != nil {
			u.fail(err)
		}
	}()
}

func (u *GrowingUpload) part(number int32, offset, size int64) error {
	select {
	case u.workers <- struct{}{}:
		defer func() { <-u.workers }()
	case <-u.ctx.Done():
		return u.ctx.Err()
	}
	id, err := u.uploadID()
	if err != nil {
		return err
	}
	out, err := u.client.UploadPart(u.ctx, &s3.UploadPartInput{
		Bucket:            aws.String(u.bucket),
		Key:               aws.String(u.key),
		UploadId:          aws.String(id),
		PartNumber:        aws.Int32(number),
		Body:              io.NewSectionReader(u.file, offset, size),
		ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		return fmt.Errorf("upload part %d: %w", number, err)
	}

	u.mu.Lock()
	u.parts = append(u.parts, s3types.CompletedPart{
		PartNumber:        aws.Int32(number),
		ETag:              out.ETag,
		ChecksumCRC32:     out.ChecksumCRC32,
		ChecksumCRC32C:    out.ChecksumCRC32C,
		ChecksumCRC64NVME: out.ChecksumCRC64NVME,
		ChecksumSHA1:      out.ChecksumSHA1,
		ChecksumSHA256:    out.ChecksumSHA256,
	})
	u.uploaded += size
	uploaded := u.uploaded
	u.mu.Unlock()

	if u.sent != nil {
		u.sent(uploaded)
	}
	return nil
}

// uploadID is the multipart upload the parts are of. The first part to be
// sent asks the store for it.
func (u *GrowingUpload) uploadID() (string, error) {
	u.create.Do(func() {
		out, err := u.client.CreateMultipartUpload(u.ctx, &s3.CreateMultipartUploadInput{
			Bucket:            aws.String(u.bucket),
			Key:               aws.String(u.key),
			ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32,
		})
		if err != nil {
			u.fail(fmt.Errorf("create multipart upload: %w", err))
			return
		}
		u.mu.Lock()
		u.id = aws.ToString(out.UploadId)
		u.mu.Unlock()
	})

	u.mu.Lock()
	defer u.mu.Unlock()
	if u.id == "" {
		return "", u.err
	}
	return u.id, nil
}

func (u *GrowingUpload) fail(err error) {
	u.mu.Lock()
	if u.err == nil {
		u.err = err
	}
	u.mu.Unlock()
	u.cancel()
}

// Finish says that the file is whole at total bytes, sends what is left of
// it, and makes the object of the parts.
func (u *GrowingUpload) Finish(total int64) error {
	u.mu.Lock()
	u.queue(total)
	if u.err == nil && total > u.queued {
		u.send(total - u.queued)
	}
	if u.err == nil && total != u.queued {
		u.err = fmt.Errorf("the file has %d bytes, and %d of it were sent", total, u.queued)
		u.cancel()
	}
	u.mu.Unlock()
	u.sending.Wait()

	u.mu.Lock()
	err, id, parts := u.err, u.id, u.parts
	u.mu.Unlock()
	if err == nil && len(parts) == 0 {
		err = errors.New("the file is empty")
	}
	if err == nil {
		slices.SortFunc(parts, func(a, b s3types.CompletedPart) int {
			return cmp.Compare(aws.ToInt32(a.PartNumber), aws.ToInt32(b.PartNumber))
		})
		_, err = u.client.CompleteMultipartUpload(u.ctx, &s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(u.bucket),
			Key:             aws.String(u.key),
			UploadId:        aws.String(id),
			MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
		})
		if err != nil {
			err = fmt.Errorf("complete multipart upload: %w", err)
		}
	}
	if err != nil {
		u.fail(err)
		u.abort()
		return err
	}
	u.cancel()
	return nil
}

// Abort gives the upload up. It returns once nothing is being sent.
func (u *GrowingUpload) Abort() {
	u.fail(errors.New("upload aborted"))
	u.sending.Wait()
	u.abort()
}

// abort has the store discard the parts it was sent. The upload's own
// context is done by now.
func (u *GrowingUpload) abort() {
	u.mu.Lock()
	id := u.id
	u.id = ""
	u.mu.Unlock()
	if id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(u.ctx), storageUploadAbortTimeout)
	defer cancel()
	// Parts the store keeps are what its lifecycle rules are for.
	_, _ = u.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(u.key),
		UploadId: aws.String(id),
	})
}
