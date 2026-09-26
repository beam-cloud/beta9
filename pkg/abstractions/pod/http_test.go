package pod

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

func TestSandboxAuthAdmitsTheOwnerOfAnAuthorizedSandbox(t *testing.T) {
	instances := common.NewSafeMap[*podInstance]()
	instances.Set("stub-1", &podInstance{AutoscaledInstance: &abstractions.AutoscaledInstance{
		Ctx:        context.Background(),
		Workspace:  &types.Workspace{ExternalId: "ws-1"},
		StubConfig: &types.StubConfigV1{Authorized: true},
	}})
	group := &podGroup{ps: &GenericPodService{podInstances: instances}}
	handler := group.withSandboxAuth(func(ctx echo.Context) error { return ctx.NoContent(http.StatusOK) })

	request := func(authInfo *auth.AuthInfo) int {
		e := echo.New()
		rec := httptest.NewRecorder()
		ctx := e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), rec)
		ctx.SetParamNames("stubId")
		ctx.SetParamValues("stub-1")
		var c echo.Context = ctx
		if authInfo != nil {
			c = &auth.HttpAuthContext{Context: ctx, AuthInfo: authInfo}
		}
		if err := handler(c); err != nil {
			if he, ok := err.(*echo.HTTPError); ok {
				return he.Code
			}
			t.Fatal(err)
		}
		return rec.Code
	}

	owner := &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "ws-1"}, Token: &types.Token{}}
	other := &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "ws-2"}, Token: &types.Token{}}
	admin := &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "ws-2"}, Token: &types.Token{TokenType: types.TokenTypeClusterAdmin}}
	for name, want := range map[string]struct {
		info *auth.AuthInfo
		code int
	}{"owner": {owner, http.StatusOK}, "other workspace": {other, http.StatusUnauthorized}, "cluster admin": {admin, http.StatusOK}, "anonymous": {nil, http.StatusBadRequest}} {
		if got := request(want.info); got != want.code {
			t.Errorf("%s: status %d, want %d", name, got, want.code)
		}
	}
}
