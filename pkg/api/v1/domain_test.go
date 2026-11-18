package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type domainTestDNSResolver struct {
	txtRecords map[string][]string
	cnames     map[string]string
}

func (r *domainTestDNSResolver) LookupTXT(_ context.Context, name string) ([]string, error) {
	if records, ok := r.txtRecords[name]; ok {
		return records, nil
	}
	return nil, errors.New("not found")
}

func (r *domainTestDNSResolver) LookupCNAME(_ context.Context, host string) (string, error) {
	if cname, ok := r.cnames[host]; ok {
		return cname, nil
	}
	return "", errors.New("not found")
}

type domainTestBackendRepo struct {
	repository.BackendRepository
	workspace    types.Workspace
	workspaces   map[string]types.Workspace
	deployments  []types.DeploymentWithRelated
	domains      map[string]*types.CustomDomain
	nextID       uint
	checkLookups int
}

func domainMapKey(workspaceId uint, domain string) string {
	return fmt.Sprintf("%d:%s", workspaceId, domain)
}

func newDomainTestBackendRepo() *domainTestBackendRepo {
	ws1 := types.Workspace{
		Id:         1,
		ExternalId: "ws-1",
		Name:       "default",
	}
	return &domainTestBackendRepo{
		workspace: ws1,
		workspaces: map[string]types.Workspace{
			"ws-1": ws1,
		},
		deployments: []types.DeploymentWithRelated{
			{
				Deployment: types.Deployment{
					Id:          10,
					ExternalId:  "dep-1",
					Name:        "my-api",
					Active:      true,
					Subdomain:   "my-api-abc1234",
					WorkspaceId: 1,
					StubType:    types.StubTypeEndpointDeployment,
					Version:     1,
				},
				Workspace: ws1,
				Stub: types.Stub{
					Type: types.StubType(types.StubTypeEndpointDeployment),
				},
			},
		},
		domains: make(map[string]*types.CustomDomain),
		nextID:  1,
	}
}

func (r *domainTestBackendRepo) GetWorkspaceByExternalId(_ context.Context, externalId string) (types.Workspace, error) {
	if ws, ok := r.workspaces[externalId]; ok {
		return ws, nil
	}
	if externalId == r.workspace.ExternalId {
		return r.workspace, nil
	}
	return types.Workspace{}, errors.New("workspace not found")
}

func (r *domainTestBackendRepo) ListDeploymentsWithRelated(_ context.Context, filters types.DeploymentFilter) ([]types.DeploymentWithRelated, error) {
	var out []types.DeploymentWithRelated
	for _, dep := range r.deployments {
		if filters.WorkspaceID != 0 && dep.Deployment.WorkspaceId != filters.WorkspaceID {
			continue
		}
		if filters.Name != "" && !strings.Contains(dep.Name, filters.Name) {
			continue
		}
		if filters.Version > 0 && dep.Version != filters.Version {
			continue
		}
		out = append(out, dep)
	}
	return out, nil
}

func (r *domainTestBackendRepo) GetDeploymentByExternalId(_ context.Context, workspaceId uint, deploymentExternalId string) (*types.DeploymentWithRelated, error) {
	for i := range r.deployments {
		dep := r.deployments[i]
		if dep.Deployment.WorkspaceId == workspaceId && dep.ExternalId == deploymentExternalId {
			return &dep, nil
		}
	}
	return nil, nil
}

func (r *domainTestBackendRepo) GetLatestDeploymentByName(_ context.Context, workspaceId uint, name, stubType string, _ bool) (*types.DeploymentWithRelated, error) {
	for i := range r.deployments {
		dep := r.deployments[i]
		if dep.Deployment.WorkspaceId == workspaceId && dep.Name == name && (stubType == "" || dep.StubType == stubType) {
			return &dep, nil
		}
	}
	return nil, nil
}

func (r *domainTestBackendRepo) GetDeploymentByNameAndVersion(_ context.Context, workspaceId uint, name string, version uint, stubType string) (*types.DeploymentWithRelated, error) {
	for i := range r.deployments {
		dep := r.deployments[i]
		if dep.Deployment.WorkspaceId == workspaceId && dep.Name == name && dep.Version == version && (stubType == "" || dep.StubType == stubType) {
			return &dep, nil
		}
	}
	return nil, nil
}

func (r *domainTestBackendRepo) CreateCustomDomain(_ context.Context, cd *types.CustomDomain) (*types.CustomDomain, error) {
	key := domainMapKey(cd.WorkspaceId, cd.Domain)
	if _, exists := r.domains[key]; exists {
		return nil, types.ErrCustomDomainAlreadyExists
	}
	now := types.Time{Time: time.Now().UTC()}
	copyCD := *cd
	copyCD.Id = r.nextID
	r.nextID++
	copyCD.ExternalId = fmt.Sprintf("dom-%d-%s", cd.WorkspaceId, cd.Domain)
	copyCD.CreatedAt = now
	copyCD.UpdatedAt = now
	if copyCD.Source == "" {
		copyCD.Source = types.CustomDomainSourceAPI
	}
	if copyCD.Status == types.CustomDomainStatusActive {
		copyCD.VerifiedAt = types.NullTime{Time: now.Time, Valid: true}
	}
	r.domains[key] = &copyCD
	out := copyCD
	return &out, nil
}

func (r *domainTestBackendRepo) GetCustomDomainByIdentifier(_ context.Context, workspaceId uint, idOrDomain string) (*types.CustomDomain, error) {
	for _, cd := range r.domains {
		if cd.WorkspaceId == workspaceId && (cd.ExternalId == idOrDomain || cd.Domain == idOrDomain) {
			out := *cd
			return &out, nil
		}
	}
	return nil, nil
}

func (r *domainTestBackendRepo) GetActiveCustomDomainByDomain(_ context.Context, domain string) (*types.CustomDomain, error) {
	r.checkLookups++
	for _, cd := range r.domains {
		if cd.Domain == domain && cd.Status == types.CustomDomainStatusActive {
			out := *cd
			return &out, nil
		}
	}
	return nil, nil
}

func (r *domainTestBackendRepo) ListCustomDomains(_ context.Context, workspaceId uint, deploymentName string) ([]types.CustomDomain, error) {
	var out []types.CustomDomain
	for _, cd := range r.domains {
		if cd.WorkspaceId == workspaceId && (deploymentName == "" || cd.DeploymentName == deploymentName) {
			out = append(out, *cd)
		}
	}
	return out, nil
}

func (r *domainTestBackendRepo) ActivateCustomDomain(_ context.Context, workspaceId uint, externalId string) (*types.CustomDomain, error) {
	var target *types.CustomDomain
	for _, cd := range r.domains {
		if cd.WorkspaceId == workspaceId && cd.ExternalId == externalId {
			target = cd
			break
		}
	}
	if target == nil {
		return nil, types.ErrCustomDomainNotFound
	}
	now := types.Time{Time: time.Now().UTC()}
	for _, cd := range r.domains {
		if cd.Domain == target.Domain && cd.Status == types.CustomDomainStatusActive && cd.Id != target.Id {
			cd.Status = types.CustomDomainStatusFailed
			cd.UpdatedAt = now
		}
	}
	target.Status = types.CustomDomainStatusActive
	target.UpdatedAt = now
	target.VerifiedAt = types.NullTime{Time: now.Time, Valid: true}
	out := *target
	return &out, nil
}

func (r *domainTestBackendRepo) DeleteCustomDomain(_ context.Context, workspaceId uint, idOrDomain string) (*types.CustomDomain, error) {
	for key, cd := range r.domains {
		if cd.WorkspaceId == workspaceId && (cd.ExternalId == idOrDomain || cd.Domain == idOrDomain) {
			delete(r.domains, key)
			out := *cd
			return &out, nil
		}
	}
	return nil, types.ErrCustomDomainNotFound
}

func newDomainGroupForTest(repo *domainTestBackendRepo, resolver domainDNSResolver) *DomainGroup {
	return &DomainGroup{
		backendRepo: repo,
		resolver:    resolver,
		config: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				HTTP: types.HTTPConfig{
					ExternalHost: "app.example.com",
					ExternalPort: 443,
					TLS:          true,

					DomainCheckAllowUnauthenticated: true,
				},
			},
		},
	}
}

func domainAuthContext(e *echo.Echo, req *http.Request, rec *httptest.ResponseRecorder, paramNames, paramValues []string) *auth.HttpAuthContext {
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames(paramNames...)
	ctx.SetParamValues(paramValues...)
	return &auth.HttpAuthContext{
		Context: ctx,
		AuthInfo: &auth.AuthInfo{
			Workspace: &types.Workspace{Id: 1, ExternalId: "ws-1", Name: "default"},
			Token:     &types.Token{TokenType: types.TokenTypeWorkspacePrimary},
		},
	}
}

func TestDomainGroupCRUDVerifyAndCheck(t *testing.T) {
	e := echo.New()
	repo := newDomainTestBackendRepo()
	resolver := &domainTestDNSResolver{
		txtRecords: map[string][]string{},
		cnames:     map[string]string{},
	}
	group := newDomainGroupForTest(repo, resolver)

	// Create
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-1", strings.NewReader(`{"domain":"api.customer.io","deployment_name":"my-api"}`))
	createReq.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	createRec := httptest.NewRecorder()
	err := group.Create(domainAuthContext(e, createReq, createRec, []string{"workspaceId"}, []string{"ws-1"}))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	require.Equal(t, "api.customer.io", created["domain"])
	require.Equal(t, "pending", created["status"])
	require.Equal(t, "https://api.customer.io", created["url"])
	require.Equal(t, "_beta9-challenge.api.customer.io", created["txt_record_name"])
	require.Equal(t, "my-api-abc1234.app.example.com", created["cname_target"])
	token, _ := created["verification_token"].(string)
	require.NotEmpty(t, token)

	// List
	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/domain/ws-1", nil)
	listRec := httptest.NewRecorder()
	err = group.List(domainAuthContext(e, listReq, listRec, []string{"workspaceId"}, []string{"ws-1"}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listed []map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listed))
	require.Len(t, listed, 1)

	// Check unverified with verified only returns 404
	checkPendingReq := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=api.customer.io", nil)
	checkPendingRec := httptest.NewRecorder()
	err = group.Check(e.NewContext(checkPendingReq, checkPendingRec))
	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusNotFound, httpErr.Code)

	// Verify fails without DNS record
	verifyFailReq := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-1/api.customer.io/verify", nil)
	verifyFailRec := httptest.NewRecorder()
	err = group.Verify(domainAuthContext(e, verifyFailReq, verifyFailRec, []string{"workspaceId", "domainId"}, []string{"ws-1", "api.customer.io"}))
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, verifyFailRec.Code)

	// Add TXT record and verify succeeds
	resolver.txtRecords["_beta9-challenge.api.customer.io"] = []string{token}
	verifyOKReq := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-1/api.customer.io/verify", nil)
	verifyOKRec := httptest.NewRecorder()
	err = group.Verify(domainAuthContext(e, verifyOKReq, verifyOKRec, []string{"workspaceId", "domainId"}, []string{"ws-1", "api.customer.io"}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, verifyOKRec.Code)

	var verified map[string]any
	require.NoError(t, json.Unmarshal(verifyOKRec.Body.Bytes(), &verified))
	require.Equal(t, "active", verified["status"])
	require.NotEmpty(t, verified["verified_at"])

	// Check with verified only now returns 200
	checkOKReq := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=api.customer.io", nil)
	checkOKRec := httptest.NewRecorder()
	err = group.Check(e.NewContext(checkOKReq, checkOKRec))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, checkOKRec.Code)

	// Delete
	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/domain/ws-1/api.customer.io", nil)
	delRec := httptest.NewRecorder()
	err = group.Delete(domainAuthContext(e, delReq, delRec, []string{"workspaceId", "domainId"}, []string{"ws-1", "api.customer.io"}))
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, delRec.Code)

	// Retrieve after delete returns 404
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/domain/ws-1/api.customer.io", nil)
	getRec := httptest.NewRecorder()
	err = group.Retrieve(domainAuthContext(e, getReq, getRec, []string{"workspaceId", "domainId"}, []string{"ws-1", "api.customer.io"}))
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusNotFound, httpErr.Code)
}

func TestDomainGroupRejectsClusterSubdomainAndRequiresDNSVerify(t *testing.T) {
	e := echo.New()
	repo := newDomainTestBackendRepo()
	group := newDomainGroupForTest(repo, &domainTestDNSResolver{})

	badReq := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-1", strings.NewReader(`{"domain":"foo.app.example.com","deployment_name":"my-api"}`))
	badReq.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	badRec := httptest.NewRecorder()
	err := group.Create(domainAuthContext(e, badReq, badRec, []string{"workspaceId"}, []string{"ws-1"}))
	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusBadRequest, httpErr.Code)

	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-1", strings.NewReader(`{"domain":"force.customer.io","deployment_name":"my-api"}`))
	createReq.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	createRec := httptest.NewRecorder()
	err = group.Create(domainAuthContext(e, createReq, createRec, []string{"workspaceId"}, []string{"ws-1"}))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, createRec.Code)

	verifyReq := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-1/force.customer.io/verify", nil)
	verifyRec := httptest.NewRecorder()
	err = group.Verify(domainAuthContext(e, verifyReq, verifyRec, []string{"workspaceId", "domainId"}, []string{"ws-1", "force.customer.io"}))
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, verifyRec.Code)

	require.Equal(t, types.CustomDomainStatusPending, repo.domains[domainMapKey(1, "force.customer.io")].Status)
}

func TestDomainGroupCreateValidationAndTargetResolution(t *testing.T) {
	e := echo.New()
	repo := newDomainTestBackendRepo()
	repo.deployments = append(repo.deployments,
		types.DeploymentWithRelated{
			Deployment: types.Deployment{
				Id: 11, ExternalId: "dep-pod", Name: "shared-app", Active: true,
				Subdomain: "shared-pod-123", WorkspaceId: 1, StubType: types.StubTypePodDeployment, Version: 1,
			},
			Workspace: repo.workspace,
			Stub:      types.Stub{Type: types.StubType(types.StubTypePodDeployment), Config: `{"ports":[8000,9000]}`},
		},
		types.DeploymentWithRelated{
			Deployment: types.Deployment{
				Id: 12, ExternalId: "dep-ep", Name: "shared-app", Active: true,
				Subdomain: "shared-ep-123", WorkspaceId: 1, StubType: types.StubTypeEndpointDeployment, Version: 1,
			},
			Workspace: repo.workspace,
			Stub:      types.Stub{Type: types.StubType(types.StubTypeEndpointDeployment), Config: `{"authorized":true}`},
		},
		types.DeploymentWithRelated{
			Deployment: types.Deployment{
				Id: 13, ExternalId: "dep-fn", Name: "my-fn", Active: true,
				Subdomain: "my-fn-123", WorkspaceId: 1, StubType: types.StubTypeFunctionDeployment, Version: 1,
			},
			Workspace: repo.workspace,
			Stub:      types.Stub{Type: types.StubType(types.StubTypeFunctionDeployment), Config: `{}`},
		},
		types.DeploymentWithRelated{
			Deployment: types.Deployment{
				Id: 14, ExternalId: "dep-tcp-pod", Name: "tcp-pod", Active: true,
				Subdomain: "tcp-pod-123", WorkspaceId: 1, StubType: types.StubTypePodDeployment, Version: 1,
			},
			Workspace: repo.workspace,
			Stub:      types.Stub{Type: types.StubType(types.StubTypePodDeployment), Config: `{"ports":[5432],"tcp":true}`},
		},
		types.DeploymentWithRelated{
			Deployment: types.Deployment{
				Id: 15, ExternalId: "dep-portless-pod", Name: "worker-pod", Active: true,
				Subdomain: "worker-pod-123", WorkspaceId: 1, StubType: types.StubTypePodDeployment, Version: 1,
			},
			Workspace: repo.workspace,
			Stub:      types.Stub{Type: types.StubType(types.StubTypePodDeployment), Config: `{}`},
		},
	)
	group := newDomainGroupForTest(repo, &domainTestDNSResolver{})

	postCreate := func(body string) (int, error) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-1", strings.NewReader(body))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
		rec := httptest.NewRecorder()
		err := group.Create(domainAuthContext(e, req, rec, []string{"workspaceId"}, []string{"ws-1"}))
		return rec.Code, err
	}

	// Out-of-range port and version are rejected.
	_, err := postCreate(`{"domain":"a.customer.io","deployment_name":"my-api","port":70000}`)
	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusBadRequest, httpErr.Code)

	_, err = postCreate(`{"domain":"a.customer.io","deployment_name":"my-api","version":3000000000}`)
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusBadRequest, httpErr.Code)

	// Conflicting deployment_id metadata is rejected.
	for _, body := range []string{
		`{"domain":"a.customer.io","deployment_id":"dep-1","deployment_name":"other"}`,
		`{"domain":"a.customer.io","deployment_id":"dep-1","stub_type":"pod"}`,
		`{"domain":"a.customer.io","deployment_id":"dep-1","version":9}`,
	} {
		_, err = postCreate(body)
		require.ErrorAs(t, err, &httpErr)
		require.Equal(t, http.StatusBadRequest, httpErr.Code)
	}

	// Unsupported stub types (function, taskqueue) are rejected.
	for _, body := range []string{
		`{"domain":"fn.customer.io","deployment_name":"my-fn"}`,
		`{"domain":"fn.customer.io","deployment_name":"my-fn","stub_type":"function"}`,
		`{"domain":"tq.customer.io","deployment_name":"my-api","stub_type":"taskqueue"}`,
	} {
		_, err = postCreate(body)
		require.ErrorAs(t, err, &httpErr)
		require.Equal(t, http.StatusBadRequest, httpErr.Code)
	}

	// Port on non-Pod deployment is rejected.
	_, err = postCreate(`{"domain":"ep-port.customer.io","deployment_name":"my-api","port":8000}`)
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusBadRequest, httpErr.Code)

	// TCP Pod, portless Pod (with or without explicit port), .cluster.local, and undeclared Pod port are rejected.
	for _, body := range []string{
		`{"domain":"db.customer.io","deployment_name":"tcp-pod"}`,
		`{"domain":"worker.customer.io","deployment_name":"worker-pod"}`,
		`{"domain":"worker2.customer.io","deployment_name":"worker-pod","port":8000}`,
		`{"domain":"svc.default.svc.cluster.local","deployment_name":"my-api"}`,
		`{"domain":"badport.customer.io","deployment_name":"shared-app","stub_type":"pod","port":7777}`,
	} {
		_, err = postCreate(body)
		require.ErrorAs(t, err, &httpErr)
		require.Equal(t, http.StatusBadRequest, httpErr.Code)
	}

	// Ambiguous deployment_name across multiple stub types requires stub_type.
	_, err = postCreate(`{"domain":"ambig.customer.io","deployment_name":"shared-app"}`)
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusBadRequest, httpErr.Code)

	// Specifying stub_type disambiguates and persists stub_type.
	code, err := postCreate(`{"domain":"pod.customer.io","deployment_name":"shared-app","stub_type":"pod","port":9000}`)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, code)
	require.Equal(t, types.StubTypePodDeployment, repo.domains[domainMapKey(1, "pod.customer.io")].StubType)
	require.Equal(t, uint32(9000), repo.domains[domainMapKey(1, "pod.customer.io")].Port)

	// CNAME verification accepts deployment's exact CNAME target and rejects another deployment's subdomain.
	cnameResolver := &domainTestDNSResolver{
		txtRecords: map[string][]string{},
		cnames:     map[string]string{"cname.customer.io": "other-app-9999999.app.example.com."},
	}
	cnameGroup := newDomainGroupForTest(repo, cnameResolver)
	code, err = postCreate(`{"domain":"cname.customer.io","deployment_name":"my-api"}`)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, code)

	vReq := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-1/cname.customer.io/verify", nil)
	vRec := httptest.NewRecorder()
	require.NoError(t, cnameGroup.Verify(domainAuthContext(e, vReq, vRec, []string{"workspaceId", "domainId"}, []string{"ws-1", "cname.customer.io"})))
	require.Equal(t, http.StatusBadRequest, vRec.Code)

	cnameResolver.cnames["cname.customer.io"] = "MY-API-ABC1234.app.example.com."
	vReq2 := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-1/cname.customer.io/verify", nil)
	vRec2 := httptest.NewRecorder()
	require.NoError(t, cnameGroup.Verify(domainAuthContext(e, vReq2, vRec2, []string{"workspaceId", "domainId"}, []string{"ws-1", "cname.customer.io"})))
	require.Equal(t, http.StatusOK, vRec2.Code)
}

func TestDomainGroupMultiWorkspaceTakeoverAndCheckToken(t *testing.T) {
	e := echo.New()
	repo := newDomainTestBackendRepo()
	ws2 := types.Workspace{Id: 2, ExternalId: "ws-2", Name: "other"}
	repo.workspaces["ws-2"] = ws2
	repo.deployments = append(repo.deployments, types.DeploymentWithRelated{
		Deployment: types.Deployment{
			Id: 20, ExternalId: "dep-2", Name: "ws2-api", Active: true,
			Subdomain: "ws2-api-def5678", WorkspaceId: 2, StubType: types.StubTypeEndpointDeployment, Version: 1,
		},
		Workspace: ws2,
		Stub:      types.Stub{Type: types.StubType(types.StubTypeEndpointDeployment)},
	})

	resolver := &domainTestDNSResolver{
		txtRecords: map[string][]string{},
		cnames:     map[string]string{},
	}
	group := newDomainGroupForTest(repo, resolver)
	group.config.GatewayService.HTTP.DomainCheckToken = "secret-check-token"
	NewDomainGroup(e.Group("/api/v1/domain"), func(next echo.HandlerFunc) echo.HandlerFunc { return next }, repo, nil, group.config)

	authFor := func(ws types.Workspace, req *http.Request, rec *httptest.ResponseRecorder, names, vals []string) *auth.HttpAuthContext {
		ctx := e.NewContext(req, rec)
		ctx.SetParamNames(names...)
		ctx.SetParamValues(vals...)
		return &auth.HttpAuthContext{
			Context: ctx,
			AuthInfo: &auth.AuthInfo{
				Workspace: &ws,
				Token:     &types.Token{TokenType: types.TokenTypeWorkspacePrimary},
			},
		}
	}

	// Workspace 1 creates and verifies the domain.
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-1", strings.NewReader(`{"domain":"shared.customer.io","deployment_name":"my-api"}`))
	req1.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec1 := httptest.NewRecorder()
	require.NoError(t, group.Create(authFor(repo.workspace, req1, rec1, []string{"workspaceId"}, []string{"ws-1"})))
	require.Equal(t, http.StatusCreated, rec1.Code)
	tok1 := repo.domains[domainMapKey(1, "shared.customer.io")].VerificationToken
	resolver.txtRecords["_beta9-challenge.shared.customer.io"] = []string{tok1}

	vRec1 := httptest.NewRecorder()
	require.NoError(t, group.Verify(authFor(repo.workspace, httptest.NewRequest(http.MethodPost, "/", nil), vRec1, []string{"workspaceId", "domainId"}, []string{"ws-1", "shared.customer.io"})))
	require.Equal(t, http.StatusOK, vRec1.Code)
	require.Equal(t, types.CustomDomainStatusActive, repo.domains[domainMapKey(1, "shared.customer.io")].Status)

	// Workspace 2 can create a pending claim without squatting error, and WS1 remains active until WS2 proves DNS.
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-2", strings.NewReader(`{"domain":"shared.customer.io","deployment_name":"ws2-api"}`))
	req2.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec2 := httptest.NewRecorder()
	require.NoError(t, group.Create(authFor(ws2, req2, rec2, []string{"workspaceId"}, []string{"ws-2"})))
	require.Equal(t, http.StatusCreated, rec2.Code)
	require.Equal(t, types.CustomDomainStatusActive, repo.domains[domainMapKey(1, "shared.customer.io")].Status)

	vRec2Fail := httptest.NewRecorder()
	require.NoError(t, group.Verify(authFor(ws2, httptest.NewRequest(http.MethodPost, "/", nil), vRec2Fail, []string{"workspaceId", "domainId"}, []string{"ws-2", "shared.customer.io"})))
	require.Equal(t, http.StatusBadRequest, vRec2Fail.Code)
	require.Equal(t, types.CustomDomainStatusActive, repo.domains[domainMapKey(1, "shared.customer.io")].Status)

	// Once WS2 proves DNS, WS2 becomes active and WS1 is demoted to failed.
	tok2 := repo.domains[domainMapKey(2, "shared.customer.io")].VerificationToken
	resolver.txtRecords["_beta9-challenge.shared.customer.io"] = []string{tok2}
	vRec2OK := httptest.NewRecorder()
	require.NoError(t, group.Verify(authFor(ws2, httptest.NewRequest(http.MethodPost, "/", nil), vRec2OK, []string{"workspaceId", "domainId"}, []string{"ws-2", "shared.customer.io"})))
	require.Equal(t, http.StatusOK, vRec2OK.Code)
	require.Equal(t, types.CustomDomainStatusActive, repo.domains[domainMapKey(2, "shared.customer.io")].Status)
	require.Equal(t, types.CustomDomainStatusFailed, repo.domains[domainMapKey(1, "shared.customer.io")].Status)

	// /check rejects missing/wrong token and allows valid token, bypassing the rate limiter even after burst.
	for i := 0; i < 50; i++ {
		checkReq := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=shared.customer.io&token=secret-check-token", nil)
		checkReq.RemoteAddr = "10.0.0.5:1234"
		checkRec := httptest.NewRecorder()
		e.ServeHTTP(checkRec, checkReq)
		require.Equal(t, http.StatusOK, checkRec.Code)
	}

	badTokenReq := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=shared.customer.io&token=wrong", nil)
	badTokenReq.RemoteAddr = "10.0.0.5:1234"
	badTokenRec := httptest.NewRecorder()
	e.ServeHTTP(badTokenRec, badTokenReq)
	require.Equal(t, http.StatusNotFound, badTokenRec.Code)

	// /check also accepts X-Domain-Check-Token and Authorization: Bearer headers (bypassing rate limit)
	// and rejects wrong header tokens even when no query param is supplied.
	for i := 0; i < 50; i++ {
		hdrReq := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=shared.customer.io", nil)
		hdrReq.RemoteAddr = "10.0.0.6:1234"
		if i%2 == 0 {
			hdrReq.Header.Set("X-Domain-Check-Token", "secret-check-token")
		} else {
			hdrReq.Header.Set("Authorization", "Bearer secret-check-token")
		}
		hdrRec := httptest.NewRecorder()
		e.ServeHTTP(hdrRec, hdrReq)
		require.Equal(t, http.StatusOK, hdrRec.Code)
	}

	badHdrReq := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=shared.customer.io", nil)
	badHdrReq.RemoteAddr = "10.0.0.6:1234"
	badHdrReq.Header.Set("X-Domain-Check-Token", "wrong")
	badHdrRec := httptest.NewRecorder()
	e.ServeHTTP(badHdrRec, badHdrReq)
	require.Equal(t, http.StatusNotFound, badHdrRec.Code)
}

func TestDomainGroupCheckTrimRateLimitAndRedisCache(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{mr.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)

	// 1. Trimmed token consistency + X-Forwarded-For cannot bypass rate limiter.
	e := echo.New()
	repo := newDomainTestBackendRepo()
	repo.domains[domainMapKey(1, "active.customer.io")] = &types.CustomDomain{
		WorkspaceId: 1,
		Domain:      "active.customer.io",
		Status:      types.CustomDomainStatusActive,
	}
	cfg := newDomainGroupForTest(repo, &domainTestDNSResolver{}).config
	cfg.GatewayService.HTTP.DomainCheckToken = "  trimmed-secret  "
	NewDomainGroup(e.Group("/api/v1/domain"), func(next echo.HandlerFunc) echo.HandlerFunc { return next }, repo, redisClient, cfg)

	for i := 0; i < 45; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=active.customer.io&token=trimmed-secret", nil)
		req.RemoteAddr = "10.1.2.3:4567"
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Unauthenticated requests from the same RemoteAddr must hit the rate limiter after burst (40),
	// even when X-Forwarded-For and X-Real-IP change on every request.
	rateLimited := false
	for i := 0; i < 50; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=active.customer.io", nil)
		req.RemoteAddr = "10.9.8.7:1111"
		req.Header.Set("X-Forwarded-For", fmt.Sprintf("203.0.113.%d", i+1))
		req.Header.Set("X-Real-IP", fmt.Sprintf("198.51.100.%d", i+1))
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code == http.StatusTooManyRequests {
			rateLimited = true
			break
		}
		require.Equal(t, http.StatusNotFound, rec.Code)
	}
	require.True(t, rateLimited, "rotating X-Forwarded-For / X-Real-IP headers must not bypass /domain/check rate limit")

	// 2. Whitespace-only DomainCheckToken is treated as unset across both Skipper and Check.
	eOpen := echo.New()
	repoOpen := newDomainTestBackendRepo()
	resolver := &domainTestDNSResolver{txtRecords: map[string][]string{}, cnames: map[string]string{}}
	openCfg := newDomainGroupForTest(repoOpen, resolver).config
	openCfg.GatewayService.HTTP.DomainCheckToken = "   "
	openGroup := NewDomainGroup(eOpen.Group("/api/v1/domain"), func(next echo.HandlerFunc) echo.HandlerFunc { return next }, repoOpen, redisClient, openCfg)
	openGroup.resolver = resolver

	// Unknown domain hits DB once, then negative cache serves subsequent /check calls.
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=cached.customer.io", nil)
		req.RemoteAddr = "10.2.3.4:2222"
		rec := httptest.NewRecorder()
		eOpen.ServeHTTP(rec, req)
		require.Equal(t, http.StatusNotFound, rec.Code)
	}
	require.Equal(t, 1, repoOpen.checkLookups)

	// Create + Verify invalidates the /check negative cache so /check immediately returns 200 and caches positive result.
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-1", strings.NewReader(`{"domain":"cached.customer.io","deployment_name":"my-api"}`))
	createReq.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	createRec := httptest.NewRecorder()
	require.NoError(t, openGroup.Create(domainAuthContext(eOpen, createReq, createRec, []string{"workspaceId"}, []string{"ws-1"})))
	require.Equal(t, http.StatusCreated, createRec.Code)

	tok := repoOpen.domains[domainMapKey(1, "cached.customer.io")].VerificationToken
	resolver.txtRecords["_beta9-challenge.cached.customer.io"] = []string{tok}
	verifyReq := httptest.NewRequest(http.MethodPost, "/api/v1/domain/ws-1/cached.customer.io/verify", nil)
	verifyRec := httptest.NewRecorder()
	require.NoError(t, openGroup.Verify(domainAuthContext(eOpen, verifyReq, verifyRec, []string{"workspaceId", "domainId"}, []string{"ws-1", "cached.customer.io"})))
	require.Equal(t, http.StatusOK, verifyRec.Code)

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=cached.customer.io", nil)
		req.RemoteAddr = "10.2.3.4:2222"
		rec := httptest.NewRecorder()
		eOpen.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}
	require.Equal(t, 2, repoOpen.checkLookups)

	// Delete invalidates the /check positive cache so /check returns 404 again.
	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/domain/ws-1/cached.customer.io", nil)
	delRec := httptest.NewRecorder()
	require.NoError(t, openGroup.Delete(domainAuthContext(eOpen, delReq, delRec, []string{"workspaceId", "domainId"}, []string{"ws-1", "cached.customer.io"})))
	require.Equal(t, http.StatusNoContent, delRec.Code)

	afterDelReq := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=cached.customer.io", nil)
	afterDelReq.RemoteAddr = "10.2.3.4:2222"
	afterDelRec := httptest.NewRecorder()
	eOpen.ServeHTTP(afterDelRec, afterDelReq)
	require.Equal(t, http.StatusNotFound, afterDelRec.Code)
	require.Equal(t, 3, repoOpen.checkLookups)
}

func TestDomainCheckDisabledWithoutToken(t *testing.T) {
	e := echo.New()
	repo := newDomainTestBackendRepo()
	repo.domains[domainMapKey(1, "active.customer.io")] = &types.CustomDomain{
		WorkspaceId: 1, Domain: "active.customer.io", Status: types.CustomDomainStatusActive,
	}
	cfg := newDomainGroupForTest(repo, &domainTestDNSResolver{}).config
	cfg.GatewayService.HTTP.DomainCheckAllowUnauthenticated = false
	NewDomainGroup(e.Group("/api/v1/domain"), func(next echo.HandlerFunc) echo.HandlerFunc { return next }, repo, nil, cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=active.customer.io", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

// Deployment targets share gateway addresses, so matching IPs must never count
// as proof of ownership (it would let any workspace claim a domain that already
// points at the gateway).
func TestVerifyCustomDomainDNSSharedAddressIsNotProof(t *testing.T) {
	cd := &types.CustomDomain{Domain: "victim.customer.io", CNAMETarget: "attacker-dep.app.example.com", VerificationToken: "attacker-token"}

	ok, _ := verifyCustomDomainDNS(context.Background(), &domainTestDNSResolver{
		// The victim's domain points at the victim's own deployment target.
		cnames: map[string]string{"victim.customer.io": "victim-dep.app.example.com."},
	}, cd)
	require.False(t, ok)

	ok, _ = verifyCustomDomainDNS(context.Background(), &domainTestDNSResolver{
		cnames: map[string]string{"victim.customer.io": "attacker-dep.app.example.com."},
	}, cd)
	require.True(t, ok)
}

func TestDomainCheckClusterRateLimit(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)

	prev := domainCheckClusterLimit
	domainCheckClusterLimit = 5
	defer func() { domainCheckClusterLimit = prev }()

	e := echo.New()
	repo := newDomainTestBackendRepo()
	cfg := newDomainGroupForTest(repo, &domainTestDNSResolver{}).config
	NewDomainGroup(e.Group("/api/v1/domain"), func(next echo.HandlerFunc) echo.HandlerFunc { return next }, repo, redisClient, cfg)

	var codes []int
	for i := 0; i < 8; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=nope.customer.io", nil)
		req.RemoteAddr = "10.5.5.5:1000"
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		codes = append(codes, rec.Code)
	}
	require.Equal(t, http.StatusNotFound, codes[4])
	require.Equal(t, http.StatusTooManyRequests, codes[5])

	// A different client is unaffected.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/domain/check?domain=nope.customer.io", nil)
	req.RemoteAddr = "10.6.6.6:1000"
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

type domainSlowTXTResolver struct {
	domainTestDNSResolver
}

// LookupTXT never answers until the deadline, like a resolver that drops TXT
// queries for names that do not exist.
func (r *domainSlowTXTResolver) LookupTXT(ctx context.Context, _ string) ([]string, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestVerifyCustomDomainDNSSlowTXTDoesNotStarveOtherChecks(t *testing.T) {
	cd := &types.CustomDomain{Domain: "api.customer.io", CNAMETarget: "dep.app.example.com", VerificationToken: "tok"}
	resolver := &domainSlowTXTResolver{domainTestDNSResolver{
		cnames: map[string]string{"api.customer.io": "dep.app.example.com."},
	}}

	start := time.Now()
	ok, _ := verifyCustomDomainDNS(context.Background(), resolver, cd)
	require.True(t, ok)
	require.Less(t, time.Since(start), 2*time.Second, "a hanging TXT lookup must not delay the other proofs")
}
