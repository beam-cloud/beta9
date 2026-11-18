package vm

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

const (
	testCompactionCommit   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testCompactionBaseTree = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testCompactionTree     = "cccccccccccccccccccccccccccccccccccccccc"
	testCompactionPRCommit = "dddddddddddddddddddddddddddddddddddddddd"
	testCompactionPath     = "pkg/orders/orders_test.go"
)

func TestCompactionRunToken(t *testing.T) {
	now := time.Unix(1800000000, 0)
	claims := compactionClaims{Workspace: "workspace", Run: "run", Expires: now.Add(30 * time.Minute).Unix()}
	secret := "shared-secret"
	token := claims.sign(secret)
	decoded, ok := verifyCompactionToken(token, secret, now)
	require.True(t, ok)
	require.Equal(t, claims, decoded)

	payload, signature, _ := strings.Cut(token, ".")
	for _, test := range []struct {
		name, token, secret string
		now                 time.Time
	}{
		{"expired", token, secret, time.Unix(claims.Expires, 0)},
		{"too far in the future", token, secret, now.Add(-compactionLifetime)},
		{"tampered payload", "A" + payload + "." + signature, secret, now},
		{"tampered signature", payload + "." + signature + "00", secret, now},
		{"different secret", token, "other-secret", now},
		{"empty secret", claims.sign(""), "", now},
		{"missing run", (compactionClaims{Workspace: claims.Workspace, Expires: claims.Expires}).sign(secret), secret, now},
		{"missing workspace", (compactionClaims{Run: claims.Run, Expires: claims.Expires}).sign(secret), secret, now},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, ok := verifyCompactionToken(test.token, test.secret, test.now)
			require.False(t, ok)
		})
	}
}

func TestCompactionPathAllowlist(t *testing.T) {
	for _, test := range []struct {
		name     string
		workflow bool
		want     bool
	}{
		{testCompactionPath, false, true},
		{"orders_test.go", false, true},
		{"pkg/orders/orders.go", false, false},
		{"pkg/orders/testdata/fixture_test.go", false, false},
		{"pkg/../orders_test.go", false, false},
		{"/pkg/orders/orders_test.go", false, false},
		{"pkg//orders_test.go", false, false},
		{"pkg\\orders_test.go", false, false},
		{"pkg/.git/config_test.go", false, false},
		{"go.mod", false, false},
		{".github/workflows/ci.yml", false, false},
		{".github/workflows/ci.yml", true, true},
		{".github/workflows/ci.yaml", true, true},
		{".github/workflows/local/ci.yml", true, false},
		{"ci.yml", true, false},
		{".github/workflows/ci.yml/../other.yml", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, validCompactionPath(test.name, test.workflow))
		})
	}
}

func compactionTestResult() compactionResult {
	return compactionResult{
		Commit: testCompactionCommit,
		Status: "complete",
		Files:  map[string]string{testCompactionPath: "package orders\n"},
		Report: compactionReport{
			BeforeMS: 200000, AfterMS: 190000, SpreadMS: 1000,
			LinesBefore: 310, LinesAfter: 110, TestsBefore: 2, TestsAfter: 1,
			CoveredBefore: 75, CoveredAfter: 75, Coverable: 100, Caught: 2, Preserved: 2,
			Groups: []compactionGroup{{
				File: testCompactionPath, NewTest: "TestOrder", Status: "merged",
				Cases: []compactionCase{
					{OriginalTest: "TestSmallOrder", Name: "small", Checks: []string{"keeps the minimum"}},
					{OriginalTest: "TestLargeOrder", Name: "large", Checks: []string{"applies the discount"}},
				},
			}},
		},
	}
}

func TestCompactionResultVerification(t *testing.T) {
	require.True(t, validCompactionResult(compactionTestResult()))
	for name, change := range map[string]func(*compactionResult){
		"product code":        func(r *compactionResult) { r.Files["pkg/orders/orders.go"] = "package orders\n" },
		"workflow overwrite":  func(r *compactionResult) { r.Files[".github/workflows/ci.yml"] = "jobs: {}\n" },
		"coverage loss":       func(r *compactionResult) { r.Report.CoveredAfter-- },
		"assertion loss":      func(r *compactionResult) { r.Report.Preserved-- },
		"missing case":        func(r *compactionResult) { r.Report.Groups[0].Cases = r.Report.Groups[0].Cases[:1] },
		"unchanged with edit": func(r *compactionResult) { r.Status = "unchanged" },
		"failed with edit":    func(r *compactionResult) { r.Status = "failed" },
	} {
		t.Run(name, func(t *testing.T) {
			result := compactionTestResult()
			change(&result)
			require.False(t, validCompactionResult(result))
		})
	}
	require.True(t, validCompactionResult(compactionResult{Commit: testCompactionCommit, Status: "unchanged"}))
}

func TestCompactionSavingsThresholds(t *testing.T) {
	for _, test := range []struct {
		name                  string
		before, after, spread int64
		linesRemoved          int
		want                  bool
	}{
		{"time at threshold", 200000, 190000, 1000, 0, true},
		{"time below absolute threshold", 20000, 11000, 10, 0, false},
		{"time below relative threshold", 400000, 389000, 1000, 0, false},
		{"within baseline spread", 200000, 189000, 11000, 0, false},
		{"size at threshold", 1000, 1000, 0, 200, true},
		{"size below threshold", 1000, 1000, 0, 199, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			report := compactionReport{
				BeforeMS: test.before, AfterMS: test.after, SpreadMS: test.spread,
				LinesBefore: 500, LinesAfter: 500 - test.linesRemoved,
			}
			require.Equal(t, test.want, report.worthwhile())
		})
	}
}

func compactionTestRepo() types.GitHubCompaction {
	return types.GitHubCompaction{
		InstallationID: 42, WorkspaceID: 7, Repository: "acme/repo",
		Branch: "main", RunCommit: testCompactionCommit,
	}
}

func compactionTestRoutes(tree string) map[string]string {
	return map[string]string{
		"GET /repos/acme/repo/pulls":                               "200 []",
		"GET /repos/acme/repo/git/ref/heads/main":                  "200 {\"object\":{\"sha\":" + quoteCompactionTest(testCompactionCommit) + "}}",
		"GET /repos/acme/repo/git/commits/" + testCompactionCommit: "200 {\"tree\":{\"sha\":" + quoteCompactionTest(testCompactionBaseTree) + "}}",
		"GET /repos/acme/repo/git/trees/" + testCompactionBaseTree: "200 {\"truncated\":false,\"tree\":" + tree + "}",
		"GET /repos/acme/repo/git/ref/heads/" + compactionBranch:   "404 {}",
	}
}

func quoteCompactionTest(value string) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func TestCompactionPublishesVerifiedPullRequest(t *testing.T) {
	r, _, _, _ := runnerFixture(t)
	routes := compactionTestRoutes(
		"[{\"path\":\"pkg\",\"mode\":\"040000\",\"type\":\"tree\"}," +
			"{\"path\":\"pkg/orders\",\"mode\":\"040000\",\"type\":\"tree\"}," +
			"{\"path\":" + quoteCompactionTest(testCompactionPath) + ",\"mode\":\"100644\",\"type\":\"blob\"}]",
	)
	routes["POST /repos/acme/repo/git/trees"] = "201 {\"sha\":" + quoteCompactionTest(testCompactionTree) + "}"
	routes["POST /repos/acme/repo/git/commits"] = "201 {\"sha\":" + quoteCompactionTest(testCompactionPRCommit) + "}"
	routes["POST /repos/acme/repo/git/refs"] = "201 {}"
	routes["POST /repos/acme/repo/pulls"] = "201 {\"number\":9,\"html_url\":\"https://github.com/acme/repo/pull/9\"}"
	calls := fakeGitHub(t, r, routes)
	ctx := context.Background()
	row := compactionTestRepo()
	token, err := r.github.installationToken(ctx, row.InstallationID)
	require.NoError(t, err)
	result := compactionTestResult()
	title, body, _ := result.Report.message("beam-4vcpu-ubuntu-2404", 360)
	require.Contains(t, title, "2 tests")
	for _, text := range []string{"3m20s", "3m10s", "5.0%", "(-200)", "75.0%", "2 of 2", "1.00 runner-hours",
		"TestSmallOrder", "TestOrder/small", "keeps the minimum", "CI Compaction"} {
		require.Contains(t, body, text)
	}
	pr, err := r.github.publishCompaction(ctx, row, token, result.Files, title, body)
	require.NoError(t, err)
	require.Equal(t, 9, pr.Number)
	require.Equal(t, "https://github.com/acme/repo/pull/9", pr.URL)
	require.Equal(t, testCompactionPRCommit, pr.SHA)

	encodedCall := func(methodPath string, payload any) string {
		data, err := json.Marshal(payload)
		require.NoError(t, err)
		return methodPath + " " + string(data)
	}
	got := calls()
	require.Contains(t, got, encodedCall("POST /repos/acme/repo/git/trees", map[string]any{
		"base_tree": testCompactionBaseTree,
		"tree":      []compactionTreeEntry{{Path: testCompactionPath, Mode: "100644", Type: "blob", Content: result.Files[testCompactionPath]}},
	}))
	require.Contains(t, got, encodedCall("POST /repos/acme/repo/git/commits", map[string]any{
		"message": title, "parents": []string{testCompactionCommit}, "tree": testCompactionTree,
	}))
	require.Contains(t, got, encodedCall("POST /repos/acme/repo/git/refs", map[string]string{
		"ref": "refs/heads/" + compactionBranch, "sha": testCompactionPRCommit,
	}))
	require.Contains(t, got, encodedCall("POST /repos/acme/repo/pulls", map[string]string{
		"title": title, "body": body, "head": compactionBranch, "base": "main",
	}))
}

func TestCompactionRejectsStaleCommitBeforeWriting(t *testing.T) {
	r, _, _, _ := runnerFixture(t)
	calls := fakeGitHub(t, r, map[string]string{
		"GET /repos/acme/repo/pulls":              "200 []",
		"GET /repos/acme/repo/git/ref/heads/main": "200 {\"object\":{\"sha\":" + quoteCompactionTest(testCompactionPRCommit) + "}}",
	})
	_, err := r.github.publishCompaction(context.Background(), compactionTestRepo(), "installation-token",
		compactionTestResult().Files, "CI Compaction", "body")
	require.ErrorIs(t, err, errCompactionStale)
	require.Equal(t, []string{"GET /repos/acme/repo/pulls", "GET /repos/acme/repo/git/ref/heads/main"}, calls())
}

func TestCompactionRejectsNonRegularPathsBeforeWriting(t *testing.T) {
	for _, test := range []struct {
		name, mode, treeType string
	}{
		{"symlink", "120000", "blob"},
		{"directory shadowing path", "040000", "tree"},
		{"missing path", "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, _, _, _ := runnerFixture(t)
			tree := "[]"
			if test.mode != "" {
				data, err := json.Marshal([]map[string]string{{
					"path": testCompactionPath, "mode": test.mode, "type": test.treeType,
				}})
				require.NoError(t, err)
				tree = string(data)
			}
			calls := fakeGitHub(t, r, compactionTestRoutes(tree))
			_, err := r.github.publishCompaction(context.Background(), compactionTestRepo(), "installation-token",
				compactionTestResult().Files, "CI Compaction", "body")
			require.Error(t, err)
			require.Contains(t, calls(), "GET /repos/acme/repo/git/trees/"+testCompactionBaseTree)
			for _, call := range calls() {
				require.True(t, strings.HasPrefix(call, "GET "), call)
			}
		})
	}
}
