package vm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
		name, framework string
		want            bool
	}{
		{testCompactionPath, "go", true},
		{"orders_test.go", "go", true},
		{"pkg/orders/orders.go", "go", false},
		{"pkg/orders/testdata/fixture_test.go", "go", false},
		{"pkg/../orders_test.go", "go", false},
		{"/pkg/orders/orders_test.go", "go", false},
		{"pkg//orders_test.go", "go", false},
		{"pkg\\orders_test.go", "go", false},
		{"pkg/.git/config_test.go", "go", false},
		{"go.mod", "go", false},
		{".github/workflows/ci.yml", "go", false},
		{"tests/test_orders.py", "pytest", true},
		{"packages/orders/test/orders_test.py", "pytest", true},
		{"test_orders.py", "pytest", false},
		{"src/orders/test_orders.py", "pytest", false},
		{"tests/orders.py", "pytest", false},
		{"tests/conftest.py", "pytest", false},
		{"tests/test_helper_test.py", "pytest", false},
		{"tests/Fixtures/test_orders.py", "pytest", false},
		{"tests/pyproject.toml", "pytest", false},
		{"tests/test_orders.py", "jest", false},
		{"src/orders.test.ts", "jest", true},
		{"src/components/order.spec.tsx", "vitest", true},
		{"orders.test.mjs", "jest", true},
		{"__tests__/orders.js", "jest", false},
		{"src/orders.ts", "vitest", false},
		{"src/orders.test.d.ts", "vitest", false},
		{"tests/vitest.config.test.ts", "vitest", false},
		{"tests/setupTests.test.ts", "jest", false},
		{"tests/package-lock.json", "jest", false},
		{"tests/__snapshots__/orders.test.ts", "jest", false},
		{"tests/__fixtures__/orders.test.ts", "vitest", false},
		{"node_modules/orders/orders.test.js", "jest", false},
		{"tests/.cache/orders.spec.js", "vitest", false},
		{"tests/\torders.test.ts", "jest", false},
		{".GitHub/orders.test.js", "vitest", false},
		{"tests/orders_test.rs", "cargo", false},
		{".github/workflows/ci.yml", "workflow", false},
	} {
		t.Run(test.framework+"/"+test.name, func(t *testing.T) {
			require.Equal(t, test.want, validCompactionTestPath(test.name, test.framework))
		})
	}
	for _, file := range []string{".github/workflows/ci.yml", ".github/workflows/ci.yaml"} {
		require.Equal(t, file, pipelineWorkflowPath(file, "main"))
	}
	for _, file := range []string{".github/workflows/local/ci.yml", "ci.yml", ".github/workflows/ci.yml/../other.yml"} {
		require.Empty(t, pipelineWorkflowPath(file, "main"))
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

func TestCompactionResultVerifiesEachFramework(t *testing.T) {
	result := compactionTestResult() // Old Go groups omit the framework.
	for _, change := range []struct{ file, framework, source string }{
		{"tests/test_orders.py", "pytest", "def test_orders(): pass\n"},
		{"src/orders.test.ts", "jest", "test('orders', () => {});\n"},
	} {
		result.Files[change.file] = change.source
		group := result.Report.Groups[0]
		group.File, group.Framework = change.file, change.framework
		result.Report.Groups = append(result.Report.Groups, group)
	}
	result.Report.TestsBefore, result.Report.TestsAfter = 6, 3
	require.True(t, validCompactionResult(result))

	group := &result.Report.Groups[1]
	group.Framework = ""
	require.False(t, validCompactionResult(result), "only Go paths work without a framework")
	group.Framework = "go"
	require.False(t, validCompactionResult(result), "changing the label does not grant access")
	group.Framework = "pytest"
	group.File = "src/orders.py"
	result.Files[group.File] = result.Files["tests/test_orders.py"]
	delete(result.Files, "tests/test_orders.py")
	require.False(t, validCompactionResult(result), "reject the entire result when one group changes product code")

	group.File, group.Framework = ".github/workflows/ci.yml", "workflow"
	result.Files[group.File] = "jobs: {}\n"
	delete(result.Files, "src/orders.py")
	require.False(t, validCompactionResult(result), "only the gateway pipeline can change workflows")
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

const pipelineTestRepository = "beam-cloud/proof"
const pipelineTestWorkflow = "name: CI\non: [push]\njobs:\n  check:\n    runs-on: ubuntu-latest\n    steps:\n      - run: go test ./...\n"

func testPipelineRun(id int64, branch, sha, file string, start, end time.Time) pipelineRun {
	run := pipelineRun{
		ID: id, WorkflowID: 1, Number: int(id), Attempt: 1,
		Path: file, SHA: sha, Status: "completed", Started: start, Updated: end,
	}
	run.Event = "push"
	run.HeadBranch = branch
	run.HeadRepository.FullName = pipelineTestRepository
	return run
}

func TestPipelineOverlap(t *testing.T) {
	now := time.Now()
	file := ".github/workflows/ci.yml"
	older := testPipelineRun(4, "feature", "older", file, now, now.Add(time.Minute))
	sameSHA := testPipelineRun(5, "feature", "older", file, now.Add(time.Second), now.Add(5*time.Second))
	outOfOrder := testPipelineRun(3, "feature", "out-of-order", file, now.Add(3*time.Second), now.Add(5*time.Second))
	newer := testPipelineRun(10, "feature", "newer", file+"@refs/heads/feature", now.Add(20*time.Second), now.Add(30*time.Second))
	otherWorkflow := testPipelineRun(8, "feature", "other", file, now.Add(4*time.Second), now.Add(5*time.Second))
	otherWorkflow.WorkflowID = 2
	otherFile := testPipelineRun(6, "feature", "other", ".github/workflows/other.yml", now.Add(4*time.Second), now.Add(5*time.Second))
	otherBranch := testPipelineRun(7, "other", "other", file, now.Add(4*time.Second), now.Add(5*time.Second))
	rerun := testPipelineRun(9, "feature", "other", file, now.Add(4*time.Second), now.Add(5*time.Second))
	rerun.Attempt = 2
	queued := testPipelineRun(9, "feature", "other", file, now.Add(4*time.Second), now.Add(5*time.Second))
	queued.Status = "queued"
	manual := queued
	manual.Event, manual.Status = "workflow_dispatch", "completed"
	mixture := []pipelineRun{newer, otherWorkflow, otherFile, otherBranch, rerun, queued, manual, older, older, sameSHA, outOfOrder}
	overlaps := pipelineOverlaps(mixture, pipelineTestRepository, "main")
	got := overlaps[pipelineRunGroup{workflow: 1, branch: "feature", file: file}]
	// The newer push is the first distinct, newer run for each of the
	// overlapping candidates. A duplicate page and other refs don't count.
	if got.superseded != 1 || got.runs != 4 || got.wasted != 40*time.Second || got.runtime != 76*time.Second {
		t.Fatalf("wrong overlap: %+v", got)
	}
	if len(pipelineOverlaps(mixture, pipelineTestRepository, "feature")) != 1 {
		t.Fatal("default branch runs must be left out")
	}
}

func TestPipelineConcurrencyAppend(t *testing.T) {
	for _, original := range []string{pipelineTestWorkflow + "# trailing comment\n", strings.TrimSpace(pipelineTestWorkflow), strings.ReplaceAll(pipelineTestWorkflow, "\n", "\r\n")} {
		updated, ok := addPipelineConcurrency(original)
		if !ok || !strings.HasPrefix(updated, original) || !strings.Contains(updated, "github.workflow_ref") || !strings.Contains(updated, "github.run_id") {
			t.Fatalf("workflow did not keep its original bytes and add concurrency:\n%s", updated)
		}
		originalRoot := pipelineMapping(original)
		updatedRoot := pipelineMapping(updated)
		if len(updatedRoot.Content) != len(originalRoot.Content)+2 || updatedRoot.Content[len(originalRoot.Content)].Value != "concurrency" {
			t.Fatal("concurrency did not append at the top level")
		}
		if _, ok := addPipelineConcurrency(updated); ok {
			t.Fatal("workflow with top-level concurrency must be left alone")
		}
	}
	for _, original := range []string{
		pipelineTestWorkflow + "...\n",
		pipelineTestWorkflow + "---\nextra: document\n",
		strings.Replace(pipelineTestWorkflow, "    runs-on:", "    concurrency: existing\n    runs-on:", 1),
		"jobs:\n  check:\n    <<: {concurrency: from-merge}\n",
		"{jobs: {check: {runs-on: ubuntu-latest}}}\n",
	} {
		if _, ok := addPipelineConcurrency(original); ok {
			t.Errorf("unsafe workflow shape accepted:\n%s", original)
		}
	}
}

func TestPipelineProposal(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	file := ".github/workflows/ci.yml"
	quietFile := ".github/workflows/quiet.yml"
	first := testPipelineRun(1, "probe/pipeline", "old", file, now.Add(-90*time.Second), now)
	second := testPipelineRun(2, "probe/pipeline", "new", file, now.Add(-30*time.Second), now)
	quiet := testPipelineRun(3, "probe/pipeline", "old", quietFile, now.Add(-20*time.Second), now)
	quiet.WorkflowID = 2
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.Header.Get("Authorization") != "Bearer staged-token" {
			t.Errorf("unexpected request %s %s", req.Method, req.URL)
		}
		const prefix = "/repos/" + pipelineTestRepository
		switch req.URL.Path {
		case prefix + "/actions/runs":
			var runs []pipelineRun
			if req.URL.Query().Get("page") == "2" {
				runs = []pipelineRun{second}
			} else {
				runs = []pipelineRun{first, quiet}
				if req.URL.Query().Get("event") != "push" || req.URL.Query().Get("created") == "" {
					t.Errorf("expected a push date range in query %q", req.URL.RawQuery)
				}
				w.Header().Set("Link", fmt.Sprintf("<http://%s%s/actions/runs?page=2>; rel=\"next\"", req.Host, prefix))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 3, "workflow_runs": runs})
		case prefix + "/git/ref/heads/probe/pipeline":
			_, _ = w.Write([]byte(`{"ref":"refs/heads/probe/pipeline"}`))
		case prefix + "/git/ref/tags/probe/pipeline":
			http.NotFound(w, req)
		case prefix + "/contents/" + file:
			if req.URL.Query().Get("ref") != "pinned-commit" {
				t.Errorf("workflow contents were not pinned: %s", req.URL)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"type": "file", "encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(pipelineTestWorkflow))})
		default:
			t.Errorf("unexpected call to GitHub: %s", req.URL)
			http.NotFound(w, req)
		}
	}))
	defer fake.Close()
	runner := &githubRunners{github: &githubApp{apiURL: fake.URL, client: fake.Client()}}
	files, title, body, saved, err := runner.compactPipeline(context.Background(), types.GitHubCompaction{Repository: pipelineTestRepository, InstallationID: 11}, compactionHead{Branch: "main", Commit: "pinned-commit"}, "staged-token")
	if err != nil || len(files) != 1 || !strings.HasPrefix(files[file], pipelineTestWorkflow) {
		require.Contains(t, saved, "of superseded workflow time")
		t.Fatalf("unexpected pipeline proposal: %s %q %v", title, files, err)
	}
	if !strings.Contains(body, "1 of 2 | 30s") {
		t.Fatalf("incorrect workflow overlap in report: %s", body)
	}
}

type compactionScheduleStore struct {
	*runnerStore
	marked   types.GitHubCompaction
	finished types.GitHubCompaction
	due      time.Time
}

func (s *compactionScheduleStore) MarkGitHubCompaction(_ context.Context, row types.GitHubCompaction) error {
	s.marked = row
	return nil
}

func (s *compactionScheduleStore) FinishGitHubCompaction(_ context.Context, row types.GitHubCompaction, due time.Time) error {
	s.finished, s.due = row, due
	return nil
}

func TestCompactionScheduleMarksOnlyOptedInDefaultBranchJobs(t *testing.T) {
	r, runners, _, _ := runnerFixture(t)
	store := &compactionScheduleStore{runnerStore: runners}
	r.s.repo = store
	r.config.CompactionURL, r.config.CompactionSecret = "https://compaction.test", "secret"
	event := runnerEvent{Action: types.GitHubJobCompleted}
	event.Repository.FullName, event.Repository.DefaultBranch = "Acme/Example", "main"
	event.Job.HeadBranch, event.Job.HeadSHA, event.Job.Conclusion = "feature", strings.Repeat("a", 40), "success"
	label := "beam-4vcpu-ubuntu-2404"

	require.NoError(t, r.markCompaction(t.Context(), runners.installation, event, label))
	require.Empty(t, store.marked.Head)
	runners.installation.CICompaction = true
	require.NoError(t, r.markCompaction(t.Context(), runners.installation, event, label))
	require.Empty(t, store.marked.Head)

	event.Job.HeadBranch = "main"
	r.config.CompactionRepository = "acme/other"
	require.NoError(t, r.markCompaction(t.Context(), runners.installation, event, label))
	require.Empty(t, store.marked.Head)
	r.config.CompactionRepository = ""
	require.NoError(t, r.markCompaction(t.Context(), runners.installation, event, label))
	require.Equal(t, event.Job.HeadSHA, store.marked.Head)
	require.Equal(t, "acme/example", store.marked.Repository)
	require.Equal(t, label, store.marked.Label)
}

func TestCompactionScheduleBacksOffAndKeepsUnprocessedCommits(t *testing.T) {
	r, runners, _, _ := runnerFixture(t)
	store := &compactionScheduleStore{runnerStore: runners}
	r.s.repo = store
	for _, tc := range []struct {
		name          string
		current, next int
		processed     bool
		pr            *compactionPR
	}{
		{name: "first empty report", current: 1, next: 2, processed: true},
		{name: "repeated empty report", current: 4, next: 8, processed: true},
		{name: "capped", current: 8, next: 8, processed: true},
		{name: "failed", current: 2, next: 1},
		{name: "opened", current: 4, next: 1, processed: true, pr: &compactionPR{Number: 15}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := types.GitHubCompaction{LastCommit: "previous", RunCommit: "current", WaitWeeks: tc.current}
			start := time.Now()
			require.NoError(t, r.finishCompaction(t.Context(), row, tc.name, tc.processed, tc.pr, "", ""))
			require.Equal(t, tc.next, store.finished.WaitWeeks)
			require.WithinDuration(t, start.Add(time.Duration(tc.next)*compactionWeek), store.due, time.Second)
			want := row.LastCommit
			if tc.processed {
				want = row.RunCommit
			}
			require.Equal(t, want, store.finished.LastCommit)
			require.Equal(t, tc.pr != nil, store.finished.PROpen)
		})
	}
}
