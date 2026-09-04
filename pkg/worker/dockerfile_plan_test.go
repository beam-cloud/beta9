package worker

import (
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	ggcrtypes "github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/stretchr/testify/require"
)

func TestParseDockerfilePlanRenderedBuild(t *testing.T) {
	// What the gateway renders for a python image with env vars and a secret.
	text := `FROM public.ecr.aws/n4e0e1y0/beta9-runner@sha256:c4fa685c265ac13340a49ecd98f6e9ebb700e24961ee5b54a4e7983ee6b0d596
ENV FOO=bar PATH_EXTRA="/opt/bin:$PATH" QUOTED="a \"b\" c" DOLLAR=\$HOME
ARG MY_SECRET
# a comment
RUN uv-b9 pip install --system --python python3.12 --link-mode copy "torch" \
    "numpy"

RUN echo $MY_SECRET > /s
WORKDIR /app
WORKDIR sub
USER 1000
LABEL org.example=yes "with space"="v v"
EXPOSE 8080 9090/udp
SHELL ["/bin/bash", "-c"]
ENTRYPOINT ["python", "-m", "app"]
CMD serve --port 8080
`
	plan, err := parseDockerfilePlan(text, map[string]string{"MY_SECRET": "s3cret"})
	require.NoError(t, err)
	require.Equal(t, "public.ecr.aws/n4e0e1y0/beta9-runner@sha256:c4fa685c265ac13340a49ecd98f6e9ebb700e24961ee5b54a4e7983ee6b0d596", plan.from)
	require.Len(t, plan.steps, 12)

	env := plan.steps[0]
	require.Equal(t, stepEnv, env.kind)
	require.Equal(t, []envPair{
		{key: "FOO", value: "bar", set: true},
		{key: "PATH_EXTRA", value: "/opt/bin:", set: true}, // $PATH is not an ENV of the Dockerfile
		{key: "QUOTED", value: `a "b" c`, set: true},
		{key: "DOLLAR", value: "$HOME", set: true},
	}, env.pairs)

	arg := plan.steps[1]
	require.Equal(t, stepArg, arg.kind)
	require.Equal(t, []envPair{{key: "MY_SECRET", value: "s3cret", set: true}}, arg.pairs)

	run := plan.steps[2]
	require.Equal(t, stepRun, run.kind)
	require.Nil(t, run.exec)
	require.Equal(t, `uv-b9 pip install --system --python python3.12 --link-mode copy "torch"     "numpy"`, run.shell)

	require.Equal(t, "echo $MY_SECRET > /s", plan.steps[3].shell) // shell form is left to the shell
	require.Equal(t, "/app", plan.steps[4].value)
	require.Equal(t, "/app/sub", plan.steps[5].value)
	require.Equal(t, "1000", plan.steps[6].value)
	require.Equal(t, []envPair{{key: "org.example", value: "yes", set: true}, {key: "with space", value: "v v", set: true}}, plan.steps[7].pairs)
	require.Equal(t, []string{"8080", "9090/udp"}, plan.steps[8].ports)
	require.Equal(t, []string{"/bin/bash", "-c"}, plan.steps[9].exec)
	require.Equal(t, []string{"python", "-m", "app"}, plan.steps[10].exec)
	require.Equal(t, "serve --port 8080", plan.steps[11].shell)
}

func TestParseDockerfilePlanExpandsVariables(t *testing.T) {
	// As in Docker, pairs on one ENV line see the environment from before
	// that line: SAME is empty here, and only the next line sees A.
	text := "FROM alpine\nENV A=1 SAME=${A}x\nENV B=${A}x C=${MISSING:-def} D=${A:+alt} E=$A$B\nARG V=2\nENV F=$V\nWORKDIR /w/$A\nCOPY --chown=1:1 src/$A dst\n"
	plan, err := parseDockerfilePlan(text, nil)
	require.NoError(t, err)
	require.Equal(t, []envPair{{key: "A", value: "1", set: true}, {key: "SAME", value: "x", set: true}}, plan.steps[0].pairs)
	require.Equal(t, []envPair{
		{key: "B", value: "1x", set: true},
		{key: "C", value: "def", set: true},
		{key: "D", value: "alt", set: true},
		{key: "E", value: "1", set: true},
	}, plan.steps[1].pairs)
	require.Equal(t, "2", plan.steps[3].pairs[0].value)
	require.Equal(t, "/w/1", plan.steps[4].value)
	copyStep := plan.steps[5]
	require.Equal(t, stepCopy, copyStep.kind)
	require.Equal(t, "1:1", copyStep.chown)
	require.Equal(t, []string{"src/1"}, copyStep.sources)
	require.Equal(t, "dst", copyStep.dest)
}

func TestParseDockerfilePlanRejectsWhatItCannotRun(t *testing.T) {
	for name, text := range map[string]string{
		"multi-stage":  "FROM a AS b\nFROM c\nCOPY --from=b /x /y\n",
		"copy-from":    "FROM a\nCOPY --from=other /x /y\n",
		"run-mount":    "FROM a\nRUN --mount=type=cache,target=/root/.cache pip install x\n",
		"healthcheck":  "FROM a\nHEALTHCHECK CMD true\n",
		"volume":       "FROM a\nVOLUME /data\n",
		"onbuild":      "FROM a\nONBUILD RUN true\n",
		"stopsignal":   "FROM a\nSTOPSIGNAL SIGTERM\n",
		"syntax":       "# syntax=docker/dockerfile:1\nFROM a\n",
		"platform":     "FROM --platform=linux/amd64 a\n",
		"no-from":      "RUN true\n",
		"shell-string": "FROM a\nSHELL /bin/bash -c\n",
		"unknown":      "FROM a\nFROB x\n",
		"unterminated": "FROM a\nENV A=\"b\n",
		"copy-json":    "FROM a\nCOPY [\"x\", \"y\"]\n",
	} {
		_, err := parseDockerfilePlan(text, nil)
		require.ErrorIs(t, err, errDockerfileUnsupported, name)
	}
}

func TestDockerfileLogicalLines(t *testing.T) {
	lines, err := dockerfileLogicalLines("FROM a\r\n\n  # comment\nRUN a \\\n  # inside\n    b\\\nc\nENV x=1")
	require.NoError(t, err)
	require.Equal(t, []string{"FROM a", "RUN a     bc", "ENV x=1"}, lines)
}

func TestLayeredBuildConfigFileAppliesInstructions(t *testing.T) {
	base, err := random.Image(256, 1)
	require.NoError(t, err)
	b := &layeredBuild{
		env:        map[string]string{"PATH": "/x", "NEW": "v"},
		envOrder:   []string{"PATH", "NEW"},
		workdir:    "/app",
		user:       "1000",
		entrypoint: []string{"python"},
		cmd:        []string{"-m", "app"},
		labels:     map[string]string{"k": "v"},
		exposed:    map[string]struct{}{"8080/tcp": {}},
		shell:      []string{"/bin/bash", "-c"},
	}
	baseCfg, err := base.ConfigFile()
	require.NoError(t, err)
	baseCfg.Config.Env = []string{"PATH=/usr/bin", "KEEP=1"}
	extra, err := random.Layer(64, ggcrtypes.DockerLayer)
	require.NoError(t, err)
	img, err := mutate.AppendLayers(base, extra)
	require.NoError(t, err)
	cfg, err := b.configFile(img)
	require.NoError(t, err)
	// The appended layer's diff id survives; PATH is replaced in place.
	require.Len(t, cfg.RootFS.DiffIDs, 2)
	require.Len(t, cfg.History, 2)
	require.Equal(t, "/app", cfg.Config.WorkingDir)
	require.Equal(t, "1000", cfg.Config.User)
	require.Equal(t, []string{"python"}, cfg.Config.Entrypoint)
	require.Equal(t, []string{"-m", "app"}, cfg.Config.Cmd)
	require.Equal(t, map[string]string{"k": "v"}, cfg.Config.Labels)
	require.Contains(t, cfg.Config.ExposedPorts, "8080/tcp")
	require.Equal(t, []string{"/bin/bash", "-c"}, cfg.Config.Shell)
	require.Contains(t, cfg.Config.Env, "NEW=v")
	require.Contains(t, cfg.Config.Env, "PATH=/x")
	require.NotContains(t, cfg.Config.Env, "PATH=/usr/bin")
}

func TestUnescapeMountField(t *testing.T) {
	require.Equal(t, "/a b/c", unescapeMountField(`/a\040b/c`))
	require.Equal(t, "/plain", unescapeMountField("/plain"))
}
