package worker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// renderDockerfile writes a Dockerfile for a repository that has none, from
// what its files say about the stack: Python, Node, Go or a static site, on
// the official image for the version the repository declares. startCommand
// and buildCommand (from the request, railway config or Procfile) win over
// what is detected.
func renderDockerfile(dir, startCommand, buildCommand string) (string, error) {
	switch {
	case fileExists(filepath.Join(dir, "requirements.txt")) || fileExists(filepath.Join(dir, "pyproject.toml")):
		return pythonDockerfile(dir, startCommand, buildCommand), nil
	case fileExists(filepath.Join(dir, "package.json")):
		return nodeDockerfile(dir, startCommand, buildCommand)
	case fileExists(filepath.Join(dir, "go.mod")):
		return goDockerfile(dir, startCommand, buildCommand), nil
	case fileExists(filepath.Join(dir, "index.html")):
		return staticDockerfile(), nil
	}
	return "", fmt.Errorf("repository has no Dockerfile and no recognised stack (Python, Node, Go or static); add a Dockerfile")
}

var (
	versionNumber = regexp.MustCompile(`\d+(\.\d+)?`)
	goDirective   = regexp.MustCompile(`(?m)^go (\d+\.\d+)`)
)

// declaredVersion is the first N or N.M in the first of files that exists
// (.python-version, runtime.txt, .nvmrc), or fallback.
func declaredVersion(dir, fallback string, files ...string) string {
	for _, name := range files {
		if text, err := readRepoFile(dir, name); err == nil {
			if v := versionNumber.FindString(text); v != "" {
				return v
			}
		}
	}
	return fallback
}

func procfileCommand(dir string) string {
	text, _ := readRepoFile(dir, "Procfile")
	for _, line := range strings.Split(text, "\n") {
		if cmd, ok := strings.CutPrefix(strings.TrimSpace(line), "web:"); ok {
			return strings.TrimSpace(cmd)
		}
	}
	return ""
}

func runLine(command string) string {
	if command == "" {
		return ""
	}
	return "RUN " + command + "\n"
}

func shellCmd(command string) string {
	quoted, _ := json.Marshal(command)
	return fmt.Sprintf("CMD [\"sh\", \"-c\", %s]\n", quoted)
}

// pythonDockerfile defaults to 3.11 when the repository declares no version:
// the last image that ships setuptools, which pinned releases of common
// packages (gunicorn < 21) still import as pkg_resources.
func pythonDockerfile(dir, start, build string) string {
	install := "pip install --no-cache-dir ."
	if fileExists(filepath.Join(dir, "requirements.txt")) {
		install = "pip install --no-cache-dir -r requirements.txt"
	}
	if start == "" {
		start = procfileCommand(dir)
	}
	if start == "" {
		start = pythonStartCommand(dir)
	}
	// The server the start command names has to be installed, whether or
	// not the repository lists it.
	for _, server := range []string{"gunicorn", "uvicorn", "hypercorn"} {
		if strings.HasPrefix(start, server) {
			install += " " + server
		}
	}
	return fmt.Sprintf(`FROM python:%s-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PORT=8000
COPY . .
RUN %s
%s%s`, declaredVersion(dir, "3.11", ".python-version", "runtime.txt"), install, runLine(build), shellCmd(gunicornBound(start)))
}

// pythonStartCommand picks the server from the entry module: Django by
// manage.py, FastAPI and Flask by the app they construct, else the script.
func pythonStartCommand(dir string) string {
	if _, err := os.Stat(filepath.Join(dir, "manage.py")); err == nil {
		if wsgi, _ := filepath.Glob(filepath.Join(dir, "*", "wsgi.py")); len(wsgi) > 0 {
			return fmt.Sprintf("python manage.py migrate && gunicorn %s.wsgi --bind 0.0.0.0:$PORT", filepath.Base(filepath.Dir(wsgi[0])))
		}
	}
	for _, module := range []string{"main", "app", "server"} {
		text, err := readRepoFile(dir, module+".py")
		if err != nil {
			continue
		}
		switch {
		case strings.Contains(text, "FastAPI("):
			return fmt.Sprintf("uvicorn %s:app --host 0.0.0.0 --port $PORT", module)
		case strings.Contains(text, "Flask("):
			return fmt.Sprintf("gunicorn %s:app --bind 0.0.0.0:$PORT", module)
		}
		return "python " + module + ".py"
	}
	return "python main.py"
}

func nodeDockerfile(dir, start, build string) (string, error) {
	var pkg struct {
		Main    string            `json:"main"`
		Scripts map[string]string `json:"scripts"`
		Engines map[string]string `json:"engines"`
	}
	text, _ := readRepoFile(dir, "package.json")
	if err := json.Unmarshal([]byte(text), &pkg); err != nil {
		return "", fmt.Errorf("package.json: %w", err)
	}
	pm, install := "npm", "npm install"
	switch {
	case fileExists(filepath.Join(dir, "pnpm-lock.yaml")):
		pm, install = "pnpm", "corepack enable && pnpm install --frozen-lockfile"
	case fileExists(filepath.Join(dir, "yarn.lock")):
		pm, install = "yarn", "corepack enable && yarn install --frozen-lockfile"
	case fileExists(filepath.Join(dir, "package-lock.json")):
		install = "npm ci"
	}
	if build == "" && pkg.Scripts["build"] != "" {
		build = pm + " run build"
	}
	if start == "" {
		start = procfileCommand(dir)
	}
	if start == "" {
		switch {
		case pkg.Scripts["start"] != "":
			start = pm + " run start"
		case pkg.Main != "":
			start = "node " + pkg.Main
		default:
			return "", fmt.Errorf("package.json has no start script; set a start command")
		}
	}
	version := declaredVersion(dir, "20", ".nvmrc", ".node-version")
	if v := versionNumber.FindString(pkg.Engines["node"]); v != "" {
		version = v
	}
	// Dev dependencies stay installed through the build step (typescript,
	// bundlers); production mode applies from start.
	return fmt.Sprintf(`FROM node:%s-slim
WORKDIR /app
COPY . .
RUN %s
%sENV NODE_ENV=production PORT=8000
%s`, version, install, runLine(build), shellCmd(start)), nil
}

func goDockerfile(dir, start, build string) string {
	if build == "" {
		build = "CGO_ENABLED=0 go build -o /out/app ."
	}
	if start == "" {
		start = "/out/app"
	}
	return fmt.Sprintf(`FROM golang:%s AS build
WORKDIR /src
COPY . .
RUN %s

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out /out
ENV PORT=8000
%s`, goVersion(dir), build, shellCmd(start))
}

func goVersion(dir string) string {
	text, _ := readRepoFile(dir, "go.mod")
	if m := goDirective.FindStringSubmatch(text); m != nil {
		return m[1]
	}
	return "1.23"
}

// staticDockerfile serves the checkout with nginx on $PORT; the image's
// envsubst templating fills the port in at start.
func staticDockerfile() string {
	return `FROM nginx:alpine
COPY . /usr/share/nginx/html
RUN printf 'server { listen ${PORT}; root /usr/share/nginx/html; location / { try_files $uri $uri/ /index.html; } }\n' > /etc/nginx/templates/default.conf.template
ENV PORT=8000
`
}
