package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
)

// errDockerfileUnsupported means a Dockerfile uses something the layered
// builder does not implement, so the build goes through buildah bud instead.
var errDockerfileUnsupported = errors.New("dockerfile not supported by the layered builder")

type dockerfileStepKind int

const (
	stepRun dockerfileStepKind = iota
	stepEnv
	stepArg
	stepWorkdir
	stepUser
	stepCopy
	stepAdd
	stepShell
	stepEntrypoint
	stepCmd
	stepLabel
	stepExpose
)

// dockerfileStep is one instruction with its arguments already expanded
// (Dockerfile variable substitution) where the instruction calls for it.
type dockerfileStep struct {
	kind dockerfileStepKind
	raw  string // the instruction line, for user-facing output

	// RUN, ENTRYPOINT, CMD, SHELL: exec is set for the JSON form, shell for
	// the string form.
	exec  []string
	shell string
	// ENV, LABEL, ARG (single pair; value empty and set=false for a bare ARG).
	pairs []envPair
	// WORKDIR, USER.
	value string
	// COPY, ADD.
	sources []string
	dest    string
	chown   string
	chmod   string
	// EXPOSE.
	ports []string
}

type envPair struct {
	key, value string
	set        bool
}

// dockerfilePlan is a single-stage Dockerfile reduced to the instructions the
// layered builder executes with buildah run/copy/config.
type dockerfilePlan struct {
	from  string
	steps []dockerfileStep
}

// parseDockerfilePlan parses a Dockerfile into a plan, expanding variables as
// Docker would (ENV and ARG values visible to later instructions; RUN in shell
// form is left to the shell). buildArgs supplies ARG values. Anything outside
// the supported subset (multi-stage, RUN/COPY flags such as --mount and
// --from, parser directives, HEALTHCHECK, ONBUILD, VOLUME, STOPSIGNAL, ...)
// returns errDockerfileUnsupported.
func parseDockerfilePlan(text string, buildArgs map[string]string) (*dockerfilePlan, error) {
	lines, err := dockerfileLogicalLines(text)
	if err != nil {
		return nil, err
	}
	plan := &dockerfilePlan{}
	env := map[string]string{}        // ENV, persisted in the image
	args := map[string]string{}       // ARG inside the stage, visible to later instructions only
	globalArgs := map[string]string{} // ARG before FROM: visible to FROM, and to a redeclaring ARG
	lookup := func(name string) (string, bool) {
		if v, ok := env[name]; ok {
			return v, true
		}
		if v, ok := args[name]; ok {
			return v, true
		}
		return "", false
	}
	globalLookup := func(name string) (string, bool) {
		v, ok := globalArgs[name]
		return v, ok
	}
	// Relative WORKDIRs resolve against the previous one; before any, they
	// would resolve against the base image's configured working directory.
	workdir := ""

	for _, line := range lines {
		instr, rest := line, ""
		if i := strings.IndexAny(line, " \t"); i >= 0 {
			instr, rest = line[:i], strings.TrimSpace(line[i+1:])
		}
		instr = strings.ToUpper(instr)
		if plan.from == "" && instr != "FROM" && instr != "ARG" {
			return nil, fmt.Errorf("%w: %s before FROM", errDockerfileUnsupported, instr)
		}
		step := dockerfileStep{raw: line}
		switch instr {
		case "FROM":
			if plan.from != "" {
				return nil, fmt.Errorf("%w: multi-stage build", errDockerfileUnsupported)
			}
			fields := strings.Fields(rest)
			if len(fields) == 0 || strings.HasPrefix(fields[0], "--") {
				return nil, fmt.Errorf("%w: FROM %q", errDockerfileUnsupported, rest)
			}
			if len(fields) == 3 && strings.EqualFold(fields[1], "AS") {
				fields = fields[:1]
			}
			if len(fields) != 1 {
				return nil, fmt.Errorf("%w: FROM %q", errDockerfileUnsupported, rest)
			}
			from, err := expandDockerfileVars(fields[0], globalLookup)
			if err != nil {
				return nil, err
			}
			plan.from = from
			continue
		case "RUN":
			if strings.HasPrefix(rest, "--") {
				return nil, fmt.Errorf("%w: RUN flags", errDockerfileUnsupported)
			}
			step.kind = stepRun
			step.exec, step.shell = parseExecOrShell(rest)
		case "SHELL":
			step.kind = stepShell
			if step.exec, _ = parseExecOrShell(rest); step.exec == nil {
				return nil, fmt.Errorf("%w: SHELL must use the JSON form", errDockerfileUnsupported)
			}
		case "ENTRYPOINT":
			step.kind = stepEntrypoint
			step.exec, step.shell = parseExecOrShell(rest)
		case "CMD":
			step.kind = stepCmd
			step.exec, step.shell = parseExecOrShell(rest)
		case "ENV":
			step.kind = stepEnv
			pairs, err := parseEnvPairs(rest, lookup)
			if err != nil {
				return nil, err
			}
			for _, p := range pairs {
				env[p.key] = p.value
			}
			step.pairs = pairs
		case "LABEL":
			step.kind = stepLabel
			pairs, err := parseEnvPairs(rest, lookup)
			if err != nil {
				return nil, err
			}
			step.pairs = pairs
		case "ARG":
			step.kind = stepArg
			name, def, hasDefault := strings.Cut(rest, "=")
			name = strings.TrimSpace(name)
			if name == "" || strings.ContainsAny(name, " \t") {
				return nil, fmt.Errorf("%w: ARG %q", errDockerfileUnsupported, rest)
			}
			value, set := buildArgs[name]
			if plan.from == "" {
				// Before FROM the ARG is global: it only serves FROM, unless
				// a stage redeclares it, and is no step of the build.
				if !set && hasDefault {
					if value, err = expandDockerfileVars(unquoteDockerfileWord(def), globalLookup); err != nil {
						return nil, err
					}
					set = true
				}
				if set {
					globalArgs[name] = value
				}
				continue
			}
			if !set && hasDefault {
				if value, err = expandDockerfileVars(unquoteDockerfileWord(def), lookup); err != nil {
					return nil, err
				}
				set = true
			}
			if !set {
				// A bare redeclaration inherits the global ARG's value.
				value, set = globalArgs[name]
			}
			if set {
				args[name] = value
			}
			step.pairs = []envPair{{key: name, value: value, set: set}}
		case "WORKDIR":
			step.kind = stepWorkdir
			dir, err := expandDockerfileVars(unquoteDockerfileWord(rest), lookup)
			if err != nil {
				return nil, err
			}
			if !path.IsAbs(dir) {
				if workdir == "" {
					// Relative to the base image's working directory, which
					// is not known here.
					return nil, fmt.Errorf("%w: relative WORKDIR %q before an absolute one", errDockerfileUnsupported, dir)
				}
				dir = path.Join(workdir, dir)
			}
			workdir = path.Clean(dir)
			step.value = workdir
		case "USER":
			step.kind = stepUser
			if step.value, err = expandDockerfileVars(rest, lookup); err != nil {
				return nil, err
			}
		case "COPY", "ADD":
			step.kind = stepCopy
			if instr == "ADD" {
				step.kind = stepAdd
			}
			fields := strings.Fields(rest)
			for len(fields) > 0 && strings.HasPrefix(fields[0], "--") {
				flag, value, _ := strings.Cut(fields[0], "=")
				if value, err = expandDockerfileVars(value, lookup); err != nil {
					return nil, err
				}
				switch flag {
				case "--chown":
					step.chown = value
				case "--chmod":
					step.chmod = value
				default:
					return nil, fmt.Errorf("%w: %s %s", errDockerfileUnsupported, instr, flag)
				}
				fields = fields[1:]
			}
			if len(fields) < 2 {
				return nil, fmt.Errorf("%w: %s needs a source and destination", errDockerfileUnsupported, instr)
			}
			if strings.HasPrefix(strings.TrimSpace(rest), "[") {
				return nil, fmt.Errorf("%w: %s JSON form", errDockerfileUnsupported, instr)
			}
			for i, f := range fields {
				if fields[i], err = expandDockerfileVars(f, lookup); err != nil {
					return nil, err
				}
			}
			step.sources, step.dest = fields[:len(fields)-1], fields[len(fields)-1]
		case "EXPOSE":
			step.kind = stepExpose
			expanded, err := expandDockerfileVars(rest, lookup)
			if err != nil {
				return nil, err
			}
			step.ports = strings.Fields(expanded)
		default:
			return nil, fmt.Errorf("%w: %s", errDockerfileUnsupported, instr)
		}
		plan.steps = append(plan.steps, step)
	}
	if plan.from == "" {
		return nil, fmt.Errorf("%w: no FROM", errDockerfileUnsupported)
	}
	return plan, nil
}

// dockerfileLogicalLines joins continuation lines and drops comments and
// blank lines. Parser directives (# syntax=, # escape=) are not supported.
func dockerfileLogicalLines(text string) ([]string, error) {
	var lines []string
	var current strings.Builder
	continued := false
	for i, raw := range strings.Split(text, "\n") {
		line := strings.TrimRight(raw, " \t\r")
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			if i == 0 || (len(lines) == 0 && !continued) {
				if directive, _, ok := strings.Cut(strings.TrimSpace(trimmed[1:]), "="); ok {
					switch strings.ToLower(strings.TrimSpace(directive)) {
					case "syntax", "escape", "check":
						return nil, fmt.Errorf("%w: parser directive %q", errDockerfileUnsupported, trimmed)
					}
				}
			}
			continue // comment lines are skipped, also inside a continuation
		}
		if !continued {
			if trimmed == "" {
				continue
			}
			line = strings.TrimLeft(line, " \t")
		}
		if strings.HasSuffix(line, "\\") {
			current.WriteString(line[:len(line)-1])
			continued = true
			continue
		}
		current.WriteString(line)
		lines = append(lines, current.String())
		current.Reset()
		continued = false
	}
	if continued {
		lines = append(lines, current.String())
	}
	return lines, nil
}

// parseExecOrShell returns the JSON exec form if rest parses as a JSON array
// of strings, else rest as a shell-form string.
func parseExecOrShell(rest string) ([]string, string) {
	if strings.HasPrefix(rest, "[") {
		var exec []string
		if err := json.Unmarshal([]byte(rest), &exec); err == nil {
			return exec, ""
		}
	}
	return nil, rest
}

// parseEnvPairs parses "K=V K2=V2" (quoted values allowed) or the legacy
// "K V..." form, expanding variables in values.
func parseEnvPairs(rest string, lookup func(string) (string, bool)) ([]envPair, error) {
	words, err := splitDockerfileWords(rest)
	if err != nil {
		return nil, err
	}
	if len(words) == 0 {
		return nil, fmt.Errorf("%w: empty ENV", errDockerfileUnsupported)
	}
	if !strings.Contains(words[0].raw, "=") {
		// Legacy form: the whole remainder after the key is the value.
		key, value, _ := strings.Cut(rest, " ")
		expanded, err := expandDockerfileVars(strings.TrimSpace(value), lookup)
		if err != nil {
			return nil, err
		}
		return []envPair{{key: key, value: expanded, set: true}}, nil
	}
	pairs := make([]envPair, 0, len(words))
	for _, w := range words {
		key, value, ok := strings.Cut(w.raw, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("%w: malformed pair %q", errDockerfileUnsupported, w.raw)
		}
		expanded, err := expandDockerfileVars(unquoteDockerfileWord(value), lookup)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, envPair{key: unquoteDockerfileWord(key), value: expanded, set: true})
	}
	return pairs, nil
}

type dockerfileWord struct{ raw string }

// splitDockerfileWords splits on unquoted whitespace, keeping quotes in place
// so the caller can unquote the value part of K=V.
func splitDockerfileWords(s string) ([]dockerfileWord, error) {
	var words []dockerfileWord
	var cur strings.Builder
	quote := byte(0)
	inWord := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case quote != 0:
			cur.WriteByte(ch)
			if ch == '\\' && quote == '"' && i+1 < len(s) {
				i++
				cur.WriteByte(s[i])
			} else if ch == quote {
				quote = 0
			}
		case ch == '"' || ch == '\'':
			quote = ch
			inWord = true
			cur.WriteByte(ch)
		case ch == '\\' && i+1 < len(s):
			inWord = true
			cur.WriteByte(ch)
			i++
			cur.WriteByte(s[i])
		case ch == ' ' || ch == '\t':
			if inWord {
				words = append(words, dockerfileWord{raw: cur.String()})
				cur.Reset()
				inWord = false
			}
		default:
			inWord = true
			cur.WriteByte(ch)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("%w: unterminated quote in %q", errDockerfileUnsupported, s)
	}
	if inWord {
		words = append(words, dockerfileWord{raw: cur.String()})
	}
	return words, nil
}

// unquoteDockerfileWord strips one level of surrounding quotes and backslash
// escapes the way the Dockerfile parser does for ENV/LABEL/ARG values.
// Variable references are kept for expansion, including inside double quotes.
func unquoteDockerfileWord(s string) string {
	var out strings.Builder
	quote := byte(0)
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case quote == '\'':
			if ch == '\'' {
				quote = 0
			} else {
				out.WriteByte(ch)
			}
		case quote == '"':
			if ch == '"' {
				quote = 0
			} else if ch == '\\' && i+1 < len(s) && s[i+1] != '$' {
				i++
				out.WriteByte(s[i])
			} else {
				out.WriteByte(ch)
			}
		case ch == '"' || ch == '\'':
			quote = ch
		case ch == '\\' && i+1 < len(s) && s[i+1] != '$':
			i++
			out.WriteByte(s[i])
		default:
			out.WriteByte(ch)
		}
	}
	return out.String()
}

// expandDockerfileVars substitutes $NAME, ${NAME}, ${NAME:-default} and
// ${NAME:+alt}; "\$" is a literal dollar sign.
func expandDockerfileVars(s string, lookup func(string) (string, bool)) (string, error) {
	var out strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\\' && i+1 < len(s) && s[i+1] == '$' {
			out.WriteByte('$')
			i++
			continue
		}
		if ch != '$' || i+1 >= len(s) {
			out.WriteByte(ch)
			continue
		}
		if s[i+1] == '{' {
			end := strings.IndexByte(s[i:], '}')
			if end < 0 {
				return "", fmt.Errorf("%w: unterminated variable in %q", errDockerfileUnsupported, s)
			}
			expr := s[i+2 : i+end]
			i += end
			name, modifier, rest := expr, "", ""
			if j := strings.IndexAny(expr, ":"); j >= 0 && j+1 < len(expr) && (expr[j+1] == '-' || expr[j+1] == '+') {
				name, modifier, rest = expr[:j], expr[j+1:j+2], expr[j+2:]
			}
			value, set := lookup(name)
			switch modifier {
			case "-":
				if !set || value == "" {
					value = rest
				}
			case "+":
				if set && value != "" {
					value = rest
				} else {
					value = ""
				}
			}
			out.WriteString(value)
			continue
		}
		j := i + 1
		for j < len(s) && (s[j] == '_' || s[j] >= 'a' && s[j] <= 'z' || s[j] >= 'A' && s[j] <= 'Z' || j > i+1 && s[j] >= '0' && s[j] <= '9') {
			j++
		}
		if j == i+1 {
			out.WriteByte(ch)
			continue
		}
		value, _ := lookup(s[i+1 : j])
		out.WriteString(value)
		i = j - 1
	}
	return out.String(), nil
}
