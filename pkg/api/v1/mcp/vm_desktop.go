package mcp

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/google/uuid"
)

type mcpPayload map[string]any

func (g *Group) vmDesktop(ctx context.Context, a *auth.AuthInfo, args toolArgs, command []string, wait bool) (any, error) {
	info, err := g.vmGet(ctx, a, args)
	if err != nil {
		return nil, err
	}
	spec, _ := info["spec"].(map[string]any)
	if spec["desktop"] != true {
		return nil, fail("DESKTOP_DISABLED", "VM was not created with desktop enabled")
	}
	return g.vmExecCommand(ctx, a, args, command, wait)
}

func (g *Group) vmDesktopCatalog() []mcpTool {
	with := func(extra props, required ...string) map[string]any {
		p := props{"name": str("VM name or ID")}
		for k, v := range extra {
			p[k] = v
		}
		return schema(p, append([]string{"name"}, required...)...)
	}
	xy := props{"x": boundedInt(0, 8192, 0), "y": boundedInt(0, 8192, 0)}
	tools := []mcpTool{
		vmTool(
			"vm_desktop_size",
			"Read the guest desktop's current pixel dimensions.",
			with(nil),
			true, true, "beam:exec",
			g.vmDesktopSize,
		),
		vmTool(
			"vm_desktop_screenshot",
			"Capture the guest desktop as a PNG image.",
			with(nil),
			true, false, "beam:exec",
			g.vmScreenshot,
		),
		vmTool(
			"vm_desktop_move_mouse",
			"Move the guest desktop pointer to pixel coordinates.",
			with(xy, "x", "y"),
			false, true, "beam:exec",
			g.vmDesktopMoveMouse,
		),
		vmTool(
			"vm_desktop_click",
			"Click at coordinates or the current pointer. button is left, middle or right; count is 1–3.",
			with(props{"x": xy["x"], "y": xy["y"], "button": enum("left", "middle", "right"), "count": boundedInt(1, 3, 1)}),
			false, false, "beam:exec",
			g.vmDesktopClick,
		),
		vmTool(
			"vm_desktop_scroll",
			"Scroll the guest desktop up/down/left/right by 1–100 steps.",
			with(props{"direction": enum("up", "down", "left", "right"), "amount": boundedInt(1, 100, 3)}),
			false, false, "beam:exec",
			g.vmDesktopScroll,
		),
		vmTool(
			"vm_desktop_press",
			"Press X11 keys or shortcuts, e.g. Return or ctrl+l.",
			with(props{"keys": map[string]any{"type": "array", "items": str(""), "minItems": 1}}, "keys"),
			false, false, "beam:exec",
			g.vmDesktopPress,
		),
		vmTool(
			"vm_desktop_write",
			"Paste literal UTF-8 text through the guest clipboard, including Unicode and newlines.",
			with(props{"text": str("Literal text")}, "text"),
			false, false, "beam:exec",
			g.vmDesktopWrite,
		),
		vmTool(
			"vm_desktop_drag",
			"Drag the left mouse button between two pixel coordinates.",
			with(props{"start_x": xy["x"], "start_y": xy["y"], "end_x": xy["x"], "end_y": xy["y"]}, "start_x", "start_y", "end_x", "end_y"),
			false, false, "beam:exec",
			g.vmDesktopDrag,
		),
		vmTool(
			"vm_desktop_launch",
			"Launch a GUI application using an argv array; returns its managed process ID.",
			with(props{"command": map[string]any{"type": "array", "items": str(""), "minItems": 1}}, "command"),
			false, false, "beam:exec",
			g.vmDesktopLaunch,
		),
		vmTool(
			"vm_desktop_resize",
			"Resize the guest desktop within 320–4096 by 200–2160 pixels.",
			with(props{"width": boundedInt(320, 4096, 1280), "height": boundedInt(200, 2160, 720)}, "width", "height"),
			false, true, "beam:exec",
			g.vmDesktopResize,
		),
		vmTool(
			"vm_desktop_record",
			"Record the guest screen to an absolute MP4 path at 1–60 FPS. Returns a PID; finish with "+
				"vm_desktop_stop_recording, then vm_read_file.",
			with(props{"path": str("Absolute MP4 path inside the VM"), "fps": boundedInt(1, 60, 30)}, "path"),
			false, false, "beam:exec",
			g.vmDesktopRecord,
		),
		vmTool(
			"vm_desktop_stop_recording",
			"Send SIGINT to ffmpeg so it finalizes its MP4. Pin the returned container ID.",
			with(props{"pid": boundedInt(1, 2147483647, 1), "container_id": str("Container ID from the recording")}, "pid", "container_id"),
			false, false, "beam:exec",
			g.vmDesktopStopRecording,
		),
	}
	for i := range tools {
		tools[i].Description += " Exec readiness is earlier than desktop readiness; first verify beam-desktop.service and port 8080 via vm_exec."
	}
	return tools
}

func (g *Group) vmScreenshot(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	path := "/run/beam-desktop/mcp-" + uuid.NewString() + ".png"
	if _, err := g.vmDesktop(ctx, a, args, []string{"scrot", "--overwrite", path}, true); err != nil {
		return nil, err
	}
	defer g.vmExecCommand(ctx, a, args, []string{"rm", "--", path}, true)
	var raw []byte
	for offset := 0; offset < 8<<20; {
		value, err := g.vmPod(ctx, a, args, http.MethodGet, "/files/download/"+url.PathEscape(path), nil, url.Values{"offset": {strconv.Itoa(offset)}, "length": {"524288"}})
		if err != nil {
			return nil, err
		}
		result, _ := value.(map[string]any)
		encoded, _ := result["data"].(string)
		chunk, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fail("INVALID_IMAGE", "invalid screenshot bytes")
		}
		raw = append(raw, chunk...)
		offset += len(chunk)
		if len(chunk) < 524288 {
			break
		}
	}
	if len(raw) >= 8<<20 {
		return nil, fail("IMAGE_TOO_LARGE", "screenshot exceeds 8 MiB; reduce desktop resolution")
	}
	if len(raw) < 8 || string(raw[:8]) != "\x89PNG\r\n\x1a\n" {
		return nil, fail("INVALID_IMAGE", "guest did not return a PNG screenshot")
	}
	data := base64.StdEncoding.EncodeToString(raw)
	return mcpPayload{
		"content": []map[string]any{{"type": "image", "mimeType": "image/png", "data": data}},
		"isError": false,
	}, nil
}

func (g *Group) vmDesktopSize(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	return g.vmDesktop(ctx, a, args, []string{"xdotool", "getdisplaygeometry"}, true)
}

func (g *Group) vmDesktopMoveMouse(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	return g.vmDesktop(ctx, a, args, []string{"xdotool", "mousemove", strconv.Itoa(int(args.num("x", 0))), strconv.Itoa(int(args.num("y", 0)))}, true)
}

func (g *Group) vmDesktopClick(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	command := []string{"xdotool"}
	_, hasX := args["x"]
	_, hasY := args["y"]
	if hasX != hasY {
		return nil, fail("INVALID_ARGS", "provide both x and y")
	}
	if hasX {
		command = append(command, "mousemove", strconv.Itoa(int(args.num("x", 0))), strconv.Itoa(int(args.num("y", 0))))
	}
	button := map[string]string{"": "1", "left": "1", "middle": "2", "right": "3"}[args.str("button")]
	command = append(command, "click", "--repeat", strconv.Itoa(int(args.num("count", 1))), "--delay", "100", button)
	return g.vmDesktop(ctx, a, args, command, true)
}

func (g *Group) vmDesktopScroll(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	button := map[string]string{"": "5", "up": "4", "down": "5", "left": "6", "right": "7"}[args.str("direction")]
	return g.vmDesktop(ctx, a, args, []string{"xdotool", "click", "--repeat", strconv.Itoa(int(args.num("amount", 3))), button}, true)
}

func (g *Group) vmDesktopPress(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	keys := args.strings("keys")
	for _, key := range keys {
		if key == "" || strings.HasPrefix(key, "-") {
			return nil, fail("INVALID_ARGS", "provide X11 keys, not command options")
		}
	}
	return g.vmDesktop(ctx, a, args, append([]string{"xdotool", "key", "--clearmodifiers"}, keys...), true)
}

func (g *Group) vmDesktopWrite(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	encoded := base64.StdEncoding.EncodeToString([]byte(args.rawString("text")))
	script := "printf %s " + shellWord(encoded) + " | base64 -d | xclip -selection clipboard -in && xdotool key --clearmodifiers ctrl+v"
	return g.vmDesktop(ctx, a, args, []string{"bash", "-lc", script}, true)
}

func (g *Group) vmDesktopDrag(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	command := []string{
		"xdotool",
		"mousemove",
		strconv.Itoa(int(args.num("start_x", 0))),
		strconv.Itoa(int(args.num("start_y", 0))),
		"mousedown",
		"1",
		"mousemove",
		strconv.Itoa(int(args.num("end_x", 0))),
		strconv.Itoa(int(args.num("end_y", 0))),
		"mouseup",
		"1",
	}
	return g.vmDesktop(ctx, a, args, command, true)
}

func (g *Group) vmDesktopLaunch(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	return g.vmDesktop(ctx, a, args, args.strings("command"), false)
}

func (g *Group) vmDesktopResize(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	w, h := int(args.num("width", 1280)), int(args.num("height", 720))
	mode := fmt.Sprintf("%dx%d", w, h)
	script := fmt.Sprintf("if ! xrandr --query | awk '$1 == %s {found=1} END {exit !found}'; then xrandr --newmode %s %g %d 0 0 %d %d 0 0 %d && xrandr --addmode VNC-0 %s; fi; xrandr --output VNC-0 --mode %s", strconv.Quote(mode), shellWord(mode), float64(w*h*60)/1000000, w, w, h, h, shellWord(mode), shellWord(mode))
	return g.vmDesktop(ctx, a, args, []string{"bash", "-lc", script}, true)
}

func (g *Group) vmDesktopRecord(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	if !strings.HasPrefix(args.str("path"), "/") {
		return nil, fail("INVALID_ARGS", "recording path must be absolute")
	}
	command := []string{
		"ffmpeg",
		"-nostdin",
		"-n",
		"-f",
		"x11grab",
		"-framerate",
		strconv.Itoa(int(args.num("fps", 30))),
		"-i",
		":1",
		"-c:v",
		"libx264",
		"-preset",
		"ultrafast",
		"-pix_fmt",
		"yuv420p",
		"-vf",
		"pad=ceil(iw/2)*2:ceil(ih/2)*2",
		"-movflags",
		"frag_keyframe+empty_moov+default_base_moof",
		"-f",
		"mp4",
		args.str("path"),
	}
	return g.vmDesktop(ctx, a, args, command, false)
}

func (g *Group) vmDesktopStopRecording(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	info, err := g.vmGet(ctx, a, args)
	if err != nil {
		return nil, err
	}
	if info["container_id"] != args.str("container_id") {
		return nil, fail("VM_RESTARTED", "recording belongs to a previous VM boot")
	}
	return g.vmDesktop(ctx, a, args, []string{"kill", "-INT", strconv.Itoa(int(args.num("pid", 0)))}, true)
}
