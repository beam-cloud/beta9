"""Desktop control over the existing authenticated process/file transports."""

import uuid
from pathlib import Path
from typing import Optional

from ...exceptions import SandboxProcessError


class VMDesktop:
    def __init__(self, vm):
        self.vm = vm

    def _sandbox(self):
        self.vm.refresh()
        if not self.vm.info["spec"].get("desktop"):
            raise ValueError("Create this VM with desktop=True to use desktop controls")
        self.vm.wait(services=True)
        return self.vm._sandbox()

    def _run(self, *args, stdin=None):
        process = self._sandbox().process.exec(*args, cwd="/", stdin=stdin)
        try:
            code = process.wait(30)
        except SandboxProcessError:
            try:
                process.kill()
            except Exception:
                pass
            raise
        if code != 0:
            raise RuntimeError(process.stderr.read() or f"Desktop command failed: {args[0]}")
        return process.stdout.read().strip()

    @property
    def url(self):
        return self.vm.desktop_url

    def screenshot(self, path: Optional[str] = None) -> bytes:
        """Capture a PNG; optionally save it to a local file."""
        remote = f"/run/beam-desktop/screenshot-{uuid.uuid4().hex}.png"
        sandbox = self._sandbox()
        captured = False
        try:
            self._run("scrot", "--overwrite", remote)
            captured = True
            data = sandbox.fs.read_bytes(remote)
            if path is not None:
                Path(path).write_bytes(data)
            return data
        finally:
            if captured:
                sandbox.fs.remove(remote)

    def screen_size(self):
        width, height = self._run("xdotool", "getdisplaygeometry").split()
        return int(width), int(height)

    def resize(self, width: int, height: int):
        if (
            not isinstance(width, int)
            or not isinstance(height, int)
            or not (320 <= width <= 4096 and 200 <= height <= 2160)
        ):
            raise ValueError("Desktop resolution must be 320–4096 by 200–2160")
        # KasmVNC advertises resize modes through RandR. The selected size
        # applies immediately to both the stream and subsequent screenshots.
        self._run("xrandr", "--output", "VNC-0", "--mode", f"{width}x{height}")

    @staticmethod
    def _move(x, y):
        # --sync waits for a motion event forever when the pointer is already
        # at these coordinates. X requests are ordered, including a following
        # click or drag, so ordinary mousemove also handles repeated positions.
        return ["mousemove", str(int(x)), str(int(y))]

    def move_mouse(self, x: int, y: int):
        self._run("xdotool", *self._move(x, y))

    def click(self, x=None, y=None, *, button="left", count=1):
        buttons = {"left": 1, "middle": 2, "right": 3}
        if button not in buttons or not isinstance(count, int) or not 1 <= count <= 3:
            raise ValueError("Use left/middle/right and a click count from 1 to 3")
        if (x is None) != (y is None):
            raise ValueError("Provide both mouse coordinates")
        args = ["xdotool"]
        if x is not None:
            args += self._move(x, y)
        self._run(
            *args,
            "click",
            "--repeat",
            str(count),
            "--delay",
            "100",
            str(buttons[button]),
        )

    def scroll(self, direction="down", amount=3):
        buttons = {"up": 4, "down": 5, "left": 6, "right": 7}
        if direction not in buttons or not isinstance(amount, int) or not 1 <= amount <= 100:
            raise ValueError("Use up/down/left/right and 1–100 scroll steps")
        self._run("xdotool", "click", "--repeat", str(amount), str(buttons[direction]))

    def press(self, *keys: str):
        if not keys or any(not key or key.startswith("-") for key in keys):
            raise ValueError("Provide X11 key names, e.g. Return or ctrl+l")
        self._run("xdotool", "key", "--clearmodifiers", *keys)

    def write(self, text: str):
        """Type literal text without shell expansion."""
        # X11 key synthesis cannot reliably represent arbitrary Unicode.
        # Clipboard paste preserves the exact UTF-8 text, including newlines.
        self._run("xclip", "-selection", "clipboard", "-in", stdin=text)
        self._run("xdotool", "key", "--clearmodifiers", "ctrl+v")

    def drag(self, start, end):
        self._run(
            "xdotool",
            *self._move(*start),
            "mousedown",
            "1",
            *self._move(*end),
            "mouseup",
            "1",
        )

    def launch(self, *command: str):
        """Launch a GUI program and return its reattachable process handle."""
        if not command:
            raise ValueError("Provide a program to launch")
        return self._sandbox().process.exec(*command)

    def record(self, path: str, *, fps=30):
        """Record the guest screen to an MP4 until the returned handle stops.

        This uses CPU encoding; it is independent of browser frame-rate stats.
        Finish with stop_recording(handle), then download the file with vm.fs.
        """
        if not isinstance(path, str) or not path.startswith("/"):
            raise ValueError("Recording requires an absolute path inside the VM")
        if not isinstance(fps, int) or not 1 <= fps <= 60:
            raise ValueError("Recording FPS must be between 1 and 60")
        width, height = self.screen_size()
        return self._sandbox().process.exec(
            "ffmpeg",
            "-nostdin",
            "-n",
            "-f",
            "x11grab",
            "-video_size",
            f"{width}x{height}",
            "-framerate",
            str(fps),
            "-i",
            ":1",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-pix_fmt",
            "yuv420p",
            "-g",
            str(fps),
            "-vf",
            "pad=ceil(iw/2)*2:ceil(ih/2)*2",
            "-movflags",
            "frag_keyframe+empty_moov+default_base_moof",
            "-f",
            "mp4",
            path,
        )

    def stop_recording(self, recording):
        """Finalize an MP4 with SIGINT, preserving its trailing frames."""
        sandbox = self._sandbox()
        if recording.sandbox_instance.container_id != sandbox.container_id:
            raise ValueError("Recording belongs to a previous VM runtime")
        self._run("kill", "-INT", str(recording.pid))
        recording.wait(30)
