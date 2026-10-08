"""Desktop control over the existing authenticated process/file transports."""

import uuid
from pathlib import Path
from typing import Optional


class VMDesktop:
    def __init__(self, vm):
        self.vm = vm

    def _sandbox(self):
        sandbox = self.vm._sandbox()
        if not self.vm.info["spec"].get("desktop"):
            raise ValueError("Create this VM with desktop=True to use desktop controls")
        return sandbox

    def _run(self, *args):
        process = self._sandbox().process.exec(*args, cwd="/")
        if process.wait(30) != 0:
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

    def move_mouse(self, x: int, y: int):
        self._run("xdotool", "mousemove", "--sync", str(int(x)), str(int(y)))

    def click(self, x=None, y=None, *, button="left", count=1):
        buttons = {"left": 1, "middle": 2, "right": 3}
        if button not in buttons or not isinstance(count, int) or not 1 <= count <= 3:
            raise ValueError("Use left/middle/right and a click count from 1 to 3")
        if (x is None) != (y is None):
            raise ValueError("Provide both mouse coordinates")
        args = ["xdotool"]
        if x is not None:
            args += ["mousemove", "--sync", str(int(x)), str(int(y))]
        self._run(*args, "click", "--repeat", str(count), "--delay", "100", str(buttons[button]))

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
        self._run("xdotool", "type", "--clearmodifiers", "--delay", "0", "--", text)

    def drag(self, start, end):
        self._run(
            "xdotool",
            "mousemove",
            "--sync",
            str(int(start[0])),
            str(int(start[1])),
            "mousedown",
            "1",
            "mousemove",
            "--sync",
            str(int(end[0])),
            str(int(end[1])),
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
        The file is inside the VM and can be downloaded with vm.fs.
        """
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
            "-movflags",
            "frag_keyframe+empty_moov",
            path,
        )
