#!/usr/bin/env python3
"""Native GUI workload with optically readable presentation/input counters.

This is a benchmark application, not an integration required by hosted apps.
Run with --toolkit gtk, qt, or opengl. Both counters and the calibration pattern
are sampled from the pixels received by the browser, across the real stream.
"""

import argparse
import math
import time


class Scene:
    def __init__(self):
        self.started = time.monotonic()
        self.inputs = 0

    def draw(self, width, height, rectangle, line, text):
        frame = int((time.monotonic() - self.started) * 60) % 65536
        rectangle(0, 0, width, height, "#18202a")
        offset = frame % 64
        for x in range(-64, width + 64, 32):
            line(x + offset, height * 0.22, x - height / 2 + offset, height, "#57788d")
        for y in range(int(height * 0.22), height, 32):
            line(0, y, width, y, "#344c5e")
        rectangle(width * (0.5 + 0.3 * math.sin(frame / 40)), height * 0.45, 100, 100, "#f59e42")
        text(
            20,
            height * 0.3,
            "Application streaming: sharp text, fine lines, continuous motion",
            "#eeeeee",
        )
        for value, y in ((frame, 0.05), (self.inputs, 0.10), (0xA55A, 0.15)):
            for bit in range(16):
                rectangle(
                    width * (0.25 + bit / 32),
                    height * y,
                    width / 32 + 1,
                    height * 0.025,
                    "#ffffff" if value & (1 << bit) else "#000000",
                )


def gtk(scene):
    import gi

    gi.require_version("Gtk", "3.0")
    from gi.repository import Gtk, GLib

    window = Gtk.Window(title="Beam application streaming probe")
    window.set_default_size(1920, 1080)
    canvas = Gtk.DrawingArea()
    window.add(canvas)

    def draw(widget, cr):
        def color(value):
            cr.set_source_rgb(*(int(value[i : i + 2], 16) / 255 for i in (1, 3, 5)))

        def rectangle(x, y, w, h, value):
            color(value)
            cr.rectangle(x, y, w, h)
            cr.fill()

        def line(x, y, x2, y2, value):
            color(value)
            cr.move_to(x, y)
            cr.line_to(x2, y2)
            cr.set_line_width(1)
            cr.stroke()

        def text(x, y, value, ink):
            color(ink)
            cr.move_to(x, y)
            cr.set_font_size(16)
            cr.show_text(value)

        scene.draw(
            widget.get_allocated_width(), widget.get_allocated_height(), rectangle, line, text
        )

    def key(_, event):
        if event.string == "a":
            scene.inputs = (scene.inputs + 1) % 65536
            canvas.queue_draw()
        elif event.string == "d":
            dialog = Gtk.Dialog(title="Application dialog", transient_for=window, modal=True)
            dialog.add_button("Close", Gtk.ResponseType.CLOSE)
            dialog.get_content_area().add(
                Gtk.Label(label="This dialog belongs to the same application.")
            )
            dialog.connect("response", lambda item, *_: item.destroy())
            dialog.show_all()
        elif event.string == "w":
            secondary = Gtk.Window(title="Secondary application window")
            secondary.set_default_size(480, 320)
            secondary.add(Gtk.Label(label="An ordinary secondary application window."))
            secondary.show_all()

    canvas.connect("draw", draw)
    window.connect("key-press-event", key)
    window.connect("destroy", Gtk.main_quit)
    GLib.timeout_add(16, lambda: (canvas.queue_draw(), True)[1])
    window.show_all()
    Gtk.main()


def qt(scene, opengl=False):
    from PyQt5.QtCore import QTimer, Qt
    from PyQt5.QtGui import QPainter, QColor
    from PyQt5.QtWidgets import QApplication, QWidget, QOpenGLWidget, QDialog, QLabel, QVBoxLayout

    class Canvas(QOpenGLWidget if opengl else QWidget):
        def __init__(self):
            super().__init__()
            self.resize(1920, 1080)
            self.setWindowTitle("Beam application streaming probe")
            self.setFocusPolicy(Qt.StrongFocus)

        def keyPressEvent(self, event):
            if event.text() == "a":
                scene.inputs = (scene.inputs + 1) % 65536
                self.update()
            elif event.text() in ("d", "w"):
                child = QDialog(self) if event.text() == "d" else QWidget(None)
                child.setWindowTitle(
                    "Application dialog" if event.text() == "d" else "Secondary application window"
                )
                child.resize(480, 320)
                layout = QVBoxLayout(child)
                layout.addWidget(QLabel("This window belongs to the same application."))
                self.child = child
                child.show()

        def paint(self):
            painter = QPainter(self)

            def rectangle(x, y, w, h, color):
                painter.fillRect(int(x), int(y), int(w), int(h), QColor(color))

            def line(x, y, x2, y2, color):
                painter.setPen(QColor(color))
                painter.drawLine(int(x), int(y), int(x2), int(y2))

            def text(x, y, value, ink):
                painter.setPen(QColor(ink))
                painter.drawText(int(x), int(y), value)

            scene.draw(self.width(), self.height(), rectangle, line, text)
            painter.end()

        def paintEvent(self, event):
            if opengl:
                super().paintEvent(event)
            else:
                self.paint()

        def paintGL(self):
            self.paint()

    app = QApplication(["beam-kiosk-probe"])
    window = Canvas()
    timer = QTimer()
    timer.timeout.connect(window.update)
    timer.start(16)
    window.show()
    app.exec()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--toolkit", choices=("gtk", "qt", "opengl"), default="gtk")
    args = parser.parse_args()
    scene = Scene()
    gtk(scene) if args.toolkit == "gtk" else qt(scene, args.toolkit == "opengl")
