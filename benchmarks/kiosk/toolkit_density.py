"""Native toolkit density fixture; its utility window must remain floating."""

import json
import re
import subprocess
import sys

mode = sys.argv[2] if len(sys.argv) > 2 else "utility"
parent = None

if sys.argv[1] == "gtk":
    import gi

    gi.require_version("Gtk", "3.0")
    gi.require_version("PangoCairo", "1.0")
    from gi.repository import Gtk, GLib, PangoCairo, Gdk

    if mode in ("dialog", "transient"):
        parent = Gtk.Window()
        parent.show_all()
    w = Gtk.Dialog(transient_for=parent) if mode == "dialog" else Gtk.Window()
    if mode == "transient":
        w.set_transient_for(parent)
    if mode == "utility":
        w.set_type_hint(Gdk.WindowTypeHint.UTILITY)
    w.set_default_size(640, 480)
    b = Gtk.Button(label="Application control")
    (w.get_content_area() if mode == "dialog" else w).add(b)

    def measure():
        _, h = (
            b.get_layout().get_pixel_size()
            if hasattr(b, "get_layout")
            else b.get_child().get_layout().get_pixel_size()
        )
        x = subprocess.check_output(["xwininfo", "-id", str(w.get_window().get_xid())], text=True)
        print(
            json.dumps(
                {
                    "toolkit": "gtk3",
                    "mode": mode,
                    "scale": w.get_scale_factor(),
                    "screen_dpi": w.get_screen().get_resolution(),
                    "pango_dpi": PangoCairo.context_get_resolution(b.get_pango_context()),
                    "font_pixel_height": h,
                    "logical_window": w.get_size(),
                    "physical_window": re.findall(r"(?:Width|Height):\s*(\d+)", x),
                }
            ),
            flush=True,
        )
        w.destroy()
        if parent:
            parent.destroy()
        Gtk.main_quit()
        return False

    w.show_all()
    GLib.timeout_add(600, measure)
    Gtk.main()
else:
    from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QVBoxLayout, QDialog
    from PyQt5.QtCore import QTimer, Qt

    app = QApplication([])
    if mode in ("dialog", "transient"):
        parent = QWidget()
        parent.show()
    # Qt.Window does not publish WM_TRANSIENT_FOR just from a QObject parent.
    # Native Qt dialogs/utilities exercise its real transient-window path;
    # the explicit NORMAL + WM_TRANSIENT_FOR case is supplied by GTK.
    if mode == "transient":
        raise ValueError("Use GTK for the explicit X11 normal-transient fixture")
    w = (
        QDialog(parent)
        if mode == "dialog"
        else QWidget(parent, Qt.Tool if mode == "utility" else Qt.Window)
    )
    w.resize(640, 480)
    b = QPushButton("Application control")
    QVBoxLayout(w).addWidget(b)

    def measure():
        x = subprocess.check_output(["xwininfo", "-id", str(int(w.winId()))], text=True)
        print(
            json.dumps(
                {
                    "toolkit": "qt5",
                    "window_hints": subprocess.check_output(
                        [
                            "xprop",
                            "-id",
                            str(int(w.winId())),
                            "WM_TRANSIENT_FOR",
                            "_NET_WM_WINDOW_TYPE",
                            "_NET_WM_STATE",
                        ],
                        text=True,
                    ),
                    "mode": mode,
                    "scale": w.devicePixelRatioF(),
                    "screen_dpi": w.screen().logicalDotsPerInch(),
                    "physical_dpi": w.screen().physicalDotsPerInch(),
                    "font_pixel_height": b.fontMetrics().height(),
                    "logical_window": [w.width(), w.height()],
                    "physical_window": re.findall(r"(?:Width|Height):\s*(\d+)", x),
                }
            ),
            flush=True,
        )
        app.quit()

    w.show()
    QTimer.singleShot(600, measure)
    app.exec()
