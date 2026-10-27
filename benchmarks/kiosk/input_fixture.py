"""Native GTK/Qt controls for the browser input regression runner."""

import json
from pathlib import Path
import sys

mode, output = sys.argv[1:]
state = {"wheel": [], "selection": "Choose", "menu": None, "zoom": 0, "popups": []}


def change(key, value):
    state[key] = value


if mode == "qt":
    from PyQt5.QtCore import QTimer, Qt
    from PyQt5.QtWidgets import (
        QApplication,
        QComboBox,
        QLabel,
        QMainWindow,
        QPushButton,
        QVBoxLayout,
        QWidget,
    )

    app = QApplication([])
    state["double_click_ms"] = app.doubleClickInterval()
    window = QMainWindow()
    body = QWidget()
    window.setCentralWidget(body)
    layout = QVBoxLayout(body)
    combo = QComboBox()
    combo.addItems(["Choose", "Alpha", "Beta", "Gamma"])
    combo.currentTextChanged.connect(lambda text: change("selection", text))
    layout.addWidget(combo)
    button = QPushButton("Zoom in")
    button.clicked.connect(lambda: change("zoom", state["zoom"] + 1))
    layout.addWidget(button)
    menu = window.menuBar().addMenu("Audit")
    submenu = menu.addMenu("Nested")
    for name in ("First", "Second", "Third"):
        action = submenu.addAction(name)
        action.triggered.connect(
            lambda checked=False, value=name: change("menu", value)
        )

    class Canvas(QLabel):
        def wheelEvent(self, event):
            state["wheel"].append(
                [event.angleDelta().x(), event.angleDelta().y(), int(event.modifiers())]
            )
            event.accept()

    canvas = Canvas("Wheel or pinch here")
    canvas.setAlignment(Qt.AlignCenter)
    layout.addWidget(canvas, 1)

    def rect(widget, local=None):
        area = local or widget.rect()
        pos = widget.mapToGlobal(area.topLeft())
        return [pos.x(), pos.y(), area.width(), area.height()]

    def measure():
        state["controls"] = {
            "combo": rect(combo),
            "button": rect(button),
            "canvas": rect(canvas),
        }
        state["controls"]["menu"] = rect(
            window.menuBar(), window.menuBar().actionGeometry(menu.menuAction())
        )
        state["popups"] = []
        for popup in (menu, submenu):
            if popup.isVisible():
                state["popups"].append(
                    {
                        "rect": rect(popup),
                        "items": {
                            a.text(): rect(popup, popup.actionGeometry(a))
                            for a in popup.actions()
                        },
                    }
                )
        if combo.view().isVisible():
            view = combo.view()
            state["popups"].append(
                {
                    "rect": rect(view),
                    "items": {
                        combo.itemText(i): rect(
                            view.viewport(), view.visualRect(combo.model().index(i, 0))
                        )
                        for i in range(combo.count())
                    },
                }
            )

    def run():
        timer = QTimer()
        timer.timeout.connect(save)
        timer.start(100)
        QTimer.singleShot(180000, app.quit)
        app.exec()

else:
    import gi

    gi.require_version("Gtk", "3.0")
    from gi.repository import Atk, Gdk, GLib, Gtk

    state["double_click_ms"] = Gtk.Settings.get_default().get_property(
        "gtk-double-click-time"
    )

    window = Gtk.Window()
    body = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=8)
    window.add(body)
    bar = Gtk.MenuBar()
    body.pack_start(bar, False, False, 0)
    menu_item = Gtk.MenuItem(label="Audit")
    bar.append(menu_item)
    menu = Gtk.Menu()
    menu_item.set_submenu(menu)
    nested = Gtk.MenuItem(label="Nested")
    menu.append(nested)
    submenu = Gtk.Menu()
    nested.set_submenu(submenu)
    for name in ("First", "Second", "Third"):
        item = Gtk.MenuItem(label=name)
        item.connect("activate", lambda _, value=name: change("menu", value))
        submenu.append(item)
    combo = Gtk.ComboBoxText()
    for name in ("Choose", "Alpha", "Beta", "Gamma"):
        combo.append_text(name)
    combo.set_active(0)
    combo.connect(
        "changed", lambda widget: change("selection", widget.get_active_text())
    )
    body.pack_start(combo, False, False, 0)
    button = Gtk.Button(label="Zoom in")
    button.connect("clicked", lambda _: change("zoom", state["zoom"] + 1))
    body.pack_start(button, False, False, 0)
    canvas = Gtk.EventBox()
    canvas.add(Gtk.Label(label="Wheel or pinch here"))
    canvas.add_events(Gdk.EventMask.SCROLL_MASK)

    def wheel(_, event):
        x, y = {
            Gdk.ScrollDirection.UP: (0, 120),
            Gdk.ScrollDirection.DOWN: (0, -120),
            Gdk.ScrollDirection.LEFT: (-120, 0),
            Gdk.ScrollDirection.RIGHT: (120, 0),
        }.get(event.direction, (0, 0))
        state["wheel"].append([x, y, int(event.state)])
        return True

    canvas.connect("scroll-event", wheel)
    body.pack_start(canvas, True, True, 0)

    def rect(widget):
        top = widget.get_toplevel()
        _, x, y = top.get_window().get_origin()
        local = widget.translate_coordinates(top, 0, 0)
        area = widget.get_allocation()
        return [x + local[0], y + local[1], area.width, area.height]

    def measure():
        state["controls"] = {
            "combo": rect(combo),
            "button": rect(button),
            "canvas": rect(canvas),
            "menu": rect(menu_item),
        }
        state["popups"] = [
            {
                "rect": rect(popup),
                "items": {
                    item.get_label(): rect(item) for item in popup.get_children()
                },
            }
            for popup in (menu, submenu)
            if popup.get_mapped()
        ]
        if combo.get_property("popup-shown"):
            popup = combo.get_popup_accessible()
            items = {}
            for i in range(popup.get_n_accessible_children()):
                child = popup.ref_accessible_child(i)
                items[child.get_name()] = list(child.get_extents(Atk.CoordType.SCREEN))
            state["popups"].append({"items": items})

    def run():
        GLib.timeout_add(100, save)
        GLib.timeout_add_seconds(180, Gtk.main_quit)
        Gtk.main()


window.set_title("Kiosk input audit") if mode == "gtk" else window.setWindowTitle(
    "Kiosk input audit"
)
window.show_all() if mode == "gtk" else window.show()


def save():
    measure()
    state["wheel"] = state["wheel"][-200:]
    temporary = Path(output).with_suffix(".tmp")
    temporary.write_text(json.dumps(state))
    temporary.replace(output)
    return True


run()
