import sys
import os
import csv
import math
import bisect
import socket
import tempfile
import struct
import serial
import serial.tools.list_ports # Do wykrywania portów COM
import time # Import time for performance measurement
from datetime import datetime
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QFrame, QSplitter,
    QGroupBox, QLineEdit, QPushButton, QToolButton, QButtonGroup, QComboBox, QLabel, QCheckBox,
    QListWidget, QListWidgetItem, QMenu,
    QTableWidget, QTableWidgetItem, QDialog, QDialogButtonBox,
    QMessageBox, QSpinBox, QPlainTextEdit, QFileDialog, QProgressBar, QAbstractItemView
)
from PyQt6.QtCore import Qt, QTimer, QRect, QRectF, QSettings, QStandardPaths, QPointF
from PyQt6.QtGui import (
    QIcon, QColor, QFont, QFontDatabase, QPainter, QPalette, QPen, QPixmap, QPolygonF, QTextCursor, QTextCharFormat
)

from struct import pack, unpack


def resource_path(name):
    """Ścieżka do zasobu - działa niezależnie od katalogu roboczego, także w exe zbudowanym przez PyInstaller."""
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, name)


# --- Ustawienia aplikacji (plik pymms.ini obok programu) ---

SETTINGS_FILE = "pymms.ini"

def app_dir():
    """Katalog aplikacji: folder skryptu, a w exe (PyInstaller) folder pliku exe - nie tymczasowy _MEIPASS."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))

def settings_path():
    """pymms.ini obok programu; gdy katalog jest tylko do odczytu (np. Program Files) - katalog konfiguracji użytkownika."""
    path = os.path.join(app_dir(), SETTINGS_FILE)
    try:
        with open(path, "a", encoding="utf-8"):
            pass
        return path
    except OSError:
        base = QStandardPaths.writableLocation(QStandardPaths.StandardLocation.GenericConfigLocation) or os.path.expanduser("~")
        folder = os.path.normpath(os.path.join(base, "pymms"))
        os.makedirs(folder, exist_ok=True)
        return os.path.join(folder, SETTINGS_FILE)

def open_settings():
    return QSettings(settings_path(), QSettings.Format.IniFormat)


# --- Motyw (ciemny, w stylu Serial Log Viewer; zielony kolor wiodący odróżnia pymms od pymss) ---

C_BG = "#14161b"
C_PANEL = "#1b1e24"
C_PANEL2 = "#21252c"
C_BORDER = "#2b3039"
C_TEXT = "#d8dbe2"
C_MUTED = "#8b919d"
C_DIM = "#5b616d"
ACCENT = "#8FCE00"        # zielony kolor wiodący
ACCENT_HOVER = "#a3dd2a"
ACCENT_INK = "#11160a"    # tekst na zielonym tle
C_SEL = "#2c3a17"         # zaznaczenie (ciemna zieleń)
STATUS_RED = "#ff5f5f"    # kolory komunikatów
STATUS_ORANGE = "#f2b43c"
C_BLUE = "#58a6ff"
C_GREEN = "#4cc38a"

ACCENT_STYLESHEET = f"""
* {{ font-family: "Segoe UI"; font-size: 9pt; color: {C_TEXT}; }}
QMainWindow, QWidget#central, QDialog {{ background: {C_BG}; }}
QWidget#scanner {{ background: {C_BG}; }}
QFrame#topbar, QFrame#requestbar, QFrame#panelbar {{ background: {C_PANEL}; border-bottom: 1px solid {C_BORDER}; }}
QFrame#sidebar {{ background: {C_PANEL}; border-right: 1px solid {C_BORDER}; }}
QFrame#card {{ background: {C_PANEL2}; border: 1px solid {C_BORDER}; border-radius: 8px; }}
QLabel#sectionTitle {{ color: {C_MUTED}; font-size: 8pt; font-weight: 600; letter-spacing: 1px; }}
QLabel#appTitle {{ font-size: 11pt; font-weight: 600; }}
QLabel#appSubtitle {{ color: {C_DIM}; }}
QLabel#fieldLabel {{ color: {C_MUTED}; }}
QLabel#statusDot {{ font-size: 12pt; }}
QLabel#cardValue {{ font-size: 13pt; font-weight: 600; }}
QLabel#cardLabel {{ color: {C_MUTED}; font-size: 8pt; }}
QLabel#hint {{ color: {C_DIM}; }}

QComboBox, QLineEdit, QSpinBox {{
    background: {C_PANEL2}; border: 1px solid {C_BORDER}; border-radius: 6px;
    padding: 4px 8px; selection-background-color: {ACCENT}; selection-color: {ACCENT_INK};
}}
QComboBox:hover, QLineEdit:hover, QSpinBox:hover {{ border-color: #3a404b; }}
QComboBox:focus, QLineEdit:focus, QSpinBox:focus {{ border-color: {ACCENT}; }}
QComboBox:disabled, QLineEdit:disabled, QSpinBox:disabled {{ color: {C_DIM}; background: {C_PANEL}; }}
QComboBox::drop-down {{ border: none; width: 18px; }}
QComboBox QAbstractItemView {{
    background: {C_PANEL2}; border: 1px solid {C_BORDER}; selection-background-color: {C_SEL}; outline: 0;
}}
QSpinBox {{ padding-right: 18px; }}
QSpinBox::up-button, QSpinBox::down-button {{ width: 16px; border: none; background: transparent; }}
QSpinBox::up-button {{ subcontrol-position: top right; margin-top: 2px; }}
QSpinBox::down-button {{ subcontrol-position: bottom right; margin-bottom: 2px; }}
QSpinBox::up-button:hover, QSpinBox::down-button:hover {{ background: #2a2f38; border-radius: 3px; }}

QPushButton, QToolButton {{
    background: {C_PANEL2}; border: 1px solid {C_BORDER}; border-radius: 6px; padding: 5px 10px;
}}
QPushButton:hover, QToolButton:hover {{ background: #2a2f38; border-color: #3a404b; }}
QPushButton:pressed, QToolButton:pressed {{ background: #323843; }}
QPushButton:checked, QToolButton:checked {{ background: #29361a; border-color: {ACCENT}; color: #dff5b0; }}
QPushButton:disabled, QToolButton:disabled {{ color: {C_DIM}; }}
QPushButton#primary {{ background: {ACCENT}; border-color: {ACCENT}; color: {ACCENT_INK}; font-weight: 600; padding: 5px 16px; }}
QPushButton#primary:hover {{ background: {ACCENT_HOVER}; }}
QPushButton#primary:disabled {{ background: #3a4a1c; border-color: #3a4a1c; color: #7d8a62; }}
QPushButton#danger {{ background: #3a2026; border-color: #6b2b35; color: #ffb3b3; font-weight: 600; padding: 5px 16px; }}
QPushButton#danger:hover {{ background: #4a2830; }}
QPushButton#segmentLeft {{ border-top-right-radius: 0; border-bottom-right-radius: 0; padding: 5px 12px; }}
QPushButton#segmentRight {{ border-top-left-radius: 0; border-bottom-left-radius: 0; padding: 5px 12px; margin-left: -1px; }}
QPushButton#segmentLeft:checked, QPushButton#segmentRight:checked {{ background: #29361a; border-color: {ACCENT}; color: {ACCENT}; font-weight: 600; }}

QTableWidget, QTableView {{
    background: {C_BG}; alternate-background-color: #171a1f; border: none;
    gridline-color: transparent; selection-background-color: {C_SEL}; selection-color: {C_TEXT}; outline: 0;
}}
QHeaderView::section {{
    background: {C_PANEL}; color: {C_MUTED}; border: none; border-bottom: 1px solid {C_BORDER};
    border-right: 1px solid {C_BORDER}; padding: 4px 8px; font-size: 8pt; font-weight: 600;
}}
QTableCornerButton::section {{ background: {C_PANEL}; border: none; }}
QPlainTextEdit {{ background: {C_BG}; border: none; selection-background-color: {ACCENT}; selection-color: {ACCENT_INK}; }}
QGroupBox {{ border: 1px solid {C_BORDER}; border-radius: 8px; margin-top: 12px; padding: 10px 8px 6px 8px; }}
QGroupBox::title {{ subcontrol-origin: margin; left: 10px; padding: 0 4px; color: {C_MUTED}; font-size: 8pt; font-weight: 600; }}
QCheckBox {{ spacing: 6px; }}
QCheckBox::indicator {{ width: 14px; height: 14px; border: 1px solid #3a404b; border-radius: 4px; background: {C_PANEL2}; }}
QCheckBox::indicator:checked {{ background: {ACCENT}; border-color: {ACCENT}; }}
QProgressBar {{ background: {C_PANEL2}; border: 1px solid {C_BORDER}; border-radius: 6px; text-align: center; color: {C_TEXT}; }}
QProgressBar::chunk {{ background: {ACCENT}; border-radius: 5px; }}
QSplitter::handle {{ background: {C_BORDER}; }}
QSplitter::handle:horizontal {{ width: 1px; }}
QSplitter::handle:vertical {{ height: 1px; }}
QStatusBar {{ background: {C_PANEL}; border-top: 1px solid {C_BORDER}; color: {C_MUTED}; }}
QStatusBar QLabel {{ color: {C_MUTED}; padding: 0 8px; }}
QStatusBar::item {{ border: none; }}
QMenu {{ background: {C_PANEL2}; border: 1px solid {C_BORDER}; padding: 4px; }}
QMenu::item {{ padding: 6px 20px; border-radius: 4px; }}
QMenu::item:selected {{ background: {C_SEL}; }}
QScrollBar:vertical {{ background: transparent; width: 11px; margin: 0; }}
QScrollBar::handle:vertical {{ background: #343a45; border-radius: 4px; min-height: 30px; margin: 2px; }}
QScrollBar::handle:vertical:hover {{ background: #454c59; }}
QScrollBar:horizontal {{ background: transparent; height: 11px; margin: 0; }}
QScrollBar::handle:horizontal {{ background: #343a45; border-radius: 4px; min-width: 30px; margin: 2px; }}
QScrollBar::add-line, QScrollBar::sub-line {{ width: 0; height: 0; }}
QScrollBar::add-page, QScrollBar::sub-page {{ background: transparent; }}
QToolTip {{ background: {C_PANEL2}; color: {C_TEXT}; border: 1px solid {C_BORDER}; padding: 4px; }}
QMessageBox QLabel {{ color: {C_TEXT}; }}
"""


def rgba(hex_color, alpha):
    c = QColor(hex_color)
    return f"rgba({c.red()},{c.green()},{c.blue()},{alpha})"


def pill_style(color):
    """Styl przełącznika-'pigułki' (filtry logu) w danym kolorze."""
    return (f"QPushButton {{ border-radius: 12px; padding: 3px 12px; color: {C_DIM};"
            f" background: transparent; border: 1px solid {C_BORDER}; }}"
            f"QPushButton:hover {{ border-color: {rgba(color, 0.6)}; }}"
            f"QPushButton:checked {{ color: {color}; background: {rgba(color, 0.13)}; border-color: {rgba(color, 0.55)}; }}")


def apply_theme(app):
    """Ciemna paleta Fusion + arkusz stylów aplikacji."""
    app.setStyle("Fusion")
    pal = QPalette()
    for role, col in (
        (QPalette.ColorRole.Window, C_BG), (QPalette.ColorRole.Base, C_BG),
        (QPalette.ColorRole.AlternateBase, "#171a1f"), (QPalette.ColorRole.Button, C_PANEL2),
        (QPalette.ColorRole.Text, C_TEXT), (QPalette.ColorRole.WindowText, C_TEXT),
        (QPalette.ColorRole.ButtonText, C_TEXT), (QPalette.ColorRole.Highlight, C_SEL),
        (QPalette.ColorRole.HighlightedText, C_TEXT), (QPalette.ColorRole.ToolTipBase, C_PANEL2),
        (QPalette.ColorRole.ToolTipText, C_TEXT), (QPalette.ColorRole.PlaceholderText, C_DIM),
    ):
        pal.setColor(role, QColor(col))
    app.setPalette(pal)
    app.setStyleSheet(ACCENT_STYLESHEET + theme_images_qss()
                      + f'QTableWidget, QPlainTextEdit {{ font-family: "{mono_family()}"; }}')


def theme_images_qss():
    """Strzałki list/pól liczbowych i znacznik checkboxa - QSS przyjmuje tylko pliki, więc są rysowane do katalogu tymczasowego."""
    folder = os.path.join(tempfile.gettempdir(), "pymms_theme")
    os.makedirs(folder, exist_ok=True)

    def draw(name, color, painter_fn, w=10, h=10):
        path = os.path.join(folder, name)
        pm = QPixmap(w * 2, h * 2)
        pm.fill(Qt.GlobalColor.transparent)
        p = QPainter(pm)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        p.scale(2, 2)
        painter_fn(p, QColor(color))
        p.end()
        pm.save(path)
        return path.replace("\\", "/")

    def arrow(up):
        def fn(p, color):
            pen = QPen(color, 1.6, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap, Qt.PenJoinStyle.RoundJoin)
            p.setPen(pen)
            if up:
                p.drawPolyline(QPolygonF([QPointF(2, 6.5), QPointF(5, 3.5), QPointF(8, 6.5)]))
            else:
                p.drawPolyline(QPolygonF([QPointF(2, 3.5), QPointF(5, 6.5), QPointF(8, 3.5)]))
        return fn

    def check(p, color):
        pen = QPen(color, 1.8, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap, Qt.PenJoinStyle.RoundJoin)
        p.setPen(pen)
        p.drawPolyline(QPolygonF([QPointF(2.5, 5.2), QPointF(4.4, 7.2), QPointF(7.8, 3)]))

    up, down = draw("up.png", C_MUTED, arrow(True)), draw("down.png", C_MUTED, arrow(False))
    up_dis, down_dis = draw("up_dis.png", C_DIM, arrow(True)), draw("down_dis.png", C_DIM, arrow(False))
    tick = draw("check.png", ACCENT_INK, check)
    return f"""
QSpinBox::up-arrow {{ image: url({up}); width: 10px; height: 10px; }}
QSpinBox::down-arrow {{ image: url({down}); width: 10px; height: 10px; }}
QSpinBox::up-arrow:disabled {{ image: url({up_dis}); }}
QSpinBox::down-arrow:disabled {{ image: url({down_dis}); }}
QComboBox::down-arrow {{ image: url({down}); width: 10px; height: 10px; margin-right: 6px; }}
QComboBox::down-arrow:disabled {{ image: url({down_dis}); }}
QCheckBox::indicator:checked {{ image: url({tick}); }}
"""


_ICON_FONT = None

def icon(glyph, color=C_TEXT, size=16):
    """Ikona z fontu Segoe Fluent Icons / Segoe MDL2 Assets (Windows 10/11)."""
    global _ICON_FONT
    if _ICON_FONT is None:
        families = QFontDatabase.families()
        _ICON_FONT = next((f for f in ("Segoe Fluent Icons", "Segoe MDL2 Assets") if f in families), "")
    if not _ICON_FONT:
        return QIcon()
    result = QIcon()
    for scale in (1, 2):
        pm = QPixmap(size * scale, size * scale)
        pm.setDevicePixelRatio(scale)
        pm.fill(Qt.GlobalColor.transparent)
        p = QPainter(pm)
        p.setRenderHint(QPainter.RenderHint.TextAntialiasing)
        f = QFont(_ICON_FONT)
        f.setPixelSize(int(size * 0.8))
        p.setFont(f)
        p.setPen(QColor(color))
        p.drawText(QRect(0, 0, size, size), Qt.AlignmentFlag.AlignCenter, glyph)
        p.end()
        result.addPixmap(pm)
    return result

G_REFRESH, G_PLAY, G_STOP, G_DELETE, G_SAVE = "", "", "", "", ""
G_SEARCH, G_DOWN, G_PLUG, G_SYNC, G_LIST, G_PAUSE = "", "", "", "", "", ""
G_CHART, G_RESTORE = "\ue9d2", "\ue777"


def mono_family():
    families = QFontDatabase.families()
    return next((f for f in ("Cascadia Mono", "Cascadia Code", "JetBrains Mono", "Consolas") if f in families), "Courier New")


def mono_font(pt=9.5):
    f = QFont(mono_family())
    f.setPointSizeF(pt)
    f.setStyleHint(QFont.StyleHint.Monospace)
    return f


def repolish(widget):
    widget.style().unpolish(widget)
    widget.style().polish(widget)


# --- Własna obsługa Modbus master (TCP / RTU) ---
# Zamiast pymodbus - pełna kontrola nad ramkami, dzięki czemu okno logów pokazuje dokładną transmisję
# (także niepełne ramki, błędy CRC i spóźnione odpowiedzi). Interfejs zgodny z wcześniej używanym pymodbus.

FUNCTION_NAMES = {
    0x01: "Read Coils",
    0x02: "Read Discrete Inputs",
    0x03: "Read Holding Registers",
    0x04: "Read Input Registers",
    0x05: "Write Single Coil",
    0x06: "Write Single Register",
    0x0F: "Write Multiple Coils",
    0x10: "Write Multiple Registers",
    0x16: "Mask Write Register",
    0x17: "Read/Write Multiple Registers",
}

EXCEPTION_NAMES = {
    0x01: "Illegal Function",
    0x02: "Illegal Data Address",
    0x03: "Illegal Data Value",
    0x04: "Slave Device Failure",
    0x05: "Acknowledge",
    0x06: "Slave Device Busy",
    0x08: "Memory Parity Error",
    0x0A: "Gateway Path Unavailable",
    0x0B: "Gateway Target Device Failed to Respond",
}


class ModbusException(Exception):
    """Błąd komunikacji Modbus (timeout, CRC, utrata połączenia, niepoprawna odpowiedź)."""


class ModbusResponse:
    """Odpowiedź slave'a - pola zgodne z obiektami odpowiedzi pymodbus używanymi w aplikacji."""
    def __init__(self, function_code, registers=None, bits=None, exception_code=None):
        self.function_code = function_code
        self.registers = registers or []
        self.bits = bits or []
        self.exception_code = exception_code

    def isError(self):
        return self.exception_code is not None

    def __str__(self):
        if self.isError():
            return (f"Exception response FC{self.function_code:02X} -> code {self.exception_code:02X} "
                    f"({EXCEPTION_NAMES.get(self.exception_code, 'Unknown')})")
        return f"Response FC{self.function_code:02X} ({FUNCTION_NAMES.get(self.function_code, 'Unknown')})"


def to_hex(data):
    return " ".join(f"{b:02X}" for b in data)

def pack_bits(bits):
    out = bytearray((len(bits) + 7) // 8)
    for i, bit in enumerate(bits):
        if bit:
            out[i // 8] |= 1 << (i % 8)
    return bytes(out)

def unpack_bits(data, count):
    return [bool((data[i // 8] >> (i % 8)) & 1) for i in range(count)]

def _make_crc_table():
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
        table.append(crc)
    return table

_CRC_TABLE = _make_crc_table()

def crc16(data):
    crc = 0xFFFF
    for b in data:
        crc = (crc >> 8) ^ _CRC_TABLE[(crc ^ b) & 0xFF]
    return crc

def crc_ok(frame):
    return len(frame) >= 4 and crc16(frame[:-2]) == (frame[-2] | (frame[-1] << 8))

def rtu_response_length(buf):
    """Oczekiwana długość ramki odpowiedzi RTU (None - nieznana lub za mało danych)."""
    if len(buf) < 2:
        return None
    fc = buf[1]
    if fc & 0x80:
        return 5
    if fc in (0x01, 0x02, 0x03, 0x04, 0x17):
        return 5 + buf[2] if len(buf) >= 3 else None
    if fc in (0x05, 0x06, 0x0F, 0x10):
        return 8
    if fc == 0x16:
        return 10
    return None

def _short_list(values, limit=16):
    text = ", ".join(str(v) for v in values[:limit])
    return f"[{text}{', ...' if len(values) > limit else ''}]"

def describe_pdu(pdu, is_request):
    """Czytelny opis PDU do okna logów."""
    if not pdu:
        return "Empty PDU"
    fc = pdu[0]
    if fc & 0x80:
        code = pdu[1] if len(pdu) > 1 else 0
        return (f"EXCEPTION FC{fc & 0x7F:02X} ({FUNCTION_NAMES.get(fc & 0x7F, 'Unknown')}) -> "
                f"code {code:02X} {EXCEPTION_NAMES.get(code, 'Unknown')}")
    text = f"FC{fc:02X} {FUNCTION_NAMES.get(fc, 'Unknown Function')}"
    try:
        if is_request:
            if fc in (0x01, 0x02, 0x03, 0x04):
                address, count = struct.unpack(">HH", pdu[1:5])
                text += f" | addr={address} (0x{address:04X}) qty={count}"
            elif fc == 0x05:
                address, value = struct.unpack(">HH", pdu[1:5])
                state = {0xFF00: "ON", 0x0000: "OFF"}.get(value, f"INVALID 0x{value:04X}")
                text += f" | addr={address} (0x{address:04X}) value={state}"
            elif fc == 0x06:
                address, value = struct.unpack(">HH", pdu[1:5])
                text += f" | addr={address} (0x{address:04X}) value={value} (0x{value:04X})"
            elif fc == 0x0F:
                address, count, byte_count = struct.unpack(">HHB", pdu[1:6])
                bits = [int(b) for b in unpack_bits(pdu[6:6 + byte_count], count)]
                text += f" | addr={address} (0x{address:04X}) qty={count} values={_short_list(bits, 32)}"
            elif fc == 0x10:
                address, count, byte_count = struct.unpack(">HHB", pdu[1:6])
                values = list(struct.unpack(f">{byte_count // 2}H", pdu[6:6 + byte_count]))
                text += f" | addr={address} (0x{address:04X}) qty={count} values={_short_list(values)}"
        else:
            if fc in (0x01, 0x02):
                byte_count = pdu[1]
                bits = [int(b) for b in unpack_bits(pdu[2:2 + byte_count], byte_count * 8)]
                text += f" | {byte_count} bytes, bits={_short_list(bits, 32)}"
            elif fc in (0x03, 0x04, 0x17):
                byte_count = pdu[1]
                values = list(struct.unpack(f">{byte_count // 2}H", pdu[2:2 + byte_count]))
                text += f" | {byte_count} bytes, values={_short_list(values)}"
            elif fc in (0x05, 0x06, 0x0F, 0x10):
                address, value = struct.unpack(">HH", pdu[1:5])
                label = "qty" if fc in (0x0F, 0x10) else "value"
                text += f" | addr={address} (0x{address:04X}) {label}={value} (echo)"
    except (struct.error, IndexError):
        text += " | (malformed)"
    return text


class ModbusMasterBase:
    """Wspólna część klientów TCP i RTU: budowa zapytań i walidacja odpowiedzi."""
    def __init__(self, timeout=1.0, log=None):
        self.timeout = timeout
        self.log = log or (lambda kind, channel, data=b"", text="": None) # log(kind, channel, data, text)
        self.connected = False

    def _request(self, unit, pdu):
        response = self._transact(unit, pdu)
        fc = pdu[0]
        if response[0] == fc | 0x80 and len(response) >= 2:
            return ModbusResponse(fc, exception_code=response[1])
        if response[0] != fc:
            raise ModbusException(f"Unexpected function code 0x{response[0]:02X} in response (expected 0x{fc:02X})")
        return response

    def _read_bits(self, fc, address, count, unit):
        response = self._request(unit, struct.pack(">BHH", fc, address, count))
        if isinstance(response, ModbusResponse):
            return response
        if len(response) < 2 or response[1] != (count + 7) // 8 or len(response) != 2 + response[1]:
            raise ModbusException("Malformed response (byte count does not match quantity)")
        return ModbusResponse(fc, bits=unpack_bits(response[2:], count))

    def _read_registers(self, fc, address, count, unit):
        response = self._request(unit, struct.pack(">BHH", fc, address, count))
        if isinstance(response, ModbusResponse):
            return response
        if len(response) < 2 or response[1] != 2 * count or len(response) != 2 + response[1]:
            raise ModbusException("Malformed response (byte count does not match quantity)")
        return ModbusResponse(fc, registers=list(struct.unpack(f">{count}H", response[2:])))

    def _write(self, pdu, unit):
        response = self._request(unit, pdu)
        if isinstance(response, ModbusResponse):
            return response
        if response[:5] != pdu[:5]:
            raise ModbusException("Write response does not echo the request")
        return ModbusResponse(pdu[0])

    # --- Interfejs zgodny z pymodbus ---

    def read_coils(self, address, count=1, slave=1):
        return self._read_bits(0x01, address, count, slave)

    def read_discrete_inputs(self, address, count=1, slave=1):
        return self._read_bits(0x02, address, count, slave)

    def read_holding_registers(self, address, count=1, slave=1):
        return self._read_registers(0x03, address, count, slave)

    def read_input_registers(self, address, count=1, slave=1):
        return self._read_registers(0x04, address, count, slave)

    def write_coil(self, address, value, slave=1):
        return self._write(struct.pack(">BHH", 0x05, address, 0xFF00 if value else 0x0000), slave)

    def write_register(self, address, value, slave=1):
        return self._write(struct.pack(">BHH", 0x06, address, value & 0xFFFF), slave)

    def write_registers(self, address, values, slave=1):
        values = [v & 0xFFFF for v in values]
        pdu = struct.pack(f">BHHB{len(values)}H", 0x10, address, len(values), 2 * len(values), *values)
        return self._write(pdu, slave)


class ModbusTcpMaster(ModbusMasterBase):
    def __init__(self, host, port=502, timeout=1.0, log=None):
        super().__init__(timeout, log)
        self.host = host
        self.port = port
        self.sock = None
        self.next_tid = 1
        self.channel = f"{host}:{port}"

    def connect(self):
        try:
            self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError as e:
            self.log("ERROR", self.channel, b"", f"Cannot connect: {e}")
            self.sock = None
            return False
        self.connected = True
        self.log("INFO", self.channel, b"", "Connected")
        return True

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None
            if self.connected:
                self.log("INFO", self.channel, b"", "Disconnected")
        self.connected = False

    def _connection_lost(self, reason):
        self.log("ERROR", self.channel, b"", f"{reason} - connection closed")
        self.close()
        raise ModbusException(f"{reason} - CLOSING CONNECTION")

    def _drain(self):
        """Usuwa dane, które przyszły bez zapytania (np. spóźniona odpowiedź po timeoucie)."""
        stale = b""
        self.sock.setblocking(False)
        try:
            while True:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                stale += chunk
        except (BlockingIOError, InterruptedError):
            pass
        except OSError as e:
            self.sock.setblocking(True)
            self._connection_lost(f"Connection error: {e}")
        self.sock.setblocking(True)
        if stale:
            self.log("ERROR", self.channel, stale, "Unexpected data received before request - discarded")

    def _transact(self, unit, pdu):
        if not self.connected:
            raise ModbusException("Not connected")
        self._drain()
        tid = self.next_tid
        self.next_tid = self.next_tid % 0xFFFF + 1
        adu = struct.pack(">HHHB", tid, 0, len(pdu) + 1, unit) + pdu
        try:
            self.sock.sendall(adu)
        except OSError as e:
            self._connection_lost(f"Send error: {e}")
        self.log("TX", self.channel, adu, f"TID={tid} PID=0 LEN={len(pdu) + 1} UID={unit} | {describe_pdu(pdu, True)}")

        deadline = time.perf_counter() + self.timeout
        buf = b""
        while True:
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                if buf:
                    self.log("ERROR", self.channel, buf, "Incomplete response")
                self.log("ERROR", self.channel, b"", f"No response within {self.timeout * 1000:.0f} ms (timeout)")
                raise ModbusException(f"No response received within {self.timeout * 1000:.0f} ms")
            self.sock.settimeout(remaining)
            try:
                chunk = self.sock.recv(4096)
            except socket.timeout:
                continue
            except OSError as e:
                self._connection_lost(f"Connection error: {e}")
            if not chunk:
                self._connection_lost("Connection closed by remote device")
            buf += chunk
            while len(buf) >= 7:
                r_tid, r_pid, length, r_uid = struct.unpack(">HHHB", buf[:7])
                if not 2 <= length <= 254:
                    self.log("ERROR", self.channel, buf, f"Invalid MBAP length field ({length}) - discarded")
                    raise ModbusException(f"Invalid MBAP header (length {length})")
                total = 6 + length
                if len(buf) < total:
                    break
                frame, buf = buf[:total], buf[total:]
                response = frame[7:]
                self.log("RX", self.channel, frame,
                         f"TID={r_tid} PID={r_pid} LEN={length} UID={r_uid} | {describe_pdu(response, False)}")
                if r_tid != tid:
                    self.log("ERROR", self.channel, b"", f"Unexpected transaction ID {r_tid} (expected {tid}) - discarded")
                    continue
                if r_uid != unit:
                    raise ModbusException(f"Response from unit ID {r_uid}, expected {unit}")
                if buf:
                    self.log("ERROR", self.channel, buf, "Unexpected bytes after response - discarded")
                return response


class ModbusRtuMaster(ModbusMasterBase):
    def __init__(self, port, baudrate=9600, parity="N", bytesize=8, stopbits=1, timeout=1.0, log=None):
        super().__init__(timeout, log)
        self.port = port
        self.baudrate = baudrate
        self.parity = parity
        self.bytesize = bytesize
        self.stopbits = stopbits
        self.ser = None
        self.channel = port
        # Czas 3.5 znaku (dla > 19200 bps stała 1.75 ms wg specyfikacji)
        bits_per_char = 1 + bytesize + (0 if parity == "N" else 1) + stopbits
        self.t35 = 3.5 * bits_per_char / baudrate if baudrate <= 19200 else 0.00175
        # Cisza kończąca ramkę o nieznanej długości - z zapasem na opóźnienia konwerterów USB-RS485
        self.silence = max(self.t35, 0.03)
        self.last_activity = 0.0

    def connect(self):
        try:
            self.ser = serial.Serial(port=self.port, baudrate=self.baudrate, parity=self.parity,
                                     bytesize=self.bytesize, stopbits=self.stopbits, timeout=0.005)
        except (serial.SerialException, ValueError, OSError) as e:
            self.log("ERROR", self.channel, b"", f"Cannot open port: {e}")
            self.ser = None
            return False
        self.connected = True
        self.log("INFO", self.channel, b"", f"Port opened: {self.baudrate} {self.bytesize}{self.parity}{self.stopbits} "
                 f"(t3.5 = {self.t35 * 1000:.2f} ms)")
        return True

    def close(self):
        if self.ser:
            try:
                self.ser.close()
            except (serial.SerialException, OSError):
                pass
            self.ser = None
            if self.connected:
                self.log("INFO", self.channel, b"", "Port closed")
        self.connected = False

    @staticmethod
    def _find_frame(buf):
        """Szuka poprawnej (z CRC) ramki odpowiedzi od przesunięcia > 0."""
        for i in range(1, len(buf) - 3):
            n = rtu_response_length(buf[i:])
            if n is not None and len(buf) - i >= n and crc_ok(buf[i:i + n]):
                return i
        return None

    def _port_error(self, e):
        self.log("ERROR", self.channel, b"", f"Serial port error: {e}")
        self.close()
        raise ModbusException(f"Serial port error: {e} - CLOSING CONNECTION")

    def _transact(self, unit, pdu):
        if not self.connected:
            raise ModbusException("Not connected")
        adu = bytes([unit]) + pdu
        adu += crc16(adu).to_bytes(2, "little")
        try:
            stale = self.ser.read(self.ser.in_waiting) if self.ser.in_waiting else b""
            if stale:
                self.log("ERROR", self.channel, stale, "Unexpected bytes received before request - discarded")
            gap = self.t35 - (time.perf_counter() - self.last_activity) # przerwa między ramkami >= 3.5 znaku
            if gap > 0:
                time.sleep(gap)
            self.ser.write(adu)
            self.ser.flush()
        except (serial.SerialException, OSError) as e:
            self._port_error(e)
        self.last_activity = time.perf_counter()
        self.log("TX", self.channel, adu, f"UID={unit} | {describe_pdu(pdu, True)}")

        deadline = time.perf_counter() + self.timeout
        buf = bytearray()
        last_rx = None
        while True:
            try:
                waiting = self.ser.in_waiting
                chunk = self.ser.read(waiting if waiting else 1)
            except (serial.SerialException, OSError) as e:
                self._port_error(e)
            now = time.perf_counter()
            if chunk:
                buf += chunk
                last_rx = now
                self.last_activity = now
            expected = rtu_response_length(buf)
            if expected is not None and len(buf) >= expected and crc_ok(buf[:expected]):
                frame, extra = bytes(buf[:expected]), bytes(buf[expected:])
                break
            if (expected is None and len(buf) >= 2) or (expected is not None and len(buf) >= expected):
                idx = self._find_frame(buf) # zakłócenia przed odpowiedzią - resynchronizacja
                if idx is not None:
                    self.log("ERROR", self.channel, bytes(buf[:idx]), "Discarded bytes before response")
                    del buf[:idx]
                    continue
                if now - last_rx >= self.silence: # nic więcej nie nadchodzi - oceniamy to, co jest
                    if expected is None: # nieznana długość - ramkę kończy cisza na linii
                        frame, extra = bytes(buf), b""
                    else:
                        frame, extra = bytes(buf[:expected]), bytes(buf[expected:])
                    break
            receiving = last_rx is not None and now - last_rx < self.silence
            if now >= deadline and not receiving: # rozpoczętą ramkę pozwalamy dokończyć
                if buf:
                    self.log("ERROR", self.channel, bytes(buf), "Incomplete response")
                self.log("ERROR", self.channel, b"", f"No response within {self.timeout * 1000:.0f} ms (timeout)")
                raise ModbusException(f"No response received within {self.timeout * 1000:.0f} ms")

        if not crc_ok(frame):
            self.log("ERROR", self.channel, frame, "Invalid CRC - response discarded")
            raise ModbusException("CRC error in response")
        response = frame[1:-2]
        self.log("RX", self.channel, frame, f"UID={frame[0]} | {describe_pdu(response, False)}")
        if extra:
            self.log("ERROR", self.channel, extra, "Unexpected bytes after response - discarded")
        if frame[0] != unit:
            raise ModbusException(f"Response from slave ID {frame[0]}, expected {unit}")
        return response


class LogPanel(QWidget):
    """Panel logu transmisji (surowe ramki HEX + dekodowanie), zadokowany pod tabelą wyników."""
    COLORS = {"RX": C_BLUE, "TX": C_GREEN, "INFO": C_MUTED, "ERROR": STATUS_RED}

    def __init__(self, parent=None):
        super().__init__(parent)
        self.entry_count = 0
        self.dropped_count = 0

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        bar = QFrame(objectName="panelbar")
        bl = QHBoxLayout(bar)
        bl.setContentsMargins(14, 6, 14, 6)
        bl.setSpacing(6)
        bl.addWidget(QLabel("TRANSMISSION LOG", objectName="sectionTitle"))
        bl.addSpacing(10)
        self.show_tx_cb = self._pill("TX", self.COLORS["TX"], "Requests sent by pymms")
        self.show_rx_cb = self._pill("RX", self.COLORS["RX"], "Responses received from the device")
        self.show_info_cb = self._pill("Info", self.COLORS["INFO"], "Connection events")
        self.show_errors_cb = self._pill("Errors", self.COLORS["ERROR"], "Timeouts, CRC errors, discarded frames")
        for b in (self.show_tx_cb, self.show_rx_cb, self.show_info_cb, self.show_errors_cb):
            bl.addWidget(b)
        bl.addSpacing(10)
        self.decode_cb = QPushButton("Decode", checkable=True, checked=True, toolTip="Decode frames (function, address, values)")
        bl.addWidget(self.decode_cb)
        self.autoscroll_cb = QPushButton("Follow", checkable=True, checked=True, icon=icon(G_DOWN),
                                         toolTip="Scroll to the newest entries automatically")
        bl.addWidget(self.autoscroll_cb)
        self.pause_cb = QPushButton("Pause", checkable=True, icon=icon(G_PAUSE), toolTip="Stop adding new entries")
        bl.addWidget(self.pause_cb)
        bl.addStretch(1)
        self.info_label = QLabel(objectName="hint")
        bl.addWidget(self.info_label)
        bl.addSpacing(6)
        bl.addWidget(QLabel("Max lines", objectName="fieldLabel"))
        self.max_lines_sb = QSpinBox()
        self.max_lines_sb.setRange(100, 1000000)
        self.max_lines_sb.setSingleStep(1000)
        self.max_lines_sb.setValue(20000)
        self.max_lines_sb.valueChanged.connect(lambda v: self.text.setMaximumBlockCount(v))
        bl.addWidget(self.max_lines_sb)
        self.clear_btn = QPushButton("Clear", icon=icon(G_DELETE))
        self.clear_btn.clicked.connect(self.clear)
        bl.addWidget(self.clear_btn)
        self.save_btn = QPushButton("Save", icon=icon(G_SAVE), toolTip="Save the log to a text file")
        self.save_btn.clicked.connect(self.save)
        bl.addWidget(self.save_btn)
        layout.addWidget(bar)

        self.text = QPlainTextEdit(readOnly=True)
        self.text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self.text.setMaximumBlockCount(self.max_lines_sb.value())
        self.text.setFont(mono_font(9))
        self.text.setPlaceholderText("Frames sent and received by pymms appear here")
        layout.addWidget(self.text)
        self.update_info()

        self.formats = {}
        for kind, color in self.COLORS.items():
            fmt = QTextCharFormat()
            fmt.setForeground(QColor(color))
            self.formats[kind] = fmt
        self.decode_format = QTextCharFormat()
        self.decode_format.setForeground(QColor(C_MUTED))
        self.time_format = QTextCharFormat()
        self.time_format.setForeground(QColor(C_DIM))

    @staticmethod
    def _pill(text, color, tooltip):
        b = QPushButton(text, checkable=True, checked=True, toolTip=tooltip)
        b.setStyleSheet(pill_style(color))
        return b

    def update_info(self):
        text = f"{self.entry_count:,} entries".replace(",", " ")
        if self.dropped_count:
            text += f"  ·  {self.dropped_count} skipped while paused"
        self.info_label.setText(text)

    def add_entry(self, entry):
        timestamp, kind, channel, data, text = entry
        if self.pause_cb.isChecked():
            self.dropped_count += 1
            self.update_info()
            return
        visible = {"RX": self.show_rx_cb, "TX": self.show_tx_cb,
                   "INFO": self.show_info_cb, "ERROR": self.show_errors_cb}[kind].isChecked()
        if not visible:
            return

        cursor = QTextCursor(self.text.document())
        cursor.movePosition(QTextCursor.MoveOperation.End)
        if not self.text.document().isEmpty():
            cursor.insertBlock()
        cursor.insertText(f"{timestamp:%H:%M:%S}.{timestamp.microsecond // 1000:03d}  ", self.time_format)
        line = f"{kind:<5} [{channel}]"
        if data:
            line += f"  ({len(data):>3} B)  {to_hex(data)}"
            cursor.insertText(line, self.formats[kind])
            if self.decode_cb.isChecked() and text:
                cursor.insertBlock()
                cursor.insertText(f"{'':<20}-> {text}", self.decode_format) # dekodowanie w osobnej, wciętej linii
        else:
            cursor.insertText(f"{line}  {text}", self.formats[kind])
        if self.autoscroll_cb.isChecked():
            bar = self.text.verticalScrollBar()
            bar.setValue(bar.maximum())
        self.entry_count += 1
        self.update_info()

    def clear(self):
        self.text.clear()
        self.entry_count = 0
        self.dropped_count = 0
        self.update_info()

    def save(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Save Transmission Log", f"pymms_log_{datetime.now():%Y%m%d_%H%M%S}.txt",
            "Text files (*.txt *.log);;All files (*)")
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.text.toPlainText())
        except OSError as e:
            QMessageBox.critical(self, "Save Error", f"Cannot save log file: {e}")


# --- Funkcje pomocnicze do konwersji danych ---
# Wszystkie konwersje zakładają kolejność bajtów Big-Endian, typową dla Modbus.

# Helper for applying byte swap for reading (before converting to int/float)
def _apply_byte_swap_for_reading(data_bytes, swap_type, num_bytes):
    if num_bytes == 2: # 16-bit, no byte swap within word
        return data_bytes
    elif num_bytes == 4: # 32-bit (2 words)
        w0_b0 = data_bytes[0:1]
        w0_b1 = data_bytes[1:2]
        w1_b0 = data_bytes[2:3]
        w1_b1 = data_bytes[3:4]

        if swap_type == "ABCD (No Swap)": # B0 B1 B2 B3
            return data_bytes
        elif swap_type == "CDAB (Word Swap)": # B2 B3 B0 B1
            return data_bytes[2:4] + data_bytes[0:2]
        elif swap_type == "BADC (Byte Swap within Words)": # B1 B0 B3 B2
            return w0_b1 + w0_b0 + w1_b1 + w1_b0
        elif swap_type == "DCBA (Full Byte Reverse)": # B3 B2 B1 B0
            return data_bytes[::-1] # Reverse all bytes
        else:
            return data_bytes # Default to no swap if unknown or invalid
    elif num_bytes == 8: # 64-bit (4 words)
        # Original words: W0 (B0B1), W1 (B2B3), W2 (B4B5), W3 (B6B7)
        w0 = data_bytes[0:2]
        w1 = data_bytes[2:4]
        w2 = data_bytes[4:6]
        w3 = data_bytes[6:8]

        if swap_type == "ABCD (No Swap)": # W0 W1 W2 W3
            return data_bytes
        elif swap_type == "CDAB (Word Swap)": # W2 W3 W0 W1 (for 32-bit, this was CDAB, now for 64-bit it's W2W3W0W1)
            return w2 + w3 + w0 + w1
        elif swap_type == "BADC (Byte Swap within Words)": # B1B0 B3B2 B5B4 B7B6
            # Reverse bytes within each 16-bit word
            return w0[::-1] + w1[::-1] + w2[::-1] + w3[::-1]
        elif swap_type == "DCBA (Full Byte Reverse)": # W3 B2 B1 B0
            return data_bytes[::-1] # Reverse all bytes
        else:
            return data_bytes
    return data_bytes # Fallback

# Helper for applying byte swap for writing (after converting to bytes)
def _apply_byte_swap_for_writing(data_bytes, swap_type, num_bytes):
    # data_bytes to kanoniczna reprezentacja big-endian (np. z pack('>f', value) lub to_bytes)
    # Ta funkcja przekształca je do formatu Modbus zgodnie z wybranym typem zamiany bajtów
    if num_bytes == 2:
        return data_bytes
    elif num_bytes == 4: # 32-bit (2 words)
        # data_bytes to b0 b1 b2 b3
        w0 = data_bytes[0:2] # b0 b1
        w1 = data_bytes[2:4] # b2 b3

        if swap_type == "ABCD (No Swap)": # B0 B1 B2 B3
            return data_bytes
        elif swap_type == "CDAB (Word Swap)": # B2 B3 B0 B1
            return w1 + w0
        elif swap_type == "BADC (Byte Swap within Words)": # B1 B0 B3 B2
            return w0[::-1] + w1[::-1]
        elif swap_type == "DCBA (Full Byte Reverse)": # B3 B2 B1 B0
            return data_bytes[::-1]
        else:
            return data_bytes
    elif num_bytes == 8: # 64-bit (4 words)
        # data_bytes to b0 b1 b2 b3 b4 b5 b6 b7
        w0 = data_bytes[0:2] # b0 b1
        w1 = data_bytes[2:4] # b2 b3
        w2 = data_bytes[4:6] # b4 b5
        w3 = data_bytes[6:8] # b6 b7

        if swap_type == "ABCD (No Swap)": # W0 W1 W2 W3
            return data_bytes
        elif swap_type == "CDAB (Word Swap)": # W2 W3 W0 W1
            return w2 + w3 + w0 + w1
        elif swap_type == "BADC (Byte Swap within Words)": # B1B0 B3B2 B5B4 B7B6
            return w0[::-1] + w1[::-1] + w2[::-1] + w3[::-1]
        elif swap_type == "DCBA (Full Byte Reverse)": # B7 B6 B5 B4 B3 B2 B1 B0
            return data_bytes[::-1]
        else:
            return data_bytes
    return data_bytes # Fallback


# --- Updated/New Conversion Functions ---

def bytes_to_int16(data_bytes):
    if len(data_bytes) != 2: return None
    return int.from_bytes(data_bytes, byteorder='big', signed=True)

def bytes_to_uint16(data_bytes):
    if len(data_bytes) != 2: return None
    return int.from_bytes(data_bytes, byteorder='big', signed=False) # Ensure signed=False

def bytes_to_int32(data_bytes, swap_type="ABCD (No Swap)"):
    if len(data_bytes) != 4: return None
    processed_bytes = _apply_byte_swap_for_reading(data_bytes, swap_type, 4)
    return int.from_bytes(processed_bytes, byteorder='big', signed=True)

def bytes_to_uint32(data_bytes, swap_type="ABCD (No Swap)"):
    if len(data_bytes) != 4: return None
    processed_bytes = _apply_byte_swap_for_reading(data_bytes, swap_type, 4)
    return int.from_bytes(processed_bytes, byteorder='big', signed=False) # Ensure signed=False

def bytes_to_float32(data_bytes, swap_type="ABCD (No Swap)"):
    if len(data_bytes) != 4: return None
    processed_bytes = _apply_byte_swap_for_reading(data_bytes, swap_type, 4)
    return unpack('>f', processed_bytes)[0]

def int16_to_bytes(value):
    return value.to_bytes(2, byteorder='big', signed=True)

def uint16_to_bytes(value):
    return value.to_bytes(2, byteorder='big', signed=False)

def int32_to_bytes(value, swap_type="ABCD (No Swap)"):
    b = value.to_bytes(4, byteorder='big', signed=True)
    return _apply_byte_swap_for_writing(b, swap_type, 4)

def uint32_to_bytes(value, swap_type="ABCD (No Swap)"):
    b = value.to_bytes(4, byteorder='big', signed=False)
    return _apply_byte_swap_for_writing(b, swap_type, 4)

def float32_to_bytes(value, swap_type="ABCD (No Swap)"):
    b = pack('>f', value)
    return _apply_byte_swap_for_writing(b, swap_type, 4)

# New 64-bit functions
def bytes_to_int64(data_bytes, swap_type="ABCD (No Swap)"):
    if len(data_bytes) != 8: return None
    processed_bytes = _apply_byte_swap_for_reading(data_bytes, swap_type, 8)
    return int.from_bytes(processed_bytes, byteorder='big', signed=True)

def bytes_to_uint64(data_bytes, swap_type="ABCD (No Swap)"):
    if len(data_bytes) != 8: return None
    processed_bytes = _apply_byte_swap_for_reading(data_bytes, swap_type, 8)
    return int.from_bytes(processed_bytes, byteorder='big', signed=False)

def bytes_to_float64(data_bytes, swap_type="ABCD (No Swap)"):
    if len(data_bytes) != 8: return None
    processed_bytes = _apply_byte_swap_for_reading(data_bytes, swap_type, 8)
    return unpack('>d', processed_bytes)[0] # '>d' for double

def int64_to_bytes(value, swap_type="ABCD (No Swap)"):
    b = value.to_bytes(8, byteorder='big', signed=True)
    return _apply_byte_swap_for_writing(b, swap_type, 8)

def uint64_to_bytes(value, swap_type="ABCD (No Swap)"):
    b = value.to_bytes(8, byteorder='big', signed=False)
    return _apply_byte_swap_for_writing(b, swap_type, 8)

def float64_to_bytes(value, swap_type="ABCD (No Swap)"):
    b = pack('>d', value)
    return _apply_byte_swap_for_writing(b, swap_type, 8)


def convert_bytes_to_modbus_registers_for_writing(data_bytes): # Usunięto swap_type z argumentów
    """
    Konwertuje bajty na listę 16-bitowych rejestrów Modbus.
    To jest funkcja używana do przygotowania wartości do zapisu.
    Zakłada, że `data_bytes` są już w odpowiedniej kolejności po zastosowaniu _apply_byte_swap_for_writing.
    """
    num_bytes = len(data_bytes)
    if num_bytes % 2 != 0:
        raise ValueError("Liczba bajtów musi być parzysta.")

    # processed_bytes = _apply_byte_swap_for_writing(data_bytes, swap_type, num_bytes) # USUNIĘTO to wywołanie
    processed_bytes = data_bytes # Bezpośrednie użycie data_bytes, które są już prawidłowo ułożone

    registers = []
    for i in range(0, num_bytes, 2):
        registers.append(int.from_bytes(processed_bytes[i:i+2], byteorder='big', signed=False))
    return registers


class ScannerWindow(QWidget):
    """Skaner Modbus: wyszukiwanie urządzeń (zakres Slave ID) lub rejestrów (zakres adresów w jednym ID).
    Zapytania wysyłane są krok po kroku z QTimer, więc GUI pozostaje responsywne, a Stop działa od razu."""
    FUNCTIONS = [ # (etykieta, kod funkcji, metoda klienta, nazwa tabeli w pymss)
        ("0x03 (Holding Registers)", 0x03, "read_holding_registers", "holding_register"),
        ("0x04 (Input Registers)", 0x04, "read_input_registers", "input_register"),
        ("0x01 (Coils)", 0x01, "read_coils", "coil"),
        ("0x02 (Discrete Inputs)", 0x02, "read_discrete_inputs", "discrete_input"),
    ]
    COLUMNS = ["Slave ID", "Address", "Address (HEX)", "Status", "Time (ms)", "Value (HEX)", "Value"]
    STATUS_COLORS = {"ok": QColor(143, 206, 0, 34), "exception": QColor(242, 180, 60, 40), "error": QColor(255, 95, 95, 40)}

    def __init__(self, app_window):
        super().__init__() # okno niezależne (bez rodzica) - nie jest trzymane nad oknem głównym
        self.app_window = app_window # główne okno pymms (połączenie, log)
        self.setWindowTitle("pymms - Scanner")
        self.setWindowIcon(app_window.windowIcon())
        self.resize(950, 600)
        self.jobs = []
        self.job_index = 0
        self.results = []
        self.scan_mode = None
        self.scan_function = None
        self.step_timer = QTimer(self)
        self.step_timer.setSingleShot(True)
        self.step_timer.timeout.connect(self.run_next_job)

        layout = QVBoxLayout(self)

        # --- Parametry skanowania ---
        params_group = QGroupBox("Scan Settings")
        params_layout = QVBoxLayout(params_group)

        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Mode:"))
        self.mode_cb = QComboBox()
        self.mode_cb.addItem("Slave ID scan (find devices)", "id")
        self.mode_cb.addItem("Address scan (find registers in one ID)", "address")
        self.mode_cb.currentIndexChanged.connect(self.on_mode_changed)
        row1.addWidget(self.mode_cb)
        row1.addWidget(QLabel("Function:"))
        self.function_cb = QComboBox()
        for label, _, _, _ in self.FUNCTIONS:
            self.function_cb.addItem(label)
        self.function_cb.currentIndexChanged.connect(self.on_function_changed)
        row1.addWidget(self.function_cb)
        row1.addStretch()
        params_layout.addLayout(row1)

        # Tryb ID: zakres Slave ID + stałe rejestry
        self.id_params = QWidget()
        id_layout = QHBoxLayout(self.id_params)
        id_layout.setContentsMargins(0, 0, 0, 0)
        id_layout.addWidget(QLabel("Slave ID from:"))
        self.id_from_sb = self._spin(1, 247, 1)
        id_layout.addWidget(self.id_from_sb)
        id_layout.addWidget(QLabel("to:"))
        self.id_to_sb = self._spin(1, 247, 247)
        id_layout.addWidget(self.id_to_sb)
        id_layout.addSpacing(15)
        id_layout.addWidget(QLabel("Address:"))
        self.id_address_sb = self._spin(0, 65535, 0)
        id_layout.addWidget(self.id_address_sb)
        id_layout.addWidget(QLabel("Quantity:"))
        self.id_count_sb = self._spin(1, 125, 1)
        id_layout.addWidget(self.id_count_sb)
        id_layout.addStretch()
        params_layout.addWidget(self.id_params)

        # Tryb adresów: jedno ID + zakres adresów
        self.address_params = QWidget()
        addr_layout = QHBoxLayout(self.address_params)
        addr_layout.setContentsMargins(0, 0, 0, 0)
        addr_layout.addWidget(QLabel("Slave ID:"))
        self.addr_slave_sb = self._spin(1, 247, 1)
        addr_layout.addWidget(self.addr_slave_sb)
        addr_layout.addSpacing(15)
        addr_layout.addWidget(QLabel("Address from:"))
        self.addr_from_sb = self._spin(0, 65535, 0)
        addr_layout.addWidget(self.addr_from_sb)
        addr_layout.addWidget(QLabel("to:"))
        self.addr_to_sb = self._spin(0, 65535, 99)
        addr_layout.addWidget(self.addr_to_sb)
        addr_layout.addWidget(QLabel("Quantity per request:"))
        self.addr_count_sb = self._spin(1, 125, 1)
        self.addr_count_sb.setToolTip("1 - every address is checked separately (finds exact register map).\n"
                                      "More - faster, but one undefined address fails the whole block.")
        addr_layout.addWidget(self.addr_count_sb)
        addr_layout.addStretch()
        params_layout.addWidget(self.address_params)
        self.address_params.hide()

        row3 = QHBoxLayout()
        row3.addWidget(QLabel("Timeout (ms):"))
        self.timeout_sb = self._spin(20, 30000, 300)
        self.timeout_sb.setToolTip("Response timeout used only by the scanner")
        row3.addWidget(self.timeout_sb)
        row3.addWidget(QLabel("Delay between requests (ms):"))
        self.delay_sb = self._spin(0, 10000, 20)
        row3.addWidget(self.delay_sb)
        self.only_found_cb = QCheckBox("Show only responding")
        self.only_found_cb.setChecked(True)
        self.only_found_cb.toggled.connect(self.rebuild_table)
        row3.addWidget(self.only_found_cb)
        self.log_frames_cb = QCheckBox("Log frames in Transmission Log")
        self.log_frames_cb.setChecked(True)
        row3.addWidget(self.log_frames_cb)
        row3.addStretch()
        params_layout.addLayout(row3)
        layout.addWidget(params_group)

        # --- Sterowanie ---
        controls = QHBoxLayout()
        self.start_btn = QPushButton("Start Scan", objectName="primary", icon=icon(G_PLAY, ACCENT_INK))
        self.start_btn.clicked.connect(self.start_scan)
        controls.addWidget(self.start_btn)
        self.stop_btn = QPushButton("Stop", icon=icon(G_STOP))
        self.stop_btn.clicked.connect(lambda: self.finish_scan("Scan stopped by user."))
        self.stop_btn.setEnabled(False)
        controls.addWidget(self.stop_btn)
        self.progress = QProgressBar()
        self.progress.setTextVisible(True)
        controls.addWidget(self.progress, 1)
        self.export_btn = QPushButton("Export CSV...", icon=icon(G_SAVE))
        self.export_btn.setToolTip("Saves successful readings as a pymss register map (can be imported into pymss)")
        self.export_btn.clicked.connect(self.export_csv)
        controls.addWidget(self.export_btn)
        self.clear_btn = QPushButton("Clear", icon=icon(G_DELETE))
        self.clear_btn.clicked.connect(self.clear_results)
        controls.addWidget(self.clear_btn)
        layout.addLayout(controls)

        # --- Wyniki ---
        self.table = QTableWidget()
        self.table.setColumnCount(len(self.COLUMNS))
        self.table.setHorizontalHeaderLabels(self.COLUMNS)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setShowGrid(False)
        self.table.setFont(mono_font())
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(24)
        self.table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        for col, width in enumerate([70, 90, 100, 300, 75, 150]):
            self.table.setColumnWidth(col, width)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        self.summary_label = QLabel("Ready")
        self.summary_label.setStyleSheet(f"background: {C_PANEL}; border: 1px solid {C_BORDER}; border-radius: 6px; color: {C_MUTED}; padding: 5px 8px;")
        layout.addWidget(self.summary_label)

    @staticmethod
    def _spin(minimum, maximum, value):
        sb = QSpinBox()
        sb.setRange(minimum, maximum)
        sb.setValue(value)
        return sb

    def on_mode_changed(self):
        is_id = self.mode_cb.currentData() == "id"
        self.id_params.setVisible(is_id)
        self.address_params.setVisible(not is_id)

    def on_function_changed(self):
        max_count = 2000 if self.FUNCTIONS[self.function_cb.currentIndex()][1] in (0x01, 0x02) else 125
        self.id_count_sb.setMaximum(max_count)
        self.addr_count_sb.setMaximum(max_count)

    def set_running(self, running):
        self.start_btn.setEnabled(not running)
        self.stop_btn.setEnabled(running)
        for w in (self.mode_cb, self.function_cb, self.id_params, self.address_params, self.timeout_sb):
            w.setEnabled(not running)

    # --- Przebieg skanowania ---

    def start_scan(self):
        client = self.app_window.modbus_client
        if not (client and client.connected):
            QMessageBox.warning(self, "Scanner", "Connect to a Modbus device or network first (main window).")
            return
        self.app_window.stop_cyclic_read() # skaner i odczyt cykliczny nie powinny przeplatać zapytań
        self.scan_mode = self.mode_cb.currentData()
        self.scan_function = self.FUNCTIONS[self.function_cb.currentIndex()]
        if self.scan_mode == "id":
            first, last = sorted((self.id_from_sb.value(), self.id_to_sb.value()))
            address, count = self.id_address_sb.value(), self.id_count_sb.value()
            if address + count > 65536:
                QMessageBox.warning(self, "Scanner", "Address + quantity exceeds 65535.")
                return
            self.jobs = [(slave, address, count) for slave in range(first, last + 1)]
        else:
            first, last = sorted((self.addr_from_sb.value(), self.addr_to_sb.value()))
            step = self.addr_count_sb.value()
            slave = self.addr_slave_sb.value()
            self.jobs = [(slave, a, min(step, last - a + 1)) for a in range(first, last + 1, step)]
        self.results = []
        self.job_index = 0
        self.table.setRowCount(0)
        self.progress.setRange(0, len(self.jobs))
        self.progress.setValue(0)
        self.set_running(True)
        self.summary_label.setText(f"Scanning... 0 / {len(self.jobs)}")
        self.app_window.log("INFO", "Scanner", b"", f"{self.mode_cb.currentText()} started: {len(self.jobs)} request(s), "
                            f"{self.scan_function[0]}, timeout {self.timeout_sb.value()} ms")
        self.step_timer.start(0)

    def run_next_job(self):
        client = self.app_window.modbus_client
        if not (client and client.connected):
            self.finish_scan("Scan aborted: connection lost.")
            return
        slave, address, count = self.jobs[self.job_index]
        result = self.execute(client, slave, address, count)
        self.results.append(result)
        self.add_result_rows(result)
        self.job_index += 1
        self.progress.setValue(self.job_index)
        self.summary_label.setText(f"Scanning... {self.job_index} / {len(self.jobs)} | {self.summary_text()}")
        if result["kind"] == "lost":
            self.app_window.update_connection_buttons()
            self.finish_scan("Scan aborted: connection lost.")
        elif self.job_index >= len(self.jobs):
            self.finish_scan("Scan finished.")
        else:
            self.step_timer.start(self.delay_sb.value())

    def execute(self, client, slave, address, count):
        """Jedno zapytanie skanera - z timeoutem skanera i opcjonalnie bez logowania ramek."""
        saved_timeout, saved_log = client.timeout, client.log
        client.timeout = self.timeout_sb.value() / 1000.0
        if not self.log_frames_cb.isChecked():
            client.log = lambda *args, **kwargs: None
        result = {"slave": slave, "address": address, "count": count, "values": [], "time": None}
        start = time.perf_counter()
        try:
            response = getattr(client, self.scan_function[2])(address=address, count=count, slave=slave)
            result["time"] = (time.perf_counter() - start) * 1000.0
            if response.isError():
                result["kind"] = "exception"
                result["status"] = (f"Exception {response.exception_code:02X} "
                                    f"({EXCEPTION_NAMES.get(response.exception_code, 'Unknown')})")
            else:
                result["kind"] = "ok"
                result["status"] = "OK"
                result["values"] = response.bits if self.scan_function[1] in (0x01, 0x02) else response.registers
        except ModbusException as e:
            text = str(e)
            result["kind"] = "lost" if "CLOSING CONNECTION" in text else ("timeout" if "No response" in text else "error")
            result["status"] = "No response (timeout)" if result["kind"] == "timeout" else text
        finally:
            client.timeout = saved_timeout
            if client.log is not saved_log:
                client.log = saved_log
        return result

    def is_found(self, result):
        # W trybie ID urządzenie istnieje także wtedy, gdy odpowiedziało wyjątkiem (np. 02 dla tego adresu)
        if self.scan_mode == "id":
            return result["kind"] in ("ok", "exception")
        return result["kind"] == "ok"

    def summary_text(self):
        found = [r for r in self.results if self.is_found(r)]
        if self.scan_mode == "id":
            ids = ", ".join(str(r["slave"]) for r in found)
            return f"Responding Slave IDs: {len(found)}" + (f" ({ids})" if ids else "")
        registers = sum(r["count"] for r in found)
        return f"Responding addresses: {registers} of {sum(r['count'] for r in self.results)}"

    def finish_scan(self, message):
        self.step_timer.stop()
        self.set_running(False)
        self.summary_label.setText(f"{message} {self.job_index} / {len(self.jobs)} request(s) | {self.summary_text()}")
        self.app_window.log("INFO", "Scanner", b"", f"{message} {self.summary_text()}")

    # --- Tabela wyników ---

    def add_result_rows(self, result):
        if self.only_found_cb.isChecked() and not self.is_found(result):
            return
        is_bits = self.scan_function[1] in (0x01, 0x02)
        if result["kind"] == "ok" and (self.scan_mode == "address" or result["count"] == 1):
            # wiersz na każdy adres (w trybie ID przy pojedynczym rejestrze to też jeden wiersz)
            for i, value in enumerate(result["values"]):
                self.append_row(result, result["address"] + i, value, is_bits)
        else:
            self.append_row(result, result["address"], None, is_bits, block_values=result["values"])

    def append_row(self, result, address, value, is_bits, block_values=None):
        row = self.table.rowCount()
        self.table.insertRow(row)
        last = result["address"] + result["count"] - 1
        span = result["count"] > 1 and value is None
        if value is not None:
            hex_text = str(int(value)) if is_bits else f"0x{value:04X}"
            dec_text = str(bool(value)) if is_bits else str(value)
        elif block_values:
            hex_text = " ".join(str(int(v)) if is_bits else f"0x{v:04X}" for v in block_values)
            dec_text = " ".join(str(int(v)) if is_bits else str(v) for v in block_values)
        else:
            hex_text = dec_text = ""
        items = [
            str(result["slave"]),
            f"{address}-{last}" if span else str(address),
            f"0x{address:04X}-0x{last:04X}" if span else f"0x{address:04X}",
            result["status"],
            f"{result['time']:.1f}" if result["time"] is not None else "",
            hex_text,
            dec_text,
        ]
        color = self.STATUS_COLORS.get(result["kind"] if result["kind"] != "lost" else "error")
        for col, text in enumerate(items):
            item = QTableWidgetItem(text)
            if color is not None:
                item.setData(Qt.ItemDataRole.BackgroundRole, color)
            self.table.setItem(row, col, item)

    def rebuild_table(self):
        self.table.setRowCount(0)
        if self.scan_function is None:
            return
        for result in self.results:
            self.add_result_rows(result)

    def clear_results(self):
        if self.step_timer.isActive():
            return
        self.results = []
        self.table.setRowCount(0)
        self.progress.setValue(0)
        self.summary_label.setText("Ready")

    def export_csv(self):
        """Zapisuje udane odczyty jako mapę rejestrów pymss (do bezpośredniego importu w symulatorze)."""
        rows = []
        if self.scan_function:
            is_bits = self.scan_function[1] in (0x01, 0x02)
            for r in self.results:
                if r["kind"] != "ok":
                    continue
                for i, value in enumerate(r["values"]):
                    rows.append([r["slave"], self.scan_function[3], r["address"] + i, "BOOL" if is_bits else "UINT16",
                                 "ABCD", str(bool(value)) if is_bits else value, f"scanned {datetime.now():%Y-%m-%d %H:%M}"])
        if not rows:
            QMessageBox.information(self, "Export CSV", "No successful readings to export.")
            return
        path, _ = QFileDialog.getSaveFileName(self, "Export Scan Results", f"scan_{datetime.now():%Y%m%d_%H%M%S}.csv",
                                              "CSV files (*.csv);;All files (*)")
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f, delimiter=";")
                writer.writerow(["slave_id", "table", "address", "data_type", "byte_order", "value", "description"])
                writer.writerows(rows)
        except OSError as e:
            QMessageBox.critical(self, "Export CSV", f"Cannot write file: {e}")
            return
        self.summary_label.setText(f"Exported {len(rows)} reading(s) to {os.path.basename(path)}")

    def closeEvent(self, event):
        if self.step_timer.isActive():
            self.finish_scan("Scan stopped (window closed).")
        super().closeEvent(event)


CHART_COLORS = [ACCENT, "#58a6ff", "#f2b43c", "#ff7ab6", "#a58bff", "#39c5cf", "#ff5f5f", "#e3d74c", "#4cc38a", "#ff9f5a"]
CHART_MAX_POINTS = 200_000   # limit próbek na serię (najstarsze są usuwane)
CHART_WINDOWS = [("30 s", 30), ("1 min", 60), ("5 min", 300), ("15 min", 900), ("1 h", 3600), ("All", None)]


def dot_icon(color, size=10):
    pm = QPixmap(size * 2, size * 2)
    pm.fill(Qt.GlobalColor.transparent)
    p = QPainter(pm)
    p.setRenderHint(QPainter.RenderHint.Antialiasing)
    p.setPen(Qt.PenStyle.NoPen)
    p.setBrush(QColor(color))
    p.drawEllipse(2, 2, size * 2 - 4, size * 2 - 4)
    p.end()
    pm.setDevicePixelRatio(2)
    return QIcon(pm)


def nice_step(span, max_ticks):
    """Krok podziałki 1/2/5 x 10^n, tak aby było co najwyżej max_ticks podziałek."""
    raw = span / max(max_ticks, 1)
    magnitude = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
    for m in (1, 2, 5, 10):
        if raw <= m * magnitude:
            return m * magnitude
    return 10 * magnitude


def format_number(value, step=None):
    if step is not None and step < 1:
        decimals = min(6, max(0, -int(math.floor(math.log10(step)))))
        return f"{value:.{decimals}f}"
    if abs(value) >= 1e7:
        return f"{value:.3g}"
    return f"{value:.0f}" if float(value).is_integer() else f"{value:.4g}"


class ChartSeries:
    def __init__(self, key, label, color):
        self.key = key
        self.label = label
        self.color = color
        self.visible = True
        self.times = []   # osobne listy - szybkie wyszukiwanie (bisect) po czasie
        self.values = []

    def add(self, t, v):
        self.times.append(t)
        self.values.append(v)
        if len(self.times) > CHART_MAX_POINTS:
            del self.times[:CHART_MAX_POINTS // 10]
            del self.values[:CHART_MAX_POINTS // 10]

    def value_at(self, t):
        """Ostatnia próbka nie późniejsza niż t."""
        i = bisect.bisect_right(self.times, t) - 1
        return (self.times[i], self.values[i]) if i >= 0 else None


class PlotWidget(QWidget):
    """Wykres liniowy wartości w czasie (QPainter, bez zewnętrznych bibliotek)."""
    MARGIN_LEFT, MARGIN_RIGHT, MARGIN_TOP, MARGIN_BOTTOM = 72, 18, 14, 30

    def __init__(self, chart):
        super().__init__()
        self.chart = chart
        self.hover_x = None
        self.setMouseTracking(True)
        self.setMinimumSize(420, 260)

    def time_range(self):
        series = [s for s in self.chart.series if s.times]
        if not series:
            now = time.time()
            return now - 60, now
        end = max(s.times[-1] for s in series)
        window = self.chart.window_seconds()
        if window is None:
            start = min(s.times[0] for s in series)
            if end - start < 1:
                start = end - 1
        else:
            start = end - window
        return start, end

    def value_range(self, t0, t1):
        lo, hi = None, None
        for s in self.chart.series:
            if not s.visible or not s.times:
                continue
            i0 = max(bisect.bisect_left(s.times, t0) - 1, 0)
            i1 = bisect.bisect_right(s.times, t1)
            chunk = s.values[i0:i1]
            if chunk:
                lo = min(chunk) if lo is None else min(lo, min(chunk))
                hi = max(chunk) if hi is None else max(hi, max(chunk))
        if lo is None:
            return 0.0, 1.0
        if hi - lo < 1e-12:
            pad = max(abs(hi) * 0.05, 1.0)
            return lo - pad, hi + pad
        pad = (hi - lo) * 0.06
        return lo - pad, hi + pad

    def plot_rect(self):
        return QRectF(self.MARGIN_LEFT, self.MARGIN_TOP, self.width() - self.MARGIN_LEFT - self.MARGIN_RIGHT,
                      self.height() - self.MARGIN_TOP - self.MARGIN_BOTTOM)

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        self.paint_chart(p, self.rect())
        p.end()

    def paint_chart(self, p, rect):
        p.fillRect(rect, QColor(C_BG))
        r = self.plot_rect()
        t0, t1 = self.time_range()
        v0, v1 = self.value_range(t0, t1)
        tx = lambda t: r.left() + (t - t0) / (t1 - t0) * r.width()
        vy = lambda v: r.bottom() - (v - v0) / (v1 - v0) * r.height()
        small = QFont("Segoe UI")
        small.setPointSizeF(8)
        p.setFont(small)

        # siatka i oś Y
        grid_pen = QPen(QColor(C_BORDER), 1)
        step = nice_step(v1 - v0, max(int(r.height() / 38), 2))
        v = math.ceil(v0 / step) * step
        while v <= v1:
            y = vy(v)
            p.setPen(grid_pen)
            p.drawLine(QPointF(r.left(), y), QPointF(r.right(), y))
            p.setPen(QColor(C_MUTED))
            p.drawText(QRectF(0, y - 8, r.left() - 8, 16),
                       Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, format_number(v, step))
            v += step
        # oś X (czas)
        span = t1 - t0
        max_ticks = max(int(r.width() / 95), 2)
        tstep = next((s for s in (1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400)
                      if span / s <= max_ticks), 28800)
        t = math.ceil(t0 / tstep) * tstep
        while t <= t1:
            x = tx(t)
            p.setPen(grid_pen)
            p.drawLine(QPointF(x, r.top()), QPointF(x, r.bottom()))
            p.setPen(QColor(C_MUTED))
            p.drawText(QRectF(x - 40, r.bottom() + 6, 80, 16), Qt.AlignmentFlag.AlignCenter,
                       time.strftime("%H:%M:%S", time.localtime(t)))
            t += tstep
        p.setPen(QPen(QColor("#3a404b"), 1))
        p.drawRect(r)

        # serie
        p.save()
        p.setClipRect(r)
        for s in self.chart.series:
            if not s.visible or not s.times:
                continue
            i0 = max(bisect.bisect_left(s.times, t0) - 1, 0)
            i1 = bisect.bisect_right(s.times, t1)
            pts = [QPointF(tx(s.times[i]), vy(s.values[i])) for i in range(i0, i1)]
            if not pts:
                continue
            color = QColor(s.color)
            p.setPen(QPen(color, 1.7, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap, Qt.PenJoinStyle.RoundJoin))
            # linia jest przerywana tam, gdzie seria nie była odczytywana (luka > 5x typowy odstęp próbek)
            gaps = sorted(s.times[i + 1] - s.times[i] for i in range(i0, i1 - 1))
            max_gap = max(5 * gaps[len(gaps) // 2], 1.0) if gaps else None
            s.max_gap = max_gap # używane też przez podgląd pod kursorem
            segment = pts[:1]
            for k in range(1, len(pts)):
                if s.times[i0 + k] - s.times[i0 + k - 1] > max_gap:
                    if len(segment) > 1:
                        p.drawPolyline(QPolygonF(segment))
                    segment = []
                segment.append(pts[k])
            if len(segment) > 1:
                p.drawPolyline(QPolygonF(segment))
            if len(pts) <= 80: # przy małej liczbie próbek pokaż punkty pomiarowe
                p.setBrush(color)
                for pt in pts:
                    p.drawEllipse(pt, 2.3, 2.3)
                p.setBrush(Qt.BrushStyle.NoBrush)
        p.restore()

        # legenda
        visible = [s for s in self.chart.series if s.visible]
        if visible:
            fm = p.fontMetrics()
            width = max(fm.horizontalAdvance(s.label) for s in visible) + 34
            box = QRectF(r.left() + 8, r.top() + 8, width, 8 + 16 * len(visible))
            p.setPen(QPen(QColor(C_BORDER), 1))
            p.setBrush(QColor(27, 30, 36, 220))
            p.drawRoundedRect(box, 6, 6)
            for i, s in enumerate(visible):
                y = box.top() + 12 + 16 * i
                p.setPen(QPen(QColor(s.color), 2.2))
                p.drawLine(QPointF(box.left() + 8, y), QPointF(box.left() + 22, y))
                p.setPen(QColor(C_TEXT))
                p.drawText(QPointF(box.left() + 28, y + 4), s.label)
        elif not self.chart.series:
            p.setPen(QColor(C_DIM))
            p.drawText(r, Qt.AlignmentFlag.AlignCenter,
                       "No series yet.\nRight-click rows in the results table and choose \"Add to chart\".")

        # kursor: pionowa linia i wartości w chwili wskazanej myszą
        if self.hover_x is not None and r.left() <= self.hover_x <= r.right() and visible:
            t = t0 + (self.hover_x - r.left()) / r.width() * (t1 - t0)
            p.setPen(QPen(QColor(C_MUTED), 1, Qt.PenStyle.DashLine))
            p.drawLine(QPointF(self.hover_x, r.top()), QPointF(self.hover_x, r.bottom()))
            lines = [(C_MUTED, time.strftime("%H:%M:%S", time.localtime(t)) + f".{int((t % 1) * 1000):03d}")]
            for s in visible:
                sample = s.value_at(t)
                max_gap = getattr(s, "max_gap", None)
                if sample is not None and (max_gap is None or t - sample[0] <= max_gap): # w luce - brak wartości
                    lines.append((s.color, f"{s.label}:  {format_number(sample[1])}"))
                    p.setPen(Qt.PenStyle.NoPen)
                    p.setBrush(QColor(s.color))
                    p.drawEllipse(QPointF(tx(sample[0]), vy(sample[1])), 3.5, 3.5)
            fm = p.fontMetrics()
            width = max(fm.horizontalAdvance(text) for _, text in lines) + 16
            height = 8 + 16 * len(lines)
            x = self.hover_x + 12 if self.hover_x + 12 + width < r.right() else self.hover_x - 12 - width
            box = QRectF(x, r.top() + 8, width, height)
            p.setPen(QPen(QColor(C_BORDER), 1))
            p.setBrush(QColor(C_PANEL2))
            p.drawRoundedRect(box, 6, 6)
            for i, (color, text) in enumerate(lines):
                p.setPen(QColor(color if i else C_MUTED))
                p.drawText(QPointF(box.left() + 8, box.top() + 16 + 16 * i), text)

    def mouseMoveEvent(self, event):
        self.hover_x = event.position().x()
        self.update()

    def leaveEvent(self, event):
        self.hover_x = None
        self.update()


class ChartWindow(QWidget):
    """Wykres wybranych rejestrów - próbki dodawane przy każdym odczycie (ręcznym i cyklicznym)."""
    ROLE_KEY = Qt.ItemDataRole.UserRole

    def __init__(self, app_window):
        super().__init__() # okno niezależne (bez rodzica) - nie jest trzymane nad oknem głównym
        self.app_window = app_window
        self.setWindowTitle("pymms - Chart")
        self.setWindowIcon(app_window.windowIcon())
        self.resize(1100, 560)
        self.series = []

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        bar = QFrame(objectName="panelbar")
        bl = QHBoxLayout(bar)
        bl.setContentsMargins(14, 8, 14, 8)
        bl.setSpacing(6)
        bl.addWidget(QLabel("CHART", objectName="sectionTitle"))
        bl.addSpacing(10)
        bl.addWidget(QLabel("Time window", objectName="fieldLabel"))
        self.window_cb = QComboBox()
        for label, seconds in CHART_WINDOWS:
            self.window_cb.addItem(label, seconds)
        self.window_cb.setCurrentIndex(1)
        self.window_cb.currentIndexChanged.connect(lambda: self.plot.update())
        bl.addWidget(self.window_cb)
        self.pause_btn = QPushButton("Pause", checkable=True, icon=icon(G_PAUSE),
                                     toolTip="Stop recording new samples (reads continue)")
        bl.addWidget(self.pause_btn)
        bl.addStretch(1)
        self.info_label = QLabel(objectName="hint")
        bl.addWidget(self.info_label)
        bl.addSpacing(6)
        self.clear_btn = QPushButton("Clear data", icon=icon(G_DELETE), toolTip="Remove recorded samples, keep series")
        self.clear_btn.clicked.connect(self.clear_data)
        bl.addWidget(self.clear_btn)
        self.csv_btn = QPushButton("Export CSV", icon=icon(G_SAVE), toolTip="Save all recorded samples to CSV")
        self.csv_btn.clicked.connect(self.export_csv)
        bl.addWidget(self.csv_btn)
        self.png_btn = QPushButton("Export PNG", icon=icon(G_SAVE), toolTip="Save the chart as an image")
        self.png_btn.clicked.connect(self.export_png)
        bl.addWidget(self.png_btn)
        layout.addWidget(bar)

        split = QSplitter(Qt.Orientation.Horizontal, childrenCollapsible=False)
        side = QFrame(objectName="sidebar")
        side.setMinimumWidth(210)
        sl = QVBoxLayout(side)
        sl.setContentsMargins(12, 12, 12, 10)
        sl.setSpacing(6)
        sl.addWidget(QLabel("SERIES", objectName="sectionTitle"))
        self.series_list = QListWidget()
        self.series_list.setToolTip("Click the checkbox to show/hide · Right-click to remove")
        self.series_list.itemChanged.connect(self.on_item_changed)
        self.series_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.series_list.customContextMenuRequested.connect(self.series_menu)
        sl.addWidget(self.series_list, 1)
        self.remove_btn = QPushButton("Remove selected", icon=icon(G_DELETE))
        self.remove_btn.clicked.connect(lambda: self.remove_keys([i.data(self.ROLE_KEY) for i in self.series_list.selectedItems()]))
        sl.addWidget(self.remove_btn)
        hint = QLabel("Add series: right-click rows in the results table and choose \"Add to chart\". "
                      "Samples are recorded on every read.", objectName="hint")
        hint.setWordWrap(True)
        sl.addWidget(hint)
        split.addWidget(side)
        self.plot = PlotWidget(self)
        split.addWidget(self.plot)
        split.setStretchFactor(1, 1)
        split.setSizes([230, 870])
        layout.addWidget(split, 1)
        self.update_info()

    def window_seconds(self):
        return self.window_cb.currentData()

    # --- Serie ---

    def find(self, key):
        return next((s for s in self.series if s.key == key), None)

    def has_series(self, key):
        return self.find(key) is not None

    def series_color(self, key):
        s = self.find(key)
        return s.color if s else None

    def add_series(self, key, label):
        if self.has_series(key):
            return
        used = {s.color for s in self.series}
        color = next((c for c in CHART_COLORS if c not in used), CHART_COLORS[len(self.series) % len(CHART_COLORS)])
        self.series.append(ChartSeries(key, label, color))
        item = QListWidgetItem(dot_icon(color), label)
        item.setData(self.ROLE_KEY, key)
        item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
        item.setCheckState(Qt.CheckState.Checked)
        self.series_list.addItem(item)
        self.update_info()
        self.plot.update()

    def remove_keys(self, keys):
        keys = set(keys)
        self.series = [s for s in self.series if s.key not in keys]
        for row in reversed(range(self.series_list.count())):
            if self.series_list.item(row).data(self.ROLE_KEY) in keys:
                self.series_list.takeItem(row)
        self.update_info()
        self.plot.update()
        self.app_window.style_result_rows()

    def on_item_changed(self, item):
        s = self.find(item.data(self.ROLE_KEY))
        if s:
            s.visible = item.checkState() == Qt.CheckState.Checked
            self.plot.update()

    def series_menu(self, pos):
        item = self.series_list.itemAt(pos)
        menu = QMenu(self)
        if item:
            menu.addAction("Remove", lambda: self.remove_keys([item.data(self.ROLE_KEY)]))
        menu.addAction("Remove all", lambda: self.remove_keys([s.key for s in self.series]))
        menu.exec(self.series_list.mapToGlobal(pos))

    # --- Dane ---

    def add_samples(self, t, values):
        """values: {klucz serii: wartość} z jednego odczytu."""
        if self.pause_btn.isChecked() or not values:
            return
        for key, value in values.items():
            s = self.find(key)
            if s:
                s.add(t, value)
        self.update_info()
        if self.isVisible():
            self.plot.update()

    def clear_data(self):
        for s in self.series:
            s.times.clear()
            s.values.clear()
        self.update_info()
        self.plot.update()

    def update_info(self):
        samples = sum(len(s.times) for s in self.series)
        self.info_label.setText(f"{len(self.series)} series  ·  {samples:,} samples".replace(",", " "))

    def export_csv(self):
        """Wszystkie zapisane próbki: wiersz = chwila odczytu, kolumna = seria (puste pole - brak próbki)."""
        if not any(s.times for s in self.series):
            QMessageBox.information(self, "Export CSV", "No samples to export.")
            return
        path, _ = QFileDialog.getSaveFileName(self, "Export Chart Data", f"chart_{datetime.now():%Y%m%d_%H%M%S}.csv",
                                              "CSV files (*.csv);;All files (*)")
        if not path:
            return
        rows = {}
        for col, s in enumerate(self.series):
            for t, v in zip(s.times, s.values):
                rows.setdefault(t, [""] * len(self.series))[col] = format_number(v)
        try:
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f, delimiter=";")
                writer.writerow(["timestamp", "unix_time"] + [s.label for s in self.series])
                for t in sorted(rows):
                    stamp = datetime.fromtimestamp(t).isoformat(sep=" ", timespec="milliseconds")
                    writer.writerow([stamp, f"{t:.3f}"] + rows[t])
        except OSError as e:
            QMessageBox.critical(self, "Export CSV", f"Cannot write file: {e}")
            return
        self.info_label.setText(f"Exported {len(rows)} rows to {os.path.basename(path)}")

    def export_png(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export Chart Image", f"chart_{datetime.now():%Y%m%d_%H%M%S}.png",
                                              "PNG images (*.png);;All files (*)")
        if not path:
            return
        hover, self.plot.hover_x = self.plot.hover_x, None # bez kursora na obrazku
        ok = self.plot.grab().save(path, "PNG")
        self.plot.hover_x = hover
        if not ok:
            QMessageBox.critical(self, "Export PNG", f"Cannot write file: {path}")
            return
        self.info_label.setText(f"Saved {os.path.basename(path)}")


class ModbusValueEditor(QDialog):
    """Dialog to edit register value."""
    def __init__(self, current_value, register_address, data_type, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Edit Register {register_address}")
        self.setModal(True) # Modal window
        self.new_value = None
        self.data_type = data_type # Store data type for validation

        layout = QVBoxLayout()

        self.label_address = QLabel(f"Register Address: {register_address}")
        layout.addWidget(self.label_address)

        self.label_type = QLabel(f"Data Type: {data_type}")
        layout.addWidget(self.label_type)

        # Zmieniona logika wyboru widgetu wejściowego dla wartości
        if "Boolean" in self.data_type:
            self.input_value = QComboBox()
            self.input_value.addItem("True")
            self.input_value.addItem("False")
            # Ustawienie aktualnej wartości na podstawie wejścia
            if str(current_value).lower() == 'true':
                self.input_value.setCurrentIndex(0)
            else:
                self.input_value.setCurrentIndex(1)
        else:
            self.input_value = QLineEdit(str(current_value))
            self.input_value.setPlaceholderText("Enter new value")
        layout.addWidget(self.input_value)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            Qt.Orientation.Horizontal, self
        )
        buttons.accepted.connect(self.accept_value)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

        self.setLayout(layout)

    def accept_value(self):
        """Accepts the entered value and validates it."""
        try:
            if "Boolean" in self.data_type:
                # Pobierz wybraną wartość z QComboBox
                selected_text = self.input_value.currentText()
                self.new_value = (selected_text.lower() == 'true')
            elif "FLOAT" in self.data_type:
                self.new_value = float(self.input_value.text())
            else:
                self.new_value = int(self.input_value.text())
            self.accept()
        except ValueError as e:
            QMessageBox.warning(self, "Error", f"Enter a valid number for the selected data type. {e}")

    def get_new_value(self):
        """Returns the new value."""
        return self.new_value


class ModbusClientApp(QMainWindow):
    # Wersja aplikacji, która będzie inkrementowana
    __version__ = "1.18"

    FUNCTIONS = ["0x03 (Holding Registers)", "0x04 (Input Registers)", "0x01 (Coils)", "0x02 (Discrete Inputs)"]
    DATA_TYPES = ["UINT16 (16-bit Unsigned)", "INT16 (16-bit Signed)",
                  "UINT32 (32-bit Unsigned)", "INT32 (32-bit Signed)", "FLOAT32 (32-bit Float)",
                  "UINT64 (64-bit Unsigned)", "INT64 (64-bit Signed)", "FLOAT64 (64-bit Double)"]
    BYTE_ORDERS = ["ABCD (No Swap)", "CDAB (Word Swap)", "BADC (Byte Swap within Words)", "DCBA (Full Byte Reverse)"]
    BAUD_RATES = ["1200", "2400", "4800", "9600", "19200", "38400", "57600", "115200"]

    def __init__(self):
        super().__init__()
        apply_theme(QApplication.instance())
        self.setWindowTitle(f"pymms (Python Modbus Master Simulator) v{self.__version__}")
        self.setWindowIcon(QIcon(resource_path('icon.png')))
        self.settings = open_settings() # pymms.ini obok programu

        self.modbus_client = None
        self.connection_type = "RTU" # Default connection type
        self.scanner_window = None # Okno skanera (tworzone przy pierwszym otwarciu)
        self.chart_window = None # Okno wykresu (tworzone przy pierwszym użyciu)
        self.last_read_params = None # parametry ostatniego udanego odczytu - identyfikują serie wykresu
        self.polling_timer = QTimer(self) # Initialize QTimer
        self.polling_timer.timeout.connect(self.read_modbus_data) # Connect timer to read function

        # Statistics variables
        self.total_polls = 0
        self.failed_polls = 0
        self.last_response_time = 0.0
        self.sum_response_times = 0.0
        self.average_response_time = 0.0

        self.init_ui()
        self.restore_settings()
        self.update_connection_buttons()
        self.update_status_bar_stats()

    # --- Budowa interfejsu ---

    @staticmethod
    def _field(text):
        return QLabel(text, objectName="fieldLabel")

    @staticmethod
    def _spin(minimum, maximum, value, suffix="", width=None):
        sb = QSpinBox()
        sb.setRange(minimum, maximum)
        sb.setValue(value)
        if suffix:
            sb.setSuffix(suffix)
        if width:
            sb.setMinimumWidth(width)
        return sb

    def init_ui(self):
        """Initializes the user interface."""
        central = QWidget(objectName="central")
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # --- Pasek połączenia ---
        top = QFrame(objectName="topbar")
        tl = QHBoxLayout(top)
        tl.setContentsMargins(14, 10, 14, 10)
        tl.setSpacing(8)
        logo = QLabel()
        logo.setPixmap(QIcon(resource_path('icon.png')).pixmap(26, 26))
        tl.addWidget(logo)
        title_box = QVBoxLayout()
        title_box.setSpacing(0)
        title_box.addWidget(QLabel("pymms", objectName="appTitle"))
        title_box.addWidget(QLabel("Modbus Master", objectName="appSubtitle"))
        tl.addLayout(title_box)
        tl.addSpacing(16)

        self.rtu_btn = QPushButton("RTU", objectName="segmentLeft", checkable=True, checked=True,
                                   toolTip="Modbus RTU over a serial port")
        self.tcp_btn = QPushButton("TCP", objectName="segmentRight", checkable=True, toolTip="Modbus TCP/IP")
        self.type_group = QButtonGroup(self)
        self.type_group.setExclusive(True)
        self.type_group.addButton(self.rtu_btn, 0)
        self.type_group.addButton(self.tcp_btn, 1)
        self.type_group.idClicked.connect(self.on_connection_type_changed)
        seg = QHBoxLayout()
        seg.setSpacing(0)
        seg.addWidget(self.rtu_btn)
        seg.addWidget(self.tcp_btn)
        tl.addLayout(seg)
        tl.addSpacing(6)

        # RTU Parameters
        self.rtu_params_widget = QWidget()
        rl = QHBoxLayout(self.rtu_params_widget)
        rl.setContentsMargins(0, 0, 0, 0)
        rl.setSpacing(6)
        self.com_port_cb = QComboBox(minimumWidth=220, toolTip="Serial port")
        rl.addWidget(self.com_port_cb)
        self.refresh_ports_btn = QToolButton(icon=icon(G_REFRESH), toolTip="Refresh COM port list")
        self.refresh_ports_btn.clicked.connect(self.populate_com_ports)
        rl.addWidget(self.refresh_ports_btn)
        self.baud_rate_cb = QComboBox(editable=True, minimumWidth=90, toolTip="Baud rate [bps]")
        self.baud_rate_cb.addItems(self.BAUD_RATES)
        self.baud_rate_cb.setCurrentText("9600")
        rl.addWidget(self.baud_rate_cb)
        self.data_bits_cb = QComboBox(toolTip="Data bits")
        self.data_bits_cb.addItems(["7", "8"])
        self.data_bits_cb.setCurrentText("8")
        rl.addWidget(self.data_bits_cb)
        self.parity_cb = QComboBox(toolTip="Parity: N - none, E - even, O - odd")
        self.parity_cb.addItems(["N", "E", "O"])
        rl.addWidget(self.parity_cb)
        self.stop_bits_cb = QComboBox(toolTip="Stop bits")
        self.stop_bits_cb.addItems(["1", "2"])
        rl.addWidget(self.stop_bits_cb)
        tl.addWidget(self.rtu_params_widget)
        self.populate_com_ports()

        # TCP Parameters
        self.tcp_params_widget = QWidget()
        cl = QHBoxLayout(self.tcp_params_widget)
        cl.setContentsMargins(0, 0, 0, 0)
        cl.setSpacing(6)
        self.ip_address_le = QLineEdit("192.168.1.1", minimumWidth=150, placeholderText="IP address / host name",
                                       toolTip="IP address or host name of the Modbus TCP device")
        cl.addWidget(self.ip_address_le)
        cl.addWidget(self._field(":"))
        self.port_le = QLineEdit("502", maximumWidth=64, toolTip="TCP port")
        cl.addWidget(self.port_le)
        tl.addWidget(self.tcp_params_widget)
        self.tcp_params_widget.hide()

        tl.addSpacing(6)
        tl.addWidget(self._field("Timeout"))
        self.timeout_ms_sb = self._spin(100, 30000, 500, " ms")
        self.timeout_ms_sb.setToolTip("Response timeout")
        tl.addWidget(self.timeout_ms_sb)

        self.connect_btn = QPushButton("Connect", objectName="primary", icon=icon(G_PLUG, ACCENT_INK))
        self.connect_btn.clicked.connect(self.toggle_connection)
        tl.addWidget(self.connect_btn)

        tl.addStretch(1)
        self.state_dot = QLabel("●", objectName="statusDot")
        self.state_label = QLabel("Disconnected")
        tl.addWidget(self.state_dot)
        tl.addWidget(self.state_label)
        root.addWidget(top)

        # --- Pasek zapytania ---
        req = QFrame(objectName="requestbar")
        ql = QHBoxLayout(req)
        ql.setContentsMargins(14, 8, 14, 8)
        ql.setSpacing(6)
        ql.addWidget(self._field("Slave ID"))
        self.slave_id_sb = self._spin(1, 247, 1, width=64)
        ql.addWidget(self.slave_id_sb)
        ql.addSpacing(6)
        ql.addWidget(self._field("Function"))
        self.modbus_function_cb = QComboBox(minimumWidth=180)
        self.modbus_function_cb.addItems(self.FUNCTIONS)
        self.modbus_function_cb.currentIndexChanged.connect(self.on_modbus_function_changed)
        ql.addWidget(self.modbus_function_cb)
        ql.addSpacing(6)
        ql.addWidget(self._field("Address"))
        self.start_address_sb = self._spin(0, 65535, 0, width=80)
        ql.addWidget(self.start_address_sb)
        ql.addWidget(self._field("Quantity"))
        self.num_registers_sb = self._spin(1, 125, 10, width=70)
        ql.addWidget(self.num_registers_sb)
        self.read_data_btn = QPushButton("Read", objectName="primary", icon=icon(G_DOWN, ACCENT_INK),
                                         toolTip="Read once")
        self.read_data_btn.clicked.connect(self.read_modbus_data)
        ql.addWidget(self.read_data_btn)
        ql.addSpacing(14)
        self.cyclic_btn = QPushButton("Cyclic read", checkable=True, icon=icon(G_SYNC),
                                      toolTip="Repeat the read with the given interval")
        self.cyclic_btn.toggled.connect(self.on_cyclic_toggled)
        ql.addWidget(self.cyclic_btn)
        ql.addWidget(self._field("every"))
        self.polling_interval_ms_sb = self._spin(100, 60000, 1000, " ms", width=90)
        ql.addWidget(self.polling_interval_ms_sb)
        ql.addStretch(1)
        self.show_scanner_btn = QPushButton("Scanner", icon=icon(G_SEARCH), toolTip="Scan Slave IDs or register addresses")
        self.show_scanner_btn.clicked.connect(self.show_scanner_window)
        ql.addWidget(self.show_scanner_btn)
        self.show_chart_btn = QPushButton("Chart", icon=icon(G_CHART), toolTip="Plot selected registers over time")
        self.show_chart_btn.clicked.connect(self.show_chart_window)
        ql.addWidget(self.show_chart_btn)
        self.show_log_btn = QPushButton("Log", checkable=True, checked=True, icon=icon(G_LIST),
                                        toolTip="Show / hide the transmission log")
        self.show_log_btn.toggled.connect(self.on_log_toggled)
        ql.addWidget(self.show_log_btn)
        root.addWidget(req)

        # --- Ciało: panel boczny | wyniki + log ---
        self.hsplit = QSplitter(Qt.Orientation.Horizontal, childrenCollapsible=False)
        root.addWidget(self.hsplit, 1)

        side = QFrame(objectName="sidebar")
        side.setMinimumWidth(220)
        sl = QVBoxLayout(side)
        sl.setContentsMargins(14, 14, 14, 12)
        sl.setSpacing(6)
        sl.addWidget(QLabel("DATA INTERPRETATION", objectName="sectionTitle"))
        sl.addWidget(self._field("Interpret as"))
        self.data_type_cb = QComboBox()
        self.data_type_cb.addItems(self.DATA_TYPES)
        self.data_type_cb.currentIndexChanged.connect(self.on_modbus_function_changed) # Connect to update quantity range
        sl.addWidget(self.data_type_cb)
        sl.addWidget(self._field("Byte order"))
        self.swap_bytes_cb = QComboBox()
        self.swap_bytes_cb.addItems(self.BYTE_ORDERS)
        sl.addWidget(self.swap_bytes_cb)
        sl.addSpacing(14)

        sl.addWidget(QLabel("STATISTICS", objectName="sectionTitle"))
        grid = QGridLayout()
        grid.setSpacing(6)
        self.stat_cards = {}
        for i, (key, label) in enumerate((("polls", "Polls"), ("failed", "Failed"), ("last", "Last [ms]"), ("avg", "Avg [ms]"))):
            card = QFrame(objectName="card")
            cv = QVBoxLayout(card)
            cv.setContentsMargins(10, 6, 10, 6)
            cv.setSpacing(0)
            value = QLabel("0", objectName="cardValue")
            cv.addWidget(value)
            cv.addWidget(QLabel(label, objectName="cardLabel"))
            grid.addWidget(card, i // 2, i % 2)
            self.stat_cards[key] = value
        sl.addLayout(grid)
        sl.addStretch(1)
        hint = QLabel("Double-click a value in the table to write it to the device.", objectName="hint")
        hint.setWordWrap(True)
        sl.addWidget(hint)
        sl.addSpacing(6)
        self.defaults_btn = QPushButton("Restore defaults", icon=icon(G_RESTORE),
                                        toolTip="Reset connection, request, interpretation and window layout to defaults")
        self.defaults_btn.clicked.connect(self.restore_defaults)
        sl.addWidget(self.defaults_btn)
        self.hsplit.addWidget(side)

        self.vsplit = QSplitter(Qt.Orientation.Vertical, childrenCollapsible=False)
        # --- Results Table ---
        self.registers_table = QTableWidget()
        self.registers_table.setColumnCount(5)
        self.registers_table.setHorizontalHeaderLabels(["REGISTER", "REGISTER (HEX)", "RAW VALUE", "VALUE", "TYPE"])
        self.registers_table.cellDoubleClicked.connect(self.on_table_double_click)
        self.registers_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.registers_table.customContextMenuRequested.connect(self.table_context_menu)
        self.registers_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.registers_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.registers_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.registers_table.setAlternatingRowColors(True)
        self.registers_table.setShowGrid(False)
        self.registers_table.setFont(mono_font())
        vh = self.registers_table.verticalHeader()
        vh.setVisible(False)
        vh.setDefaultSectionSize(24)
        hh = self.registers_table.horizontalHeader()
        hh.setHighlightSections(False)
        hh.setDefaultAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        hh.setStretchLastSection(True)
        for col, width in enumerate([100, 120, 220, 180]):
            self.registers_table.setColumnWidth(col, width)
        self.vsplit.addWidget(self.registers_table)

        self.log_window = LogPanel(self) # Panel logu transmisji
        self.vsplit.addWidget(self.log_window)
        self.vsplit.setStretchFactor(0, 3)
        self.vsplit.setStretchFactor(1, 2)
        self.vsplit.setSizes([420, 260])
        self.hsplit.addWidget(self.vsplit)
        self.hsplit.setStretchFactor(1, 1)
        self.hsplit.setSizes([240, 1000])

        # Status Bar
        sb = self.statusBar()
        sb.setSizeGripEnabled(False)
        self.status_message_label = QLabel("Ready")
        sb.addWidget(self.status_message_label, 1)
        self.channel_label = QLabel("")
        sb.addPermanentWidget(self.channel_label)
        sb.addPermanentWidget(QLabel(f"v{self.__version__}"))

    # --- Połączenie ---

    def populate_com_ports(self):
        """Populates the port list (device name + description), keeping the current selection on refresh."""
        current = self.com_port_cb.currentData() or self.settings.value("port", "")
        self.com_port_cb.clear()
        ports = sorted(serial.tools.list_ports.comports(), key=lambda p: (len(p.device), p.device)) # COM3 przed COM12
        for port in ports:
            desc = port.description if port.description and port.description != "n/a" else ""
            if desc.endswith(f"({port.device})"):
                desc = desc[: -len(port.device) - 2].strip()
            self.com_port_cb.addItem(f"{port.device}  ·  {desc}" if desc else port.device, port.device)
        if not ports:
            self.com_port_cb.addItem("No COM ports", None)
        index = self.com_port_cb.findData(current)
        if index >= 0:
            self.com_port_cb.setCurrentIndex(index)

    def on_connection_type_changed(self, index):
        """Toggles visibility of RTU/TCP parameters based on selected connection type."""
        self.connection_type = "RTU" if index == 0 else "TCP"
        self.rtu_btn.setChecked(index == 0)
        self.tcp_btn.setChecked(index == 1)
        self.rtu_params_widget.setVisible(index == 0)
        self.tcp_params_widget.setVisible(index == 1)

    def set_connection_type(self, connection_type):
        self.on_connection_type_changed(0 if connection_type == "RTU" else 1)

    def on_modbus_function_changed(self, index=None):
        """Adjusts UI elements based on the selected Modbus function."""
        selected_function_text = self.modbus_function_cb.currentText()
        if "Coils" in selected_function_text or "Discrete Inputs" in selected_function_text:
            # Bity - interpretacja typów nie ma zastosowania
            self.data_type_cb.setEnabled(False)
            self.swap_bytes_cb.setEnabled(False)
            self.num_registers_sb.setRange(1, 2000) # Max quantity for coils/discrete inputs
        else:
            self.data_type_cb.setEnabled(True)
            self.swap_bytes_cb.setEnabled(True)
            selected_data_type = self.data_type_cb.currentText()
            if "64-bit" in selected_data_type:
                self.num_registers_sb.setRange(4, 125) # Min 4 registers for 64-bit
            elif "32-bit" in selected_data_type:
                self.num_registers_sb.setRange(2, 125) # Min 2 registers for 32-bit
            else:
                self.num_registers_sb.setRange(1, 125) # Min 1 register for 16-bit

    def toggle_connection(self):
        if self.modbus_client and self.modbus_client.connected:
            self.disconnect_modbus()
        else:
            self.connect_modbus()

    def update_connection_buttons(self):
        """Updates the state of Connect/Disconnect, Read, Cyclic Read and the connection indicator."""
        is_connected = bool(self.modbus_client and self.modbus_client.connected)
        is_polling = self.polling_timer.isActive()

        if is_connected:
            self.connect_btn.setText("Disconnect")
            self.connect_btn.setObjectName("danger")
            self.connect_btn.setIcon(icon(G_STOP, "#ffb3b3"))
        else:
            self.connect_btn.setText("Connect")
            self.connect_btn.setObjectName("primary")
            self.connect_btn.setIcon(icon(G_PLUG, ACCENT_INK))
        repolish(self.connect_btn)

        # parametry połączenia można zmieniać tylko bez połączenia
        for w in (self.rtu_btn, self.tcp_btn, self.rtu_params_widget, self.tcp_params_widget, self.timeout_ms_sb):
            w.setEnabled(not is_connected)
        self.read_data_btn.setEnabled(is_connected and not is_polling)
        self.cyclic_btn.setEnabled(is_connected)
        self.cyclic_btn.blockSignals(True)
        self.cyclic_btn.setChecked(is_polling)
        self.cyclic_btn.setIcon(icon(G_STOP if is_polling else G_SYNC, "#dff5b0" if is_polling else C_TEXT))
        self.cyclic_btn.blockSignals(False)
        self.polling_interval_ms_sb.setEnabled(not is_polling)

        if is_connected:
            channel = self.modbus_client.channel
            self.state_label.setText(f"Connected · {self.connection_type} {channel}")
            self.state_dot.setStyleSheet(f"color: {C_GREEN};")
            self.state_label.setStyleSheet(f"color: {C_TEXT};")
            self.channel_label.setText(f"{self.connection_type} {channel}")
        else:
            self.state_label.setText("Disconnected")
            self.state_dot.setStyleSheet(f"color: {C_DIM};")
            self.state_label.setStyleSheet(f"color: {C_MUTED};")
            self.channel_label.setText("")

    def update_status_bar_stats(self):
        """Updates the statistics cards in the sidebar."""
        self.stat_cards["polls"].setText(str(self.total_polls))
        self.stat_cards["failed"].setText(str(self.failed_polls))
        self.stat_cards["failed"].setStyleSheet(f"color: {STATUS_RED};" if self.failed_polls else "")
        self.stat_cards["last"].setText(f"{self.last_response_time:.1f}" if self.total_polls else "–")
        self.stat_cards["avg"].setText(f"{self.average_response_time:.1f}" if self.total_polls else "–")

    def connect_modbus(self):
        """Establishes Modbus connection to the device."""
        if self.modbus_client and self.modbus_client.connected:
            self.disconnect_modbus()

        try:
            timeout_seconds = self.timeout_ms_sb.value() / 1000.0 # Convert ms to seconds

            if self.connection_type == "RTU":
                port = self.com_port_cb.currentData()
                if not port:
                    QMessageBox.warning(self, "Port Error", "No COM port selected.")
                    return
                self.modbus_client = ModbusRtuMaster(
                    port=port,
                    baudrate=int(self.baud_rate_cb.currentText()),
                    parity=self.parity_cb.currentText(),
                    bytesize=int(self.data_bits_cb.currentText()),
                    stopbits=int(self.stop_bits_cb.currentText()),
                    timeout=timeout_seconds,
                    log=self.log
                )
            else: # TCP
                self.modbus_client = ModbusTcpMaster(
                    host=self.ip_address_le.text().strip(),
                    port=int(self.port_le.text()),
                    timeout=timeout_seconds,
                    log=self.log
                )

            if self.modbus_client.connect():
                self.status_message_label.setText(f"Connected to Modbus ({self.connection_type}).")
                self.status_message_label.setStyleSheet("") # Reset color on success
                self.save_settings()
            else:
                self.status_message_label.setText(f"Connection error with Modbus ({self.connection_type}). "
                                                  "Check parameters (details in Transmission Log).")
                self.status_message_label.setStyleSheet(f"color: {STATUS_RED};")
                self.modbus_client = None # Reset client on error
        except Exception as e:
            self.status_message_label.setText(f"Configuration or connection error: {e}")
            self.status_message_label.setStyleSheet(f"color: {STATUS_RED};")
            self.modbus_client = None # Reset client on error
        finally:
            self.update_connection_buttons()
            self.update_status_bar_stats()

    def disconnect_modbus(self):
        """Disconnects Modbus connection."""
        self.stop_cyclic_read() # Stop polling before disconnecting
        if self.modbus_client:
            self.modbus_client.close()
            self.modbus_client = None

        # Reset statistics on disconnect
        self.total_polls = 0
        self.failed_polls = 0
        self.last_response_time = 0.0
        self.sum_response_times = 0.0
        self.average_response_time = 0.0

        self.update_connection_buttons()
        self.registers_table.setRowCount(0) # Clear table on disconnect
        self.status_message_label.setText("Ready")
        self.status_message_label.setStyleSheet("")
        self.update_status_bar_stats()

    # --- Odczyt cykliczny ---

    def on_cyclic_toggled(self, checked):
        if checked:
            self.start_cyclic_read()
        else:
            self.stop_cyclic_read()

    def start_cyclic_read(self):
        """Starts cyclic Modbus data reading."""
        if not (self.modbus_client and self.modbus_client.connected):
            QMessageBox.warning(self, "Connection Error", "Not connected to Modbus device to start cyclic read.")
            self.update_connection_buttons()
            return

        interval_ms = self.polling_interval_ms_sb.value()
        self.polling_timer.setInterval(interval_ms)
        self.polling_timer.start()
        self.status_message_label.setText(f"Cyclic read started with interval {interval_ms} ms.")
        self.status_message_label.setStyleSheet("")
        self.update_connection_buttons()
        self.update_status_bar_stats()

    def stop_cyclic_read(self):
        """Stops cyclic Modbus data reading."""
        if self.polling_timer.isActive():
            self.polling_timer.stop()
            self.status_message_label.setText("Cyclic read stopped.")
            self.status_message_label.setStyleSheet("")
        self.update_connection_buttons()
        self.update_status_bar_stats()

    def style_result_rows(self):
        """Kolory kolumn tabeli wyników: adresy przygaszone, wartość wyróżniona."""
        dim, muted, accent = QColor(C_DIM), QColor(C_MUTED), QColor(ACCENT)
        for row in range(self.registers_table.rowCount()):
            for col, color in ((0, None), (1, dim), (2, muted), (3, accent), (4, muted)):
                item = self.registers_table.item(row, col)
                if item and color is not None:
                    item.setForeground(color)
            # kolorowa kropka przy rejestrach rysowanych na wykresie
            item = self.registers_table.item(row, 0)
            if item:
                key = self.row_series_key(row)
                color = self.chart_window.series_color(key) if self.chart_window and key else None
                item.setIcon(dot_icon(color) if color else QIcon())

    def read_modbus_data(self):
        """Reads data from Modbus device based on selected function code."""
        if not (self.modbus_client and self.modbus_client.connected):
            # If called by timer and connection dropped, stop timer
            if self.polling_timer.isActive():
                self.stop_cyclic_read()
            QMessageBox.warning(self, "Connection Error", "Not connected to Modbus device.")
            return

        # Zawsze czyść tabelę na początku każdej próby odczytu
        self.registers_table.setRowCount(0)

        try:
            slave_id = self.slave_id_sb.value()
            start_address = self.start_address_sb.value()
            num_elements = self.num_registers_sb.value() # Renamed for generic use
        except ValueError:
            QMessageBox.warning(self, "Parameter Error", "Slave ID, Start Address, and Quantity must be integers.")
            return

        selected_function_text = self.modbus_function_cb.currentText()
        response = None
        data_to_display = []
        
        start_time = time.time() # Start timing the request

        try:
            if "Holding Registers" in selected_function_text:
                response = self.modbus_client.read_holding_registers(
                    address=start_address, count=num_elements, slave=slave_id
                )
            elif "Input Registers" in selected_function_text:
                response = self.modbus_client.read_input_registers(
                    address=start_address, count=num_elements, slave=slave_id
                )
            elif "Coils" in selected_function_text:
                response = self.modbus_client.read_coils(
                    address=start_address, count=num_elements, slave=slave_id
                )
            elif "Discrete Inputs" in selected_function_text:
                response = self.modbus_client.read_discrete_inputs(
                    address=start_address, count=num_elements, slave=slave_id
                )
            else:
                QMessageBox.warning(self, "Function Error", "Unsupported Modbus function selected.")
                return

            if response is None or response.isError(): # Check for None in case of some silent failures or if isError() not available
                # Handle cases where response might be an error response object or None from Modbus functions
                raise ModbusException(f"Modbus Error: {response}")

            # Zmieniona logika przypisywania data_to_display
            if "Coils" in selected_function_text or "Discrete Inputs" in selected_function_text:
                data_to_display = response.bits
            elif hasattr(response, 'registers'):
                data_to_display = response.registers
            else:
                # Fallback, jeśli żaden z powyższych warunków nie jest spełniony (np. nieoczekiwany typ odpowiedzi)
                data_to_display = [] 


            end_time = time.time() # End timing
            duration = end_time - start_time
            duration = duration * 1000.00

            self.total_polls += 1
            self.last_response_time = duration
            self.sum_response_times += duration
            self.average_response_time = self.sum_response_times / self.total_polls

            self.last_read_params = {
                "slave": slave_id, "function": selected_function_text,
                "data_type": self.data_type_cb.currentText(), "byte_order": self.swap_bytes_cb.currentText(),
            }
            self.display_registers(start_address, data_to_display, selected_function_text)
            self.style_result_rows()
            self.feed_chart()
            
            # Nowy, bardziej precyzyjny komunikat w zależności od ilości odebranych elementów
            if len(data_to_display) > 0:
                self.status_message_label.setText(f"Successfully read {len(data_to_display)} elements.")
                self.status_message_label.setStyleSheet("") # Reset color on success
            else:
                self.status_message_label.setText("Successfully read, but no data received for specified parameters. Check slave configuration (address/quantity).")
                self.status_message_label.setStyleSheet(f"color: {STATUS_ORANGE};") # Użyj pomarańczowego dla ostrzeżenia/braku danych
            
            self.update_status_bar_stats() # Update status bar with new stats

        except ModbusException as e: # Catch all Modbus communication errors, including timeouts
            error_message = str(e)
            # Obsługa komunikatów o braku odpowiedzi lub zamknięciu połączenia (traktujemy jako timeout)
            self.status_message_label.setText(f"Modbus communication error: {error_message}")
            self.status_message_label.setStyleSheet(f"color: {STATUS_RED};")
            
            self.failed_polls += 1 # Zwiększenie licznika nieudanych odpytań dla każdego błędu Modbus
            self.update_status_bar_stats() # Natychmiastowa aktualizacja statystyk
            
            # Dodatkowo, jeśli błąd wskazuje na utratę połączenia, zatrzymaj cykliczne odczyty.
            if "CLOSING CONNECTION" in error_message or "timed out" in error_message:
                if self.polling_timer.isActive():
                    self.stop_cyclic_read()
                self.update_connection_buttons() # po utracie połączenia odblokuj "Connect"
            return # Nie kontynuujemy wyświetlania rejestrów w przypadku błędu

        except Exception as e: # Catch any other unexpected errors
            self.status_message_label.setText(f"An unexpected error occurred: {e}")
            self.status_message_label.setStyleSheet(f"color: {STATUS_RED};")
            self.failed_polls += 1 # Increment failed polls for unexpected errors
            self.update_status_bar_stats() # Update stats immediately
            if self.polling_timer.isActive():
                self.stop_cyclic_read() # Stop cyclic read if a persistent error occurs
            QMessageBox.critical(self, "Error", f"An unexpected error occurred: {e}")
            return # Don't proceed to display registers if error

    def display_registers(self, start_address, data, function_code_text):
        """Displays read data in the table, interpreting it according to the selected type."""
        # Tabela jest już czyszczona na początku read_modbus_data
        # self.registers_table.setRowCount(0) 

        row_count = 0

        if "Coils" in function_code_text or "Discrete Inputs" in function_code_text:
            # Handle boolean data (Coils, Discrete Inputs)
            for i, bit_value in enumerate(data):
                current_address = start_address + i
                self.registers_table.insertRow(row_count)
                self.registers_table.setItem(row_count, 0, QTableWidgetItem(str(current_address)))
                current_address_hex = f"0x{current_address:04X}"
                self.registers_table.setItem(row_count, 1, QTableWidgetItem(str(current_address_hex)))
                self.registers_table.setItem(row_count, 2, QTableWidgetItem(str(bit_value))) # Raw value is boolean
                self.registers_table.setItem(row_count, 3, QTableWidgetItem(str(bit_value))) # Interpreted value is boolean
                self.registers_table.setItem(row_count, 4, QTableWidgetItem("Boolean"))
                row_count += 1
        else:
            # Handle register data (Holding Registers, Input Registers)
            data_type = self.data_type_cb.currentText()
            swap_type = self.swap_bytes_cb.currentText() # Get selected swap type

            i = 0
            while i < len(data):
                current_address = start_address + i
                self.registers_table.insertRow(row_count)

                # Column 0: Register Address
                self.registers_table.setItem(row_count, 0, QTableWidgetItem(str(current_address)))
                current_address_hex = f"0x{current_address:04X}"
                self.registers_table.setItem(row_count, 1, QTableWidgetItem(str(current_address_hex)))

                interpreted_value = "N/A"
                num_words_needed = 1 # Default 16-bit
                raw_hex_str = ""

                if "16-bit" in data_type:
                    raw_hex_str = f"0x{data[i]:04X}"
                    if "UINT16" in data_type: # Check UINT16 first
                        bytes_val = data[i].to_bytes(2, 'big')
                        interpreted_value = str(bytes_to_uint16(bytes_val))
                    elif "INT16" in data_type: # Then check INT16
                        bytes_val = data[i].to_bytes(2, 'big')
                        interpreted_value = str(bytes_to_int16(bytes_val))
                    num_words_needed = 1
                elif "32-bit" in data_type or "FLOAT32" in data_type:
                    if i + 1 < len(data): # Need 2 words (4 bytes)
                        bytes_for_conversion = \
                            data[i].to_bytes(2, 'big') + \
                            data[i+1].to_bytes(2, 'big')
                        raw_hex_str = f"0x{data[i]:04X} 0x{data[i+1]:04X}"

                        if "UINT32" in data_type: # Check UINT32 first
                            interpreted_value = str(bytes_to_uint32(bytes_for_conversion, swap_type))
                        elif "INT32" in data_type: # Then check INT32
                            interpreted_value = str(bytes_to_int32(bytes_for_conversion, swap_type))
                        elif "FLOAT32" in data_type:
                            interpreted_value = f"{bytes_to_float32(bytes_for_conversion, swap_type):.4f}" # Format to 4 decimal places
                        num_words_needed = 2
                    else:
                        interpreted_value = "Missing second register for 32-bit"
                        raw_hex_str = f"0x{data[i]:04X}"
                        num_words_needed = 2 # Still skip 2, even if missing
                elif "64-bit" in data_type: # New 64-bit types
                    if i + 3 < len(data): # Need 4 words (8 bytes)
                        bytes_for_conversion = \
                            data[i].to_bytes(2, 'big') + \
                            data[i+1].to_bytes(2, 'big') + \
                            data[i+2].to_bytes(2, 'big') + \
                            data[i+3].to_bytes(2, 'big')
                        raw_hex_str = f"0x{data[i]:04X} 0x{data[i+1]:04X} 0x{data[i+2]:04X} 0x{data[i+3]:04X}"

                        if "UINT64" in data_type: # Check UINT64 first
                            interpreted_value = str(bytes_to_uint64(bytes_for_conversion, swap_type))
                        elif "INT64" in data_type: # Then check INT64
                            interpreted_value = str(bytes_to_int64(bytes_for_conversion, swap_type))
                        elif "FLOAT64" in data_type:
                            interpreted_value = f"{bytes_to_float64(bytes_for_conversion, swap_type):.8f}" # Format to 8 decimal places for double
                        num_words_needed = 4
                    else:
                        interpreted_value = "Missing registers for 64-bit"
                        raw_hex_str = " ".join([f"0x{d:04X}" for d in data[i:]]) # Show available raw data
                        num_words_needed = 4 # Still skip 4, even if incomplete

                # Column 1: Raw Value (HEX)
                self.registers_table.setItem(row_count, 2, QTableWidgetItem(raw_hex_str))

                # Column 2: Interpreted Value
                self.registers_table.setItem(row_count, 3, QTableWidgetItem(str(interpreted_value)))

                # Column 3: Type (for informational purposes only, not editable)
                self.registers_table.setItem(row_count, 4, QTableWidgetItem(data_type + (f" ({swap_type})" if num_words_needed > 1 else "")))

                row_count += 1
                i += num_words_needed # Skip by appropriate number of registers (1 for 16-bit, 2 for 32-bit, 4 for 64-bit)

    def on_table_double_click(self, row, column):
        """Handles double-click on a table cell, opening the value editor."""
        if column != 3: # Allow editing only the interpreted value column
            return

        item_address = self.registers_table.item(row, 0) # Register Address
        item_current_value = self.registers_table.item(row, 3) # Current Value
        item_data_type = self.registers_table.item(row, 4) # Data Type (e.g., "UINT16 (16-bit Unsigned)")

        if not (item_address and item_current_value and item_data_type): return

        register_address = int(item_address.text())
        current_value_str = item_current_value.text()
        data_type_str = item_data_type.text() # Full data type string from table

        # Ensure we have a connected device
        if not (self.modbus_client and self.modbus_client.connected):
            QMessageBox.warning(self, "Connection Error", "First connect to a Modbus device.")
            return
        
        # Check if it's "Missing second register for 32-bit" or "Missing registers for 64-bit", if so, do not allow editing
        if "Missing" in current_value_str:
            QMessageBox.warning(self, "Error", "Cannot edit incomplete data type.")
            return

        # Pass the data type string to the editor for validation
        dialog = ModbusValueEditor(current_value_str, register_address, data_type_str, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            new_value = dialog.get_new_value()
            if new_value is not None:
                # Pass data type and byte swap setting to the write function
                swap_type = self.swap_bytes_cb.currentText()
                self.write_modbus_value(register_address, new_value, data_type_str, swap_type)


    def write_modbus_value(self, address, value, data_type_str, swap_type):
        """Writes value(s) to register(s) or coil(s) depending on data type."""
        if not (self.modbus_client and self.modbus_client.connected):
            QMessageBox.warning(self, "Connection Error", "Not connected to Modbus device.")
            return

        # Stop cyclic read if active before writing
        self.stop_cyclic_read()

        try:
            slave_id = self.slave_id_sb.value()
            response = None
            registers_to_write = []
            num_registers_needed = 0

            if "Boolean" in data_type_str:
                # Write a single coil (0x05)
                if not isinstance(value, bool):
                    QMessageBox.warning(self, "Value Error", "Boolean value must be True or False.")
                    return
                response = self.modbus_client.write_coil(
                    address=address,
                    value=value,
                    slave=slave_id
                )
            elif "UINT16" in data_type_str:
                if not (0 <= value <= 65535):
                    QMessageBox.warning(self, "Value Error", "UINT16 value must be in range 0-65535.")
                    return
                # For 16-bit unsigned, pass the integer directly
                response = self.modbus_client.write_register(
                    address=address,
                    value=value, # Pass the integer directly
                    slave=slave_id
                )
            elif "INT16" in data_type_str:
                if not (-32768 <= value <= 32767):
                    QMessageBox.warning(self, "Value Error", "INT16 value must be in range -32768 to 32767.")
                    return
                # For 16-bit signed, pass the integer directly
                response = self.modbus_client.write_register(
                    address=address,
                    value=value, # Pass the integer directly
                    slave=slave_id
                )
            elif "UINT32" in data_type_str:
                if not (0 <= value <= 4294967295):
                    QMessageBox.warning(self, "Value Error", "UINT32 value must be in range 0-4294967295.")
                    return
                raw_bytes = uint32_to_bytes(value, swap_type)
                registers_to_write = convert_bytes_to_modbus_registers_for_writing(raw_bytes) # Zmieniono wywołanie
                num_registers_needed = 2
            elif "INT32" in data_type_str:
                if not (-2147483648 <= value <= 2147483647):
                    QMessageBox.warning(self, "Value Error", "INT32 value must be in range -2147483648 to 2147483647.")
                    return
                raw_bytes = int32_to_bytes(value, swap_type)
                registers_to_write = convert_bytes_to_modbus_registers_for_writing(raw_bytes) # Zmieniono wywołanie
                num_registers_needed = 2
            elif "FLOAT32" in data_type_str:
                raw_bytes = float32_to_bytes(value, swap_type)
                registers_to_write = convert_bytes_to_modbus_registers_for_writing(raw_bytes) # Zmieniono wywołanie
                num_registers_needed = 2
            elif "UINT64" in data_type_str: # New 64-bit types
                if not (0 <= value <= 18446744073709551615):
                    QMessageBox.warning(self, "Value Error", "UINT64 value must be in range 0-18446744073709551615.")
                    return
                raw_bytes = uint64_to_bytes(value, swap_type)
                registers_to_write = convert_bytes_to_modbus_registers_for_writing(raw_bytes) # Zmieniono wywołanie
                num_registers_needed = 4
            elif "INT64" in data_type_str:
                if not (-9223372036854775808 <= value <= 9223372036854775807):
                    QMessageBox.warning(self, "Value Error", "INT64 value must be in range -9223372036854775808 to 9223372036854775807.")
                    return
                raw_bytes = int64_to_bytes(value, swap_type)
                registers_to_write = convert_bytes_to_modbus_registers_for_writing(raw_bytes) # Zmieniono wywołanie
                num_registers_needed = 4
            elif "FLOAT64" in data_type_str:
                raw_bytes = float64_to_bytes(value, swap_type)
                registers_to_write = convert_bytes_to_modbus_registers_for_writing(raw_bytes) # Zmieniono wywołanie
                num_registers_needed = 4
            else:
                QMessageBox.warning(self, "Error", "Unknown data type for writing.")
                return

            # For 32-bit and 64-bit types, use write_registers
            if num_registers_needed > 1:
                response = self.modbus_client.write_registers(
                    address=address,
                    values=registers_to_write,
                    slave=slave_id
                )
            # If boolean or 16-bit, response is already handled above by write_coil/write_register

            if response and response.isError():
                QMessageBox.critical(self.centralWidget(), "Write Error", f"Modbus Error during write: {response}")
                self.status_message_label.setText(f"Write error: {response}")
                self.status_message_label.setStyleSheet(f"color: {STATUS_RED};")
            elif response: # This covers successful register/coil writes that return a response object
                self.status_message_label.setText(f"Successfully wrote {value} to address {address}.")
                self.status_message_label.setStyleSheet("")
                # After writing, refresh data to see the change
                self.read_modbus_data()
            else:
                # This else block is for cases where response is None (e.g., boolean write success does not return a response object with isError())
                # For write_coil, if no exception, it's generally successful.
                if "Boolean" in data_type_str:
                    self.status_message_label.setText(f"Successfully wrote {value} to address {address}.")
                    self.status_message_label.setStyleSheet("")
                    self.read_modbus_data()
                else:
                    QMessageBox.critical(self.centralWidget(), "Write Error", "No response from device during write operation.")
                    self.status_message_label.setText("No response during write operation.")
                    self.status_message_label.setStyleSheet(f"color: {STATUS_RED};")

        except ModbusException as e:
            QMessageBox.critical(self, "Modbus Error", f"Modbus communication error during write: {e}")
            self.status_message_label.setText(f"Modbus communication error during write: {e}")
            self.status_message_label.setStyleSheet(f"color: {STATUS_RED};")
            self.update_connection_buttons()
        except ValueError as e:
            QMessageBox.critical(self, "Value Error", f"Entered value is not valid for the selected type: {e}")
            self.status_message_label.setText(f"Error: Entered value is not valid: {e}")
            self.status_message_label.setStyleSheet(f"color: {STATUS_RED};")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"An unexpected error occurred during write: {e}")
            self.status_message_label.setText(f"Unexpected write error: {e}")
            self.status_message_label.setStyleSheet(f"color: {STATUS_RED};")

    # --- Log transmisji, skaner, ustawienia ---

    def log(self, kind, channel, data=b"", text=""):
        """Wpis do panelu logów (wywoływany przez klienta Modbus w wątku GUI)."""
        self.log_window.add_entry((datetime.now(), kind, channel, bytes(data), text))

    def on_log_toggled(self, visible):
        self.log_window.setVisible(visible)

    def show_log_window(self):
        self.show_log_btn.setChecked(True)

    def show_scanner_window(self):
        if self.scanner_window is None:
            self.scanner_window = ScannerWindow(self)
        self.scanner_window.show()
        self.scanner_window.raise_()
        self.scanner_window.activateWindow()

    SETTINGS_WIDGETS = { # klucz w pymms.ini -> nazwa widgetu
        "baud": "baud_rate_cb", "parity": "parity_cb", "data_bits": "data_bits_cb", "stop_bits": "stop_bits_cb",
        "function": "modbus_function_cb", "data_type": "data_type_cb", "byte_order": "swap_bytes_cb",
    }
    SETTINGS_SPINS = {
        "timeout": "timeout_ms_sb", "slave_id": "slave_id_sb", "address": "start_address_sb",
        "quantity": "num_registers_sb", "interval": "polling_interval_ms_sb",
    }

    def apply_defaults(self):
        """Wartości domyślne wszystkich zapamiętywanych ustawień (start bez pymms.ini i przycisk Restore defaults)."""
        self.set_connection_type("RTU")
        if self.com_port_cb.count():
            self.com_port_cb.setCurrentIndex(0)
        self.baud_rate_cb.setCurrentText("9600")
        self.data_bits_cb.setCurrentText("8")
        self.parity_cb.setCurrentText("N")
        self.stop_bits_cb.setCurrentText("1")
        self.ip_address_le.setText("192.168.1.1")
        self.port_le.setText("502")
        self.timeout_ms_sb.setValue(500)
        self.slave_id_sb.setValue(1)
        self.modbus_function_cb.setCurrentIndex(0)
        self.data_type_cb.setCurrentIndex(0)
        self.swap_bytes_cb.setCurrentIndex(0)
        self.on_modbus_function_changed()
        self.start_address_sb.setValue(0)
        self.num_registers_sb.setValue(10)
        self.polling_interval_ms_sb.setValue(1000)
        self.show_log_btn.setChecked(True)
        self.resize(1280, 780)
        self.hsplit.setSizes([240, 1000])
        self.vsplit.setSizes([420, 260])

    def restore_defaults(self):
        answer = QMessageBox.question(
            self, "Restore defaults",
            "Restore default settings?\n\nConnection, request, data interpretation and window layout will be reset "
            "and pymms.ini will be cleared." + ("\nThe current connection will be closed." if self.modbus_client else ""))
        if answer != QMessageBox.StandardButton.Yes:
            return
        if self.modbus_client:
            self.disconnect_modbus()
        self.settings.clear()
        self.apply_defaults()
        self.save_settings()
        self.status_message_label.setText("Default settings restored.")
        self.status_message_label.setStyleSheet("")

    def restore_settings(self):
        s = self.settings
        self.apply_defaults()
        geometry = s.value("geometry")
        if geometry is not None:
            self.restoreGeometry(geometry)
        for key, splitter in (("hsplit", self.hsplit), ("vsplit", self.vsplit)):
            state = s.value(key)
            if state is not None:
                splitter.restoreState(state)
        self.set_connection_type(s.value("connection_type", "RTU"))
        port_index = self.com_port_cb.findData(s.value("port"))
        if port_index >= 0:
            self.com_port_cb.setCurrentIndex(port_index)
        for key, name in self.SETTINGS_WIDGETS.items():
            value = s.value(key)
            if value is not None:
                getattr(self, name).setCurrentText(str(value))
        self.on_modbus_function_changed()
        for key, name in self.SETTINGS_SPINS.items():
            value = s.value(key)
            if value is not None:
                getattr(self, name).setValue(int(value))
        self.ip_address_le.setText(str(s.value("ip", self.ip_address_le.text())))
        self.port_le.setText(str(s.value("tcp_port", self.port_le.text())))
        self.show_log_btn.setChecked(s.value("log_visible", True, type=bool))

    def save_settings(self):
        s = self.settings
        s.setValue("geometry", self.saveGeometry())
        s.setValue("hsplit", self.hsplit.saveState())
        s.setValue("vsplit", self.vsplit.saveState())
        s.setValue("connection_type", self.connection_type)
        if self.com_port_cb.currentData():
            s.setValue("port", self.com_port_cb.currentData())
        for key, name in self.SETTINGS_WIDGETS.items():
            s.setValue(key, getattr(self, name).currentText())
        for key, name in self.SETTINGS_SPINS.items():
            s.setValue(key, getattr(self, name).value())
        s.setValue("ip", self.ip_address_le.text())
        s.setValue("tcp_port", self.port_le.text())
        s.setValue("log_visible", self.show_log_btn.isChecked())
        s.sync()

    # --- Wykres ---

    def show_chart_window(self):
        if self.chart_window is None:
            self.chart_window = ChartWindow(self)
        self.chart_window.show()
        self.chart_window.raise_()
        self.chart_window.activateWindow()

    def row_series_key(self, row):
        """Identyfikator serii: Slave ID, funkcja, adres, typ danych i kolejność bajtów ostatniego odczytu."""
        p = self.last_read_params
        item = self.registers_table.item(row, 0)
        if not p or item is None:
            return None
        try:
            address = int(item.text())
        except ValueError:
            return None
        is_bits = "Coils" in p["function"] or "Discrete Inputs" in p["function"]
        data_type = "BOOL" if is_bits else p["data_type"].split()[0]
        order = p["byte_order"].split()[0] if data_type not in ("BOOL", "UINT16", "INT16") else ""
        return (p["slave"], p["function"][:4], address, data_type, order)

    @staticmethod
    def series_label(key):
        slave, function, address, data_type, order = key
        table = {"0x01": "CO", "0x02": "DI", "0x03": "HR", "0x04": "IR"}.get(function, function)
        return f"ID{slave} {table} {address} {data_type}" + (f" {order}" if order else "")

    def row_value(self, row):
        item = self.registers_table.item(row, 3)
        if item is None:
            return None
        text = item.text()
        if text in ("True", "False"):
            return 1.0 if text == "True" else 0.0
        try:
            return float(text)
        except ValueError:
            return None # np. niepełna wartość 32/64-bit

    def selected_result_rows(self):
        return sorted({index.row() for index in self.registers_table.selectionModel().selectedRows()})

    def table_context_menu(self, pos):
        row = self.registers_table.rowAt(pos.y())
        if row >= 0 and row not in self.selected_result_rows():
            self.registers_table.selectRow(row)
        rows = self.selected_result_rows()
        keys = [k for k in (self.row_series_key(r) for r in rows) if k]
        on_chart = [k for k in keys if self.chart_window and self.chart_window.has_series(k)]
        menu = QMenu(self)
        add = menu.addAction(icon(G_CHART), f"Add to chart ({len(keys)})" if len(keys) > 1 else "Add to chart",
                             lambda: self.add_rows_to_chart(rows))
        add.setEnabled(len(on_chart) < len(keys))
        remove = menu.addAction("Remove from chart", lambda: self.chart_window.remove_keys(on_chart))
        remove.setEnabled(bool(on_chart))
        menu.addSeparator()
        menu.addAction("Open chart", self.show_chart_window)
        menu.exec(self.registers_table.viewport().mapToGlobal(pos))

    def add_rows_to_chart(self, rows):
        if self.chart_window is None:
            self.chart_window = ChartWindow(self)
        t = time.time()
        values = {}
        for row in rows:
            key = self.row_series_key(row)
            value = self.row_value(row)
            if key is None or value is None:
                continue
            self.chart_window.add_series(key, self.series_label(key))
            values[key] = value
        self.chart_window.add_samples(t, values) # bieżąca wartość jako pierwsza próbka
        self.style_result_rows()
        self.show_chart_window()

    def feed_chart(self):
        """Po każdym udanym odczycie dopisuje próbki serii obecnych w tabeli wyników."""
        if not (self.chart_window and self.chart_window.series):
            return
        t = time.time()
        values = {}
        for row in range(self.registers_table.rowCount()):
            key = self.row_series_key(row)
            if key and self.chart_window.has_series(key):
                value = self.row_value(row)
                if value is not None:
                    values[key] = value
        self.chart_window.add_samples(t, values)

    def closeEvent(self, event):
        self.polling_timer.stop()
        self.save_settings()
        if self.scanner_window:
            self.scanner_window.close()
        if self.chart_window:
            self.chart_window.close()
        if self.modbus_client:
            self.modbus_client.close()
        super().closeEvent(event)


if __name__ == "__main__":
    try: # Własna ikona na pasku zadań Windows (zamiast ikony pythonw)
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("pymms.modbus.master.simulator")
    except Exception:
        pass
    app = QApplication(sys.argv)
    apply_theme(app)
    window = ModbusClientApp()
    window.show()
    sys.exit(app.exec())
