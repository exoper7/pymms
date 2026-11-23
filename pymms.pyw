import sys
import os
import csv
import socket
import struct
import serial
import serial.tools.list_ports # Do wykrywania portów COM
import time # Import time for performance measurement
from datetime import datetime
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGroupBox, QLineEdit, QPushButton, QComboBox, QLabel, QCheckBox,
    QTableWidget, QTableWidgetItem, QDialog, QDialogButtonBox,
    QMessageBox, QSpinBox, QPlainTextEdit, QFileDialog, QProgressBar, QAbstractItemView
)
from PyQt6.QtCore import Qt, QTimer, QObject # Dodano QObject dla disconnect
from PyQt6.QtGui import QIcon, QColor, QFont, QTextCursor, QTextCharFormat

from struct import pack, unpack


def resource_path(name):
    """Ścieżka do zasobu - działa niezależnie od katalogu roboczego, także w exe zbudowanym przez PyInstaller."""
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, name)


# --- Akcent kolorystyczny (odróżnia pymms od pymss przy pracy obok siebie) ---

ACCENT = "#8FCE00"        # zielony - nagłówek tabeli, pasek statusu, ramki, zaznaczenie
ACCENT_TEXT = "#4F7300"   # ciemniejsza zieleń - tytuły sekcji (czytelna na jasnym tle)
ACCENT_BORDER = "#7DB300" # linie między kolumnami nagłówka
STATUS_RED = "#B00020"    # kolory komunikatów czytelne na zielonym pasku statusu
STATUS_ORANGE = "#8A4B00"

ACCENT_STYLESHEET = f"""
QGroupBox {{
    border: 1px solid {ACCENT};
    border-radius: 4px;
    margin-top: 10px;
    padding-top: 6px;
}}
QGroupBox::title {{
    subcontrol-origin: margin;
    subcontrol-position: top left;
    left: 8px;
    padding: 0 4px;
    color: {ACCENT_TEXT};
    font-weight: bold;
}}
QHeaderView::section {{
    background-color: {ACCENT};
    color: #1A1A1A;
    font-weight: bold;
    padding: 4px;
    border: none;
    border-right: 1px solid {ACCENT_BORDER};
}}
QTableWidget {{
    gridline-color: #D0D7E0;
    selection-background-color: {ACCENT};
    selection-color: #1A1A1A;
}}
QStatusBar {{
    background-color: {ACCENT};
    color: #1A1A1A;
}}
QStatusBar QLabel {{
    color: #1A1A1A;
}}
"""


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


class LogWindow(QWidget):
    """Okno pokazujące dokładną transmisję (surowe ramki HEX + dekodowanie) - jak w pymss."""
    COLORS = {"RX": "#0050B4", "TX": "#007A00", "INFO": "#606060", "ERROR": "#C00000"}

    def __init__(self, parent=None):
        super().__init__(parent, Qt.WindowType.Window)
        self.setWindowTitle("pymms - Transmission Log")
        self.resize(1100, 500)
        self.entry_count = 0
        self.dropped_count = 0

        layout = QVBoxLayout(self)

        options_layout = QHBoxLayout()
        self.show_rx_cb = QCheckBox("RX")
        self.show_tx_cb = QCheckBox("TX")
        self.show_info_cb = QCheckBox("Info")
        self.show_errors_cb = QCheckBox("Errors")
        self.decode_cb = QCheckBox("Decode frames")
        self.autoscroll_cb = QCheckBox("Auto-scroll")
        self.pause_cb = QCheckBox("Pause")
        for cb in (self.show_rx_cb, self.show_tx_cb, self.show_info_cb, self.show_errors_cb,
                   self.decode_cb, self.autoscroll_cb):
            cb.setChecked(True)
        for cb in (self.show_rx_cb, self.show_tx_cb, self.show_info_cb, self.show_errors_cb):
            options_layout.addWidget(cb)
        options_layout.addSpacing(20)
        options_layout.addWidget(self.decode_cb)
        options_layout.addWidget(self.autoscroll_cb)
        options_layout.addWidget(self.pause_cb)
        options_layout.addSpacing(20)
        options_layout.addWidget(QLabel("Max lines:"))
        self.max_lines_sb = QSpinBox()
        self.max_lines_sb.setRange(100, 1000000)
        self.max_lines_sb.setSingleStep(1000)
        self.max_lines_sb.setValue(20000)
        self.max_lines_sb.valueChanged.connect(lambda v: self.text.setMaximumBlockCount(v))
        options_layout.addWidget(self.max_lines_sb)
        options_layout.addStretch()
        self.clear_btn = QPushButton("Clear")
        self.clear_btn.clicked.connect(self.clear)
        options_layout.addWidget(self.clear_btn)
        self.save_btn = QPushButton("Save...")
        self.save_btn.clicked.connect(self.save)
        options_layout.addWidget(self.save_btn)
        layout.addLayout(options_layout)

        self.text = QPlainTextEdit()
        self.text.setReadOnly(True)
        self.text.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self.text.setMaximumBlockCount(self.max_lines_sb.value())
        font = QFont("Consolas")
        font.setStyleHint(QFont.StyleHint.Monospace)
        font.setPointSize(9)
        self.text.setFont(font)
        layout.addWidget(self.text)

        self.info_label = QLabel()
        self.info_label.setStyleSheet(f"background-color: {ACCENT}; color: #1A1A1A; padding: 3px 6px;")
        layout.addWidget(self.info_label)
        self.update_info()

        self.formats = {}
        for kind, color in self.COLORS.items():
            fmt = QTextCharFormat()
            fmt.setForeground(QColor(color))
            self.formats[kind] = fmt

    def update_info(self):
        text = f"Entries: {self.entry_count}"
        if self.dropped_count:
            text += f" | Skipped while paused: {self.dropped_count}"
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

        line = f"{timestamp:%H:%M:%S}.{timestamp.microsecond // 1000:03d}  {kind:<5} [{channel}]"
        if data:
            line += f"  ({len(data):>3} B)  {to_hex(data)}"
            if self.decode_cb.isChecked() and text:
                line += f"\n{'':<20}-> {text}" # dekodowanie w osobnej, wciętej linii
        else:
            line += f"  {text}"

        cursor = QTextCursor(self.text.document())
        cursor.movePosition(QTextCursor.MoveOperation.End)
        if not self.text.document().isEmpty():
            cursor.insertBlock()
        cursor.insertText(line, self.formats[kind])
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
    STATUS_COLORS = {"ok": QColor(214, 240, 170), "exception": QColor(255, 226, 170), "error": QColor(255, 200, 200)}

    def __init__(self, app_window):
        super().__init__(app_window, Qt.WindowType.Window)
        self.app_window = app_window # główne okno pymms (połączenie, log)
        self.setWindowTitle("pymms - Scanner")
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
        self.start_btn = QPushButton("Start Scan")
        self.start_btn.clicked.connect(self.start_scan)
        controls.addWidget(self.start_btn)
        self.stop_btn = QPushButton("Stop")
        self.stop_btn.clicked.connect(lambda: self.finish_scan("Scan stopped by user."))
        self.stop_btn.setEnabled(False)
        controls.addWidget(self.stop_btn)
        self.progress = QProgressBar()
        self.progress.setTextVisible(True)
        controls.addWidget(self.progress, 1)
        self.export_btn = QPushButton("Export CSV...")
        self.export_btn.setToolTip("Saves successful readings as a pymss register map (can be imported into pymss)")
        self.export_btn.clicked.connect(self.export_csv)
        controls.addWidget(self.export_btn)
        self.clear_btn = QPushButton("Clear")
        self.clear_btn.clicked.connect(self.clear_results)
        controls.addWidget(self.clear_btn)
        layout.addLayout(controls)

        # --- Wyniki ---
        self.table = QTableWidget()
        self.table.setColumnCount(len(self.COLUMNS))
        self.table.setHorizontalHeaderLabels(self.COLUMNS)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.verticalHeader().setVisible(False)
        for col, width in enumerate([70, 90, 100, 300, 75, 150]):
            self.table.setColumnWidth(col, width)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        self.summary_label = QLabel("Ready")
        self.summary_label.setStyleSheet(f"background-color: {ACCENT}; color: #1A1A1A; padding: 3px 6px;")
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
    __version__ = "1.13"

    def __init__(self):
        super().__init__()
        # Zmieniono tytuł aplikacji, dodając numer wersji
        self.setWindowTitle(f"pymms (Python Modbus Master Simulator) v{self.__version__}")
        self.setGeometry(100, 100, 900, 650) # x, y, width, height

        # Ustawienie ikony aplikacji
        try:
            self.setWindowIcon(QIcon(resource_path('icon.png')))
        except Exception as e:
            # W przypadku problemów z ładowaniem ikony, wyświetl błąd na pasku statusu (jeśli jest już zainicjalizowany)
            # Lub po prostu wypisz do konsoli, jeśli pasku statusu jeszcze nie ma
            print(f"Błąd ładowania ikony: {e}. Upewnij się, że plik 'icon.png' znajduje się w tym samym folderze co aplikacja.")


        self.modbus_client = None
        self.connection_type = "RTU" # Default connection type
        self.log_window = LogWindow(self) # Okno logów transmisji
        self.scanner_window = None # Okno skanera (tworzone przy pierwszym otwarciu)

        # Initialize button attributes before calling init_ui
        self.connect_btn = None
        self.disconnect_btn = None
        self.read_data_btn = None # Renamed from read_holding_registers_btn
        self.start_cyclic_read_btn = None # New button for cyclic read
        self.stop_cyclic_read_btn = None  # New button for cyclic read
        self.polling_timer = QTimer(self) # Initialize QTimer
        self.polling_timer.timeout.connect(self.read_modbus_data) # Connect timer to read function

        # Statistics variables
        self.total_polls = 0
        self.failed_polls = 0 # Now storing failed polls
        self.last_response_time = 0.0
        self.sum_response_times = 0.0
        self.average_response_time = 0.0

        self.init_ui()

    def init_ui(self):
        """Initializes the user interface."""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)

        # --- Connection Configuration Section ---
        connection_group = QGroupBox("Connection Configuration")
        connection_layout = QHBoxLayout()
        connection_group.setLayout(connection_layout)

        # Connection Type
        connection_layout.addWidget(QLabel("Type:"))
        self.connection_type_cb = QComboBox()
        self.connection_type_cb.addItems(["RTU (Serial)", "TCP/IP"])
        self.connection_type_cb.currentIndexChanged.connect(self.on_connection_type_changed)
        connection_layout.addWidget(self.connection_type_cb)

        # RTU Parameters
        self.rtu_params_widget = QWidget() # New widget for RTU parameters
        self.rtu_params_layout = QHBoxLayout(self.rtu_params_widget) # Set layout for this widget
        self.rtu_params_layout.addWidget(QLabel("COM Port:"))
        self.com_port_cb = QComboBox()
        self.com_port_cb.setMinimumWidth(110)
        self.populate_com_ports() # Populate list of available COM ports
        self.rtu_params_layout.addWidget(self.com_port_cb)
        self.refresh_ports_btn = QPushButton("↻")
        self.refresh_ports_btn.setToolTip("Refresh COM port list")
        self.refresh_ports_btn.setFixedWidth(28)
        self.refresh_ports_btn.clicked.connect(self.populate_com_ports)
        self.rtu_params_layout.addWidget(self.refresh_ports_btn)
        self.rtu_params_layout.addWidget(QLabel("Baud Rate:"))
        self.baud_rate_cb = QComboBox()
        self.baud_rate_cb.addItems(["9600", "19200", "38400", "57600", "115200"])
        self.baud_rate_cb.setCurrentText("9600")
        self.rtu_params_layout.addWidget(self.baud_rate_cb)
        self.rtu_params_layout.addWidget(QLabel("Parity:"))
        self.parity_cb = QComboBox()
        self.parity_cb.addItems(["N", "E", "O"]) # None, Even, Odd
        self.parity_cb.setCurrentText("N")
        self.rtu_params_layout.addWidget(self.parity_cb)
        self.rtu_params_layout.addWidget(QLabel("Data Bits:"))
        self.data_bits_cb = QComboBox()
        self.data_bits_cb.addItems(["7", "8"])
        self.data_bits_cb.setCurrentText("8")
        self.rtu_params_layout.addWidget(self.data_bits_cb)
        self.rtu_params_layout.addWidget(QLabel("Stop Bits:"))
        self.stop_bits_cb = QComboBox()
        self.stop_bits_cb.addItems(["1", "2"])
        self.stop_bits_cb.setCurrentText("1")
        self.rtu_params_layout.addWidget(self.stop_bits_cb)

        # TCP Parameters
        self.tcp_params_widget = QWidget() # New widget for TCP parameters
        self.tcp_params_layout = QHBoxLayout(self.tcp_params_widget) # Set layout for this widget
        self.tcp_params_layout.addWidget(QLabel("IP Address:"))
        self.ip_address_le = QLineEdit("192.168.1.1")
        self.tcp_params_layout.addWidget(self.ip_address_le)
        self.tcp_params_layout.addWidget(QLabel("Port:"))
        self.port_le = QLineEdit("502")
        self.tcp_params_layout.addWidget(self.port_le)

        connection_layout.addWidget(self.rtu_params_widget) # Add widget to main connection layout
        connection_layout.addWidget(self.tcp_params_widget) # Add widget to main connection layout
        self.tcp_params_widget.hide() # Hide TCP at start

        # Timeout Parameter
        connection_layout.addWidget(QLabel("Timeout (ms):"))
        self.timeout_ms_sb = QSpinBox()
        self.timeout_ms_sb.setRange(100, 30000) # 100ms to 30 seconds
        self.timeout_ms_sb.setValue(500) # Default 1 second
        connection_layout.addWidget(self.timeout_ms_sb)

        # Connection Buttons
        self.connect_btn = QPushButton("Connect")
        self.connect_btn.clicked.connect(self.connect_modbus)
        connection_layout.addWidget(self.connect_btn)
        self.disconnect_btn = QPushButton("Disconnect")
        self.disconnect_btn.clicked.connect(self.disconnect_modbus)
        self.disconnect_btn.setEnabled(False) # Set initial state
        connection_layout.addWidget(self.disconnect_btn)

        main_layout.addWidget(connection_group)

        # --- Read / Write Section ---
        read_write_group = QGroupBox("Modbus Operations")
        rw_layout = QVBoxLayout()
        read_write_group.setLayout(rw_layout)

        # Read Parameters
        read_params_layout = QHBoxLayout()
        read_params_layout.addWidget(QLabel("Slave ID:"))
        self.slave_id_sb = QSpinBox()
        self.slave_id_sb.setRange(1, 247) # Modbus slave ID range
        self.slave_id_sb.setValue(1)
        read_params_layout.addWidget(self.slave_id_sb)
        read_params_layout.addWidget(QLabel("Start Address:"))
        self.start_address_sb = QSpinBox()
        self.start_address_sb.setRange(0, 65535) # Modbus register address range
        self.start_address_sb.setValue(0)
        read_params_layout.addWidget(self.start_address_sb)
        read_params_layout.addWidget(QLabel("Quantity:"))
        self.num_registers_sb = QSpinBox()
        self.num_registers_sb.setRange(1, 2000) # Max quantity for coils/discrete inputs is 2000, for registers is 125
        self.num_registers_sb.setValue(10)
        read_params_layout.addWidget(self.num_registers_sb)

        # Modbus Function Selection
        self.modbus_function_cb = QComboBox()
        self.modbus_function_cb.addItems([
            "0x03 (Holding Registers)",
            "0x04 (Input Registers)",
            "0x01 (Coils)",
            "0x02 (Discrete Inputs)"
        ])
        self.modbus_function_cb.setCurrentText("0x03 (Holding Registers)")
        self.modbus_function_cb.currentIndexChanged.connect(self.on_modbus_function_changed)
        read_params_layout.addWidget(self.modbus_function_cb)


        self.read_data_btn = QPushButton("Read Data") # Renamed button
        self.read_data_btn.clicked.connect(self.read_modbus_data)
        self.read_data_btn.setEnabled(False) # Set initial state
        read_params_layout.addWidget(self.read_data_btn)
        read_params_layout.addStretch() # Stretch to push buttons to the right

        self.show_scanner_btn = QPushButton("Scanner")
        self.show_scanner_btn.setToolTip("Scan Slave IDs or register addresses")
        self.show_scanner_btn.clicked.connect(self.show_scanner_window)
        read_params_layout.addWidget(self.show_scanner_btn)

        self.show_log_btn = QPushButton("Transmission Log")
        self.show_log_btn.clicked.connect(self.show_log_window)
        read_params_layout.addWidget(self.show_log_btn)

        rw_layout.addLayout(read_params_layout)

        # New layout to hold Data Interpretation and Cyclic Read Options side-by-side
        interpretation_and_polling_layout = QHBoxLayout()

        # Data Interpretation Type (Left side of new layout)
        data_type_group = QGroupBox("Data Interpretation")
        data_type_vbox = QVBoxLayout()
        data_type_group.setLayout(data_type_vbox)

        data_type_h_layout = QHBoxLayout()
        data_type_h_layout.addWidget(QLabel("Interpret as:"))
        self.data_type_cb = QComboBox()
        self.data_type_cb.addItems(["UINT16 (16-bit Unsigned)", "INT16 (16-bit Signed)",
                                    "UINT32 (32-bit Unsigned)", "INT32 (32-bit Signed)",
                                    "FLOAT32 (32-bit Float)",
                                    "UINT64 (64-bit Unsigned)", "INT64 (64-bit Signed)",
                                    "FLOAT64 (64-bit Double)"]) # Added 64-bit types
        self.data_type_cb.setCurrentText("UINT16 (16-bit Unsigned)")
        self.data_type_cb.currentIndexChanged.connect(self.on_modbus_function_changed) # Connect to update quantity range
        data_type_h_layout.addWidget(self.data_type_cb)
        data_type_h_layout.addStretch()
        data_type_vbox.addLayout(data_type_h_layout)

        swap_bytes_h_layout = QHBoxLayout()
        swap_bytes_h_layout.addWidget(QLabel("Byte Order:"))
        self.swap_bytes_cb = QComboBox()
        self.swap_bytes_cb.addItems(["ABCD (No Swap)", "CDAB (Word Swap)", "BADC (Byte Swap within Words)", "DCBA (Full Byte Reverse)"]) # Added more swap options
        self.swap_bytes_cb.setCurrentIndex(0)
        swap_bytes_h_layout.addWidget(self.swap_bytes_cb)
        swap_bytes_h_layout.addStretch()
        data_type_vbox.addLayout(swap_bytes_h_layout)
        
        interpretation_and_polling_layout.addWidget(data_type_group)


        # Cyclic Read Options (Right side of new layout)
        cyclic_read_group = QGroupBox("Cyclic Read Options")
        cyclic_read_vbox = QVBoxLayout()
        cyclic_read_group.setLayout(cyclic_read_vbox)

        polling_interval_layout = QHBoxLayout()
        polling_interval_layout.addWidget(QLabel("Polling Interval (ms):"))
        self.polling_interval_ms_sb = QSpinBox()
        self.polling_interval_ms_sb.setRange(100, 60000) # 100ms to 60 seconds
        self.polling_interval_ms_sb.setValue(1000) # Default 1 second
        polling_interval_layout.addWidget(self.polling_interval_ms_sb)
        polling_interval_layout.addStretch()
        cyclic_read_vbox.addLayout(polling_interval_layout)

        cyclic_buttons_layout = QHBoxLayout()
        self.start_cyclic_read_btn = QPushButton("Start Cyclic Read")
        self.start_cyclic_read_btn.clicked.connect(self.start_cyclic_read)
        self.start_cyclic_read_btn.setEnabled(False)
        cyclic_buttons_layout.addWidget(self.start_cyclic_read_btn)

        self.stop_cyclic_read_btn = QPushButton("Stop Cyclic Read")
        self.stop_cyclic_read_btn.clicked.connect(self.stop_cyclic_read)
        self.stop_cyclic_read_btn.setEnabled(False)
        cyclic_buttons_layout.addWidget(self.stop_cyclic_read_btn)
        cyclic_buttons_layout.addStretch()
        cyclic_read_vbox.addLayout(cyclic_buttons_layout)
        
        interpretation_and_polling_layout.addWidget(cyclic_read_group)
        interpretation_and_polling_layout.addStretch() # Ensure groups don't expand too much

        rw_layout.addLayout(interpretation_and_polling_layout) # Add combined layout to Modbus Operations group

        main_layout.addWidget(read_write_group)

        # --- Results Table ---
        self.registers_table = QTableWidget()
        self.registers_table.setColumnCount(5) # Register, Raw Value, Interpreted Value, Type
        self.registers_table.setHorizontalHeaderLabels([
            "Register","Register (HEX)" ,"Raw Value (HEX/Bool)", "Value", "Type"
        ])
        self.registers_table.cellDoubleClicked.connect(self.on_table_double_click)
        self.registers_table.setColumnWidth(0, 100)
        self.registers_table.setColumnWidth(1, 100)
        self.registers_table.setColumnWidth(2, 130)
        self.registers_table.setColumnWidth(3, 150)
        self.registers_table.horizontalHeader().setStretchLastSection(True) # Last column stretches
        main_layout.addWidget(self.registers_table)

        # Status Bar
        self.status_message_label = QLabel("Ready") # Persistent status message label
        self.status_message_label.setStyleSheet("") # Default color
        self.statusBar().addWidget(self.status_message_label, 1) # Add to status bar with stretch

        self.stats_label = QLabel("") # Persistent stats label
        self.statusBar().addPermanentWidget(self.stats_label) # Add to status bar, right-aligned
        self.update_status_bar_stats() # Initial update

    def populate_com_ports(self):
        """Populates the QComboBox with available COM ports (keeps the current selection on refresh)."""
        current = self.com_port_cb.currentText()
        self.com_port_cb.clear()
        ports = serial.tools.list_ports.comports()
        if not ports:
            self.com_port_cb.addItem("No COM Ports")
            self.com_port_cb.setEnabled(False)
            return

        for port in sorted(ports, key=lambda p: (len(p.device), p.device)): # COM3 przed COM12
            self.com_port_cb.addItem(port.device)
            self.com_port_cb.setItemData(self.com_port_cb.count() - 1, port.description, Qt.ItemDataRole.ToolTipRole)
        self.com_port_cb.setEnabled(True)
        index = self.com_port_cb.findText(current)
        if index >= 0:
            self.com_port_cb.setCurrentIndex(index)

    def on_connection_type_changed(self, index):
        """Toggles visibility of RTU/TCP parameters based on selected connection type."""
        if index == 0: # RTU (Serial)
            self.connection_type = "RTU"
            self.rtu_params_widget.show() # Show widget with RTU parameters
            self.tcp_params_widget.hide() # Hide widget with TCP parameters
        else: # TCP/IP
            self.connection_type = "TCP"
            self.rtu_params_widget.hide() # Hide widget with RTU parameters
            self.tcp_params_widget.show() # Show widget with TCP parameters

    def on_modbus_function_changed(self, index):
        """Adjusts UI elements based on the selected Modbus function."""
        selected_function_text = self.modbus_function_cb.currentText()
        if "Coils" in selected_function_text or "Discrete Inputs" in selected_function_text:
            # For coils/discrete inputs, data types are boolean, so hide interpretation options
            self.data_type_cb.setCurrentText("UINT16 (16-bit Unsigned)") # Reset to default for consistency, though not used
            self.data_type_cb.setEnabled(False)
            self.swap_bytes_cb.setEnabled(False)
            self.num_registers_sb.setRange(1, 2000) # Max quantity for coils/discrete inputs
        else:
            # For registers, enable interpretation options
            self.data_type_cb.setEnabled(True)
            self.swap_bytes_cb.setEnabled(True)
            # Adjust max quantity based on selected data type for registers
            selected_data_type = self.data_type_cb.currentText()
            if "64-bit" in selected_data_type:
                self.num_registers_sb.setRange(4, 125) # Min 4 registers for 64-bit
            elif "32-bit" in selected_data_type:
                self.num_registers_sb.setRange(2, 125) # Min 2 registers for 32-bit
            else:
                self.num_registers_sb.setRange(1, 125) # Min 1 register for 16-bit

    def update_connection_buttons(self):
        """Updates the state of Connect/Disconnect/Read/Cyclic Read buttons."""
        is_connected = bool(self.modbus_client and self.modbus_client.connected)
        is_polling = self.polling_timer.isActive()

        # Connect/Disconnect buttons
        if self.connect_btn:
            self.connect_btn.setEnabled(not is_connected and not is_polling) # Cannot connect if polling
        if self.disconnect_btn:
            self.disconnect_btn.setEnabled(is_connected and not is_polling) # Cannot disconnect if polling

        # Read Data button
        if self.read_data_btn:
            self.read_data_btn.setEnabled(is_connected and not is_polling) # Cannot read manually if polling

        # Cyclic Read buttons
        if self.start_cyclic_read_btn:
            self.start_cyclic_read_btn.setEnabled(is_connected and not is_polling)
        if self.stop_cyclic_read_btn:
            self.stop_cyclic_read_btn.setEnabled(is_connected and is_polling)
        if self.polling_interval_ms_sb:
            self.polling_interval_ms_sb.setEnabled(not is_polling) # Disable interval input when polling

    def update_status_bar_stats(self):
        """Updates the status bar with polling statistics."""
        stats_text = f"Polls: {self.total_polls}"
        if self.failed_polls > 0:
            stats_text += f" (Failed: {self.failed_polls})" # Add failed count
        if self.total_polls > 0:
            stats_text += f" | Last: {self.last_response_time:.1f}ms"
            stats_text += f" | Avg: {self.average_response_time:.1f}ms"
        self.stats_label.setText(stats_text) # Update dedicated stats label


    def connect_modbus(self):
        """Establishes Modbus connection to the device."""
        if self.modbus_client and self.modbus_client.connected:
            self.disconnect_modbus()

        try:
            timeout_seconds = self.timeout_ms_sb.value() / 1000.0 # Convert ms to seconds

            if self.connection_type == "RTU":
                port = self.com_port_cb.currentText()
                if port == "No COM Ports":
                    QMessageBox.warning(self, "Port Error", "No COM port selected.")
                    return
                baudrate = int(self.baud_rate_cb.currentText())
                parity = self.parity_cb.currentText()
                data_bits = int(self.data_bits_cb.currentText())
                stop_bits = int(self.stop_bits_cb.currentText())
                self.modbus_client = ModbusRtuMaster(
                    port=port,
                    baudrate=baudrate,
                    parity=parity,
                    bytesize=data_bits,
                    stopbits=stop_bits,
                    timeout=timeout_seconds, # Use new timeout
                    log=self.log
                )
            else: # TCP
                host = self.ip_address_le.text()
                port = int(self.port_le.text())
                self.modbus_client = ModbusTcpMaster(
                    host=host,
                    port=port,
                    timeout=timeout_seconds, # Use new timeout
                    log=self.log
                )

            if self.modbus_client.connect():
                self.status_message_label.setText(f"Connected to Modbus ({self.connection_type}).")
                self.status_message_label.setStyleSheet("") # Reset color on success
            else:
                self.status_message_label.setText(f"Connection error with Modbus ({self.connection_type}). "
                                                  "Check parameters (details in Transmission Log).")
                self.status_message_label.setStyleSheet(f"color: {STATUS_RED};") # Set color to red on error
                self.modbus_client = None # Reset client on error
        except Exception as e:
            self.status_message_label.setText(f"Configuration or connection error: {e}")
            self.status_message_label.setStyleSheet(f"color: {STATUS_RED};") # Set color to red on error
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
        self.failed_polls = 0 # Reset failed polls
        self.last_response_time = 0.0
        self.sum_response_times = 0.0
        self.average_response_time = 0.0

        self.update_connection_buttons()
        self.registers_table.setRowCount(0) # Clear table on disconnect
        self.status_message_label.setText("Ready") # Reset status message
        self.status_message_label.setStyleSheet("") # Reset color
        self.update_status_bar_stats() # Update stats after resetting


    def start_cyclic_read(self):
        """Starts cyclic Modbus data reading."""
        if not (self.modbus_client and self.modbus_client.connected):
            QMessageBox.warning(self, "Connection Error", "Not connected to Modbus device to start cyclic read.")
            return

        interval_ms = self.polling_interval_ms_sb.value()
        if interval_ms <= 0:
            QMessageBox.warning(self, "Invalid Interval", "Polling interval must be greater than 0 ms.")
            return

        self.polling_timer.setInterval(interval_ms)
        self.polling_timer.start()
        self.status_message_label.setText(f"Cyclic read started with interval {interval_ms} ms.")
        self.status_message_label.setStyleSheet("") # Reset color on start
        self.update_connection_buttons()
        self.update_status_bar_stats()

    def stop_cyclic_read(self):
        """Stops cyclic Modbus data reading."""
        if self.polling_timer.isActive():
            self.polling_timer.stop()
            # Disconnect the signal to prevent multiple connections if started again.
            # This is safer than just stopping, as multiple connects could lead to issues.
            try:
                self.polling_timer.timeout.disconnect(self.read_modbus_data)
            except TypeError: # If already disconnected, this will raise TypeError
                pass
            self.polling_timer.timeout.connect(self.read_modbus_data) # Reconnect for next start
            self.status_message_label.setText("Cyclic read stopped.")
            self.status_message_label.setStyleSheet("") # Reset color on stop
        self.update_connection_buttons()
        self.update_status_bar_stats()

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

            self.display_registers(start_address, data_to_display, selected_function_text)
            
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

    # --- Okno logów transmisji ---

    def log(self, kind, channel, data=b"", text=""):
        """Wpis do okna logów (wywoływany przez klienta Modbus w wątku GUI)."""
        self.log_window.add_entry((datetime.now(), kind, channel, bytes(data), text))

    def show_log_window(self):
        self.log_window.show()
        self.log_window.raise_()
        self.log_window.activateWindow()

    def show_scanner_window(self):
        if self.scanner_window is None:
            self.scanner_window = ScannerWindow(self)
        self.scanner_window.show()
        self.scanner_window.raise_()
        self.scanner_window.activateWindow()

    def closeEvent(self, event):
        self.polling_timer.stop()
        if self.scanner_window:
            self.scanner_window.close()
        if self.modbus_client:
            self.modbus_client.close()
        self.log_window.close()
        super().closeEvent(event)


if __name__ == "__main__":
    try: # Własna ikona na pasku zadań Windows (zamiast ikony pythonw)
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("pymms.modbus.master.simulator")
    except Exception:
        pass
    app = QApplication(sys.argv)
    app.setStyleSheet(ACCENT_STYLESHEET)
    window = ModbusClientApp()
    window.show()
    sys.exit(app.exec())
