import sys
import serial.tools.list_ports # Do wykrywania portów COM
import time # Import time for performance measurement
import logging # Import modułu logging
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGroupBox, QLineEdit, QPushButton, QComboBox, QLabel,
    QTableWidget, QTableWidgetItem, QDialog, QDialogButtonBox,
    QMessageBox, QSpinBox
)
from PyQt6.QtCore import Qt, QTimer, QObject # Dodano QObject dla disconnect

from pymodbus.client import ModbusTcpClient, ModbusSerialClient
from pymodbus.exceptions import ModbusException # Import bazowej klasy wyjątku Modbus
from struct import pack, unpack

# Konfiguracja logowania dla pymodbus, aby wyciszyć komunikaty o ponownych próbach/timeoutach
# Możesz zmienić poziom na logging.INFO lub logging.DEBUG, jeśli potrzebujesz więcej szczegółów
logging.getLogger("pymodbus.client").setLevel(logging.ERROR)


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
    __version__ = "1.06"

    def __init__(self):
        super().__init__()
        # Zmieniono tytuł aplikacji, dodając numer wersji
        self.setWindowTitle(f"pymms (Python Modbus Master Simulator) v{self.__version__}")
        self.setGeometry(100, 100, 900, 650) # x, y, width, height

        self.modbus_client = None
        self.connection_type = "RTU" # Default connection type

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
        self.populate_com_ports() # Populate list of available COM ports
        self.rtu_params_layout.addWidget(self.com_port_cb)
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
        self.ip_address_le = QLineEdit("127.0.0.1")
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
        self.timeout_ms_sb.setValue(1000) # Default 1 second
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
        self.registers_table.setColumnCount(4) # Register, Raw Value, Interpreted Value, Type
        self.registers_table.setHorizontalHeaderLabels([
            "Register", "Raw Value (HEX/Bool)", "Value", "Type"
        ])
        self.registers_table.cellDoubleClicked.connect(self.on_table_double_click)
        self.registers_table.setColumnWidth(0, 80)
        self.registers_table.setColumnWidth(1, 120)
        self.registers_table.setColumnWidth(2, 150)
        self.registers_table.horizontalHeader().setStretchLastSection(True) # Last column stretches
        main_layout.addWidget(self.registers_table)

        # Status Bar
        self.status_message_label = QLabel("Ready") # Persistent status message label
        self.status_message_label.setStyleSheet("color: black;") # Default color
        self.statusBar().addWidget(self.status_message_label, 1) # Add to status bar with stretch

        self.stats_label = QLabel("") # Persistent stats label
        self.statusBar().addPermanentWidget(self.stats_label) # Add to status bar, right-aligned
        self.update_status_bar_stats() # Initial update

    def populate_com_ports(self):
        """Populates the QComboBox with available COM ports."""
        ports = serial.tools.list_ports.comports()
        if not ports:
            self.com_port_cb.addItem("No COM Ports")
            self.com_port_cb.setEnabled(False)
            return

        self.com_port_cb.clear()
        for port in ports:
            self.com_port_cb.addItem(port.device)
        self.com_port_cb.setEnabled(True)

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
            stats_text += f" | Last: {self.last_response_time:.3f}s"
            stats_text += f" | Avg: {self.average_response_time:.3f}s"
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
                self.modbus_client = ModbusSerialClient(
                    port=port,
                    baudrate=baudrate,
                    parity=parity,
                    bytesize=data_bits,
                    stopbits=stop_bits,
                    timeout=timeout_seconds, # Use new timeout
                    retries=0 # Set retries to 0 to prevent automatic retries
                )
            else: # TCP
                host = self.ip_address_le.text()
                port = int(self.port_le.text())
                self.modbus_client = ModbusTcpClient(
                    host=host,
                    port=port,
                    timeout=timeout_seconds, # Use new timeout
                    retries=0 # Set retries to 0 to prevent automatic retries
                )

            if self.modbus_client.connect():
                self.status_message_label.setText(f"Connected to Modbus ({self.connection_type}).")
                self.status_message_label.setStyleSheet("") # Reset color on success
            else:
                self.status_message_label.setText(f"Connection error with Modbus ({self.connection_type}). Check parameters.")
                self.status_message_label.setStyleSheet("color: red;") # Set color to red on error
                self.modbus_client = None # Reset client on error
        except Exception as e:
            self.status_message_label.setText(f"Configuration or connection error: {e}")
            self.status_message_label.setStyleSheet("color: red;") # Set color to red on error
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
                self.status_message_label.setStyleSheet("color: orange;") # Użyj pomarańczowego dla ostrzeżenia/braku danych
            
            self.update_status_bar_stats() # Update status bar with new stats

        except ModbusException as e: # Catch all Modbus communication errors, including timeouts
            error_message = str(e)
            # Obsługa komunikatów o braku odpowiedzi lub zamknięciu połączenia (traktujemy jako timeout)
            self.status_message_label.setText(f"Modbus communication error: {error_message}")
            self.status_message_label.setStyleSheet("color: red;")
            
            self.failed_polls += 1 # Zwiększenie licznika nieudanych odpytań dla każdego błędu Modbus
            self.update_status_bar_stats() # Natychmiastowa aktualizacja statystyk
            
            # Dodatkowo, jeśli błąd wskazuje na utratę połączenia, zatrzymaj cykliczne odczyty.
            if "CLOSING CONNECTION" in error_message or "timed out" in error_message:
                if self.polling_timer.isActive():
                    self.stop_cyclic_read()
            return # Nie kontynuujemy wyświetlania rejestrów w przypadku błędu

        except Exception as e: # Catch any other unexpected errors
            self.status_message_label.setText(f"An unexpected error occurred: {e}")
            self.status_message_label.setStyleSheet("color: red;")
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
                self.registers_table.setItem(row_count, 1, QTableWidgetItem(str(bit_value))) # Raw value is boolean
                self.registers_table.setItem(row_count, 2, QTableWidgetItem(str(bit_value))) # Interpreted value is boolean
                self.registers_table.setItem(row_count, 3, QTableWidgetItem("Boolean"))
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
                self.registers_table.setItem(row_count, 1, QTableWidgetItem(raw_hex_str))

                # Column 2: Interpreted Value
                self.registers_table.setItem(row_count, 2, QTableWidgetItem(str(interpreted_value)))

                # Column 3: Type (for informational purposes only, not editable)
                self.registers_table.setItem(row_count, 3, QTableWidgetItem(data_type + (f" ({swap_type})" if num_words_needed > 1 else "")))

                row_count += 1
                i += num_words_needed # Skip by appropriate number of registers (1 for 16-bit, 2 for 32-bit, 4 for 64-bit)

    def on_table_double_click(self, row, column):
        """Handles double-click on a table cell, opening the value editor."""
        if column != 2: # Allow editing only the interpreted value column
            return

        item_address = self.registers_table.item(row, 0) # Register Address
        item_current_value = self.registers_table.item(row, 2) # Current Value
        item_data_type = self.registers_table.item(row, 3) # Data Type (e.g., "UINT16 (16-bit Unsigned)")

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
                self.status_message_label.setStyleSheet("color: red;")
            elif response: # This covers successful register/coil writes that return a response object
                self.status_message_label.setText(f"Successfully wrote {value} to address {address}.")
                self.status_message_label.setStyleSheet("color: black;")
                # After writing, refresh data to see the change
                self.read_modbus_data()
            else:
                # This else block is for cases where response is None (e.g., boolean write success does not return a response object with isError())
                # For write_coil, if no exception, it's generally successful.
                if "Boolean" in data_type_str:
                    self.status_message_label.setText(f"Successfully wrote {value} to address {address}.")
                    self.status_message_label.setStyleSheet("color: black;")
                    self.read_modbus_data()
                else:
                    QMessageBox.critical(self.centralWidget(), "Write Error", "No response from device during write operation.")
                    self.status_message_label.setText("No response during write operation.")
                    self.status_message_label.setStyleSheet("color: red;")

        except ModbusException as e:
            QMessageBox.critical(self, "Modbus Error", f"Modbus communication error during write: {e}")
            self.status_message_label.setText(f"Modbus communication error during write: {e}")
            self.status_message_label.setStyleSheet("color: red;")
        except ValueError as e:
            QMessageBox.critical(self, "Value Error", f"Entered value is not valid for the selected type: {e}")
            self.status_message_label.setText(f"Error: Entered value is not valid: {e}")
            self.status_message_label.setStyleSheet("color: red;")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"An unexpected error occurred during write: {e}")
            self.status_message_label.setText(f"Unexpected write error: {e}")
            self.status_message_label.setStyleSheet("color: red;")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ModbusClientApp()
    window.show()
    sys.exit(app.exec())
