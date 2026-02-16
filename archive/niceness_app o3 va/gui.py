import sys
from PyQt6.QtWidgets import QApplication, QTableWidget, QTableWidgetItem, QWidget, QVBoxLayout
from PyQt6.QtGui import QColor
from niceness_app.weather_api import get_all_cities
from niceness_app.niceness import score

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Niceness Weather App")
        layout = QVBoxLayout(self)
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["City", "Temp (°F)", "Niceness"])
        layout.addWidget(self.table)
        self.refresh()

    def refresh(self):
        data = get_all_cities()
        data.sort(key=lambda d: score(d), reverse=True)
        self.table.setRowCount(len(data))
        for r, row in enumerate(data):
            niceness = score(row)
            self.table.setItem(r, 0, QTableWidgetItem(row["city"]))
            self.table.setItem(r, 1, QTableWidgetItem(f"{row['temp_f']:.1f}"))
            niceness_item = QTableWidgetItem(f"{niceness:.0f}")
            # color row background based on niceness
            g = int(255 * niceness / 100)
            niceness_item.setBackground(QColor(255 - g, g, 0, 40))
            self.table.setItem(r, 2, niceness_item)


def run():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.resize(400, 300)
    w.show()
    sys.exit(app.exec())
