import s7broker
import sys
import pyqtgraph as pg
from PyQt6.QtWidgets import (QApplication, QWidget, QPushButton, QLabel, 
                             QVBoxLayout, QHBoxLayout, QFormLayout, QGridLayout)
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QFont



class TankGraphWidget(QWidget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Utils
        self.buffer_time = list(range(101))
        self.buffer_pv = [i for i in self.buffer_time]
        
        # Prepare widgets 
        self.widget_graph = pg.PlotWidget()
        self.widget_graph.setYRange(0, 300)
        self.widget_graph.setTitle('Tank', color='w', size='18pt')
        self.widget_graph.setLabel('left',  "<span style=\"color:white;font-size:18px\">Level, cm</span>")
        self.widget_graph.setLabel('bottom',  "<span style=\"color:white;font-size:18px\">Time, s</span>")
        self.widget_graph.showGrid(x=True, y=True)
        
        self.widget_info = QWidget(self)
        self.layout_info = QFormLayout()
        self.label_pv = QLabel('108.8', self)
        self.label_cv = QLabel('75', self)
        self.label_sp = QLabel('120', self)
        self.layout_info.addRow('Process Variable:\t\t', self.label_pv)
        self.layout_info.addRow('Control Variable:\t\t', self.label_cv)
        self.layout_info.addRow('Set Point:\t\t', self.label_sp)
        self.widget_info.setLayout(self.layout_info)

        
        # Prepare Main layout
        self.layout_main = QGridLayout()
        self.layout_main.addWidget(self.widget_graph, 0, 0)
        self.layout_main.addWidget(self.widget_info, 1, 0, Qt.AlignmentFlag.AlignCenter)
        
        self.setLayout(self.layout_main)
        self.show()




if __name__ == '__main__':
    app = QApplication(sys.argv)
    graph = TankGraphWidget()
    sys.exit(app.exec())




