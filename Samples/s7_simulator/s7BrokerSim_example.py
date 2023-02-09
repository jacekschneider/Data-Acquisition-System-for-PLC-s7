import s7comm
import sys
import pyqtgraph as pg
from PyQt6.QtWidgets import (QApplication, QWidget, QPushButton, QLabel, 
                             QVBoxLayout, QHBoxLayout, QFormLayout, QGridLayout,
                             QMainWindow, QMessageBox)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal as Signal
from PyQt6.QtGui import QIcon



class TankGraphWidget(QWidget):
    def __init__(self, title:str, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Utils
        self.interval_update = 1000
        self.data_time = [0 for i in range(101)]
        self.data_pv = [0 for i in self.data_time]
        self.data_sp = [0 for i in self.data_time]
        
        # Prepare widgets 
        self.widget_graph = pg.PlotWidget()
        self.widget_graph.setYRange(0, 300)
        self.widget_graph.setTitle(title, color='w', size='18pt')
        self.widget_graph.setLabel('left',  "<span style=\"color:white;font-size:18px\">Level, cm</span>")
        self.widget_graph.setLabel('bottom',  "<span style=\"color:white;font-size:18px\">Time, s</span>")
        self.widget_graph.showGrid(x=True, y=True)
        self.widget_graph.addLegend()
        pen_w = pg.mkPen(color=(255,255,255))
        pen_g = pg.mkPen(color=(255,255,0))
        self.data_line_pv = self.widget_graph.plot(self.data_time, self.data_pv, pen=pen_w, name='Process Variable')
        self.data_line_sp = self.widget_graph.plot(self.data_time, self.data_sp, pen=pen_g, name='Set Point')
        
        self.widget_info = QWidget(self)
        self.layout_info = QFormLayout()
        self.label_pv = QLabel('xxx')
        self.label_cv = QLabel('xxx')
        self.label_sp = QLabel('xxx')
        self.layout_info.addRow('Process Variable:\t\t', self.label_pv)
        self.layout_info.addRow('Control Variable:\t\t', self.label_cv)
        self.layout_info.addRow('Set Point:\t\t', self.label_sp)
        self.widget_info.setLayout(self.layout_info)

        # Prepare Main layout
        self.layout_main = QGridLayout()
        self.layout_main.addWidget(self.widget_graph, 0, 0)
        self.layout_main.addWidget(self.widget_info, 1, 0, Qt.AlignmentFlag.AlignCenter)
        self.setLayout(self.layout_main)


    def update_plot(self, data:dict):
        self.data_time = self.data_time[1:]
        self.data_time.append(self.data_time[-1] + self.interval_update/1000)
        
        self.data_pv = self.data_pv[1:]
        self.data_pv.append(data['pv'])
        
        self.data_sp = self.data_sp[1:]
        self.data_sp.append(data['sp'])
        
        self.data_line_pv.setData(self.data_time, self.data_pv)
        self.data_line_sp.setData(self.data_time, self.data_sp)
        self.label_pv.setText(f"{data['pv']}")
        self.label_cv.setText(f"{data['cv']:.2f}%")
        self.label_sp.setText(f"{data['sp']}")
        
    def change_interval(self, interval:int):
        self.interval_update = interval
    
    def reset(self):
        self.data_time = [0 for i in range(101)]
        self.data_pv = [0 for i in self.data_time]
        self.data_sp = [0 for i in self.data_time]


class MainWindow(QMainWindow):
    # Signals
    signal_tank1 = Signal(dict)
    signal_tank2 = Signal(dict)
    signal_tank3 = Signal(dict)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setWindowTitle('S7broker Example')
        self.setWindowIcon(QIcon('Icons/Python.ico'))
        
        # Prepare Widgets
        self.widget_tank1 = TankGraphWidget('Tank1')
        self.widget_tank2 = TankGraphWidget('Tank2')
        self.widget_tank3 = TankGraphWidget('Tank3')
        self.btn_start = QPushButton('Start')
        self.btn_reset = QPushButton('Reset')
        self.btn_reset.setDisabled(True)
        
        # Button Widget
        self.btn_widget = QWidget(self)
        self.layout_btn_widget = QHBoxLayout()
        self.layout_btn_widget.addWidget(self.btn_start)
        self.layout_btn_widget.addWidget(self.btn_reset)
        self.btn_widget.setLayout(self.layout_btn_widget)
        
        # Main Widget
        self.widget_main = QWidget(self)
        self.layout_main = QGridLayout(self.widget_main)
        self.layout_main.addWidget(self.btn_widget, 0, 0)
        self.layout_main.addWidget(self.widget_tank1, 1, 0)
        self.layout_main.addWidget(self.widget_tank2, 1, 1)
        self.layout_main.addWidget(self.widget_tank3, 1, 2)
        
        # Timer plot refresh
        self.timer = QTimer()
        self.timer.setInterval(1000)
        self.timer.timeout.connect(self.update_plots)
        self.timer.start()
        
        # Connections
        self.btn_start.clicked.connect(self.start)
        self.btn_reset.clicked.connect(self.reset)
        self.signal_tank1.connect(self.widget_tank1.update_plot)
        self.signal_tank2.connect(self.widget_tank2.update_plot)
        self.signal_tank3.connect(self.widget_tank3.update_plot)

        self.setCentralWidget(self.widget_main)
        self.show()
        
    def start(self):
        self.broker = s7comm.BrokerSim('logs/plc_data.txt','ExchangeData.xlsx')
        self.broker.auto_config()
        self.broker.start()
        self.btn_reset.setDisabled(False)
        self.btn_start.setDisabled(True)
    
    def reset(self):
        self.broker.stop()
        self.broker.join()
        del self.broker
        self.btn_reset.setDisabled(True)
        self.btn_start.setDisabled(False)
        self.widget_tank1.reset()
        self.widget_tank2.reset()
        self.widget_tank3.reset()
        
    def update_plots(self):
        try:
            data_plc = self.broker.get_values()
            data_tank1 = self.prepare_plot_data(data_plc.loc['iT1_LVL'].Value, data_plc.loc['rT1_MV'].Value, data_plc.loc['iT1_SP'].Value)
            data_tank2 = self.prepare_plot_data(data_plc.loc['iT2_LVL'].Value, data_plc.loc['rT2_MV'].Value, data_plc.loc['iT2_SP'].Value)
            data_tank3 = self.prepare_plot_data(data_plc.loc['iT3_LVL'].Value, data_plc.loc['rT3_MV'].Value, data_plc.loc['iT3_SP'].Value)
            self.signal_tank1.emit(data_tank1)
            self.signal_tank2.emit(data_tank2)
            self.signal_tank3.emit(data_tank3)
            
        except AttributeError: pass # TO DO 
    
    def closeEvent(self, event):
        try: self.reset()
        except (AssertionError, RuntimeError, AttributeError): print('Closing ')
        
    def prepare_plot_data(self, pv:int, cv:float, sp:int):
        data = {
            'pv'    : pv,
            'cv'    : cv,
            'sp'    : sp,
        }
        return data
        
 
if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    sys.exit(app.exec())




