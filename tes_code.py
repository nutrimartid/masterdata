import sys
from PyQt5 import QtCore, QtGui, uic, QtWidgets
from PyQt5.QtWidgets import QMainWindow, QApplication
from PyQt5.QtCore import *
import pandas as pd
 
qtCreatorFile = "tes.ui"
 
Ui_MainWindow, QtBaseClass = uic.loadUiType(qtCreatorFile)
 
class MyApp(QtWidgets.QMainWindow, Ui_MainWindow):
    def __init__(self):
        QtWidgets.QMainWindow.__init__(self)
        Ui_MainWindow.__init__(self)
        self.setupUi(self)
        self.label = QtWidgets.QLabel(self)
        self.pushButton.clicked.connect(self.inputdata)
        self.calendarWidget_2.setMinimumDate(self.calendarWidget.selectedDate())
        self.calendarWidget.clicked.connect(self.calendarWidget_2.setMinimumDate)

    def inputdata(self):
        total = str(self.lineEdit_2.text())
        nama = str(self.lineEdit.text())
        channel = str(self.comboBox_3.currentText())
        start_date = str(self.calendarWidget.selectedDate().toString("yyyy-MM-dd"))
        end_date = str(self.calendarWidget_2.selectedDate().toString("yyyy-MM-dd"))
        data_bulan = pd.DataFrame([{'Bulan' : 'December', 'Number' : 12} ,
            {'Bulan' : 'January' , 'Number': 1},
            {'Bulan' : 'February' , 'Number': 2},
            {'Bulan' : 'March' , 'Number': 3},
            {'Bulan' : 'April' , 'Number': 4},
            {'Bulan' : 'May' , 'Number': 5},
            {'Bulan' : 'June', 'Number': 6},
            {'Bulan' : 'July' , 'Number': 7},
            {'Bulan' : 'August', 'Number' : 8},
            {'Bulan' : 'September', 'Number' : 9},
            {'Bulan' : 'October' , 'Number': 10},
            {'Bulan' : 'November' , 'Number': 11}])
        data_MC = pd.read_excel(r'SQL/Marketing Cost.xlsx')
        data_MC = data_MC.append({'Start Date' : pd.to_datetime(start_date),
                                    'Start Month' : data_bulan[data_bulan['Number'] == int(start_date[5:7])]['Bulan'].values[0],
                                    'Start Year' : int(start_date[0:4]),
                                    'End Date' : pd.to_datetime(end_date),
                                    'End Month' : data_bulan[data_bulan['Number'] == int(end_date[5:7])]['Bulan'].values[0],
                                    'End Year' : int(end_date[0:4]),
                                    'Channel' : channel,
                                    'Nama' : nama,
                                    'Value' : total
                                    }, ignore_index = True)
        data_MC.to_excel(r'Marketing Cost.xlsx', index = False)                        
        self.label.move(350,850)
        self.label.setText('Upload Success')

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MyApp()
    window.show()
    sys.exit(app.exec_())