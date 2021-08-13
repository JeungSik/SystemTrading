import sys
import sqlite3
import datetime
import numpy as np
import pandas as pd

from pandas import DataFrame

from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5 import uic
from PyQt5 import QtCore, QtWidgets, QtGui

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas

import mpl_finance

from dateutil.relativedelta import relativedelta


main_form = uic.loadUiType("simulation_trading_main.ui")[0]

stockTable_column = {'종목코드': 0, '종목명': 1, '구분': 2, '기준일': 3, '현재가': 4, '거래량': 5}
stockSimTable_column = {'일자': 0, '구분': 1, '수량': 2, '매매가': 3, '수수료': 4, '세금': 5, '수익률': 6, '수익금': 7,
                        '누적수익금': 8}
stockRemainTable_column = {'종목명': 0, '매수일': 1, '수량': 2, '매수가': 3, '평가손익': 4, '수익률': 5, '현재가': 6,
                           '매수금액': 7, '평가금액': 8, '수수료': 9, '세금': 10}
stockProfitTable_column = {'종목명': 0, '매도일': 1, '실현손익': 2, '수익률': 3, '매수금액': 4, '매도금액': 5, '매수가': 6,
                           '매도가': 7, '수수료': 8, '세금': 9}

sim_df_column = {'Date': '일자', 'Gubun': '구분', 'Amount': '수량', 'Price': '매매가', 'Charge': '수수료', 'Tax': '세금',
                 'Rate': '수익률', 'Profit': '수익금'}

volume_ma = {'5일평균': 'VMA5', '10일평균': 'VMA10', '20일평균': 'VMA20', '60일평균': 'VMA60', '120일평균': 'VMA120'}

OPEN_TIME = 9           # 장 시작시간(09시)
CLOSE_TIME = 16         # 장 마감시간(16시)
CHART_MOVE = 10         # 차트화면 STEP
CHARGE_RATE = 0.00015   # 매매 수수료율
TAX_RATE = 0.0025       # 매매 수수료율

DELAY_TIME = 0.2

class SimulationTrading(QMainWindow, main_form):
    def __init__(self):
        super().__init__()
        self.setupUi(self)

        # Event Handler
        self.stockTable.cellClicked.connect(lambda:self.draw_stock_chart('DAY'))
        self.rangeComboBox.currentTextChanged.connect(self.redraw_stock_chart)
        self.chartScrollBar.valueChanged.connect(self.scroll_stock_chart)
        #self.chartScrollBar.actionTriggered.connect(self.scroll_stock_chart)
        self.runPushButton.clicked.connect(self.run_simulation)
        self.run_totalPushButton.clicked.connect(self.run_total_simulation)

        # DB Connect 설정
        self.kospi_db = sqlite3.connect("./datas/kospi.db")
        self.kosdaq_db = sqlite3.connect("./datas/kosdaq.db")
        self.kospi_cur = self.kospi_db.cursor()
        self.kosdaq_cur = self.kosdaq_db.cursor()

        self.kospi_analyze_db = sqlite3.connect("./datas/kospi_analyze.db")
        self.kosdaq_analyze_db = sqlite3.connect("./datas/kosdaq_analyze.db")
        self.kospi_analyze_cur = self.kospi_analyze_db.cursor()
        self.kosdaq_analyze_cur = self.kosdaq_analyze_db.cursor()

        self.sim_db = sqlite3.connect("./datas/sim_result.db")
        self.sim_cur = self.sim_db.cursor()

        # 차트 출력요 화면 구성
        self.fig = plt.Figure(figsize=(1000,7.2), dpi=80, facecolor='k')
        self.canvas = FigureCanvas(self.fig)

        self.scroll = QtWidgets.QScrollBar(QtCore.Qt.Horizontal)
        self.top_axes, self.bottom_axes = self.fig.subplots(nrows=2, sharex=True)

        self.top_axes.set_position([0.02, 0.37, 0.88, 0.6])
        self.top_axes.tick_params(axis='both', color='#ffffff', labelcolor='#ffffff')
        self.top_axes.grid(color='lightgray', linewidth=.5, linestyle=':')
        self.top_axes.yaxis.tick_right()
        self.top_axes.autoscale_view()
        self.top_axes.set_facecolor('#041105')

        self.bottom_axes.set_position([0.02, 0.15, 0.88, 0.22])
        self.bottom_axes.tick_params(axis='both', color='#ffffff', labelcolor='#ffffff')
        self.bottom_axes.grid(color='lightgray', linewidth=.5, linestyle=':')
        self.bottom_axes.yaxis.tick_right()
        self.bottom_axes.autoscale_view()
        self.bottom_axes.set_facecolor('#041105')

        self.chartVerticalLayout.addWidget(self.canvas)

        # 종목별 정보 화면 표출
        self.create_stock_table()

        # 시뮬레이션 조건탭 초기화
        self.init_simulation_config_tab()

        # 시뮬레이션 결과 테이블 초기화
        self.clear_simulation_result_table()

        # 시뮬레이션 잔고 테이블 초기화
        self.clear_simulation_remain_table()

        # 시뮬레이션 수익 테이블 초기화
        self.clear_simulation_profit_table()

        # 전역변수 초기화
        self.chart_df = None

    def create_stock_table(self):
        kospi_market_info = pd.read_sql("SELECT Code, Name FROM MARKET_INFO", self.kospi_db)
        kosdaq_market_info = pd.read_sql("SELECT Code, Name FROM MARKET_INFO", self.kosdaq_db)

        kospi_market_info['type'] = 'KO'
        kosdaq_market_info['type'] = 'KQ'

        kospi_cnt = len(kospi_market_info.index)
        kosdaq_cnt = len(kosdaq_market_info.index)

        # 종목명과 코드로 정렬
        kospi_market_info = kospi_market_info.sort_values(by=['Name', 'Code'])
        kosdaq_market_info = kosdaq_market_info.sort_values(by=['Name', 'Code'])

        df = pd.concat([kospi_market_info, kosdaq_market_info])
        total_cnt = len(df.index)

        # 종목별 갯수 표출
        self.ko_cntLabel.setText(str(format(kospi_cnt, ',')))
        self.kq_cntLabel.setText(str(format(kosdaq_cnt, ',')))
        self.allcntLabel.setText(str(format(total_cnt, ',')))

        # 종목리스트에 표출될 전체 ROW수 설정
        self.stockTable.setRowCount(total_cnt)

        # 종목리스트 출력
        for i in range(total_cnt):
            # 종목코드, 종목명, 구분 출력
            for j in range(3):
                if j == stockTable_column['종목코드'] or j == stockTable_column['구분']:
                    item = QTableWidgetItem(df.iloc[i, j])
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
                else:
                    item = QTableWidgetItem(df.iloc[i, j])
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)

                self.stockTable.setItem(i, j, item)

            code = df.iloc[i, stockTable_column['종목코드']]
            type = df.iloc[i, stockTable_column['구분']]

            # 기준일 출력
            query = "SELECT DATE FROM DB_INFO WHERE TABLE_NAME = 'D" + code + "'"
            if type == 'KO':
                self.kospi_cur.execute(query)
                date = self.kospi_cur.fetchone()
            else:
                self.kosdaq_cur.execute(query)
                date = self.kosdaq_cur.fetchone()

            if date is None:
                continue

            item = QTableWidgetItem(date[0])
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
            self.stockTable.setItem(i, stockTable_column['기준일'], item)

            # 현재가 및 거래량
            query1 = "SELECT * FROM sqlite_master WHERE type='table' AND name='MA_D" + code + "'"
            query2 = "SELECT Close, Volume FROM MA_D" + code + " WHERE Date = (SELECT max(Date) FROM MA_D" + code + ")"
            if type == 'KO':
                self.kospi_analyze_cur.execute(query1)
                table_yn = self.kospi_analyze_cur.fetchone()

                if table_yn is not None:
                    self.kospi_analyze_cur.execute(query2)
                    datas = self.kospi_analyze_cur.fetchone()
                else:
                    datas = ('None', 'None')
            else:
                self.kosdaq_analyze_cur.execute(query1)
                table_yn = self.kosdaq_analyze_cur.fetchone()

                if table_yn is not None:
                    self.kosdaq_analyze_cur.execute(query2)
                    datas = self.kosdaq_analyze_cur.fetchone()
                else:
                    datas = ('None', 'None')

            if datas[0] != 'None':
                item = QTableWidgetItem(str(format(datas[0], ',')))
            else:
                item = QTableWidgetItem(datas[0])
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.stockTable.setItem(i, stockTable_column['현재가'], item)

            if datas[1] != 'None':
                item = QTableWidgetItem(str(format(datas[1], ',')))
            else:
                item = QTableWidgetItem(datas[1])
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.stockTable.setItem(i, stockTable_column['거래량'], item)

        self.stockTable.setColumnWidth(stockTable_column['종목코드'], 70)
        self.stockTable.setColumnWidth(stockTable_column['종목명'], 150)
        self.stockTable.setColumnWidth(stockTable_column['구분'], 50)
        self.stockTable.setColumnWidth(stockTable_column['기준일'], 150)
        self.stockTable.setColumnWidth(stockTable_column['현재가'], 80)
        self.stockTable.setColumnWidth(stockTable_column['거래량'], 80)

        self.stockTable.resizeRowsToContents()


    def init_simulation_config_tab(self):
        reg_ex = QtCore.QRegExp("(\d{0,3},)?(\d{3},)?\d{0,3}")

        input_validator = QtGui.QRegExpValidator(reg_ex, self.buy_cond1LineEdit)
        self.buy_cond1LineEdit.setValidator(input_validator)

        input_validator = QtGui.QRegExpValidator(reg_ex, self.sell_cond1LineEdit)
        self.sell_cond1LineEdit.setValidator(input_validator)

        input_validator = QtGui.QRegExpValidator(reg_ex, self.depositLineEdit)
        self.depositLineEdit.setValidator(input_validator)

        self.sim_endDateEdit.setDate(datetime.datetime.today())

        # 시뮬레이션 결과 저장용 DataFrame
        self.sim_df = DataFrame({'Date':[], 'Gubun':[], 'Amount':[], 'Price':[], 'Charge':[], 'Tax':[], 'Rate':[],
                                 'Profit':[], 'Volume':[], 'TempVolume':[]})
        self.temp_df = DataFrame({'Date':[], 'Gubun':[], 'Amount':[], 'Price':[], 'Charge':[], 'Tax':[], 'Rate':[],
                                 'Profit':[], 'Volume':[], 'TempVolume':[]})

        # 시뮬레이션 잔고 저장용 DataFrame
        self.remain_df = DataFrame({'Name':[], 'Date':[], 'Amount':[], 'Price':[], 'Profit':[], 'Rate':[], 'Current':[],
                                    'BuyValue':[], 'CurrValue':[], 'Charge':[], 'Tax':[]})

        # 시뮬레이션 수익 저장용 DataFrame
        self.profit_df = DataFrame({'Name':[], 'Date':[], 'Profit':[], 'Rate':[], 'BuyValue':[], 'SellValue':[],
                                    'BuyPrice':[], 'SellPrice':[], 'Charge':[], 'Tax':[]})


    def clear_simulation_result_table(self):
        self.stockSimTable.setRowCount(0)
        self.stockSimTable.setColumnWidth(stockSimTable_column['일자'], 80)
        self.stockSimTable.setColumnWidth(stockSimTable_column['구분'], 50)
        self.stockSimTable.setColumnWidth(stockSimTable_column['수량'], 50)
        self.stockSimTable.setColumnWidth(stockSimTable_column['매매가'], 80)
        self.stockSimTable.setColumnWidth(stockSimTable_column['수수료'], 60)
        self.stockSimTable.setColumnWidth(stockSimTable_column['세금'], 60)
        self.stockSimTable.setColumnWidth(stockSimTable_column['수익률'], 70)
        self.stockSimTable.setColumnWidth(stockSimTable_column['수익금'], 80)
        self.stockSimTable.setColumnWidth(stockSimTable_column['누적수익금'], 100)


    def clear_simulation_remain_table(self):
        self.stockRemainTable.setRowCount(0)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['종목명'], 220)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['매수일'], 80)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['수량'], 50)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['매수가'], 70)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['평가손익'], 100)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['수익률'], 60)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['현재가'], 70)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['매수금액'], 100)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['평가금액'], 100)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['수수료'], 60)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['세금'], 60)


    def clear_simulation_profit_table(self):
        self.stockProfitTable.setRowCount(0)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['종목명'], 220)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['매도일'], 80)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['실현손익'], 100)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['수익률'], 60)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['매수금액'], 100)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['매도금액'], 100)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['매수가'], 70)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['매도가'], 70)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['수수료'], 60)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['세금'], 60)


    def draw_chart_plot(self, df_datas):
        self.fig.clear()

        self.top_axes, self.bottom_axes = self.fig.subplots(nrows=2, sharex=True)

        timedates = [datetime.datetime.strptime(i, '%Y%m%d') for i in df_datas.index]

        day_list=[]
        name_list=[]
        for i, day in enumerate(timedates):
            iso = day.isocalendar()
            if self.rangeComboBox.currentText() == '월':
                if iso[2] == 1:                                     # 매주 월요일 기준 날짜 표시
                    day_list.append(i)
                    name_list.append(day.strftime('%Y/%m/%d'))
            elif self.rangeComboBox.currentText() == '분기':
                if iso[2] == 1 and (iso[1]%2) == 0:                 # 2주 단위 월요일 기준 날짜 표시
                    day_list.append(i)
                    name_list.append(day.strftime('%Y/%m/%d'))
            elif self.rangeComboBox.currentText() == '반기':
                if iso[2] == 1 and (iso[1]%4) == 0:                 # 매월 월요일 기준 날짜 표시
                    day_list.append(i)
                    name_list.append(day.strftime('%Y/%m/%d'))
            elif self.rangeComboBox.currentText() == '년':
                if iso[2] == 1 and (iso[1]%8) == 0:                 # 2개월 단위 월요일 기준 날짜 표시
                    day_list.append(i)
                    name_list.append(day.strftime('%Y/%m/%d'))

        mpl_finance.candlestick2_ochl(self.top_axes, df_datas['Open'], df_datas['Close'], df_datas['High'], df_datas['Low'],
                                      width=0.5, colorup='red', colordown='aqua')

        self.top_axes.plot(df_datas['MA5'],   linestyle='solid', marker='None', color='m', label='MA_5')
        self.top_axes.plot(df_datas['MA10'],  linestyle='solid', marker='None', color='b', label='MA_10')
        self.top_axes.plot(df_datas['MA20'],  linestyle='solid', marker='None', color='orange', label='MA_20')
        self.top_axes.plot(df_datas['MA60'],  linestyle='solid', marker='None', color='g', label='MA_60')
        self.top_axes.plot(df_datas['MA120'], linestyle='solid', marker='None', color='gray', label='MA_120')

        self.top_axes.set_position([0.02, 0.37, 0.88, 0.6])
        self.top_axes.tick_params(axis='both', color='#ffffff', labelcolor='#ffffff')
        self.top_axes.grid(color='lightgray', linewidth=.5, linestyle=':')
        self.top_axes.legend(loc='upper left', ncol=5, fontsize='xx-small')
        self.top_axes.yaxis.tick_right()
        self.top_axes.autoscale_view()
        self.top_axes.set_facecolor('#041105')

        self.bottom_axes.xaxis.set_major_locator(ticker.FixedLocator(day_list))
        self.bottom_axes.xaxis.set_major_formatter(ticker.FixedFormatter(name_list))

        self.bottom_axes.bar(np.arange(len(df_datas.index)), df_datas['Volume'], color='white', width=0.5, align='center')

        self.bottom_axes.plot(df_datas['VMA5'],   linestyle='solid', marker='None', color='m', label='MA_5')
        self.bottom_axes.plot(df_datas['VMA10'],  linestyle='solid', marker='None', color='b', label='MA_10')
        self.bottom_axes.plot(df_datas['VMA20'],  linestyle='solid', marker='None', color='orange', label='MA_20')
        self.bottom_axes.plot(df_datas['VMA60'],  linestyle='solid', marker='None', color='g', label='MA_60')
        self.bottom_axes.plot(df_datas['VMA120'], linestyle='solid', marker='None', color='gray', label='MA_120')

        self.bottom_axes.set_position([0.02, 0.15, 0.88, 0.22])
        self.bottom_axes.tick_params(axis='both', color='#ffffff', labelcolor='#ffffff')
        self.bottom_axes.grid(color='lightgray', linewidth=.5, linestyle=':')
        self.bottom_axes.legend(loc='upper left', ncol=5, fontsize='xx-small')
        self.bottom_axes.yaxis.tick_right()
        self.bottom_axes.autoscale_view()
        self.bottom_axes.set_facecolor('#041105')

        self.canvas.draw()


    def draw_stock_chart(self, chart_type):
        self.clear_simulation_result_table()

        selected_rows = self.stockTable.selectedIndexes()
        if len(selected_rows) != 0:
            index = self.stockTable.selectedIndexes()[0].row()

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥
            value_item = self.stockTable.item(index, stockTable_column['현재가'])
            if value_item is not None:
                value = value_item.text()
            else:
                return

            if value != 'None':     # 현재가가 None이면 chart 테이블이 존재하지 않음
                if chart_type == 'DAY':
                    table_name = 'MA_D'+code
                else:
                    table_name = 'MA_S'+code

                if type == 'KO':
                    self.chart_df = pd.read_sql("SELECT * from " + table_name, con=self.kospi_analyze_db)
                else:
                    self.chart_df = pd.read_sql("SELECT * from " + table_name, con=self.kosdaq_analyze_db)

                self.chart_df = self.chart_df.set_index('Date')

                # 출력할 차트데이터의 기준범위 날짜 계산
                self.open_index = self.chart_df.index[0]
                self.last_index = self.chart_df.index[-1]
                self.last_iloc = self.chart_df.index.get_loc(self.last_index)

                calc_date = datetime.datetime.strptime(self.last_index, '%Y%m%d')

                if self.rangeComboBox.currentText() == '월':
                    calc_date = calc_date - relativedelta(months=1)
                elif self.rangeComboBox.currentText() == '분기':
                    calc_date = calc_date - relativedelta(months=3)
                elif self.rangeComboBox.currentText() == '반기':
                    calc_date = calc_date - relativedelta(months=6)
                elif self.rangeComboBox.currentText() == '년':
                    calc_date = calc_date - relativedelta(years=1)

                begin_index = datetime.datetime.strftime(calc_date, '%Y%m%d')
                sliced_df = self.chart_df.loc[begin_index:self.last_index]

                # ScrollBar Movement를 위한  설정
                total_row = len(self.chart_df.index)
                self.sliced_count = len(sliced_df.index)
                self.step_count = round(self.sliced_count / CHART_MOVE)
                if self.step_count == 0:
                    self.step_count = 1

                self.scroll_max = int(total_row / self.step_count)
                if self.scroll_max < CHART_MOVE:
                    self.scroll_max = CHART_MOVE

                self.chartScrollBar.setMinimum(CHART_MOVE)
                self.chartScrollBar.setMaximum(self.scroll_max)
                self.chartScrollBar.setValue(self.scroll_max)

                #self.draw_chart_plot(sliced_df)


    def redraw_stock_chart(self):
        if self.chart_df is None:
            return

        calc_date = datetime.datetime.strptime(self.last_index, '%Y%m%d')

        if self.rangeComboBox.currentText() == '월':
            calc_date = calc_date - relativedelta(months=1)
        elif self.rangeComboBox.currentText() == '분기':
            calc_date = calc_date - relativedelta(months=3)
        elif self.rangeComboBox.currentText() == '반기':
            calc_date = calc_date - relativedelta(months=6)
        elif self.rangeComboBox.currentText() == '년':
            calc_date = calc_date - relativedelta(years=1)

        begin_index = datetime.datetime.strftime(calc_date, '%Y%m%d')
        sliced_df = self.chart_df.loc[begin_index:self.last_index]

        # ScrollBar Movement를 위한  설정
        total_row = len(self.chart_df.index)
        self.sliced_count = len(sliced_df.index)
        self.step_count = round(self.sliced_count / CHART_MOVE)
        self.scroll_max = int(total_row / self.step_count)
        self.chartScrollBar.setMinimum(CHART_MOVE)
        self.chartScrollBar.setMaximum(self.scroll_max)
        self.chartScrollBar.setValue(self.scroll_max)

        #self.draw_chart_plot(sliced_df)


    def scroll_stock_chart(self):
        pos = self.chartScrollBar.value()

        end_iloc = self.last_iloc - ((self.scroll_max - pos) * self.step_count)
        if end_iloc == 0:
            end_iloc = 1

        begin_iloc = end_iloc - self.sliced_count

        if begin_iloc <  0:
            begin_iloc = 0

        sliced_df = self.chart_df.iloc[begin_iloc:end_iloc]

        self.draw_chart_plot(sliced_df)


    def add_buy_sim_df(self, index_date, price, volume, temp_volume):
        self.sim_df = self.sim_df.set_value(len(self.sim_df), 'Date', index_date)
        index = len(self.sim_df) - 1
        self.sim_df['Gubun'].iloc[index] = '매수'
        self.sim_df['Amount'].iloc[index] = int(self.deposit / price)                       # 매수수량(종가 매수)
        self.sim_df['Price'].iloc[index] = price                                            # 단주매수 금액(종가)
        buy_value = self.sim_df['Amount'].iloc[index] * price                               # 총매수 금액
        self.sim_df['Charge'].iloc[index] = int((buy_value * CHARGE_RATE) / 10) * 10        # 수수료 10원이하 절사

        # 매수에 따른 예수금 감소
        self.deposit = self.deposit - buy_value

        # 매수 시 거래량 저장
        self.sim_df['Volume'].iloc[index] = volume

        # 매도 시 기준 거래량 저장
        self.sim_df['TempVolume'].iloc[index] = temp_volume

        self.write_stockSimTable('매수')

        print("{} 매수 : 수량[{}], 매수가[{}], 거래량[{}]".format(index_date, self.sim_df['Amount'].iloc[index], price, volume))


    def add_buy_temp_df(self, index_date, price, volume):
        if len(self.temp_df) > 0:
            index = len(self.temp_df) - 1
            self.temp_df['Date'].iloc[index] = index_date
        else:
            self.temp_df = self.temp_df.set_value(len(self.temp_df), 'Date', index_date)
            index = len(self.temp_df) - 1

        self.temp_df['Gubun'].iloc[index] = '매수'

        # 거래량 저장
        self.temp_df['Volume'].iloc[index] = volume

        print("{} Temp : 거래량[{}]".format(index_date, volume))


    def write_stockSimTable(self, type):
        index = len(self.sim_df) - 1

        # 종목리스트에 표출될 전체 ROW수 설정
        self.stockSimTable.setRowCount(index+1)

        date = datetime.datetime.strptime(self.sim_df['Date'].iloc[index], '%Y%m%d')
        item = QTableWidgetItem(date.strftime('%Y-%m-%d'))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
        self.stockSimTable.setItem(index, stockSimTable_column['일자'], item)

        item = QTableWidgetItem(self.sim_df['Gubun'].iloc[index])
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
        self.stockSimTable.setItem(index, stockSimTable_column['구분'], item)

        item = QTableWidgetItem(str(format(self.sim_df['Amount'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockSimTable.setItem(index, stockSimTable_column['수량'], item)

        item = QTableWidgetItem(str(format(self.sim_df['Price'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockSimTable.setItem(index, stockSimTable_column['매매가'], item)

        item = QTableWidgetItem(str(format(self.sim_df['Charge'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockSimTable.setItem(index, stockSimTable_column['수수료'], item)

        if type == '매도':
            item = QTableWidgetItem(str(format(self.sim_df['Tax'].iloc[index].astype('int'), ',')))
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.stockSimTable.setItem(index, stockSimTable_column['세금'], item)

            item = QTableWidgetItem(str(round(self.sim_df['Rate'].iloc[index].astype('float') * 100, 2)) + '%')
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.stockSimTable.setItem(index, stockSimTable_column['수익률'], item)

            item = QTableWidgetItem(str(format(self.sim_df['Profit'].iloc[index].astype('int'), ',')))
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.stockSimTable.setItem(index, stockSimTable_column['수익금'], item)

            # 누적 수익금 계산
            item = QTableWidgetItem(str(format(self.sim_df['Profit'].sum().astype('int'), ',')))
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.stockSimTable.setItem(index, stockSimTable_column['누적수익금'], item)
        else:
            pass


    def sim_buy_condition1(self, code, type, index_date, sim_data):
        # 당일 시작가과 종가 구하기
        day_open = sim_data['Open']
        day_close = sim_data['Close']

        if self.buy_cond1 == '기준 거래량초과':
            # 당일 거래량과 매수조건의 거래량 구하기
            day_volume = sim_data['Volume']

            if sim_data[volume_ma[self.buy_vol_ma]] is not None:
                buy_ma_volume = sim_data[volume_ma[self.buy_vol_ma]] * self.buy_cond1_value
            else:
                return

            # 최소 거래량 조건 만족여부 판별
            if day_volume < self.buy_cond11_value:
                return

            # 당일 거래량이 매수조건의 거래량을 초과하는 경우
            if day_volume > buy_ma_volume:
                # 종가 매수
                if self.buy_value_type == '양봉' and day_open <= day_close and self.deposit >= (day_close * 10) and day_close > self.buy_cond0_value:
                    if self.buy_cond2 == '없음':
                        self.add_buy_sim_df(index_date, day_close, day_volume, day_volume)
                    else:
                        self.add_buy_temp_df(index_date, day_close, day_volume)
                elif self.buy_value_type == '음봉' and day_open >= day_close and self.deposit >= (day_close * 10) and day_close > self.buy_cond0_value:
                    if self.buy_cond2 == '없음':
                        self.add_buy_sim_df(index_date, day_close, day_volume, day_volume)
                    else:
                        self.add_buy_temp_df(index_date, day_close, day_volume)
                elif self.deposit >= (day_close * 10) and day_close > self.buy_cond0_value:
                    if self.buy_cond2 == '없음':
                        self.add_buy_sim_df(index_date, day_close, day_volume, day_volume)
                    else:
                        self.add_buy_temp_df(index_date, day_close, day_volume)


    def sim_buy_condition2(self, code, type, index_date, sim_data):
        # 조건1을 만족하는 데이터가 있을 경우에만 매수
        if len(self.temp_df) > 0:
            temp_index = len(self.temp_df) - 1
        else:
            return

        if self.temp_df['Gubun'].iloc[temp_index] == '매수':
            cond_volume = self.temp_df['Volume'].iloc[temp_index]
            if cond_volume is None or cond_volume == 0:
                return
        else:
            return

        # 동일날짜이면 처리하지 않음
        if self.temp_df['Date'].iloc[temp_index] == index_date:
            return

        # 당일 종가 구하기
        day_close = sim_data['Close']

        # 매수조건0
        day_ma = None
        if self.buy_cond0 == '이평 MA5 골든크로스':
            if sim_data['MA5'] is not None:
                if day_close >= sim_data['MA5']:
                    day_ma = sim_data['MA5']
        elif self.buy_cond0 == '이평 MA10 골든크로스':
            if sim_data['MA5'] is not None and sim_data['MA10'] is not None:
                if day_close >= sim_data['MA5'] and sim_data['MA5'] >= sim_data['MA10']:
                    day_ma = sim_data['MA10']
        elif self.buy_cond0 == '이평 MA20 골든크로스':
            if sim_data['MA5'] is not None and sim_data['MA10'] is not None and sim_data['MA20'] is not None:
                if day_close >= sim_data['MA5'] and sim_data['MA5'] >= sim_data['MA10'] and sim_data['MA10'] >= sim_data['MA20']:
                    day_ma = sim_data['MA20']
        elif self.buy_cond0 == '이평 MA60 골든크로스':
            if sim_data['MA5'] is not None and sim_data['MA10'] is not None and sim_data['MA20'] is not None and sim_data['MA60'] is not None:
                if day_close >= sim_data['MA5'] and sim_data['MA5'] >= sim_data['MA10'] and sim_data['MA10'] >= sim_data['MA20'] and sim_data['MA20'] >= sim_data['MA60']:
                    day_ma = sim_data['MA60']
        elif self.buy_cond0 == '이평 MA120 골든크로스':
            if sim_data['MA5'] is not None and sim_data['MA10'] is not None and sim_data['MA20'] is not None and sim_data['MA60'] is not None and sim_data['MA120'] is not None:
                if day_close >= sim_data['MA5'] and sim_data['MA5'] >= sim_data['MA10'] and sim_data['MA10'] >= sim_data['MA20'] and sim_data['MA20'] >= sim_data['MA60'] and sim_data['MA60'] >= sim_data['MA120']:
                    day_ma = sim_data['MA120']
        else:
            day_ma = None

        if day_ma is None or day_ma == 0:
            return

        # 당일 거래량과 매수조건의 거래량 구하기
        day_volume = sim_data['Volume']
        buy_volume = cond_volume * self.buy_cond2_value

        # 당일 거래량이 매수조건의 거래량 이하인 경우
        if self.buy_cond2 == '조건1 거래량 이하' and day_volume <= buy_volume and self.deposit >= (day_close * 10) and day_close > self.buy_cond0_value:
            self.add_buy_sim_df(index_date, day_close, day_volume, cond_volume)
        # 당일 거래량이 매수조건의 거래량 이상인 경우
        elif self.buy_cond2 == '조건1 거래량 이상' and day_volume >= buy_volume and self.deposit >= (day_close * 10) and day_close > self.buy_cond0_value:
            self.add_buy_sim_df(index_date, day_close, day_volume, cond_volume)
        else:
            return

        self.temp_df.drop(self.temp_df.index, inplace=True)


    def write_stockProfitTable(self):
        index = len(self.profit_df) - 1

        # 리스트에 표출될 전체 ROW수 설정
        self.stockProfitTable.setRowCount(index+1)

        item = QTableWidgetItem(self.profit_df['Name'].iloc[index])
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        self.stockProfitTable.setItem(index, stockProfitTable_column['종목명'], item)

        date = datetime.datetime.strptime(self.profit_df['Date'].iloc[index], '%Y%m%d')
        item = QTableWidgetItem(date.strftime('%Y-%m-%d'))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
        self.stockProfitTable.setItem(index, stockProfitTable_column['매도일'], item)

        item = QTableWidgetItem(str(format(self.profit_df['Profit'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockProfitTable.setItem(index, stockProfitTable_column['실현손익'], item)

        item = QTableWidgetItem(str(round(self.profit_df['Rate'].iloc[index].astype('float') * 100, 2)) + '%')
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockProfitTable.setItem(index, stockProfitTable_column['수익률'], item)

        item = QTableWidgetItem(str(format(self.profit_df['BuyValue'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockProfitTable.setItem(index, stockProfitTable_column['매수금액'], item)

        item = QTableWidgetItem(str(format(self.profit_df['SellValue'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockProfitTable.setItem(index, stockProfitTable_column['매도금액'], item)

        item = QTableWidgetItem(str(format(self.profit_df['BuyPrice'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockProfitTable.setItem(index, stockProfitTable_column['매수가'], item)

        item = QTableWidgetItem(str(format(self.profit_df['SellPrice'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockProfitTable.setItem(index, stockProfitTable_column['매도가'], item)

        item = QTableWidgetItem(str(format(self.profit_df['Charge'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockProfitTable.setItem(index, stockProfitTable_column['수수료'], item)

        item = QTableWidgetItem(str(format(self.profit_df['Tax'].sum().astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockProfitTable.setItem(index, stockProfitTable_column['세금'], item)


    def add_sell_sim_df(self, index_date, sell_amount, price, buy_charge, buy_value, buy_price, volume):
        # 시뮬레이션 결과 설정
        self.sim_df = self.sim_df.set_value(len(self.sim_df), 'Date', index_date)
        index = len(self.sim_df) - 1
        self.sim_df['Gubun'].iloc[index] = '매도'
        self.sim_df['Amount'].iloc[index] = sell_amount                                 # 매도수량
        self.sim_df['Price'].iloc[index] = price                                        # 단주매도 금액(종가)
        sell_value = sell_amount * price                                                # 매도 총금액
        self.sim_df['Charge'].iloc[index] = int((sell_value * CHARGE_RATE) / 10) * 10   # 수수료 10원이하 절사
        self.sim_df['Tax'].iloc[index] = int((sell_value * TAX_RATE))                   # 세금

        # 매수/매도 수수료 및 세금 계산
        charge = buy_charge + self.sim_df['Charge'].iloc[index]
        tax = self.sim_df['Tax'].iloc[index]
        total_tax = charge + tax

        # 수익금 계산
        profit = sell_value - buy_value - total_tax
        self.sim_df['Profit'].iloc[index] = profit

        # 수익률 계산 (수수료 및 세금을 제외한 순수 수익률)
        rate = profit / buy_value
        self.sim_df['Rate'].iloc[index] = rate

        # 예수금 계산
        self.deposit = self.deposit + (sell_value - total_tax)

        self.write_stockSimTable('매도')

        print("{} 매도 : 수량[{}], 매매가[{}], 수익금[{}], 수익률[{}], 거래량[{}]".format(index_date, sell_amount, price, str(format(profit, ',')), str(round(rate * 100, 2)) + '%', volume))

        # 시뮬레이션 수익 설정
        index = self.stockTable.selectedIndexes()[0].row()

        # 종목명
        code = self.stockTable.item(index, stockTable_column['종목코드']).text()
        name = self.stockTable.item(index, stockTable_column['종목명']).text()
        name = '(' + code + ')' + name

        data = [name, index_date, profit, rate, buy_value, sell_value, buy_price, price, charge, tax]
        self.profit_df.loc[len(self.profit_df)] = data

        self.write_stockProfitTable()

        # 전체 실현손익 및 평균 수익률 계산
        total_profit = int(self.profit_df['Profit'].sum())
        avg_rate = self.profit_df['Rate'].sum() / len(self.profit_df)

        self.profit_profitLabel.setText(str(format(total_profit, ',')))
        self.profit_rateLabel.setText(str(round(avg_rate * 100, 2)) + '%')

        print("전체 실현손익 : 수익[ {} ], 수익률[ {} ]".format(str(format(total_profit, ',')), str(round(avg_rate * 100, 2)) + '%'))


    def sim_sell_condition1(self, code, type, index_date, sim_data):
        if self.sell_cond1 == '기준 거래량초과':
            # 당일 거래량과 매도조건의 거래량 구하기
            day_volume = sim_data['Volume']
            sell_vol_ma = self.sell_vol_maComboBox.currentText()

            if sim_data[volume_ma[sell_vol_ma]] is not None:
                sell_ma_volume = sim_data[volume_ma[sell_vol_ma]] * self.sell_cond1_value
            else:
                return

            # 당일 거래량이 매수조건의 거래량을 초과하는 경우
            if day_volume > sell_ma_volume:
                # 당일 시작가과 종가 구하기
                day_open = sim_data['Open']
                day_close = sim_data['Close']

                if len(self.sim_df) > 0:
                    buy_index = len(self.sim_df) - 1
                else:
                    return

                if self.sim_df['Gubun'].iloc[buy_index] == '매수':
                    sell_amount =  self.sim_df['Amount'].iloc[buy_index]
                    buy_price = self.sim_df['Price'].iloc[buy_index]
                    buy_value = sell_amount * buy_price
                    buy_charge = self.sim_df['Charge'].iloc[buy_index]
                else:
                    return

                # 동일날짜이면 처리하지 않음
                if self.sim_df['Date'].iloc[buy_index] == index_date:
                    return

                # 종가 매도
                if self.sell_value_type == '양봉' and day_open <= day_close and sell_amount > 0:
                    self.add_sell_sim_df(index_date, sell_amount, day_close, buy_charge, buy_value, buy_price, day_volume)
                elif self.buy_value_type == '음봉' and day_open >= day_close and sell_amount > 0:
                    self.add_sell_sim_df(index_date, sell_amount, day_close, buy_charge, buy_value, buy_price, day_volume)

        elif self.sell_cond1 == '매수조건1 거래량':
            # 당일 거래량과 매도조건의 거래량 구하기
            day_volume = sim_data['Volume']

            if len(self.sim_df) > 0:
                buy_index = len(self.sim_df) - 1
            else:
                return

            if self.sim_df['Gubun'].iloc[buy_index] == '매수':
                buy_volume = self.sim_df['TempVolume'].iloc[buy_index]
                if buy_volume is None or buy_volume == 0:
                    return
            else:
                return

            # 동일날짜이면 처리하지 않음
            if self.sim_df['Date'].iloc[buy_index] == index_date:
                return

            # 당일 거래량이 매수조건의 거래량을 초과하는 경우
            if day_volume > (buy_volume * self.sell_cond1_value):
                # 당일 시작가과 종가 구하기
                day_open = sim_data['Open']
                day_close = sim_data['Close']

                sell_amount =  self.sim_df['Amount'].iloc[buy_index]
                buy_price = self.sim_df['Price'].iloc[buy_index]
                buy_value = sell_amount * buy_price
                buy_charge = self.sim_df['Charge'].iloc[buy_index]

                # 종가 매도
                if self.sell_value_type == '양봉' and day_open <= day_close and sell_amount > 0:
                    self.add_sell_sim_df(index_date, sell_amount, day_close, buy_charge, buy_value, buy_price, day_volume)
                elif self.buy_value_type == '음봉' and day_open >= day_close and self.deposit >= day_close:
                    self.add_sell_sim_df(index_date, sell_amount, day_close, buy_charge, buy_value, buy_price, day_volume)


    def sim_sell_condition2(self, code, type, index_date, sim_data):
        if len(self.sim_df) > 0:
            buy_index = len(self.sim_df) - 1
        else:
            return

        if self.sim_df['Gubun'].iloc[buy_index] == '매수':
            buy_volume = self.sim_df['TempVolume'].iloc[buy_index]
            if buy_volume is None or buy_volume == 0:
                return
        else:
            return

        # 동일날짜이면 처리하지 않음
        if self.sim_df['Date'].iloc[buy_index] == index_date:
            return

        sell_amount = self.sim_df['Amount'].iloc[buy_index]
        buy_price = self.sim_df['Price'].iloc[buy_index]
        buy_value = sell_amount * buy_price
        buy_charge = self.sim_df['Charge'].iloc[buy_index]

        # 당일 가격과 매도조건의 가격 구하기
        day_close = sim_data['Close']

        day_ma = None
        if self.sell_cond2 == '이평 MA5 데드크로스':
            if sim_data['MA5'] is not None:
                if day_close < sim_data['MA5']:
                    day_ma = sim_data['MA5']
        elif self.sell_cond2 == '이평 MA10 데드크로스':
            if sim_data['MA10'] is not None:
                if day_close < sim_data['MA10']:
                    day_ma = sim_data['MA10']
        elif self.sell_cond2 == '이평 MA20 데드크로스':
            if sim_data['MA20'] is not None:
                if day_close < sim_data['MA20']:
                    day_ma = sim_data['MA20']
        elif self.sell_cond2 == '이평 MA60 데드크로스':
            if sim_data['MA60'] is not None:
                if day_close < sim_data['MA60']:
                    day_ma = sim_data['MA60']
        elif self.sell_cond2 == '이평 MA120 데드크로스':
            if sim_data['MA120'] is not None:
                if day_close < sim_data['MA120']:
                    day_ma = sim_data['MA120']
        else:
            day_ma = None

        if day_ma is None or day_ma == 0:
            return

        # 당일 거래량이 매수조건의 거래량을 초과하는 경우
        if day_close < day_ma:
            # 종가 매도
            self.add_sell_sim_df(index_date, sell_amount, day_close, buy_charge, buy_value, buy_price, sim_data['Volume'])


    def sim_sell_condition3(self, code, type, index_date, sim_data):
        if len(self.sim_df) > 0:
            buy_index = len(self.sim_df) - 1
        else:
            return

        if self.sim_df['Gubun'].iloc[buy_index] == '매수':
            buy_volume = self.sim_df['TempVolume'].iloc[buy_index]
            if buy_volume is None or buy_volume == 0:
                return
        else:
            return

        # 동일날짜이면 처리하지 않음
        if self.sim_df['Date'].iloc[buy_index] == index_date:
            return

        sell_amount = self.sim_df['Amount'].iloc[buy_index]
        buy_price = self.sim_df['Price'].iloc[buy_index]
        buy_value = sell_amount * buy_price
        buy_charge = self.sim_df['Charge'].iloc[buy_index]

        # 당일 가격과 매도조건의 가격 구하기
        day_close = sim_data['Close']                       # 매도 시 단가
        sell_value = sell_amount * day_close                # 매도 시 총금액

        # 매수/매도 수수료 및 세금 계산
        charge = buy_charge + (int((sell_value * CHARGE_RATE) / 10) * 10)
        tax = int((sell_value * TAX_RATE))
        total_tax = charge + tax

        # 수익금 계산
        profit = sell_value - buy_value - total_tax

        # 수익률 계산 (수수료 및 세금을 제외한 순수 수익률)
        rate = profit / buy_value

        # 당일 수익률이 매도조건의 수익률을 초과하는 경우 매도
        if rate > self.sell_cond3_value:
            # 종가 매도
            self.add_sell_sim_df(index_date, sell_amount, day_close, buy_charge, buy_value, buy_price, sim_data['Volume'])

        if rate < self.sell_cond31_value:
            # 종가 매도
            self.add_sell_sim_df(index_date, sell_amount, day_close, buy_charge, buy_value, buy_price, sim_data['Volume'])


    def write_stockRemainTable(self):
        index = len(self.remain_df) - 1

        # 리스트에 표출될 전체 ROW수 설정
        self.stockRemainTable.setRowCount(index+1)

        item = QTableWidgetItem(self.remain_df['Name'].iloc[index])
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        self.stockRemainTable.setItem(index, stockRemainTable_column['종목명'], item)

        date = datetime.datetime.strptime(self.remain_df['Date'].iloc[index], '%Y%m%d')
        item = QTableWidgetItem(date.strftime('%Y-%m-%d'))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
        self.stockRemainTable.setItem(index, stockRemainTable_column['매수일'], item)

        item = QTableWidgetItem(str(format(self.remain_df['Amount'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockRemainTable.setItem(index, stockRemainTable_column['수량'], item)

        item = QTableWidgetItem(str(format(self.remain_df['Price'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockRemainTable.setItem(index, stockRemainTable_column['매수가'], item)

        item = QTableWidgetItem(str(format(self.remain_df['Profit'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockRemainTable.setItem(index, stockRemainTable_column['평가손익'], item)

        item = QTableWidgetItem(str(round(self.remain_df['Rate'].iloc[index].astype('float') * 100, 2)) + '%')
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockRemainTable.setItem(index, stockRemainTable_column['수익률'], item)

        item = QTableWidgetItem(str(format(self.remain_df['Current'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockRemainTable.setItem(index, stockRemainTable_column['현재가'], item)

        item = QTableWidgetItem(str(format(self.remain_df['BuyValue'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockRemainTable.setItem(index, stockRemainTable_column['매수금액'], item)

        item = QTableWidgetItem(str(format(self.remain_df['CurrValue'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockRemainTable.setItem(index, stockRemainTable_column['평가금액'], item)

        item = QTableWidgetItem(str(format(self.remain_df['Charge'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockRemainTable.setItem(index, stockRemainTable_column['수수료'], item)

        item = QTableWidgetItem(str(format(self.remain_df['Tax'].sum().astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockRemainTable.setItem(index, stockRemainTable_column['세금'], item)


    def add_remain_df(self):
        # 시뮬레이션 잔고 설정
        if len(self.sim_df) > 0:
            buy_index = len(self.sim_df) - 1
        else:
            return

        if self.sim_df['Gubun'].iloc[buy_index] == '매수':
            date = self.sim_df['Date'].iloc[buy_index]
            amount = self.sim_df['Amount'].iloc[buy_index]
            price = self.sim_df['Price'].iloc[buy_index]
            buy_value = amount * price
            buy_charge = self.sim_df['Charge'].iloc[buy_index]
        else:
            return

        index = self.stockTable.selectedIndexes()[0].row()
        code = self.stockTable.item(index, stockTable_column['종목코드']).text()
        name = self.stockTable.item(index, stockTable_column['종목명']).text()
        name = '(' + code + ')' + name                                                                      # 종목명
        current = int(self.stockTable.item(index, stockTable_column['현재가']).text().replace(',', ''))      # 현재가
        sell_value = amount * current                                                       # 현재가로 매도했을 때의 금액
        sell_charge = int((sell_value * CHARGE_RATE) / 10) * 10                             # 매도 시 수수료 10원이하 절사
        tax = int(sell_value * TAX_RATE)                                                    # 세금
        charge = buy_charge + sell_charge                                                   # 매수/매도 수수료
        profit = sell_value - buy_value - charge - tax                                      # 평가손익
        rate = profit / buy_value

        data = [name, date, amount, price, profit, rate, current, buy_value, sell_value, charge, tax]
        self.remain_df.loc[len(self.remain_df)] = data

        self.write_stockRemainTable()

        # 전체 평가손익 및 평균 수익률 계산
        total_profit = int(self.remain_df['Profit'].sum())
        avg_rate = self.remain_df['Rate'].sum() / len(self.remain_df)

        self.remain_profitLabel.setText(str(format(total_profit, ',')))
        self.remain_rateLabel.setText(str(round(avg_rate * 100, 2)) + '%')

        print("시뮬레이션 잔고 : 손익[ {} ], 수익률[ {} ]".format(str(format(total_profit, ',')), str(round(avg_rate * 100, 2)) + '%'))


    def run_simulation(self):
        # 매수조건 파라메터
        self.buy_vol_ma = self.buy_vol_maComboBox.currentText()
        self.buy_value_type = self.buy_value_typeComboBox.currentText()
        self.buy_cond0 = self.buy_cond0ComboBox.currentText()
        self.buy_cond0_value = int(self.buy_cond0LineEdit.text().replace(',', ''))
        self.buy_cond1 = self.buy_cond1ComboBox.currentText()
        self.buy_cond1_value = int(self.buy_cond1LineEdit.text().replace(',', '')) / 100
        self.buy_cond11_value = int(self.buy_cond11LineEdit.text().replace(',', ''))
        self.buy_cond2 = self.buy_cond2ComboBox.currentText()
        self.buy_cond2_value = int(self.buy_cond2LineEdit.text().replace(',', '')) / 100

        self.deposit = int(self.depositLineEdit.text().replace(',',''))

        # 매도조건 파라메터
        self.sell_value_type = self.sell_value_typeComboBox.currentText()
        self.sell_cond1 = self.sell_cond1ComboBox.currentText()
        self.sell_cond1_value = int(self.sell_cond1LineEdit.text().replace(',','')) / 100
        self.sell_cond2 = self.sell_cond2ComboBox.currentText()
        self.sell_cond3_value = int(self.sell_cond3LineEdit.text().replace(',','')) / 100
        self.sell_cond31_value = int(self.sell_cond31LineEdit.text().replace(',','')) / 100

        selected_rows = self.stockTable.selectedIndexes()
        if len(selected_rows) != 0:
            index = self.stockTable.selectedIndexes()[0].row()

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            # 시뮬레이션 기간
            sim_start_date = self.sim_startDateEdit.date().toPyDate()
            sim_end_date = self.sim_endDateEdit.date().toPyDate()

            start_index = datetime.datetime.strftime(sim_start_date, '%Y%m%d')
            end_index = datetime.datetime.strftime(sim_end_date, '%Y%m%d')

            # 시뮬레이션 결과 DataFrame Clear
            if len(self.sim_df.index) > 0:
                self.sim_df.drop(self.sim_df.index, inplace=True)

            if len(self.temp_df.index) > 0:
                self.temp_df.drop(self.temp_df.index, inplace=True)

            self.clear_simulation_result_table()

            # 시뮬레이션 대상 데이타
            sim_datas = self.chart_df.loc[start_index:end_index]

            for i in range(len(sim_datas.index)):
                # 매수 조건 실행
                self.sim_buy_condition1(code, type, sim_datas.index[i], sim_datas.iloc[i])
                self.sim_buy_condition2(code, type, sim_datas.index[i], sim_datas.iloc[i])

                # 매도 조건 실행
                self.sim_sell_condition1(code, type, sim_datas.index[i], sim_datas.iloc[i])
                self.sim_sell_condition2(code, type, sim_datas.index[i], sim_datas.iloc[i])
                self.sim_sell_condition3(code, type, sim_datas.index[i], sim_datas.iloc[i])

            # 시뮬레이션 잔고 설정
            self.add_remain_df()

            QtWidgets.QMessageBox.about(self, "시뮬레이션 완료", "시뮬레이션이 완료되었습니다. 결과를 확인하세요!")

        else:
            QtWidgets.QMessageBox.warning(self, "메세지", "시뮬레이션 할 종목를 선택하세요!")


    def sleep_time(self, millisecond):
        loop = QEventLoop()
        QTimer.singleShot(millisecond, loop.quit)
        loop.exec_()


    def run_total_simulation(self):
        # 매수조건 파라메터
        self.buy_vol_ma = self.buy_vol_maComboBox.currentText()
        self.buy_value_type = self.buy_value_typeComboBox.currentText()
        self.buy_cond0 = self.buy_cond0ComboBox.currentText()
        self.buy_cond0_value = int(self.buy_cond0LineEdit.text().replace(',', ''))
        self.buy_cond1 = self.buy_cond1ComboBox.currentText()
        self.buy_cond1_value = int(self.buy_cond1LineEdit.text().replace(',', '')) / 100
        self.buy_cond11_value = int(self.buy_cond11LineEdit.text().replace(',', ''))
        self.buy_cond2 = self.buy_cond2ComboBox.currentText()
        self.buy_cond2_value = int(self.buy_cond2LineEdit.text().replace(',', '')) / 100

        # 매도조건 파라메터
        self.sell_value_type = self.sell_value_typeComboBox.currentText()
        self.sell_cond1 = self.sell_cond1ComboBox.currentText()
        self.sell_cond1_value = int(self.sell_cond1LineEdit.text().replace(',','')) / 100
        self.sell_cond2 = self.sell_cond2ComboBox.currentText()
        self.sell_cond3_value = int(self.sell_cond3LineEdit.text().replace(',','')) / 100
        self.sell_cond31_value = int(self.sell_cond31LineEdit.text().replace(',','')) / 100

        # 시뮬레이션 기간
        sim_start_date = self.sim_startDateEdit.date().toPyDate()
        sim_end_date = self.sim_endDateEdit.date().toPyDate()
        start_index = datetime.datetime.strftime(sim_start_date, '%Y%m%d')
        end_index = datetime.datetime.strftime(sim_end_date, '%Y%m%d')

        # 전체 시뮬레이션 결과 DataFrame Clear
        if len(self.remain_df.index) > 0:
            self.remain_df.drop(self.remain_df.index, inplace=True)
        if len(self.profit_df.index) > 0:
            self.profit_df.drop(self.profit_df.index, inplace=True)

        self.clear_simulation_remain_table()
        self.clear_simulation_profit_table()

        start_cnt = 0
        total_cnt = self.stockTable.rowCount()

        # 종목별 기본정보 DB 저장
        for index in range(start_cnt, total_cnt):
            print("{} / {}".format(index+1, total_cnt))
            self.runcntLabel.setText(str(format(index+1, ',')))

            # 해당 ROW 선택
            self.stockTable.selectRow(index)

            # 종목 차트데이로 로딩
            self.draw_stock_chart('DAY')
            self.sleep_time(DELAY_TIME * 1000)

            # 예수금 초기화
            self.deposit = int(self.depositLineEdit.text().replace(',', ''))

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            # 종목 시뮬레이션 결과 DataFrame Clear
            if len(self.sim_df.index) > 0:
                self.sim_df.drop(self.sim_df.index, inplace=True)

            if len(self.temp_df.index) > 0:
                self.temp_df.drop(self.temp_df.index, inplace=True)

            self.clear_simulation_result_table()

            # 시뮬레이션 대상 데이타
            sim_datas = self.chart_df.loc[start_index:end_index]

            for i in range(len(sim_datas.index)):
                # 매수 조건1 실행
                self.sim_buy_condition1(code, type, sim_datas.index[i], sim_datas.iloc[i])
                self.sim_buy_condition2(code, type, sim_datas.index[i], sim_datas.iloc[i])

                # 매도 조건1 실행
                self.sim_sell_condition1(code, type, sim_datas.index[i], sim_datas.iloc[i])
                self.sim_sell_condition2(code, type, sim_datas.index[i], sim_datas.iloc[i])
                self.sim_sell_condition3(code, type, sim_datas.index[i], sim_datas.iloc[i])

            # 시뮬레이션 잔고 설정
            self.add_remain_df()

        QtWidgets.QMessageBox.about(self, "전체 시뮬레이션 완료", "전체 시뮬레이션이 완료되었습니다. 결과를 확인하세요!")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    myWindow = SimulationTrading()
    myWindow.show()
    sys.exit(app.exec_())