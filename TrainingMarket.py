import sys
import sqlite3
import datetime
import numpy as np
import pandas as pd
import os.path
from pandas import DataFrame

from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5 import uic
from PyQt5 import QtCore, QtWidgets, QtGui

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import mpl_finance

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from dateutil.relativedelta import relativedelta
from StockDataset import *
from tensorflow import keras

from multiprocessing.pool import ThreadPool

main_form = uic.loadUiType("market_training_eval.ui")[0]

stockTable_column = {'종목코드': 0, '종목명': 1, '구분': 2, '기준일': 3, '현재가': 4, '거래량': 5}
stockSimTable_column = {'종목명': 0, '일자': 1, '구분': 2, '수량': 3, '매매가': 4, '수수료': 5, '세금': 6, '수익률': 7,
                        '수익금': 8, '누적수익금': 9}
stockRemainTable_column = {'종목명': 0, '매수일': 1, '수량': 2, '매수가': 3, '평가손익': 4, '수익률': 5, '현재가': 6,
                           '매수금액': 7, '평가금액': 8, '수수료': 9, '세금': 10}
stockProfitTable_column = {'종목명': 0, '매도일': 1, '실현손익': 2, '수익률': 3, '매수금액': 4, '매도금액': 5, '매수가': 6,
                           '매도가': 7, '수수료': 8, '세금': 9}
stockPredictTable_column = {'기준일': 0, '종목명': 1, '정확도': 2, '예상값': 3}

sim_df_column = {'Date': '일자', 'Gubun': '구분', 'Amount': '수량', 'Price': '매매가', 'Charge': '수수료', 'Tax': '세금',
                 'Rate': '수익률', 'Profit': '수익금'}

sel_market_list = {'All': 0, 'KO':1, 'KQ':2}
sel_top_list = {'All': 0, 'TOP10':1, 'TOP20':2, 'TOP50':3, 'TOP100':4, 'TOP200':5}

OPEN_TIME = 9           # 장 시작시간(09시)
CLOSE_TIME = 16         # 장 마감시간(16시)
CHART_MOVE = 10         # 차트화면 STEP
CHARGE_RATE = 0.00015   # 매매 수수료율
TAX_RATE = 0.0025       # 매매 수수료율

DELAY_TIME = 0.2

EPOCH = 32              # 머신러닝 EPOCH
BATCH_SIZE = 1          # 머신러닝 BATCH_SIZE

DATA_DIM = 5

class TrainingMarket(QMainWindow, main_form):
    def __init__(self):
        super().__init__()
        self.setupUi(self)

        # Event Handler
        self.sel_marketComboBox.currentTextChanged.connect(self.create_stock_table)
        self.sel_topComboBox.currentTextChanged.connect(self.create_stock_table)
        self.stockTable.cellClicked.connect(lambda:self.draw_stock_chart('DAY'))
        self.stockTable.itemSelectionChanged.connect(lambda:self.draw_stock_chart('DAY'))
        self.rangeComboBox.currentTextChanged.connect(self.redraw_stock_chart)
        self.chartScrollBar.valueChanged.connect(self.scroll_stock_chart)
        self.runPushButton.clicked.connect(self.run_simulation)
        self.run_totalPushButton.clicked.connect(self.run_total_simulation)
        self.run_rankPushButton.clicked.connect(self.run_rank_simulation)
        self.run_all_trainPushButton.clicked.connect(self.run_all_train_simulation)
        self.run_one_trainPushButton.clicked.connect(self.run_one_train_simulation)
        self.run_all_predictPushButton.clicked.connect(self.run_all_predict_simulation)
        self.run_one_predictPushButton.clicked.connect(self.run_one_predict_simulation)
        self.clear_sim_resultPushButton.clicked.connect(self.clear_sim_result)

        # DB Connect 설정
        self.kospi_db = sqlite3.connect("./datas/kospi.db")
        self.kosdaq_db = sqlite3.connect("./datas/kosdaq.db")
        self.kospi_cur = self.kospi_db.cursor()
        self.kosdaq_cur = self.kosdaq_db.cursor()

        self.sim_db = sqlite3.connect("./datas/sim_result.db")
        self.sim_cur = self.sim_db.cursor()

        # 차트 출력용 화면 구성
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

        # 시뮬레이션 예상 테이블 초기화
        self.clear_simulation_predict_table()

        # 전역변수 초기화
        self.chart_df = None

        # 머신러닝(RNN) 결과 저장용 테이블 생성
        self.create_db_rnn_train_table()
        self.create_db_rnn_test_table()
        self.create_db_rnn_predict_table()


    def create_stock_table(self):
        self.stockTable.setRowCount(0)

        sel_market = self.sel_marketComboBox.currentText()
        sel_top = self.sel_topComboBox.currentText()

        if sel_top == '전체':
            sel_sql = "SELECT Code, Name FROM " \
                      "(SELECT 종목코드 as Code, 종목명 as Name, cast(시가총액 as decimal) as Price " \
                      "FROM STOCKS_INFO ORDER BY Price desc)"
        else:
            top_num = sel_top[3:]
            sel_sql = "SELECT Code, Name FROM " \
                      "(SELECT 종목코드 as Code, 종목명 as Name, cast(시가총액 as decimal) as Price " \
                      "FROM STOCKS_INFO ORDER BY Price desc limit {})".format(top_num)

        if sel_market == '코스피':
            if self.sel_topComboBox.isEnabled() == False:
                self.sel_topComboBox.setEnabled(True)

            kospi_market_info = pd.read_sql(sel_sql, self.kospi_db)
            kospi_market_info['type'] = 'KO'
            kospi_cnt = len(kospi_market_info.index)
            kosdaq_cnt = 0

            df = kospi_market_info

        elif sel_market == '코스닥':
            if self.sel_topComboBox.isEnabled() == False:
                self.sel_topComboBox.setEnabled(True)

            kosdaq_market_info = pd.read_sql(sel_sql, self.kosdaq_db)
            kosdaq_market_info['type'] = 'KQ'
            kospi_cnt = 0
            kosdaq_cnt = len(kosdaq_market_info.index)

            df = kosdaq_market_info

        else:
            self.sel_topComboBox.setCurrentIndex(0)
            if self.sel_topComboBox.isEnabled() == True:
                self.sel_topComboBox.setEnabled(False)

            kospi_market_info = pd.read_sql(sel_sql, self.kospi_db)
            kosdaq_market_info = pd.read_sql(sel_sql, self.kosdaq_db)

            kospi_market_info['type'] = 'KO'
            kosdaq_market_info['type'] = 'KQ'

            kospi_cnt = len(kospi_market_info.index)
            kosdaq_cnt = len(kosdaq_market_info.index)

            # 종목명과 코드로 정렬
#            kospi_market_info = kospi_market_info.sort_values(by=['Name', 'Code'])
#            kosdaq_market_info = kosdaq_market_info.sort_values(by=['Name', 'Code'])

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
            query1 = "SELECT * FROM sqlite_master WHERE type='table' AND name='D" + code + "'"
            query2 = "SELECT Close, Volume FROM D" + code + " WHERE Date = (SELECT max(Date) FROM D" + code + ")"

            if type == 'KO':
                self.kospi_cur.execute(query1)
                table_yn = self.kospi_cur.fetchone()

                if table_yn is not None:
                    self.kospi_cur.execute(query2)
                    datas = self.kospi_cur.fetchone()
                else:
                    datas = ('None', 'None')
            else:
                self.kosdaq_cur.execute(query1)
                table_yn = self.kosdaq_cur.fetchone()

                if table_yn is not None:
                    self.kosdaq_cur.execute(query2)
                    datas = self.kosdaq_cur.fetchone()
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

        input_validator = QtGui.QRegExpValidator(reg_ex, self.buy_cond0LineEdit)
        self.buy_cond0LineEdit.setValidator(input_validator)

        input_validator = QtGui.QRegExpValidator(reg_ex, self.depositLineEdit)
        self.depositLineEdit.setValidator(input_validator)

        self.sim_endDateEdit.setDate(datetime.datetime.today())

        # 시뮬레이션 결과 저장용 DataFrame
        self.sim_df = DataFrame({'Name':[], 'Date':[], 'Gubun':[], 'Amount':[], 'Price':[], 'Charge':[], 'Tax':[], 'Rate':[],
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
        self.stockSimTable.setColumnWidth(stockSimTable_column['종목명'], 220)
        self.stockSimTable.setColumnWidth(stockSimTable_column['일자'], 85)
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
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['매수일'], 85)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['수량'], 50)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['매수가'], 70)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['평가손익'], 100)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['수익률'], 60)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['현재가'], 70)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['매수금액'], 100)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['평가금액'], 100)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['수수료'], 60)
        self.stockRemainTable.setColumnWidth(stockRemainTable_column['세금'], 60)
        self.remain_profitLabel.setText(str(format(0, ',')))
        self.remain_rateLabel.setText(str(round(0.0, 2)) + '%')


    def clear_simulation_profit_table(self):
        self.stockProfitTable.setRowCount(0)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['종목명'], 220)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['매도일'], 85)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['실현손익'], 100)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['수익률'], 60)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['매수금액'], 100)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['매도금액'], 100)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['매수가'], 70)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['매도가'], 70)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['수수료'], 60)
        self.stockProfitTable.setColumnWidth(stockProfitTable_column['세금'], 60)
        self.profit_profitLabel.setText(str(format(0, ',')))
        self.profit_rateLabel.setText(str(round(0.0, 2)) + '%')


    def clear_simulation_predict_table(self):
        self.stockPredictTable.setRowCount(0)
        self.stockPredictTable.setColumnWidth(stockPredictTable_column['기준일'], 85)
        self.stockPredictTable.setColumnWidth(stockPredictTable_column['종목명'], 220)
        self.stockPredictTable.setColumnWidth(stockPredictTable_column['정확도'], 100)
        self.stockPredictTable.setColumnWidth(stockPredictTable_column['예상값'], 100)


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

#        self.top_axes.plot(df_datas['MA5'],   linestyle='solid', marker='None', color='m', label='MA_5')
#        self.top_axes.plot(df_datas['MA10'],  linestyle='solid', marker='None', color='b', label='MA_10')
#        self.top_axes.plot(df_datas['MA20'],  linestyle='solid', marker='None', color='orange', label='MA_20')
#        self.top_axes.plot(df_datas['MA60'],  linestyle='solid', marker='None', color='g', label='MA_60')
#        self.top_axes.plot(df_datas['MA120'], linestyle='solid', marker='None', color='gray', label='MA_120')

        self.top_axes.set_position([0.02, 0.37, 0.88, 0.6])
        self.top_axes.tick_params(axis='both', color='#ffffff', labelcolor='#ffffff')
        self.top_axes.grid(color='lightgray', linewidth=.5, linestyle=':')
#        self.top_axes.legend(loc='upper left', ncol=5, fontsize='xx-small')
        self.top_axes.yaxis.tick_right()
        self.top_axes.autoscale_view()
        self.top_axes.set_facecolor('#041105')

        self.bottom_axes.xaxis.set_major_locator(ticker.FixedLocator(day_list))
        self.bottom_axes.xaxis.set_major_formatter(ticker.FixedFormatter(name_list))

        self.bottom_axes.bar(np.arange(len(df_datas.index)), df_datas['Volume'], color='white', width=0.5, align='center')

#        self.bottom_axes.plot(df_datas['VMA5'],   linestyle='solid', marker='None', color='m', label='MA_5')
#        self.bottom_axes.plot(df_datas['VMA10'],  linestyle='solid', marker='None', color='b', label='MA_10')
#        self.bottom_axes.plot(df_datas['VMA20'],  linestyle='solid', marker='None', color='orange', label='MA_20')
#        self.bottom_axes.plot(df_datas['VMA60'],  linestyle='solid', marker='None', color='g', label='MA_60')
#        self.bottom_axes.plot(df_datas['VMA120'], linestyle='solid', marker='None', color='gray', label='MA_120')

        self.bottom_axes.set_position([0.02, 0.15, 0.88, 0.22])
        self.bottom_axes.tick_params(axis='both', color='#ffffff', labelcolor='#ffffff')
        self.bottom_axes.grid(color='lightgray', linewidth=.5, linestyle=':')
#        self.bottom_axes.legend(loc='upper left', ncol=5, fontsize='xx-small')
        self.bottom_axes.yaxis.tick_right()
        self.bottom_axes.autoscale_view()
        self.bottom_axes.set_facecolor('#041105')

        self.canvas.draw()


    def draw_stock_chart(self, chart_type):
#        self.clear_simulation_result_table()

        selected_rows = self.stockTable.selectedIndexes()
        if len(selected_rows) != 0:
            index = self.stockTable.selectedIndexes()[0].row()

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥
            value_item = self.stockTable.item(index, stockTable_column['현재가'])
            if value_item is not None:
                value = value_item.text()
            else:
                self.fig.clear()
                self.top_axes.cla()
                self.bottom_axes.cla()
                self.canvas.draw()

                self.chart_df = DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume', 'Gubun'])
                return

            if value != 'None':     # 현재가가 None이면 chart 테이블이 존재하지 않음
                if chart_type == 'DAY':
                    table_name = 'D'+code
                else:
                    table_name = 'S'+code

                if type == 'KO':
                    self.chart_df = pd.read_sql("SELECT * from " + table_name + " WHERE Volume != 0 ORDER BY Date", con=self.kospi_db)
                else:
                    self.chart_df = pd.read_sql("SELECT * from " + table_name + " WHERE Volume != 0 ORDER BY Date", con=self.kosdaq_db)

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
                sliced_df = self.chart_df.loc[begin_index:]

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

        sliced_df = self.chart_df.iloc[begin_iloc:end_iloc+1]

        self.draw_chart_plot(sliced_df)


    def add_buy_sim_df(self, index_date, price):
        # 종목명
        index = self.stockTable.selectedIndexes()[0].row()
        code = self.stockTable.item(index, stockTable_column['종목코드']).text()
        name = self.stockTable.item(index, stockTable_column['종목명']).text()
        name = '(' + code + ')' + name

        amount = int(self.deposit / price)                       # 매수수량(종가 매수)
        buy_value = amount * price                               # 총매수 금액
        charge = int((buy_value * CHARGE_RATE) / 10) * 10        # 수수료 10원이하 절사

        # 매수에 따른 예수금 감소
        self.deposit = self.deposit - buy_value

        data = [name, index_date, '매수', amount, price, charge, 0, 0, 0, 0, 0]
        self.sim_df.loc[len(self.sim_df)] = data

        self.write_stockSimTable('매수')
        print("[{}] ({}) 매수 : 수량[{}], 매수가[{}]".format(name, index_date, amount, price))


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
        row_cnt = self.stockSimTable.rowCount()
        self.stockSimTable.setRowCount(row_cnt+1)

        item = QTableWidgetItem(self.sim_df['Name'].iloc[index])
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        self.stockSimTable.setItem(row_cnt, stockSimTable_column['종목명'], item)

        date = datetime.datetime.strptime(self.sim_df['Date'].iloc[index], '%Y%m%d')
        item = QTableWidgetItem(date.strftime('%Y-%m-%d'))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
        self.stockSimTable.setItem(row_cnt, stockSimTable_column['일자'], item)

        item = QTableWidgetItem(self.sim_df['Gubun'].iloc[index])
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
        self.stockSimTable.setItem(row_cnt, stockSimTable_column['구분'], item)

        item = QTableWidgetItem(str(format(self.sim_df['Amount'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockSimTable.setItem(row_cnt, stockSimTable_column['수량'], item)

        item = QTableWidgetItem(str(format(self.sim_df['Price'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockSimTable.setItem(row_cnt, stockSimTable_column['매매가'], item)

        item = QTableWidgetItem(str(format(self.sim_df['Charge'].iloc[index].astype('int'), ',')))
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
        self.stockSimTable.setItem(row_cnt, stockSimTable_column['수수료'], item)

        if type == '매도':
            item = QTableWidgetItem(str(format(self.sim_df['Tax'].iloc[index].astype('int'), ',')))
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.stockSimTable.setItem(row_cnt, stockSimTable_column['세금'], item)

            item = QTableWidgetItem(str(round(self.sim_df['Rate'].iloc[index].astype('float') * 100, 2)) + '%')
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.stockSimTable.setItem(row_cnt, stockSimTable_column['수익률'], item)

            item = QTableWidgetItem(str(format(self.sim_df['Profit'].iloc[index].astype('int'), ',')))
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.stockSimTable.setItem(row_cnt, stockSimTable_column['수익금'], item)

            # 누적 수익금 계산
            item = QTableWidgetItem(str(format(self.sim_df['Profit'].sum().astype('int'), ',')))
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.stockSimTable.setItem(row_cnt, stockSimTable_column['누적수익금'], item)
        else:
            pass


    def MFI_indicater(self, index_date, period):
        last_loc = np.where(self.chart_df.index==index_date)[0]
        begin_loc = last_loc - period

        if begin_loc < 0:
            return None

        PMF = 0
        NMF = 0
        prev_price = 0
        for i in range(begin_loc[0], last_loc[0]+1):
            high = self.chart_df.iloc[i]['High']
            low = self.chart_df.iloc[i]['Low']
            close = self.chart_df.iloc[i]['Close']
            volume = self.chart_df.iloc[i]['Volume']
            price = (high + low + close) / 3
            MF = price * volume

            if i == begin_loc[0]:
                prev_price = price
                continue

            if price > prev_price:
                PMF += MF

            if price < prev_price:
                NMF += MF

            prev_price = price

        if NMF != 0:
            MR = PMF / NMF
        else:
            MR = 0

        MFI = 100 - (100/(1+MR))
        return round(MFI, 2)


    def RSI_indicater(self, index_date, period, signal):
        last_loc = np.where(self.chart_df.index==index_date)[0]
        begin_loc = last_loc - period - signal

        if begin_loc < 0:
            return None, None

        SIG = np.array([])
        for s in range(1, signal+1):
            U = 0
            D = 0
            prev_price = 0
            for i in range(begin_loc[0]+s+1, begin_loc[0]+s+period+2):
                price = self.chart_df.iloc[i]['Close']

                if i == begin_loc[0]+s+1:
                    prev_price = price
                    continue

                if price > prev_price:
                    U += (price - prev_price)

                if price < prev_price:
                    D += (prev_price - price)

                prev_price = price

            AU = U / period
            AD = D / period

            if (AU+AD) != 0:
                RSI = 100*AU/(AU+AD)
            else:
                RSI = 0

            SIG = np.append(SIG, RSI)

        return round(RSI, 2), round(SIG.mean(), 2)


    def RSI2_indicater(self, index_date, period, signal):
        last_loc = np.where(self.chart_df.index==index_date)[0]
        length = signal+period
        begin_loc = last_loc - period - length

        if begin_loc < 0:
            return None, None

        SIG = np.array([])
        SIG2 = np.array([])
        for s in range(0, length):
            U, D = 0, 0
            prev_price = 0
            for i in range(begin_loc[0]+s+1, begin_loc[0]+s+period+2):
                U2, D2 = 0, 0
                price = self.chart_df.iloc[i]['Close']

                if i == begin_loc[0]+s+1:
                    prev_price = price
                    continue

                if price > prev_price:
                    U += (price - prev_price)
                    U2 = price - prev_price


                if price < prev_price:
                    D += (prev_price - price)
                    D2 = prev_price - price

                prev_price = price

            AU = U / period
            AD = D / period

            if AD != 0:
                RS = AU / AD
            else:
                RS = 0

            RSI = 100 - 100 / (1+RS)

            SIG = np.append(SIG, RSI)

            if s != 0:
                AU2 = (AU2*(period-1) + U2) / period
                AD2 = (AD2*(period-1) + D2) / period

                if AD2 != 0:
                    RS2 = AU2 / AD2
                else:
                    RS2 = 0
                RSI2 = 100 - 100 / (1 + RS2)
            else:
                AU2, AD2 = AU, AD
                RSI2 = RSI

            SIG2 = np.append(SIG2, RSI2)

        return round(RSI2, 2), round(SIG2[-1*signal:].mean(), 2)


    def sim_buy_condition1(self, code, type, index_date, sim_data, accuracy):
        # 당일 시작가과 종가
        day_open = sim_data['Open']
        day_close = sim_data['Close']

        # 기준단가 만족여부 검사
        if day_close < self.buy_cond0_value:
            return False

        # MFI 값이 20 이하이면 매수
        if self.buy_cond1 == 'MFI(20이하)':
            mfi_value = self.MFI_indicater(index_date, 14)
            if mfi_value == None or mfi_value > 20:
                return False

        # RSI 값이 시그널 이하이면 매수
        elif self.buy_cond1 == 'RSI(30이하)':
            rsi_value, rsi_signal = self.RSI2_indicater(index_date, 14, 6)
            if rsi_value == None or rsi_value > 30:
                return False
        else:
            pass

        # 종가 매수
        if self.deposit >= (day_close*2) and day_close >= self.buy_cond0_value:
            self.add_buy_sim_df(index_date, day_close)

        return True


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


    def add_sell_sim_df(self, index_date, sell_amount, price, buy_charge, buy_value, buy_price):       # 시뮬레이션 결과 설정
        # 종목명
        index = self.stockTable.selectedIndexes()[0].row()
        code = self.stockTable.item(index, stockTable_column['종목코드']).text()
        name = self.stockTable.item(index, stockTable_column['종목명']).text()
        name = '(' + code + ')' + name

        # 시뮬레이션 결과 설정
        sell_value = sell_amount * price                     # 매도 총금액
        charge = int((sell_value * CHARGE_RATE) / 10) * 10   # 수수료 10원이하 절사
        tax = int((sell_value * TAX_RATE))                   # 세금

        # 매수/매도 수수료 및 세금 계산
        charge += buy_charge
        total_tax = charge + tax

        # 수익금 계산
        profit = sell_value - buy_value - total_tax

        # 수익률 계산 (수수료 및 세금을 제외한 순수 수익률)
        rate = profit / buy_value

        # 예수금 계산
        self.deposit = self.deposit + (sell_value - total_tax)

        data = [name, index_date, '매도', sell_amount, price, charge, tax, rate, profit, 0, 0]
        self.sim_df.loc[len(self.sim_df)] = data

        self.write_stockSimTable('매도')
        print("[{}] ({}) 매도 : 수량[{}], 매매가[{}], 수익금[{}], 수익률[{}]".format(name, index_date, int(sell_amount), price, str(format(int(profit), ',')), str(round(rate * 100, 2)) + '%'))

        data = [name, index_date, profit, rate, buy_value, sell_value, buy_price, price, charge, tax]
        self.profit_df.loc[len(self.profit_df)] = data

        self.write_stockProfitTable()

        # 전체 실현손익 및 평균 수익률 계산
        total_profit = int(self.profit_df['Profit'].sum())
        stock_cnt = self.profit_df.Name.nunique()
        avg_rate = total_profit / (stock_cnt * self.init_deposit)

        self.profit_profitLabel.setText(str(format(total_profit, ',')))
        self.profit_rateLabel.setText(str(round(avg_rate * 100, 2)) + '% (매매종목수 : ' + str(stock_cnt) + ')')

        print("* 총 {} 종목 전체 실현손익 : 수익[ {} ], 수익률[ {} ]".format(str(stock_cnt), str(format(total_profit, ',')), str(round(avg_rate * 100, 2)) + '%'))


    def sim_sell_condition1(self, code, type, index_date, sim_data):
        # 당일 시작가과 종가 구하기
        day_open = sim_data['Open']
        day_close = sim_data['Close']

        if len(self.sim_df) > 0:
            buy_index = len(self.sim_df) - 1
        else:
            return

        # 동일날짜이면 처리하지 않음
        if self.sim_df['Date'].iloc[buy_index] == index_date:
            return

        if self.sim_df['Gubun'].iloc[buy_index] == '매수':
            sell_amount = self.sim_df['Amount'].iloc[buy_index]
            buy_price = self.sim_df['Price'].iloc[buy_index]
            buy_value = sell_amount * buy_price
            buy_charge = self.sim_df['Charge'].iloc[buy_index]
        else:
            return

        # 손절선 확인
        cut_rate = (day_close - buy_price) / buy_price
        if self.sell_cond0_value < cut_rate:
            # MFI 값이 80 이상이면 매도
            if self.sell_cond1 == 'MFI(80이상)':
                mfi_value = self.MFI_indicater(index_date, 14)
                if mfi_value == None or mfi_value < 80:
                    return
            # RSI 값이 시그널 이상이면 매도
            elif self.sell_cond1 == 'RSI(70이상)':
                rsi_value, rsi_signal = self.RSI2_indicater(index_date, 14, 6)
                if rsi_value == None or rsi_value < 70:
                    return
            else:
                pass

        if sell_amount > 0:
            self.add_sell_sim_df(index_date, sell_amount, day_close, buy_charge, buy_value, buy_price)


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
        print("[{}] ({}) 잔고 : 수량[{}], 매매가[{}], 수익금[{}], 수익률[{}]".format(name, date, int(amount), current, str(format(int(profit), ',')), str(round(rate * 100, 2)) + '%'))

        # 전체 평가손익 및 평균 수익률 계산
        total_profit = int(self.remain_df['Profit'].sum())
        stock_cnt = self.remain_df.Name.nunique()
        avg_rate = total_profit / (stock_cnt * self.init_deposit)

        self.remain_profitLabel.setText(str(format(total_profit, ',')))
        self.remain_rateLabel.setText(str(round(avg_rate * 100, 2)) + '% (매매종목수 : ' + str(stock_cnt) + ')')

        print("* 총 {} 종목 전체 잔고 : 손익[ {} ], 수익률[ {} ]".format(self.remain_df.Name.nunique(), str(format(total_profit, ',')), str(round(avg_rate * 100, 2)) + '%'))


    def create_rnn_model(self, seq_len, data_dim):
        model = keras.models.Sequential([
            keras.layers.LSTM(units=256, batch_input_shape=(1, seq_len, data_dim), return_sequences=True,
                              kernel_initializer='he_uniform'),
#            keras.layers.Dropout(0.5),
            keras.layers.LSTM(units=128, return_sequences=False, kernel_initializer='he_uniform'),
#            keras.layers.Dropout(0.5),
            keras.layers.Dense(64, activation='relu', kernel_initializer='he_uniform'),
#            keras.layers.Dropout(0.5),
            keras.layers.Dense(1, activation='sigmoid', kernel_initializer='he_uniform')
        ])
#        model.compile(optimizer='adam', loss='mse', metrics=['accuracy'])
#        model.compile(optimizer=keras.optimizers.Adam(1e-4), loss='mse', metrics=['accuracy'])
        model.compile(optimizer=keras.optimizers.Adam(1e-4), loss='binary_crossentropy', metrics=['accuracy'])
        return model


    def create_nn_model(self, seq_len, data_dim):
        model = keras.models.Sequential([
            keras.layers.Flatten(input_shape=(seq_len, data_dim)),
            keras.layers.Dense(256, activation='relu', kernel_initializer='he_uniform'),
            keras.layers.Dense(64, activation='relu', kernel_initializer='he_uniform'),
            keras.layers.Dense(1, activation='sigmoid', kernel_initializer='he_uniform')
        ])
        model.compile(optimizer=keras.optimizers.Adam(1e-4), loss='binary_crossentropy', metrics=['accuracy'])
        return model


    def create_db_rnn_train_table(self):
        # 해당 종목의 차트 테이블 생성
        query = "CREATE TABLE IF NOT EXISTS RNN_TRAIN (code TEXT, predict_start_date TEXT, seq_len INTEGER, data_dim INTEGER, epoch INTEGER, PRIMARY KEY(code, predict_start_date, seq_len, data_dim, epoch))"

        self.sim_cur.execute(query)
        self.sim_db.commit()


    def create_db_rnn_test_table(self):
        # 해당 종목의 차트 테이블 생성
        query = "CREATE TABLE IF NOT EXISTS RNN_TEST (code TEXT, predict_start_date TEXT, accuracy_sum REAL, accuracy_cnt REAL, PRIMARY KEY(code, predict_start_date))"

        self.sim_cur.execute(query)
        self.sim_db.commit()


    def get_rnn_test(self, code, start_date):
        query = "SELECT accuracy_sum, accuracy_cnt FROM RNN_TEST WHERE code='{}' AND predict_start_date='{}'".format(code, start_date)

        self.sim_cur.execute(query)
        result = self.sim_cur.fetchone()

        if result is None:
            return (None, None)
        else:
            (accuracy_sum, accuracy_cnt) = (result[0], result[1])
            return (accuracy_sum, accuracy_cnt)


    def save_rnn_test(self, code, start_date, accuracy_sum, accuracy_cnt):
        query1 = "INSERT OR IGNORE INTO RNN_TEST(code, predict_start_date, accuracy_sum, accuracy_cnt)VALUES('{}', '{}', '{}', '{}');".format(code, start_date, accuracy_sum, accuracy_cnt)
        query2 = "UPDATE RNN_TEST SET accuracy_sum='{}', accuracy_cnt='{}' WHERE code='{}' AND predict_start_date='{}';".format(accuracy_sum, accuracy_cnt, code, start_date)

        self.sim_cur.execute(query1)
        self.sim_db.commit()
        self.sim_cur.execute(query2)
        self.sim_db.commit()


    def create_db_rnn_predict_table(self):
        # 해당 종목의 차트 테이블 생성
        query = "CREATE TABLE IF NOT EXISTS RNN_PREDICT (code TEXT, predict_last_date TEXT, accuracy_sum REAL, accuracy_cnt REAL, PRIMARY KEY(code, predict_last_date))"

        self.sim_cur.execute(query)
        self.sim_db.commit()


    def get_rnn_predict(self, code, last_date):
        query = "SELECT accuracy_sum, accuracy_cnt FROM RNN_PREDICT WHERE code='{}' AND predict_last_date='{}'".format(code, last_date)

        self.sim_cur.execute(query)
        result = self.sim_cur.fetchone()

        if result is None:
            return (None, None)
        else:
            (accuracy_sum, accuracy_cnt) = (result[0], result[1])
            return (accuracy_sum, accuracy_cnt)


    def save_rnn_predict(self, code, last_date, accuracy_sum, accuracy_cnt):
        query1 = "INSERT OR IGNORE INTO RNN_PREDICT(code, predict_last_date, accuracy_sum, accuracy_cnt)VALUES('{}', '{}', '{}', '{}');".format(code, last_date, accuracy_sum, accuracy_cnt)
        query2 = "UPDATE RNN_PREDICT SET accuracy_sum='{}', accuracy_cnt='{}' WHERE code='{}' AND predict_last_date='{}';".format(accuracy_sum, accuracy_cnt, code, last_date)

        self.sim_cur.execute(query1)
        self.sim_db.commit()
        self.sim_cur.execute(query2)
        self.sim_db.commit()


    def train_only_run(self, code, market_type, start_index, end_index, model):
        # 스탁데이터셋 클래스 생성
        stock_market = StockDataset(start_index, end_index, self.seq_length)

        # 종목 트레이닝 데이터 및 테스트 데이터 로딩
        (train_x, train_y1, train_y2), (test_x, test_y1, test_y2) = stock_market.load_train_data(code, market_type, 'DAY')

        if train_x is None:
            print("데이터로딩 에러", "학습할 데이터가 없거나 읽어올수 없습니다!")
            return (model, False)

        train_x = np.append(train_x, test_x, axis=0)
        train_y1 = np.append(train_y1, test_y1, axis=0)
        train_y2 = np.append(train_y2, test_y2, axis=0)

        history = model.fit(train_x, train_y2, batch_size=1, epochs=EPOCH)

        return (model, True)


    def predict_only_run(self, code, market_type, start_index, end_index, sim_datas, model):
        # 스탁데이터셋 클래스 생성
        stock_market = StockDataset(start_index, end_index, self.seq_length)

        # 종목 트레이닝 데이터 및 테스트 데이터 로딩
        (train_x, train_y1, train_y2), (test_x, test_y1, test_y2) = stock_market.load_train_data(code, market_type, 'DAY')

        if train_x is None:
            print("데이터로딩 에러", "학습할 데이터가 없거나 읽어올수 없습니다!")
            return (model, False)

        logs = []  # log 기록용 리스트 변수 정의

        # 매매 시뮬레이션 시작 -------------------------------------------------------------------------------------------
        correct = np.array([])          # 예측이 맞는 경우의 정답률 계산
        index_cnt = 0
        accuracy_cnt = 0
        accuracy = 0

        # 시뮬레이션 시작일과 예측일(D+2)간 하루차이가 발생해야하므로 첫번째 데이터는 넘김(시작일:01.02 --> 예측일:01.03)
        (test_x, test_y1, test_y2, today, today_close, tomorrow, tomorrow_close) = stock_market.load_next_step_data()

        while (True):
            (test_x, test_y1, test_y2, today, today_close, tomorrow, tomorrow_close) = stock_market.load_next_step_data()

            if test_x is None:
                break

            if sim_datas.index[index_cnt] != today:
                print("[CODE={}] 시뮬레이션 날짜 불일치 =======> sim_datas {} <-> load_next {}".format(code, sim_datas.index[index_cnt], today))
                return False

            # 예측값 도출
            predict_y = model.predict(test_x)

            if predict_y[0][0] > self.buy_accuracy_value:
                # 매수조건 실행
                result = self.sim_buy_condition1(code, market_type, sim_datas.index[index_cnt], sim_datas.iloc[index_cnt], accuracy=1)

                if result == False:
                    # 매도조건 실행
                    self.sim_sell_condition1(code, market_type, sim_datas.index[index_cnt], sim_datas.iloc[index_cnt])

                if test_y2[0] == 1:
                    correct = np.append(correct, [1])
                else:
                    correct = np.append(correct, [0])

                accuracy_cnt += 1
#                accuracy = correct.sum() / accuracy_cnt
                accuracy = correct.mean()
            else:
                # 매도조건 실행
                self.sim_sell_condition1(code, market_type, sim_datas.index[index_cnt], sim_datas.iloc[index_cnt])

            # 하루치 데이터 학습
#            model.fit(test_x, test_y2, batch_size=1, epochs=EPOCH)

            index_cnt += 1

        # 시뮬레이션 잔고 설정
        self.add_remain_df()

        if len(correct) > 0:
            accuracy_sum = correct.sum()

        last_fit_date = sim_datas.index[index_cnt-2]
        total_profit = str(format(self.sim_df['Profit'].sum().astype('int'), ','))
        total_rate = round((self.sim_df['Profit'].sum().astype('int') / self.init_deposit)*100, 2)
        logs.append("------------------------------------------------------------")
        logs.append("[{}][PREDICT 전체 정확도] {} (학습마지막일:{}) 총수익금[{}], 총수익률[{}]".format(code, round(accuracy, 2), last_fit_date, total_profit, str(total_rate)+'%'))
        logs.append("------------------------------------------------------------")

        # 이전 머신러닝 PREDICT 결과를 불러옴
#        (saved_accuracy_sum, saved_accuracy_cnt) = self.get_rnn_predict(code, last_fit_date)

#        if saved_accuracy_sum is None:
#            logs.append("[종목코드({}) | 마지막예측일({})] 최초 학습모델 저장!!!!!!!!!".format(code, last_fit_date))
#            self.save_rnn_predict(code, last_fit_date, accuracy_sum, accuracy_cnt)
#            model.save('./models/rnn_' + code + '_' + last_fit_date + '.h5')
#        else:
#            saved_accuracy = saved_accuracy_sum / saved_accuracy_cnt

#            if saved_accuracy < accuracy:
#                logs.append("[종목코드({}) | 예측요청일({})] 새로운 수익모델 저장 *******************".format(code, last_fit_date))
#                logs.append("----->저장된 모델 정확도 : {} <-> 현재 모델 정확도 : {}".format(saved_accuracy, accuracy))
#               self.save_rnn_test(code, last_fit_date, accuracy_sum, accuracy_cnt)
#                model.save('./models/rnn_' + code + '_' + last_fit_date + '.h5')
#        logs.append("------------------------------------------------------------")

        for log in logs:
            print(log)

        return (model, True)

        
    def ai_model_run(self, code, market_type, start_index, end_index, sim_datas):
        # 스탁데이터셋 클래스 생성
        stock_market = StockDataset(start_index, end_index, self.seq_length)

        # 종목 트레이닝 데이터 및 테스트 데이터 로딩
        (train_x, train_y1, train_y2), (test_x, test_y1, test_y2) = stock_market.load_train_data(code, market_type, 'DAY')

        if train_x is None:
            print("데이터로딩 에러", "학습할 데이터가 없거나 읽어올수 없습니다!")
            return False

        logs = []  # log 기록용 리스트 변수 정의

        # 머신러링 시작 ----------------------------------------------------------------------------------------
        # RNN 모델 생성 및 학습
        model = self.create_rnn_model(stock_market.seq_len, stock_market.data_dim)

        # 이전 머신러닝 Train 결과를 불러옴
        (saved_accuracy_sum, saved_accuracy_cnt) = self.get_rnn_test(code, start_index)

        if saved_accuracy_sum is None:
            history = model.fit(train_x, train_y2, batch_size=1, epochs=EPOCH)

            loss, acc = model.evaluate(test_x, test_y2)
            print("Training accuracy: {:5.2f}%".format(100 * acc))

            # Test 데이터로 학습한 결과로 예측
            predict_ys = model.predict(test_x)

            correct = np.array([])  # 실제주가가 상승한 날을 맞춘 일수 카운트

            logs.append("------------------------------------------------------------")
            for i in range(len(test_y2)):
                predict_y, real_y = predict_ys[i], test_y2[i]
                if predict_y > self.buy_accuracy_value:
                    if real_y == 1:
                        logs.append("[index={} 매수] {} ({}:{})".format(i, '빙고', predict_y[0], real_y))
                        correct = np.append(correct, [1])
                    else:
                        logs.append("[index={} 매수] {} ({}:{})".format(i, 'T-T', predict_y[0], real_y))
                        correct = np.append(correct, [0])

            accuracy_cnt = len(correct)
            if accuracy_cnt > 0:
                accuracy_sum = correct.sum()
                accuracy = accuracy_sum / accuracy_cnt
            else:
                accuracy_sum = 0.0
                accuracy = 0.0

            logs.append("------------------------------------------------------------")
            logs.append("[종목코드({}) TEST 정확도] {}".format(code, accuracy))
            logs.append("------------------------------------------------------------")

            # Test 데이터 학습
            model.fit(test_x, test_y2, batch_size=1, epochs=EPOCH)

            logs.append("[종목코드({}) | 예측요청일({})] 최초 학습모델 저장!!!!!!!!!!!!!!!!!!!!!".format(code, start_index))
            self.save_rnn_test(code, start_index, accuracy_sum, accuracy_cnt)
            model.save('./models/rnn_' + code + '_' + start_index + '.h5')

        else:
            saved_accuracy = saved_accuracy_sum / saved_accuracy_cnt
            logs.append("[종목코드({}) | 예측요청일({})] 저장된 학습모델 재사용!!!!!!!!!!!!!!!!!!".format(code, start_index))
            model = keras.models.load_model('./models/rnn_' + code + '_' + start_index + '.h5')

            accuracy_cnt = saved_accuracy_cnt
            accuracy_sum = saved_accuracy_sum
            accuracy = accuracy_sum / accuracy_cnt

        logs.append("------------------------------------------------------------")

        # 매매 시뮬레이션 시작 -------------------------------------------------------------------------------------------
        correct = np.array([])          # 예측이 맞는 경우의 정답률 계산
        index_cnt = 0

        # 시뮬레이션 시작일과 예측일(D+2)간 하루차이가 발생해야하므로 첫번째 데이터는 넘김(시작일:01.02 --> 예측일:01.03)
        (test_x, test_y1, test_y2, today, today_close, tomorrow, tomorrow_close) = stock_market.load_next_step_data()

        while (True):
            (test_x, test_y1, test_y2, today, today_close, tomorrow, tomorrow_close) = stock_market.load_next_step_data()

            if test_x is None:
                break

            print("sim_datas today={}(index={}) load......".format(today, sim_datas.index[index_cnt]))

            if sim_datas.index[index_cnt] != today:
                print("시뮬레이션 날짜 불일치 =======> sim_datas {} <-> load_next {}".format(sim_datas.index[index_cnt], today))
                return False

            # 예측값 도출
            predict_y = model.predict(test_x)

            if predict_y[0][0] > self.buy_accuracy_value:
                # 매수조건 실행
                result = self.sim_buy_condition1(code, market_type, sim_datas.index[index_cnt], sim_datas.iloc[index_cnt], accuracy)

                if result == False:
                    # 매도조건 실행
                    self.sim_sell_condition1(code, market_type, sim_datas.index[index_cnt], sim_datas.iloc[index_cnt])

                if test_y2[0] == 1:
                    correct = np.append(correct, [1])
                else:
                    correct = np.append(correct, [0])

                accuracy_cnt += 1
                accuracy = (accuracy_sum + correct.sum()) / accuracy_cnt
            else:
                # 매도조건 실행
                self.sim_sell_condition1(code, market_type, sim_datas.index[index_cnt], sim_datas.iloc[index_cnt])

            # 하루치 데이터 학습
            model.fit(test_x, test_y2, batch_size=1, epochs=EPOCH)

            index_cnt += 1

        # 시뮬레이션 잔고 설정
        self.add_remain_df()

        if len(correct) > 0:
            accuracy_sum = accuracy_sum + correct.sum()

        last_fit_date = sim_datas.index[index_cnt-2]

        logs.append("------------------------------------------------------------")
        logs.append("[{}][PREDICT 전체 정확도] {} (학습마지막일:{})".format(code, accuracy, last_fit_date))
        logs.append("------------------------------------------------------------")

        # 이전 머신러닝 PREDICT 결과를 불러옴
        (saved_accuracy_sum, saved_accuracy_cnt) = self.get_rnn_predict(code, last_fit_date)

        if saved_accuracy_sum is None:
            logs.append("[종목코드({}) | 마지막예측일({})] 최초 학습모델 저장!!!!!!!!!".format(code, last_fit_date))
            self.save_rnn_predict(code, last_fit_date, accuracy_sum, accuracy_cnt)
            model.save('./models/rnn_' + code + '_' + last_fit_date + '.h5')
        else:
            saved_accuracy = saved_accuracy_sum / saved_accuracy_cnt

            if saved_accuracy < accuracy:
                logs.append("[종목코드({}) | 예측요청일({})] 새로운 수익모델 저장 *******************".format(code, last_fit_date))
                logs.append("----->저장된 모델 정확도 : {} <-> 현재 모델 정확도 : {}".format(saved_accuracy, accuracy))
                self.save_rnn_test(code, last_fit_date, accuracy_sum, accuracy_cnt)
                model.save('./models/rnn_' + code + '_' + last_fit_date + '.h5')
        logs.append("------------------------------------------------------------")

        for log in logs:
            print(log)

        keras.backend.clear_session()
        return True


    def run_simulation(self):
        # 매수조건 파라메터
        self.buy_cond0_value = int(self.buy_cond0LineEdit.text().replace(',', ''))
        self.buy_accuracy_value = int(self.buy_accuracyLineEdit.text().replace(',', '')) / 100
        self.buy_cond1 = self.buy_cond1ComboBox.currentText()
        self.sell_cond0_value = int(self.sell_cond0LineEdit.text().replace(',','')) / 100
        self.sell_cond1 = self.sell_cond1ComboBox.currentText()

        self.seq_length = int( self.seq_lengthLineEdit.text())

        # 예수금
        self.deposit = int(self.depositLineEdit.text().replace(',',''))
        self.init_deposit = int(self.depositLineEdit.text().replace(',', ''))

        selected_rows = self.stockTable.selectedIndexes()
        if len(selected_rows) == 0:
            QtWidgets.QMessageBox.warning(self, "메세지", "시뮬레이션 할 종목를 선택하세요!")
            return

        index = self.stockTable.selectedIndexes()[0].row()

        code = self.stockTable.item(index, stockTable_column['종목코드']).text()
        market_type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

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

#        self.clear_simulation_result_table()

        # 전체 시뮬레이션 결과 DataFrame Clear
#        if len(self.remain_df.index) > 0:
#            self.remain_df.drop(self.remain_df.index, inplace=True)
#        if len(self.profit_df.index) > 0:
#            self.profit_df.drop(self.profit_df.index, inplace=True)

#        self.clear_simulation_remain_table()
#        self.clear_simulation_profit_table()

        # 시뮬레이션 대상 데이타
        sim_datas = self.chart_df.loc[start_index:end_index]

        if len(sim_datas.index) != 0:
            start_index = sim_datas.index[0]
            end_index = sim_datas.index[-1]

            self.ai_model_run(code, market_type, start_index, end_index, sim_datas)

        QtWidgets.QMessageBox.about(self, "시뮬레이션 완료", "시뮬레이션이 완료되었습니다. 결과를 확인하세요!")


    def sleep_time(self, millisecond):
        loop = QEventLoop()
        QTimer.singleShot(millisecond, loop.quit)
        loop.exec_()


    def run_total_simulation(self):
        # 매수조건 파라메터
        self.buy_cond0_value = int(self.buy_cond0LineEdit.text().replace(',', ''))
        self.buy_accuracy_value = int(self.buy_accuracyLineEdit.text().replace(',', '')) / 100
        self.buy_cond1 = self.buy_cond1ComboBox.currentText()
        self.sell_cond0_value = int(self.sell_cond0LineEdit.text().replace(',','')) / 100
        self.sell_cond1 = self.sell_cond1ComboBox.currentText()
        self.init_deposit = int(self.depositLineEdit.text().replace(',', ''))

        self.seq_length = int(self.seq_lengthLineEdit.text())

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

        self.clear_simulation_result_table()
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
            market_type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            # 종목 시뮬레이션 결과 DataFrame Clear
            if len(self.sim_df.index) > 0:
                self.sim_df.drop(self.sim_df.index, inplace=True)

            if len(self.temp_df.index) > 0:
                self.temp_df.drop(self.temp_df.index, inplace=True)

#            self.clear_simulation_result_table()

            # 시뮬레이션 대상 데이타
            sim_datas = self.chart_df.loc[start_index:end_index]

            if len(sim_datas.index) != 0:
                start_index = sim_datas.index[0]
                end_index = sim_datas.index[-1]

                self.ai_model_run(code, market_type, start_index, end_index, sim_datas)

        QtWidgets.QMessageBox.about(self, "전체 시뮬레이션 완료", "전체 시뮬레이션이 완료되었습니다. 결과를 확인하세요!")


    def run_rank_simulation(self):
        QtWidgets.QMessageBox.about(self, "전체 시뮬레이션 완료", "전체 시뮬레이션이 완료되었습니다. 결과를 확인하세요!")


    def load_rnn_model(self, sel_market, start_date, model, seq_len, data_dim, epoch):
        if os.path.isfile("./models/rnn_seq{}_data{}_epoch{}_{}_{}.h5".format(seq_len, data_dim, epoch, sel_market, start_date)):
            print("[구분:{} | 예측요청일:{} | SEQ:{} | DATA:{}] 저장된 학습모델 로딩...............".format(sel_market, start_date, seq_len, data_dim))
            model = keras.models.load_model("./models/rnn_seq{}_data{}_epoch{}_{}_{}.h5".format(seq_len, data_dim, epoch, sel_market, start_date))
            return(model, True)
        else:
            return(model, False)


    def save_rnn_model(self, sel_market, start_date, model, seq_len, data_dim, epoch):
        print("[구분:{} | 예측요청일:{} | SEQ:{} | DATA:{}] 학습모델 저장!!!!!!!!!!!!!!!!!!".format(sel_market, start_date, seq_len, data_dim))
        model.save("./models/rnn_seq{}_data{}_epoch{}_{}_{}.h5".format(seq_len, data_dim, epoch, sel_market, start_date))


    def get_trained_stock(self, code, start_date, seq_len, data_dim, epoch):
        query = "SELECT * FROM RNN_TRAIN WHERE code='{}' AND predict_start_date='{}' AND seq_len ='{}' AND data_dim='{}' AND epoch='{}'".format(code, start_date, seq_len, data_dim, epoch)

        self.sim_cur.execute(query)
        result = self.sim_cur.fetchone()

        if result is None:
            return False
        else:
            return True


    def save_trained_stock(self, code, start_date, seq_len, data_dim, epoch):
        query1 = "INSERT OR IGNORE INTO RNN_TRAIN(code, predict_start_date, seq_len, data_dim, epoch)VALUES('{}', '{}', '{}', '{}', '{}');".format(code, start_date, seq_len, data_dim, epoch)

        self.sim_cur.execute(query1)
        self.sim_db.commit()


    def run_all_train_simulation(self):
        # 매수조건 파라메터
        self.buy_cond0_value = int(self.buy_cond0LineEdit.text().replace(',', ''))
        self.buy_accuracy_value = int(self.buy_accuracyLineEdit.text().replace(',', '')) / 100
        self.buy_cond1 = self.buy_cond1ComboBox.currentText()
        self.sell_cond0_value = int(self.sell_cond0LineEdit.text().replace(',','')) / 100
        self.sell_cond1 = self.sell_cond1ComboBox.currentText()
        self.init_deposit = int(self.depositLineEdit.text().replace(',', ''))

        self.seq_length = int(self.seq_lengthLineEdit.text())

        # 시뮬레이션 기간
        sim_start_date = self.sim_startDateEdit.date().toPyDate()
        sim_end_date = self.sim_endDateEdit.date().toPyDate()

        start_date = datetime.datetime.strftime(sim_start_date, '%Y%m%d')
        end_date = datetime.datetime.strftime(sim_end_date, '%Y%m%d')

        # 마켓종류 및 TOP순위
        sel_market = self.sel_marketComboBox.currentText()
        if sel_market == '전체':
            sel_market = 'All'
        elif sel_market == '코스피':
            sel_market = 'KO'
        else:
            sel_market = 'KQ'

        # 전체 시뮬레이션 결과 DataFrame Clear
        if len(self.remain_df.index) > 0:
            self.remain_df.drop(self.remain_df.index, inplace=True)
        if len(self.profit_df.index) > 0:
            self.profit_df.drop(self.profit_df.index, inplace=True)

        self.clear_simulation_result_table()
        self.clear_simulation_remain_table()
        self.clear_simulation_profit_table()

        start_cnt = 0
        end_cnt = self.stockTable.rowCount()

        # RNN 모델 생성 및 학습
        model = self.create_rnn_model(seq_len=self.seq_length, data_dim=DATA_DIM)

        print("[전체 학습시작] -----------------------------------------------------")

        # 기존 학습모델이 있는 경우에는 해당 모델 로딩
        (model, load_yn) = self.load_rnn_model(sel_market, start_date, model, seq_len=self.seq_length, data_dim=DATA_DIM, epoch=EPOCH)

        for index in range(start_cnt, end_cnt):
            # 해당 ROW 선택
            self.stockTable.selectRow(index)

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            market_type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            print("{} / {} : CODE[{}]".format(index+1, end_cnt, code))
            self.runcntLabel.setText(str(format(index+1, ',')))

            if load_yn and self.get_trained_stock(code, start_date, seq_len=self.seq_length, data_dim=DATA_DIM, epoch=EPOCH):
                continue

            # 종목 차트데이로 로딩
            self.draw_stock_chart('DAY')
            self.sleep_time(DELAY_TIME * 1000)

            # 시뮬레이션 대상 데이타
            sim_datas = self.chart_df.loc[start_date:end_date]

            if len(sim_datas.index) != 0:
                start_index = sim_datas.index[0]
                end_index = sim_datas.index[-1]
                print("{} / {} : CODE[{}] 학습시작 .....".format(index+1, end_cnt, code))
                (model, result) = self.train_only_run(code, market_type, start_index, end_index, model)

                if result:
                    self.save_rnn_model(sel_market, start_date, model, seq_len=self.seq_length, data_dim=DATA_DIM, epoch=EPOCH)
                    self.save_trained_stock(code, start_date, seq_len=self.seq_length, data_dim=DATA_DIM, epoch=EPOCH)
                print("{} / {} : CODE[{}] 학습완료 !!!!!".format(index+1, end_cnt, code))

        print("[전체 학습종료] -----------------------------------------------------")

        # 종목별 예측시작
        for index in range(0, end_cnt):
            # 해당 ROW 선택
            self.stockTable.selectRow(index)

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            market_type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            print("{} / {} : CODE[{}]".format(index+1, end_cnt, code))
            self.runcntLabel.setText(str(format(index+1, ',')))

            # 종목 차트데이로 로딩
            self.draw_stock_chart('DAY')
            self.sleep_time(DELAY_TIME * 1000)

            # 예수금 초기화
            self.deposit = int(self.depositLineEdit.text().replace(',', ''))

            # 종목 시뮬레이션 결과 DataFrame Clear
            if len(self.sim_df.index) > 0:
                self.sim_df.drop(self.sim_df.index, inplace=True)

            if len(self.temp_df.index) > 0:
                self.temp_df.drop(self.temp_df.index, inplace=True)

            # 시뮬레이션 대상 데이타
            sim_datas = self.chart_df.loc[start_date:end_date]

            if len(sim_datas.index) != 0:
                start_index = sim_datas.index[0]
                end_index = sim_datas.index[-1]

                (model, result) = self.predict_only_run(code, market_type, start_index, end_index, sim_datas, model)

        QtWidgets.QMessageBox.about(self, "전체 시뮬레이션 완료", "전체 시뮬레이션이 완료되었습니다. 결과를 확인하세요!")


    def run_one_train_simulation(self):
        # 매수조건 파라메터
        self.buy_cond0_value = int(self.buy_cond0LineEdit.text().replace(',', ''))
        self.buy_accuracy_value = int(self.buy_accuracyLineEdit.text().replace(',', '')) / 100
        self.buy_cond1 = self.buy_cond1ComboBox.currentText()
        self.sell_cond0_value = int(self.sell_cond0LineEdit.text().replace(',','')) / 100
        self.sell_cond1 = self.sell_cond1ComboBox.currentText()
        self.init_deposit = int(self.depositLineEdit.text().replace(',', ''))

        self.seq_length = int(self.seq_lengthLineEdit.text())

        # 예수금 초기화
        self.deposit = int(self.depositLineEdit.text().replace(',', ''))

        # 시뮬레이션 기간
        sim_start_date = self.sim_startDateEdit.date().toPyDate()
        sim_end_date = self.sim_endDateEdit.date().toPyDate()

        start_date = datetime.datetime.strftime(sim_start_date, '%Y%m%d')
        end_date = datetime.datetime.strftime(sim_end_date, '%Y%m%d')

        # 마켓종류 및 TOP순위
        sel_market = self.sel_marketComboBox.currentText()
        if sel_market == '전체':
            sel_market = 'All'
        elif sel_market == '코스피':
            sel_market = 'KO'
        else:
            sel_market = 'KQ'

        selected_rows = self.stockTable.selectedIndexes()
        if len(selected_rows) == 0:
            QtWidgets.QMessageBox.warning(self, "메세지", "시뮬레이션 할 종목를 선택하세요!")
            return
        index = self.stockTable.selectedIndexes()[0].row()

        code = self.stockTable.item(index, stockTable_column['종목코드']).text()
        market_type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

        # 종목 차트데이로 로딩
        self.draw_stock_chart('DAY')
        self.sleep_time(DELAY_TIME * 1000)

        # 종목 시뮬레이션 결과 DataFrame Clear
        if len(self.sim_df.index) > 0:
            self.sim_df.drop(self.sim_df.index, inplace=True)

        if len(self.temp_df.index) > 0:
            self.temp_df.drop(self.temp_df.index, inplace=True)

        # RNN 모델 생성 및 학습
        model = self.create_rnn_model(seq_len=self.seq_length, data_dim=DATA_DIM)

        # 기존 학습모델이 있는 경우에는 해당 모델 로딩
        (model, load_yn) = self.load_rnn_model(sel_market, start_date, model, seq_len=self.seq_length, data_dim=DATA_DIM, epoch=EPOCH)

        # 시뮬레이션 대상 데이타
        sim_datas = self.chart_df.loc[start_date:end_date]

        if len(sim_datas.index) == 0:
            QtWidgets.QMessageBox.warning(self, "시뮬레이션 완료", "선택한 종목에 시뮬레이션할 데이터가 없습니다.")
            return

        start_index = sim_datas.index[0]
        end_index = sim_datas.index[-1]

        if load_yn == False or self.get_trained_stock(code, start_date, seq_len=self.seq_length, data_dim=DATA_DIM, epoch=EPOCH) == False:
            print("[CODE:{}] 학습시작 .....".format(code))
            (model, result) = self.train_only_run(code, market_type, start_index, end_index, model)

            if result:
                self.save_rnn_model(sel_market, start_date, model, seq_len=self.seq_length, data_dim=DATA_DIM, epoch=EPOCH)
                self.save_trained_stock(code, start_date, seq_len=self.seq_length, data_dim=DATA_DIM, epoch=EPOCH)
            print("[CODE:{}] 학습완료 !!!!!".format(code))

        (model, result) = self.predict_only_run(code, market_type, start_index, end_index, sim_datas, model)
        QtWidgets.QMessageBox.about(self, "시뮬레이션 완료", "시뮬레이션이 완료되었습니다. 결과를 확인하세요!")


    def exist_db_result_table(self):
        query = "SELECT * FROM SIM_RESULT"

        self.sim_cur.execute(query)
        result = self.sim_cur.fetchone()

        if result is None:
            return False
        else:
            return True


    def get_db_result_table(self, code):
        query = "SELECT * FROM SIM_RESULT WHERE code='{}'".format(code)

        self.sim_cur.execute(query)
        result = self.sim_cur.fetchone()

        if result is None:
            return False
        else:
            return True


    def run_all_predict_simulation(self):
        # 매수조건 파라메터
        self.buy_cond0_value = int(self.buy_cond0LineEdit.text().replace(',', ''))
        self.buy_accuracy_value = int(self.buy_accuracyLineEdit.text().replace(',', '')) / 100
        self.buy_cond1 = self.buy_cond1ComboBox.currentText()
        self.sell_cond0_value = int(self.sell_cond0LineEdit.text().replace(',','')) / 100
        self.sell_cond1 = self.sell_cond1ComboBox.currentText()
        self.init_deposit = int(self.depositLineEdit.text().replace(',', ''))

        self.seq_length = int(self.seq_lengthLineEdit.text())

        # 시뮬레이션 기간
        sim_start_date = self.sim_startDateEdit.date().toPyDate()
        sim_end_date = self.sim_endDateEdit.date().toPyDate()

        start_date = datetime.datetime.strftime(sim_start_date, '%Y%m%d')
        end_date = datetime.datetime.strftime(sim_end_date, '%Y%m%d')

        # 마켓종류 및 TOP순위
        sel_market = self.sel_marketComboBox.currentText()
        if sel_market == '전체':
            sel_market = 'All'
        elif sel_market == '코스피':
            sel_market = 'KO'
        else:
            sel_market = 'KQ'

        # 전체 시뮬레이션 결과 DataFrame Clear
        if len(self.remain_df.index) > 0:
            self.remain_df.drop(self.remain_df.index, inplace=True)
        if len(self.profit_df.index) > 0:
            self.profit_df.drop(self.profit_df.index, inplace=True)

        self.clear_simulation_result_table()
        self.clear_simulation_remain_table()
        self.clear_simulation_profit_table()

        use_result = False
        if self.exist_db_result_table():
            reply = QtWidgets.QMessageBox.question(self, '결과정보 이용여부', "기존에 저장한 결과정보를 이용하여 종목를 검색하시겠습니까?", QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
            if reply == QMessageBox.Yes:
                use_result = True

        start_cnt = 0
        end_cnt = self.stockTable.rowCount()

        # RNN 모델 생성 및 학습
        model = self.create_rnn_model(seq_len=self.seq_length, data_dim=DATA_DIM)

        # 기존 학습모델이 있는 경우에는 해당 모델 로딩
#        (model, load_yn) = self.load_rnn_model(sel_market, start_date, model, seq_len=self.seq_length, data_dim=DATA_DIM, epoch=EPOCH)
        (model, load_yn) = self.load_rnn_model('KO', '20190101', model, seq_len=self.seq_length, data_dim=DATA_DIM, epoch=EPOCH)

        if load_yn == False:
            QtWidgets.QMessageBox.warning(self, "학습모델 로딩 에러", "기존에 학습된 모델을 찾을 수 없습니다.!")
            return

        # 종목별 예측시작
        for index in range(0, end_cnt):
            # 해당 ROW 선택
            self.stockTable.selectRow(index)

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            market_type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            print("{} / {} : CODE[{}]".format(index+1, end_cnt, code))
            self.runcntLabel.setText(str(format(index+1, ',')))

            if use_result and self.get_db_result_table(code) == False:
                continue

            # 종목 차트데이로 로딩
            self.draw_stock_chart('DAY')
            self.sleep_time(DELAY_TIME * 1000)

            # 예수금 초기화
            self.deposit = int(self.depositLineEdit.text().replace(',', ''))

            # 종목 시뮬레이션 결과 DataFrame Clear
            if len(self.sim_df.index) > 0:
                self.sim_df.drop(self.sim_df.index, inplace=True)

            if len(self.temp_df.index) > 0:
                self.temp_df.drop(self.temp_df.index, inplace=True)

            # 시뮬레이션 대상 데이타
            sim_datas = self.chart_df.loc[start_date:end_date]

            if len(sim_datas.index) != 0:
                start_index = sim_datas.index[0]
                end_index = sim_datas.index[-1]

                (model, result) = self.predict_only_run(code, market_type, start_index, end_index, sim_datas, model)

        QtWidgets.QMessageBox.about(self, "전체 시뮬레이션 완료", "전체 시뮬레이션이 완료되었습니다. 결과를 확인하세요!")


    def run_one_predict_simulation(self):
        # 매수조건 파라메터
        self.buy_cond0_value = int(self.buy_cond0LineEdit.text().replace(',', ''))
        self.buy_accuracy_value = int(self.buy_accuracyLineEdit.text().replace(',', '')) / 100
        self.buy_cond1 = self.buy_cond1ComboBox.currentText()
        self.sell_cond0_value = int(self.sell_cond0LineEdit.text().replace(',','')) / 100
        self.sell_cond1 = self.sell_cond1ComboBox.currentText()
        self.init_deposit = int(self.depositLineEdit.text().replace(',', ''))

        self.seq_length = int(self.seq_lengthLineEdit.text())

        # 예수금 초기화
        self.deposit = int(self.depositLineEdit.text().replace(',', ''))

        # 시뮬레이션 기간
        sim_start_date = self.sim_startDateEdit.date().toPyDate()
        sim_end_date = self.sim_endDateEdit.date().toPyDate()

        start_date = datetime.datetime.strftime(sim_start_date, '%Y%m%d')
        end_date = datetime.datetime.strftime(sim_end_date, '%Y%m%d')

        # 마켓종류 및 TOP순위
        sel_market = self.sel_marketComboBox.currentText()
        if sel_market == '전체':
            sel_market = 'All'
        elif sel_market == '코스피':
            sel_market = 'KO'
        else:
            sel_market = 'KQ'

        selected_rows = self.stockTable.selectedIndexes()
        if len(selected_rows) == 0:
            QtWidgets.QMessageBox.warning(self, "메세지", "시뮬레이션 할 종목를 선택하세요!")
            return
        index = self.stockTable.selectedIndexes()[0].row()

        code = self.stockTable.item(index, stockTable_column['종목코드']).text()
        market_type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

        # 종목 차트데이로 로딩
        self.draw_stock_chart('DAY')
        self.sleep_time(DELAY_TIME * 1000)

        # 종목 시뮬레이션 결과 DataFrame Clear
        if len(self.sim_df.index) > 0:
            self.sim_df.drop(self.sim_df.index, inplace=True)

        if len(self.temp_df.index) > 0:
            self.temp_df.drop(self.temp_df.index, inplace=True)

        # RNN 모델 생성 및 학습
        model = self.create_rnn_model(seq_len=self.seq_length, data_dim=DATA_DIM)

        # 기존 학습모델이 있는 경우에는 해당 모델 로딩
        (model, load_yn) = self.load_rnn_model(sel_market, start_date, model, seq_len=self.seq_length, data_dim=DATA_DIM, epoch=EPOCH)

        if load_yn == False:
            QtWidgets.QMessageBox.warning(self, "학습모델 로딩 에러", "기존에 학습된 모델을 찾을 수 없습니다.!")
            return

        # 시뮬레이션 대상 데이타
        sim_datas = self.chart_df.loc[start_date:end_date]

        if len(sim_datas.index) == 0:
            QtWidgets.QMessageBox.warning(self, "시뮬레이션 완료", "선택한 종목에 시뮬레이션할 데이터가 없습니다.")
            return

        start_index = sim_datas.index[0]
        end_index = sim_datas.index[-1]

        (model, result) = self.predict_only_run(code, market_type, start_index, end_index, sim_datas, model)
        QtWidgets.QMessageBox.about(self, "시뮬레이션 완료", "시뮬레이션이 완료되었습니다. 결과를 확인하세요!")


    def clear_sim_result(self):
        # 전체 시뮬레이션 결과 DataFrame Clear
        if len(self.remain_df.index) > 0:
            self.remain_df.drop(self.remain_df.index, inplace=True)
        if len(self.profit_df.index) > 0:
            self.profit_df.drop(self.profit_df.index, inplace=True)

        self.clear_simulation_result_table()
        self.clear_simulation_remain_table()
        self.clear_simulation_profit_table()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    myWindow = TrainingMarket()
    myWindow.show()
    sys.exit(app.exec_())