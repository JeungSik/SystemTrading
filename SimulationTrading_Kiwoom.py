import sqlite3
import time

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import mpl_finance
import numpy as np
import pandas as pd
from PyQt5 import QtCore, QtWidgets, QtGui
from PyQt5 import uic
from dateutil.relativedelta import relativedelta
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas

from Kiwoom import *
from LookupTable import *

main_form = uic.loadUiType("SimulationTrading_Kiwoom.ui")[0]

stockTable_column = {'종목코드': 0, '종목명': 1, '구분': 2, '기준일': 3, '현재가': 4, '거래량': 5}
stockSimTable_column = {'종목명': 0, '일자': 1, '구분': 2, '수량': 3, '매매가': 4, '수수료': 5, '세금': 6, '수익률': 7,
                        '수익금': 8, '누적수익금': 9}
stockRemainTable_column = {'종목명': 0, '매수일': 1, '수량': 2, '매수가': 3, '평가손익': 4, '수익률': 5, '현재가': 6,
                           '매수금액': 7, '평가금액': 8, '수수료': 9, '세금': 10}
stockProfitTable_column = {'종목명': 0, '매도일': 1, '실현손익': 2, '수익률': 3, '매수금액': 4, '매도금액': 5, '매수가': 6,
                           '매도가': 7, '수수료': 8, '세금': 9}

sim_df_column = {'Date': '일자', 'Gubun': '구분', 'Amount': '수량', 'Price': '매매가', 'Charge': '수수료', 'Tax': '세금',
                 'Rate': '수익률', 'Profit': '수익금'}

OPEN_TIME = 9  # 장 시작시간(09시)
CLOSE_TIME = 16  # 장 마감시간(16시)
CHART_MOVE = 10  # 차트화면 STEP
CHARGE_RATE = 0.00015  # 매매 수수료율
TAX_RATE = 0.0025  # 매매 수수료율

DELAY_TIME = 0.1


class SimulationTrading(QMainWindow, main_form):
    def __init__(self):
        super().__init__()
        self.setupUi(self)

        # Event Handler
        self.sel_marketComboBox.currentTextChanged.connect(self.create_stock_table)
        self.sel_topComboBox.currentTextChanged.connect(self.create_stock_table)
        #        self.stockTable.cellClicked.connect(lambda:self.draw_stock_chart('DAY'))
        self.stockTable.itemSelectionChanged.connect(lambda: self.draw_stock_chart('DAY'))
        self.rangeComboBox.currentTextChanged.connect(self.redraw_stock_chart)
        self.chartScrollBar.valueChanged.connect(self.scroll_stock_chart)
        self.runPushButton.clicked.connect(self.run_simulation)
        self.run_totalPushButton.clicked.connect(self.run_total_simulation)
        self.clear_sim_resultPushButton.clicked.connect(self.clear_sim_result)
        self.save_sim_resultPushButton.clicked.connect(self.save_sim_result)
        self.search_buyPushButton.clicked.connect(lambda: self.search_buy_recommand(update_yn=False))
        self.search_buyOnePushButton.clicked.connect(lambda: self.search_buyOne_recommand(update_yn=False))
        self.search_sellPushButton.clicked.connect(lambda: self.search_sell_recommand(update_yn=False))
        self.search_sellOnePushButton.clicked.connect(lambda: self.search_sellOne_recommand(update_yn=False))
        self.update_search_buyPushButton.clicked.connect(lambda: self.search_buy_recommand(update_yn=True))
        self.update_search_sellPushButton.clicked.connect(lambda: self.search_sell_recommand(update_yn=True))
        self.save_stocks_infoPushButton.clicked.connect(self.save_stocks_info)

        # 키움증권 연결
        self.login_message()

        # DB Connect 설정
        self.kospi_db = sqlite3.connect("./datas/kospi.db")
        self.kospi_db.execute("ATTACH DATABASE './datas/sim_result.db' AS sim_db")
        self.kosdaq_db = sqlite3.connect("./datas/kosdaq.db")
        self.kosdaq_db.execute("ATTACH DATABASE './datas/sim_result.db' AS sim_db")
        self.kospi_cur = self.kospi_db.cursor()
        self.kosdaq_cur = self.kosdaq_db.cursor()

        self.sim_db = sqlite3.connect("./datas/sim_result.db")
        self.sim_cur = self.sim_db.cursor()

        if self.login_yn:
            self.save_db_kospi_market_info()
            self.save_db_kosdaq_market_info()

        # 차트 출력요 화면 구성
        self.fig = plt.Figure(figsize=(1000, 7.2), dpi=80, facecolor='k')
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

        # 결과 저장용 테이블 생성
        self.create_db_result_table()

    def login_message(self):
        reply = QtWidgets.QMessageBox.question(self, '키움증권 로그인', "증권사에 로그인 하시겠습니까?\n(로그인 하지 않는 경우 시뮬레이션만 가능합니다)",
                                               QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.kiwoom_connect()
            self.kiwoom.OnEventConnect.connect(self.event_connect)
            self.login_yn = True
        else:
            self.save_stocks_infoPushButton.setEnabled(False)
            self.update_search_buyPushButton.setEnabled(False)
            self.update_search_sellPushButton.setEnabled(False)
            self.login_yn = False

    def closeEvent(self, event):
        self.kiwoom.comm_terminate()
        event.accept()

    def kiwoom_connect(self):
        self.kiwoom = Kiwoom()
        self.kiwoom.comm_connect()

    def kiwoom_disconnect(self):
        self.kiwoom.comm_terminate()

    def event_connect(self, err_code):
        if err_code == 0:
            self.statusBar.showMessage("서버 연결 중....")
        elif err_code == -100:
            self.statusBar.showMessage("사용자 정보교환 실패!")
        elif err_code == -101:
            self.statusBar.showMessage("서버접속 실패!")
        elif err_code == -102:
            self.statusBar.showMessage("버전처리 실패!")
        else:
            self.statusBar.showMessage("서버연결 안됨!")

    def create_db_result_table(self):
        # 해당 종목의 차트 테이블 생성
        query = "CREATE TABLE IF NOT EXISTS SIM_RESULT (code TEXT, PRIMARY KEY(code))"

        self.sim_cur.execute(query)
        self.sim_db.commit()

    def save_db_result_table(self, code):
        query1 = "INSERT OR IGNORE INTO SIM_RESULT(code)VALUES('{}');".format(code)

        self.sim_cur.execute(query1)
        self.sim_db.commit()

    def get_db_result_table(self, code):
        query = "SELECT * FROM SIM_RESULT WHERE code='{}'".format(code)

        self.sim_cur.execute(query)
        result = self.sim_cur.fetchone()

        if result is None:
            return False
        else:
            return True

    def exist_db_result_table(self):
        query = "SELECT * FROM SIM_RESULT"

        self.sim_cur.execute(query)
        result = self.sim_cur.fetchone()

        if result is None:
            return False
        else:
            return True

    def delete_db_result_table(self):
        query1 = "DELETE FROM SIM_RESULT"

        self.sim_cur.execute(query1)
        self.sim_db.commit()

    def get_market_info_by_codes(self, codes):
        names = []
        dates = []
        cnts = []
        prices = []
        constructs = []
        states = []

        for i, code in enumerate(codes):
            names.append(self.kiwoom.get_master_code_name(code))
            dates.append(self.kiwoom.get_master_listed_stock_date(code))
            cnts.append(self.kiwoom.get_master_listed_stock_cnt(code))
            prices.append(self.kiwoom.get_master_last_price(code))
            constructs.append(self.kiwoom.get_master_construction(code))
            states.append(self.kiwoom.get_master_stock_state(code))
        return DataFrame({'Code': codes, 'Name': names, 'StockDate': dates, 'StockCnt': cnts, 'LastPrice': prices,
                          'Construction': constructs, 'StockState': states})

    def update_date_db_info(self, market, table_name):
        now = datetime.datetime.now()
        today = now.strftime('%Y-%m-%d %H:%M:%S')
        query = "REPLACE INTO DB_INFO(TABLE_NAME, DATE)VALUES('" + table_name + "', '" + today + "')"

        if market == '코스피':
            self.kospi_cur.execute(query)
            self.kospi_db.commit()
        elif market == '코스닥':
            self.kosdaq_cur.execute(query)
            self.kosdaq_db.commit()

    def save_db_kospi_market_info(self):
        # 코스피 종목코드 가져오기
        codes = self.kiwoom.get_code_list_by_market(market_lookup['장내'])

        # 장내 종목코드에서 ETF 종목 제거
        etf_codes = self.kiwoom.get_code_list_by_market(market_lookup['ETF'])
        codes = list(set(codes) - set(etf_codes))

        # 코스피 종목정보 가져오기
        df = self.get_market_info_by_codes(codes)

        # 종목명에서 ETN이 포함된 종목 제거
        df = df[df['Name'].str.contains('ETN') == False]

        # 종목명과 코드로 정렬
        df = df.sort_values(by=['Name', 'Code'])

        # 코스피 종목정보 DB 저장
        df.to_sql('MARKET_INFO', self.kospi_db, index=False, if_exists='replace')

        # DB_INFO 테이블이 존재하지 않으면 생성
        self.kospi_cur.execute("CREATE TABLE IF NOT EXISTS DB_INFO(TABLE_NAME text PRIMARY KEY, DATE text)")
        self.kosdaq_db.commit()

        # DB_INFO 테이블에 MARKET_INFO 정보 UPDATE
        self.update_date_db_info('코스피', 'MARKET_INFO')

    def save_db_kosdaq_market_info(self):
        # 코스닥 종목코드 가져오기
        codes = self.kiwoom.get_code_list_by_market(market_lookup['코스닥'])

        # 코스닥 종목정보 가져오기
        df = self.get_market_info_by_codes(codes)

        # 종목명에서 스팩이 포함된 종목 제거 (왜 동작하지 않을까? 한글이 문제인것 같다)
        df = df[df['Name'].str.contains('스팩') == False]

        # 종목명과 코드로 정렬
        df = df.sort_values(by=['Name', 'Code'])

        # 코스닥 종목정보 DB 저장
        df.to_sql('MARKET_INFO', self.kosdaq_db, index=False, if_exists='replace')

        # DB_INFO 테이블이 존재하지 않으면 생성
        self.kosdaq_cur.execute("CREATE TABLE IF NOT EXISTS DB_INFO(TABLE_NAME text PRIMARY KEY, DATE text)")
        self.kosdaq_db.commit()

        # DB_INFO 테이블에 MARKET_INFO 정보 UPDATE
        self.update_date_db_info('코스닥', 'MARKET_INFO')

    def get_table_saved_date(self, table_name, type):
        query = "SELECT DATE FROM DB_INFO WHERE TABLE_NAME = '" + table_name + "'"

        if type == 'KO':
            self.kospi_cur.execute(query)
            saved_date = self.kospi_cur.fetchone()
        else:
            self.kosdaq_cur.execute(query)
            saved_date = self.kosdaq_cur.fetchone()

        if saved_date is not None:
            return datetime.datetime.strptime(saved_date[0], "%Y-%m-%d %H:%M:%S")
        else:
            return None

    # 주말을 뺀 최근 날짜를 리턴함
    def get_now_day(self):
        now = datetime.datetime.now()

        if now.weekday() == 5:  # 오늘이 토요일인 경우
            now = now - datetime.timedelta(days=1)
        elif now.weekday() == 6:  # 오늘이 일요일인 경우
            now = now - datetime.timedelta(days=2)
        else:
            return now

        date = now.strftime('%Y-%m-%d')
        date = date + ' ' + str(CLOSE_TIME) + ':00:00'
        now = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        return now

    def check_update_yn(self, saved_date):
        now = self.get_now_day()
        diff_date = now - saved_date

        if diff_date.days > 0:
            return True

        if now.weekday() == saved_date.weekday():
            if now.hour < OPEN_TIME and saved_date.hour < OPEN_TIME:
                return False
            if now.hour >= CLOSE_TIME and saved_date.hour >= CLOSE_TIME:
                return False
        elif now.weekday() - saved_date.weekday() > 0:
            if now.hour < OPEN_TIME and saved_date.hour >= CLOSE_TIME:
                return False
        else:
            return False

        return True

    def check_daily_updated_db_info(self, table_name):
        first_kospi = 0  # 0: KOSPI DB에 해당 TABLE이 존재하지 않음
        first_kosdaq = 0  # 0: KOSDAQ DB에 해당 TABLE이 존재하지 않음

        now = self.get_now_day()
        saved_date_ko = self.get_table_saved_date(table_name, "KO")
        saved_date_kq = self.get_table_saved_date(table_name, "KQ")

        # 코스피 주식기본정보 갱신여부 체크
        if saved_date_ko is not None:
            result = self.check_update_yn(saved_date_ko)

            if result:
                first_kospi = 1
            else:
                first_kospi = 2

        # 코스닥 주식기본정보 갱신여부 체크
        if saved_date_kq is not None:
            result = self.check_update_yn(saved_date_kq)

            if result:
                first_kosdaq = 1
            else:
                first_kosdaq = 2

        return first_kospi, first_kosdaq  # 0:데이터없음, 1: UPDATE요청, 2: 당일데이터(최신데이터)

    def mark_stocks_info_in_stock_table(self, sel_type):
        first_kospi, first_kosdaq = self.check_daily_updated_db_info(table_name='STOCKS_INFO')

        # 이미 갱신된 주식기본정보를 가진 종목의 INFO필드 UP으로 변경
        query = "SELECT 종목코드 as Code FROM STOCKS_INFO"

        data1 = []
        data2 = []

        if sel_type != 'KQ':
            if first_kospi >= 1:  # 데이터 존재
                self.kospi_cur.execute(query)
                data1 = self.kospi_cur.fetchall()

        if sel_type != 'KO':
            if first_kosdaq >= 1:  # 데이터 존재
                self.kosdaq_cur.execute(query)
                data2 = self.kosdaq_cur.fetchall()

        datas = []
        if first_kospi >= 1 and first_kosdaq >= 1:
            datas = data1 + data2
        elif first_kospi >= 1 and first_kosdaq == 0:
            datas = data1
        elif first_kospi == 0 and first_kosdaq >= 1:
            datas = data2

        if len(datas) == 0:
            return

        datas_df = DataFrame(datas, columns=['Code'])
        datas = datas_df.values

        total_cnt = self.stockTable.rowCount()

        for i in range(total_cnt):
            code = self.stockTable.item(i, stockTable_column['종목코드']).text()
            type = self.stockTable.item(i, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            if code in datas:
                if (type == 'KO' and first_kospi == 1) or (type == 'KQ' and first_kosdaq == 1):
                    self.stockTable.item(i, stockTable_column['구분']).setBackground(QtGui.QColor(255, 255, 0))
            else:
                self.stockTable.item(i, stockTable_column['구분']).setBackground(QtGui.QColor(255, 0, 0))

    def mark_stocks_chart_in_stock_table(self, sel_type):
        query = "select A.Code, OpenDate, LastDate, DayFinish " \
                "from (select TA.Code as Code, case when TA.StockDate < TB.DayRecordDate then TB.DayRecordDate " \
                "else TA.StockDate end as OpenDate, TB.DayFinish from MARKET_INFO TA " \
                "left outer join CHART_NOTE TB on TA.CODE = TB.CODE ) A " \
                "left outer join (select substr(TABLE_NAME, 2, 7) as Code, DATE as LastDate from DB_INFO " \
                "where TABLE_NAME like 'D%') B on A.code = B.code"

        if sel_type != 'KQ':
            # 코스피 종목별 데이터 저장일 로드
            self.kospi_cur.execute(query)
            kospi_date = self.kospi_cur.fetchall()

            kospi_df = DataFrame(kospi_date, columns=['Code', 'OpenDate', 'LastDate', 'DayFinish'])
            kospi_df = kospi_df.set_index('Code')

        if sel_type != 'KO':
            # 코스닥 종목별 데이터 저장일 로드
            self.kosdaq_cur.execute(query)
            kosdaq_date = self.kosdaq_cur.fetchall()

            kosdaq_df = DataFrame(kosdaq_date, columns=['Code', 'OpenDate', 'LastDate', 'DayFinish'])
            kosdaq_df = kosdaq_df.set_index('Code')

        total_cnt = self.stockTable.rowCount()

        for i in range(total_cnt):
            code = self.stockTable.item(i, stockTable_column['종목코드']).text()
            type = self.stockTable.item(i, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            if type == 'KO':
                dates = kospi_df.loc[code]
            else:
                dates = kosdaq_df.loc[code]

            db_open_date = None  # 종목 최초 저장일
            db_last_date = None  # 종목 마지막 저장일
            db_finish_yn = None  # 종목 오픈일까지 저장여부

            if dates['OpenDate'] is not None and dates['OpenDate'] != '':
                db_open_date = datetime.datetime.strptime(dates['OpenDate'], '%Y%m%d')
                db_finish_yn = dates['DayFinish']

            if dates['LastDate'] is not None and dates['LastDate'] != '':
                db_last_date = datetime.datetime.strptime(dates['LastDate'], '%Y-%m-%d %H:%M:%S')

            if db_open_date is None or db_last_date is None or db_finish_yn != True:
                self.stockTable.item(i, stockTable_column['기준일']).setBackground(QtGui.QColor(255, 0, 0))
                continue

            result = self.check_update_yn(db_last_date)

            if result:
                self.stockTable.item(i, stockTable_column['기준일']).setBackground(QtGui.QColor(255, 255, 0))
            else:
                self.stockTable.item(i, stockTable_column['기준일']).setBackground(QtGui.QColor(255, 255, 255))

    def get_db_date_db_info(self, code, type):
        # 기준일 출력
        query = "SELECT DATE FROM DB_INFO WHERE TABLE_NAME = 'D" + code + "'"
        if type == 'KO':
            self.kospi_cur.execute(query)
            date = self.kospi_cur.fetchone()
        else:
            self.kosdaq_cur.execute(query)
            date = self.kosdaq_cur.fetchone()

        if date != None:
            return date[0]
        else:
            return None

    def create_stock_table(self):
        self.stockTable.setRowCount(0)

        sel_market = self.sel_marketComboBox.currentText()
        sel_top = self.sel_topComboBox.currentText()

        if sel_top == '전체':
            sel_sql = "SELECT A.Code, A.Name FROM MARKET_INFO A LEFT OUTER JOIN " \
                      "(SELECT 종목코드 as Code, 종목명 as Name, cast(시가총액 as decimal) as Price " \
                      "FROM STOCKS_INFO ORDER BY Price desc) B ON A.Code = B.Code ORDER BY B.Price desc"
            if self.login_yn:
                self.save_stocks_infoPushButton.setEnabled(True)
        elif sel_top[:3] == 'TOP':
            top_num = sel_top[3:]
            sel_sql = "SELECT A.Code, A.Name FROM " \
                      "(SELECT 종목코드 as Code, 종목명 as Name, cast(시가총액 as decimal) as Price " \
                      "FROM STOCKS_INFO) A, MARKET_INFO B WHERE A.Code=B.Code ORDER BY " \
                      "A.Price desc limit {}".format(top_num)
            if self.login_yn:
                self.save_stocks_infoPushButton.setEnabled(False)
        else:
            sel_sql = "SELECT A.Code, A.Name FROM (" \
                      "SELECT A.Code, A.Name FROM MARKET_INFO A LEFT OUTER JOIN " \
                      "(SELECT 종목코드 as Code, 종목명 as Name, cast(시가총액 as decimal) as Price " \
                      "FROM STOCKS_INFO ORDER BY Price desc) B ON A.Code = B.Code ORDER BY B.Price desc) A, " \
                      "(SELECT Code FROM sim_db.SIM_RESULT) B WHERE A.Code = B.Code"
            if self.login_yn:
                self.save_stocks_infoPushButton.setEnabled(False)

        if sel_market == '코스피':
            sel_type = 'KO'
            kospi_market_info = pd.read_sql(sel_sql, self.kospi_db)
            kospi_market_info['type'] = 'KO'
            kospi_cnt = len(kospi_market_info.index)
            kosdaq_cnt = 0

            df = kospi_market_info

        elif sel_market == '코스닥':
            sel_type = 'KQ'
            kosdaq_market_info = pd.read_sql(sel_sql, self.kosdaq_db)
            kosdaq_market_info['type'] = 'KQ'
            kospi_cnt = 0
            kosdaq_cnt = len(kosdaq_market_info.index)

            df = kosdaq_market_info

        else:
            sel_type = 'All'
            kospi_market_info = pd.read_sql(sel_sql, self.kospi_db)
            kosdaq_market_info = pd.read_sql(sel_sql, self.kosdaq_db)

            kospi_market_info['type'] = 'KO'
            kosdaq_market_info['type'] = 'KQ'

            kospi_cnt = len(kospi_market_info.index)
            kosdaq_cnt = len(kosdaq_market_info.index)

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
            date = self.get_db_date_db_info(code, type)

            item = QTableWidgetItem(date)
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
            self.stockTable.setItem(i, stockTable_column['기준일'], item)

            if date is None:
                continue

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

            if datas[0] != None:
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

        self.mark_stocks_info_in_stock_table(sel_type)
        self.mark_stocks_chart_in_stock_table(sel_type)

    def init_simulation_config_tab(self):
        reg_ex = QtCore.QRegExp("(\d{0,3},)?(\d{3},)?\d{0,3}")

        input_validator = QtGui.QRegExpValidator(reg_ex, self.depositLineEdit)
        self.depositLineEdit.setValidator(input_validator)

        self.sim_endDateEdit.setDate(datetime.datetime.today())

        # 시뮬레이션 결과 저장용 DataFrame
        self.sim_df = DataFrame(
            {'Name': [], 'Date': [], 'Gubun': [], 'Amount': [], 'Price': [], 'Charge': [], 'Tax': [], 'Rate': [],
             'Profit': [], 'Volume': [], 'TempVolume': []})
        self.temp_df = DataFrame(
            {'Date': [], 'Gubun': [], 'Amount': [], 'Price': [], 'Charge': [], 'Tax': [], 'Rate': [],
             'Profit': [], 'Volume': [], 'TempVolume': []})

        # 시뮬레이션 잔고 저장용 DataFrame
        self.remain_df = DataFrame(
            {'Name': [], 'Date': [], 'Amount': [], 'Price': [], 'Profit': [], 'Rate': [], 'Current': [],
             'BuyValue': [], 'CurrValue': [], 'Charge': [], 'Tax': []})

        # 시뮬레이션 수익 저장용 DataFrame
        self.profit_df = DataFrame({'Name': [], 'Date': [], 'Profit': [], 'Rate': [], 'BuyValue': [], 'SellValue': [],
                                    'BuyPrice': [], 'SellPrice': [], 'Charge': [], 'Tax': []})

    def clear_simulation_result_table(self):
        self.stockSimTable.setRowCount(0)
        self.stockSimTable.setColumnWidth(stockSimTable_column['종목명'], 220)
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

        day_list = []
        name_list = []
        for i, day in enumerate(timedates):
            iso = day.isocalendar()
            if self.rangeComboBox.currentText() == '월':
                if iso[2] == 1:  # 매주 월요일 기준 날짜 표시
                    day_list.append(i)
                    name_list.append(day.strftime('%Y/%m/%d'))
            elif self.rangeComboBox.currentText() == '분기':
                if iso[2] == 1 and (iso[1] % 2) == 0:  # 2주 단위 월요일 기준 날짜 표시
                    day_list.append(i)
                    name_list.append(day.strftime('%Y/%m/%d'))
            elif self.rangeComboBox.currentText() == '반기':
                if iso[2] == 1 and (iso[1] % 4) == 0:  # 매월 월요일 기준 날짜 표시
                    day_list.append(i)
                    name_list.append(day.strftime('%Y/%m/%d'))
            elif self.rangeComboBox.currentText() == '년':
                if iso[2] == 1 and (iso[1] % 8) == 0:  # 2개월 단위 월요일 기준 날짜 표시
                    day_list.append(i)
                    name_list.append(day.strftime('%Y/%m/%d'))

        mpl_finance.candlestick2_ochl(self.top_axes, df_datas['Open'], df_datas['Close'], df_datas['High'],
                                      df_datas['Low'],
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

        self.bottom_axes.bar(np.arange(len(df_datas.index)), df_datas['Volume'], color='white', width=0.5,
                             align='center')

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
            self.runcntLabel.setText(str(format(index + 1, ',')))

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

            if value != 'None':  # 현재가가 None이면 chart 테이블이 존재하지 않음
                if chart_type == 'DAY':
                    table_name = 'D' + code
                else:
                    table_name = 'S' + code

                if type == 'KO':
                    self.chart_df = pd.read_sql("SELECT * from " + table_name + " WHERE Volume != 0 ORDER BY Date",
                                                con=self.kospi_db)
                else:
                    self.chart_df = pd.read_sql("SELECT * from " + table_name + " WHERE Volume != 0 ORDER BY Date",
                                                con=self.kosdaq_db)

                self.chart_df = self.chart_df.set_index('Date')

                # 출력할 차트데이터의 기준범위 날짜 계산
                self.open_index = self.chart_df.index[0]
                self.last_index = self.chart_df.index[-1]
                self.last_iloc = self.chart_df.index.get_loc(self.last_index)

                calc_date = datetime.datetime.strptime(self.last_index, '%Y%m%d')

                if self.rangeComboBox.currentText() == '월':
                    calc_date = calc_date - relativedelta(months=1)
                    calc_iloc = (self.last_iloc + 1) - 21
                elif self.rangeComboBox.currentText() == '분기':
                    calc_date = calc_date - relativedelta(months=3)
                    calc_iloc = (self.last_iloc + 1) - 63
                elif self.rangeComboBox.currentText() == '반기':
                    calc_date = calc_date - relativedelta(months=6)
                    calc_iloc = (self.last_iloc + 1) - 126
                elif self.rangeComboBox.currentText() == '년':
                    calc_date = calc_date - relativedelta(years=1)
                    calc_iloc = (self.last_iloc + 1) - 252

                begin_index = datetime.datetime.strftime(calc_date, '%Y%m%d')
                #                sliced_df = self.chart_df.loc[begin_index:self.last_index]

                if calc_iloc > 0:
                    sliced_df = self.chart_df.iloc[calc_iloc:self.last_iloc + 1]
                else:
                    sliced_df = self.chart_df.iloc[:self.last_iloc + 1]

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

                if self.scroll_max == CHART_MOVE:
                    self.scroll_stock_chart()

    def redraw_stock_chart(self):
        if self.chart_df is None:
            return

        calc_date = datetime.datetime.strptime(self.last_index, '%Y%m%d')

        if self.rangeComboBox.currentText() == '월':
            calc_date = calc_date - relativedelta(months=1)
            calc_iloc = (self.last_iloc + 1) - 21
        elif self.rangeComboBox.currentText() == '분기':
            calc_date = calc_date - relativedelta(months=3)
            calc_iloc = (self.last_iloc + 1) - 63
        elif self.rangeComboBox.currentText() == '반기':
            calc_date = calc_date - relativedelta(months=6)
            calc_iloc = (self.last_iloc + 1) - 126
        elif self.rangeComboBox.currentText() == '년':
            calc_date = calc_date - relativedelta(years=1)
            calc_iloc = (self.last_iloc + 1) - 252

        begin_index = datetime.datetime.strftime(calc_date, '%Y%m%d')
        #        sliced_df = self.chart_df.loc[begin_index:self.last_index]

        if calc_iloc > 0:
            sliced_df = self.chart_df.iloc[calc_iloc:self.last_iloc + 1]
        else:
            sliced_df = self.chart_df.iloc[:self.last_iloc + 1]

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

    def scroll_stock_chart(self):
        pos = self.chartScrollBar.value()

        end_iloc = (self.last_iloc + 1) - ((self.scroll_max - pos) * self.step_count)
        if end_iloc == 0:
            end_iloc = 1

        begin_iloc = end_iloc - self.sliced_count

        if begin_iloc < 0:
            begin_iloc = 0

        sliced_df = self.chart_df.iloc[begin_iloc:end_iloc]

        self.draw_chart_plot(sliced_df)

    def add_buy_sim_df(self, index_date, price):
        # 종목명
        index = self.stockTable.selectedIndexes()[0].row()
        code = self.stockTable.item(index, stockTable_column['종목코드']).text()
        name = self.stockTable.item(index, stockTable_column['종목명']).text()
        name = '(' + code + ')' + name

        amount = int(self.deposit / price)  # 매수수량(종가 매수)
        buy_value = amount * price  # 총매수 금액
        charge = int((buy_value * CHARGE_RATE) / 10) * 10  # 수수료 10원이하 절사

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
        self.stockSimTable.setRowCount(row_cnt + 1)

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

    def MFI(self, index_date, period):
        last_loc = np.where(self.chart_df.index == index_date)[0]
        begin_loc = last_loc - period

        if begin_loc < 0:
            return None

        PMF = 0
        NMF = 0
        prev_price = 0
        for i in range(begin_loc[0], last_loc[0] + 1):
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

        MFI = 100 - (100 / (1 + MR))
        return round(MFI, 2)

    def CRSI(self, index_date, period, signal):
        last_loc = np.where(self.chart_df.index == index_date)[0]
        begin_loc = last_loc - period - signal

        if begin_loc < 0:
            return None, None

        SIG = np.array([])
        for s in range(0, signal):
            U = 0
            D = 0
            prev_price = 0
            for i in range(begin_loc[0] + s + 1, begin_loc[0] + s + period + 2):
                price = self.chart_df.iloc[i]['Close']

                if i == begin_loc[0] + s + 1:
                    prev_price = price
                    continue

                if price > prev_price:
                    U += (price - prev_price)

                if price < prev_price:
                    D += (prev_price - price)

                prev_price = price

            if (U + D) != 0:
                RSI = U / (U + D) * 100
            else:
                RSI = 0

            data = self.chart_df.iloc[i]
            SIG = np.append(SIG, RSI)

        return round(RSI, 2), round(SIG.mean(), 2)

    def RSI(self, index_date, period, signal):
        last_loc = np.where(self.chart_df.index == index_date)[0]
        begin_loc = last_loc - period - signal

        if begin_loc < 0:
            return None, None

        SIG = np.array([])
        for s in range(1, signal + 1):
            U = 0
            D = 0
            prev_price = 0
            for i in range(begin_loc[0] + s + 1, begin_loc[0] + s + period + 2):
                price = self.chart_df.iloc[i]['Close']

                if i == begin_loc[0] + s + 1:
                    prev_price = price
                    continue

                if price > prev_price:
                    U += (price - prev_price)

                if price < prev_price:
                    D += (prev_price - price)

                prev_price = price

            AU = U / period
            AD = D / period

            if (AU + AD) != 0:
                RSI = 100 * AU / (AU + AD)
            else:
                RSI = 0

            SIG = np.append(SIG, RSI)

        return round(RSI, 2), round(SIG.mean(), 2)

    def RSI2(self, index_date, period, signal):
        last_loc = np.where(self.chart_df.index == index_date)[0]
        length = signal + period
        begin_loc = last_loc - period - length

        if begin_loc < 0:
            return None, None

        SIG = np.array([])
        SIG2 = np.array([])
        for s in range(0, length):
            U, D = 0, 0
            prev_price = 0
            for i in range(begin_loc[0] + s + 1, begin_loc[0] + s + period + 2):
                U2, D2 = 0, 0
                price = self.chart_df.iloc[i]['Close']

                if i == begin_loc[0] + s + 1:
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

            RSI = 100 - 100 / (1 + RS)

            SIG = np.append(SIG, RSI)

            if s != 0:
                AU2 = (AU2 * (period - 1) + U2) / period
                AD2 = (AD2 * (period - 1) + D2) / period

                if AD2 != 0:
                    RS2 = AU2 / AD2
                else:
                    RS2 = 0
                RSI2 = 100 - 100 / (1 + RS2)
            else:
                AU2, AD2 = AU, AD
                RSI2 = RSI

            SIG2 = np.append(SIG2, RSI2)

        return round(RSI2, 2), round(SIG2[-1 * signal:].mean(), 2)

    def Envelope(self, index_date, period, percent):
        last_loc = np.where(self.chart_df.index == index_date)[0]
        begin_loc = last_loc - period

        sample_df = self.chart_df.iloc[begin_loc[0] + 1:last_loc[0] + 1]

        avg = sample_df['Close'].mean()

        high = avg + avg * (percent / 100)
        low = avg - avg * (percent / 100)

        return low, high

    def sim_buy_condition1(self, code, type, index_date, sim_data, open_stay):
        # 당일 시작가과 종가 구하기
        day_open = sim_data['Open']
        day_close = sim_data['Close']

        # 시작단가를 유지하지 않거나 기준단가 이하이면 False 리턴
        if open_stay == False and day_close < self.buy_cond0_value:
            return False

        # MFI 값이 20 이하이면 매수
        if self.buy_cond1 == 'MFI14(20이하)':
            mfi_value = self.MFI(index_date, 14)
            if mfi_value == None or mfi_value > 20:
                return False
        # RSI 값이 30 이하이면 매수
        elif self.buy_cond1 == 'RSI14(30이하)':
            rsi_value, rsi_signal = self.RSI2(index_date, 14, 6)
            if rsi_value == None or rsi_value > 30:
                return False
        # Envelope 값이 -10 이하이면 매수
        elif self.buy_cond1 == 'Envelope20(-10)':
            low_value, high_value = self.Envelope(index_date, 20, 10)
            if low_value == None or low_value < day_close:
                return False
        else:
            pass

        # MFI 값이 20 이하이면 매수
        if self.buy_cond2 == 'MFI14(20이하)':
            mfi_value = self.MFI(index_date, 14)
            if mfi_value == None or mfi_value > 20:
                return False
        # RSI 값이 30 이하이면 매수
        elif self.buy_cond2 == 'RSI14(30이하)':
            rsi_value, rsi_signal = self.RSI2(index_date, 14, 6)
            if rsi_value == None or rsi_value > 30:
                return False
        # Envelope 값이 -10 이하이면 매수
        elif self.buy_cond2 == 'Envelope20(-10)':
            low_value, high_value = self.Envelope(index_date, 20, 10)
            if low_value == None or low_value < day_close:
                return False
        else:
            pass

        # 종가 매수
        if self.deposit >= (day_close * 2) and day_close >= self.buy_cond0_value:
            self.add_buy_sim_df(index_date, day_close)

        return True

    def write_stockProfitTable(self):
        index = len(self.profit_df) - 1

        # 리스트에 표출될 전체 ROW수 설정
        self.stockProfitTable.setRowCount(index + 1)

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

    def add_sell_sim_df(self, index_date, sell_amount, price, buy_charge, buy_value, buy_price):
        # 종목명
        index = self.stockTable.selectedIndexes()[0].row()
        code = self.stockTable.item(index, stockTable_column['종목코드']).text()
        name = self.stockTable.item(index, stockTable_column['종목명']).text()
        name = '(' + code + ')' + name

        # 시뮬레이션 결과 설정
        sell_value = sell_amount * price  # 매도 총금액
        charge = int((sell_value * CHARGE_RATE) / 10) * 10  # 수수료 10원이하 절사
        tax = int((sell_value * TAX_RATE))  # 세금

        # 매수/매도 수수료 및 세금 계산
        charge += buy_charge
        total_tax = charge + tax

        # 수익금 계산
        profit = int(sell_value - buy_value - total_tax)

        # 수익률 계산 (수수료 및 세금을 제외한 순수 수익률)
        rate = profit / buy_value

        # 예수금 계산
        self.deposit = self.deposit + (sell_value - total_tax)

        data = [name, index_date, '매도', sell_amount, price, charge, tax, rate, profit, 0, 0]
        self.sim_df.loc[len(self.sim_df)] = data

        self.write_stockSimTable('매도')
        print("[{}] ({}) 매도 : 수량[{}], 매매가[{}], 수익금[{}], 수익률[{}]".format(name, index_date, int(sell_amount), price,
                                                                        str(format(profit, ',')),
                                                                        str(round(rate * 100, 2)) + '%'))

        data = [name, index_date, profit, rate, buy_value, sell_value, buy_price, price, charge, tax]
        self.profit_df.loc[len(self.profit_df)] = data

        self.write_stockProfitTable()

        # 전체 실현손익 및 평균 수익률 계산
        total_profit = int(self.profit_df['Profit'].sum())
        stock_cnt = self.profit_df.Name.nunique()
        avg_rate = total_profit / (stock_cnt * self.init_deposit)

        self.profit_profitLabel.setText(str(format(total_profit, ',')))
        self.profit_rateLabel.setText(str(round(avg_rate * 100, 2)) + '% (매매종목수 : ' + str(stock_cnt) + ')')

        print("* 총 {} 종목 전체 실현손익 : 수익[ {} ], 수익률[ {} ]".format(str(stock_cnt), str(format(total_profit, ',')),
                                                               str(round(avg_rate * 100, 2)) + '%'))

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
            if self.sell_cond1 == 'MFI14(80이상)':
                mfi_value = self.MFI(index_date, 14)
                if mfi_value == None or mfi_value < 80:
                    return
            # RSI 값이 70 이상이면 매도
            elif self.sell_cond1 == 'RSI14(70이상)':
                rsi_value, rsi_signal = self.RSI2(index_date, 14, 6)
                if rsi_value == None or rsi_value < 70:
                    return
            # Envelope 값이 +10 이상이면 매도
            elif self.sell_cond1 == 'Envelope20(+10)':
                low_value, high_value = self.Envelope(index_date, 20, 10)
                if high_value == None or high_value > day_close:
                    return
            else:
                pass

            # MFI 값이 80 이상이면 매도
            if self.sell_cond2 == 'MFI14(80이상)':
                mfi_value = self.MFI(index_date, 14)
                if mfi_value == None or mfi_value < 80:
                    return
            # RSI 값이 70 이상이면 매도
            elif self.sell_cond2 == 'RSI14(70이상)':
                rsi_value, rsi_signal = self.RSI2(index_date, 14, 6)
                if rsi_value == None or rsi_value < 70:
                    return
            # Envelope 값이 +10 이상이면 매도
            elif self.sell_cond2 == 'Envelope20(+10)':
                low_value, high_value = self.Envelope(index_date, 20, 10)
                if high_value == None or high_value > day_close:
                    return
            else:
                pass

        if sell_amount > 0:
            self.add_sell_sim_df(index_date, sell_amount, day_close, buy_charge, buy_value, buy_price)

    def write_stockRemainTable(self):
        index = len(self.remain_df) - 1

        # 리스트에 표출될 전체 ROW수 설정
        self.stockRemainTable.setRowCount(index + 1)

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

    def add_remain_df(self, last_date):
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
        name = '(' + code + ')' + name  # 종목명
        current = int(self.stockTable.item(index, stockTable_column['현재가']).text().replace(',', ''))  # 현재가
        sell_value = amount * current  # 현재가로 매도했을 때의 금액
        sell_charge = int((sell_value * CHARGE_RATE) / 10) * 10  # 매도 시 수수료 10원이하 절사
        tax = int(sell_value * TAX_RATE)  # 세금
        charge = buy_charge + sell_charge  # 매수/매도 수수료
        profit = int(sell_value - buy_value - charge - tax)  # 평가손익
        rate = profit / buy_value

        data = [name, date, amount, price, profit, rate, current, buy_value, sell_value, charge, tax]
        self.remain_df.loc[len(self.remain_df)] = data

        self.write_stockRemainTable()
        print("[{}] ({}) 잔고 : 수량[{}], 현재가[{}], 수익금[{}], 수익률[{}]".format(name, last_date, int(amount), current,
                                                                        str(format(profit, ',')),
                                                                        str(round(rate * 100, 2)) + '%'))

        # 전체 평가손익 및 평균 수익률 계산
        total_profit = int(self.remain_df['Profit'].sum())
        stock_cnt = self.remain_df.Name.nunique()
        avg_rate = total_profit / (stock_cnt * self.init_deposit)

        self.remain_profitLabel.setText(str(format(total_profit, ',')))
        self.remain_rateLabel.setText(str(round(avg_rate * 100, 2)) + '% (매매종목수 : ' + str(stock_cnt) + ')')

        print("* 총 {} 종목 잔고 : 손익[ {} ], 수익률[ {} ]".format(str(stock_cnt), str(format(total_profit, ',')),
                                                          str(round(avg_rate * 100, 2)) + '%'))

    def set_condition_parameter(self):
        # 매수조건 파라메터
        self.buy_cond0_value = int(self.buy_cond0LineEdit.text().replace(',', ''))
        self.buy_cond1 = self.buy_cond1ComboBox.currentText()
        self.buy_cond2 = self.buy_cond2ComboBox.currentText()

        self.deposit = int(self.depositLineEdit.text().replace(',', ''))
        self.init_deposit = int(self.depositLineEdit.text().replace(',', ''))

        # 매도조건 파라메터
        self.sell_cond0_value = int(self.sell_cond0LineEdit.text().replace(',', '')) / 100
        self.sell_cond1 = self.sell_cond1ComboBox.currentText()
        self.sell_cond2 = self.sell_cond2ComboBox.currentText()

        # 시뮬레이션 기간
        sim_start_date = self.sim_startDateEdit.date().toPyDate()
        sim_end_date = self.sim_endDateEdit.date().toPyDate()
        start_index = datetime.datetime.strftime(sim_start_date, '%Y%m%d')
        end_index = datetime.datetime.strftime(sim_end_date, '%Y%m%d')

        return (start_index, end_index)

    def run_simulation(self):
        # 파라메터 설정
        (start_index, end_index) = self.set_condition_parameter()
        self.save_sim_resultPushButton.setEnabled(False)

        selected_rows = self.stockTable.selectedIndexes()
        if len(selected_rows) != 0:
            index = self.stockTable.selectedIndexes()[0].row()

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥
            name = self.stockTable.item(index, stockTable_column['종목명']).text()
            name = '(' + code + ')' + name
            value_item = self.stockTable.item(index, stockTable_column['현재가'])

            # 시뮬레이션 결과 DataFrame Clear
            if len(self.sim_df.index) > 0:
                self.sim_df.drop(self.sim_df.index, inplace=True)

            if len(self.temp_df.index) > 0:
                self.temp_df.drop(self.temp_df.index, inplace=True)

            #            self.clear_simulation_result_table()

            # 시뮬레이션 대상 데이터 존재여부 검사
            if value_item is None:
                print("[{}] 시뮬레이션 대상 데이터가 없습니다......".format(name))
                QtWidgets.QMessageBox.about(self, "시뮬레이션 SKIP", "시뮬레이션 대상 데이터가 없습니다.")
                return

            # 시뮬레이션 시작일이 상장일보다 작으면 시뮬레이션 종료
            if start_index < self.open_index:
                print("[{}] 시뮬 시작일({})이 상장일({}) 보다 작아서 시뮬을 종료합니다......".format(name, start_index, self.open_index))
                QtWidgets.QMessageBox.about(self, "시뮬레이션 SKIP",
                                            "시작일({})이 상장일({}) 보다 작습니다.".format(start_index, self.open_index))
                return

            # 시뮬레이션 대상 데이타
            sim_datas = self.chart_df.loc[start_index:end_index]

            # 시뮬레이션 대상 데이터 존재여부 검사
            if sim_datas.empty:
                print("[{}] 시뮬레이션 대상 데이터가 없습니다......".format(name))
                QtWidgets.QMessageBox.about(self, "시뮬레이션 SKIP", "시뮬레이션 대상 데이터가 없습니다.")
                return

            # 시뮬 시작일의 주가가 기준단가보다 낮으면 시뮬레이션 종료
            open_value = sim_datas.iloc[0]['Open']
            if open_value < self.buy_cond0_value:
                print("[{}] 시뮬 시작일의 주가({})가 기준단가({}) 보다 낮아서 시뮬을 종료합니다......".format(name, open_value,
                                                                                    self.buy_cond0_value))
                QtWidgets.QMessageBox.about(self, "시뮬레이션 SKIP",
                                            "시작주가({})가 기준단가({}) 보다 낮습니다.".format(open_value, self.buy_cond0_value))
                return

            for i in range(len(sim_datas.index)):
                # 매수 조건 실행
                self.sim_buy_condition1(code, type, sim_datas.index[i], sim_datas.iloc[i], True)

                # 매도 조건 실행
                self.sim_sell_condition1(code, type, sim_datas.index[i], sim_datas.iloc[i])

            # 시뮬레이션 잔고 설정
            if len(sim_datas.index) > 0:
                self.add_remain_df(sim_datas.index[-1])

            QtWidgets.QMessageBox.about(self, "시뮬레이션 완료", "시뮬레이션이 완료되었습니다. 결과를 확인하세요!")
        else:
            QtWidgets.QMessageBox.warning(self, "메세지", "시뮬레이션 할 종목를 선택하세요!")

    def sleep_time(self, millisecond):
        loop = QEventLoop()
        QTimer.singleShot(millisecond, loop.quit)
        loop.exec_()

    def run_total_simulation(self):
        # 파라메터 설정
        (start_index, end_index) = self.set_condition_parameter()

        # 전체 시뮬레이션 결과 DataFrame Clear
        self.clear_sim_result()

        start_cnt = 0
        total_cnt = self.stockTable.rowCount()

        # 종목별 기본정보 DB 저장
        for index in range(start_cnt, total_cnt):
            print("{} / {}".format(index + 1, total_cnt))

            # 해당 ROW 선택
            self.stockTable.selectRow(index)

            # 종목 차트데이로 로딩
            self.draw_stock_chart('DAY')
            self.sleep_time(DELAY_TIME * 1000)

            # 예수금 초기화
            self.deposit = int(self.depositLineEdit.text().replace(',', ''))

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥
            value_item = self.stockTable.item(index, stockTable_column['현재가'])

            # 종목 시뮬레이션 결과 DataFrame Clear
            if len(self.sim_df.index) > 0:
                self.sim_df.drop(self.sim_df.index, inplace=True)

            if len(self.temp_df.index) > 0:
                self.temp_df.drop(self.temp_df.index, inplace=True)

            # 시뮬레이션 대상 데이터 존재여부 검사
            if value_item is None:
                continue

            # 시뮬레이션 대상 데이타
            sim_datas = self.chart_df.loc[start_index:end_index]

            # 시뮬레이션 대상 데이터 존재여부 검사
            if sim_datas.empty:
                continue

            # 시뮬 시작일의 주가가 기준단가보다 낮으면 시뮬레이션 종료
            open_value = sim_datas.iloc[0]['Open']

            # 시뮬레이션 시작일이 상장일보다 작거나 시작주가가 기준단가보다 낮으면 시뮬레이션 종료
            if start_index >= self.open_index and open_value >= self.buy_cond0_value:
                for i in range(len(sim_datas.index)):
                    # 매수 조건1 실행
                    self.sim_buy_condition1(code, type, sim_datas.index[i], sim_datas.iloc[i], True)

                    # 매도 조건1 실행
                    self.sim_sell_condition1(code, type, sim_datas.index[i], sim_datas.iloc[i])

            # 시뮬레이션 잔고 설정
            if len(sim_datas.index) > 0:
                self.add_remain_df(sim_datas.index[-1])

        if len(self.profit_df) > 0:
            self.save_sim_resultPushButton.setEnabled(True)

        QtWidgets.QMessageBox.about(self, "전체 시뮬레이션 완료", "전체 시뮬레이션이 완료되었습니다. 결과를 확인하세요!")

    def clear_sim_result(self):
        # 전체 시뮬레이션 결과 DataFrame Clear
        if len(self.remain_df.index) > 0:
            self.remain_df.drop(self.remain_df.index, inplace=True)
        if len(self.profit_df.index) > 0:
            self.profit_df.drop(self.profit_df.index, inplace=True)

        self.clear_simulation_result_table()
        self.clear_simulation_remain_table()
        self.clear_simulation_profit_table()
        self.save_sim_resultPushButton.setEnabled(False)

    def save_sim_result(self):
        stock_name = self.profit_df.Name.unique()
        minus_name = self.profit_df['Name'][self.profit_df['Rate'] < 0].values
        recommand_name = np.setdiff1d(stock_name, minus_name)

        # 기존 결과정보 삭제
        self.delete_db_result_table()

        for name in recommand_name:
            code = name[1:7]
            self.save_db_result_table(code)

        QtWidgets.QMessageBox.about(self, "결과 저장완료", "시뮬레이션 결과저장이 완료되었습니다.")
        self.save_sim_resultPushButton.setEnabled(False)

    def saved_sim_result_use_yn(self):
        if self.exist_db_result_table():
            reply = QtWidgets.QMessageBox.question(self, '결과정보 이용여부', "기존에 저장한 결과정보를 이용하여 종목를 검색하시겠습니까?",
                                                   QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
            if reply == QMessageBox.Yes:
                return True
        return False

    def get_stock_open_date(self, code, type):
        query = "SELECT StockDate FROM MARKET_INFO WHERE Code='" + code + "'"

        if type == 'KO':
            self.kospi_cur.execute(query)
            open_date = self.kospi_cur.fetchone()
        else:
            self.kosdaq_cur.execute(query)
            open_date = self.kosdaq_cur.fetchone()

        if open_date[0] < '19850104':  # 최초 증권데이터 생성일
            stock_date = '19850104'
        else:
            stock_date = open_date[0]

        return stock_date

    def get_chart_record_date(self, code, type):
        record_date = None
        finish_yn = None

        # 종목코드에 해당하는 테이블이 존재유무 판단
        query1 = "SELECT * FROM sqlite_master WHERE type='table' AND name='CHART_NOTE'"
        query2 = "SELECT DayRecordDate, DayFinish FROM CHART_NOTE WHERE Code='" + code + "'"

        if type == 'KO':
            self.kospi_cur.execute(query1)
            table_yn = self.kospi_cur.fetchone()

            if table_yn is not None:
                self.kospi_cur.execute(query2)
                kospi_info = self.kospi_cur.fetchone()
                if kospi_info is not None:
                    record_date = kospi_info[0]
                    if kospi_info[1]:
                        finish_yn = True
                    else:
                        finish_yn = False

        if type == 'KQ':
            self.kosdaq_cur.execute(query1)
            table_yn = self.kosdaq_cur.fetchone()

            if table_yn is not None:
                self.kosdaq_cur.execute(query2)
                kosdaq_info = self.kosdaq_cur.fetchone()
                if kosdaq_info is not None:
                    record_date = kosdaq_info[0]
                    if kosdaq_info[1]:
                        finish_yn = True
                    else:
                        finish_yn = False

        return (record_date, finish_yn)

    def get_minmax_date_chart_table(self, table_name, type):
        min_date = None
        max_date = None

        # 종목코드에 해당하는 테이블이 존재유무 판단
        query1 = "SELECT * FROM sqlite_master WHERE type='table' AND name='" + table_name + "'"
        # 해당 종목코드에 저장된 최대/최소 날짜 조회
        query2 = "SELECT min(Date), max(Date) FROM " + table_name

        if type == 'KO':
            self.kospi_cur.execute(query1)
            table_yn = self.kospi_cur.fetchone()

            if table_yn is not None:
                self.kospi_cur.execute(query2)
                kospi_info = self.kospi_cur.fetchone()
                min_date = kospi_info[0]
                max_date = kospi_info[1]
        if type == 'KQ':
            self.kosdaq_cur.execute(query1)
            table_yn = self.kosdaq_cur.fetchone()

            if table_yn is not None:
                self.kosdaq_cur.execute(query2)
                kosdaq_info = self.kosdaq_cur.fetchone()
                min_date = kosdaq_info[0]
                max_date = kosdaq_info[1]

        return min_date, max_date

    def create_db_chart_table(self, table_name, type):
        # 해당 종목의 차트 테이블 생성
        query = "CREATE TABLE IF NOT EXISTS " + table_name + \
                "(Date TEXT PRIMARY KEY, Open INTEGER, High INTEGER, Low INTEGER, Close INTEGER, Volume INTEGER, Gubun TEXT)"

        if type == 'KO':
            self.kospi_cur.execute(query)
            self.kospi_db.commit()
        else:
            self.kosdaq_cur.execute(query)
            self.kosdaq_db.commit()

    def update_chart_record_date(self, code, type, date, finish):
        if date < '19850104':
            date = '19850104'

        if finish is None:
            query1 = "INSERT OR IGNORE INTO CHART_NOTE(Code, DayRecordDate)VALUES('{}', '{}');".format(code, date)
            query2 = "UPDATE CHART_NOTE SET DayRecordDate = '{}' WHERE Code = '{}';".format(date, code)
        else:
            query1 = "INSERT OR IGNORE INTO CHART_NOTE(Code, DayRecordDate, DayFinish)VALUES('{}', '{}', '{}');".format(
                code, date, finish)
            query2 = "UPDATE CHART_NOTE SET DayRecordDate = '{}', DayFinish = '{}' WHERE Code = '{}';".format(date,
                                                                                                              finish,
                                                                                                              code)

        if type == 'KO':
            self.kospi_cur.execute(query1)
            self.kospi_db.commit()
            self.kospi_cur.execute(query2)
            self.kospi_db.commit()

        if type == 'KQ':
            self.kosdaq_cur.execute(query1)
            self.kosdaq_db.commit()
            self.kosdaq_cur.execute(query2)
            self.kosdaq_db.commit()

    def save_db_stocks_chart_datas(self, code, type, chart_datas):
        # 종목별 주식상장일 조회
        table_name = 'D' + code  # S:분봉, D:일봉, W:주봉, M:월봉
        open_date = self.get_stock_open_date(code, type)

        # CHART_NOTE에 기록된 종목별 데이터 시작일 조회
        (record_date, finish_yn) = self.get_chart_record_date(code, type)

        # CHART_NOTE에 종목별 데이터 기록일이 존재하는 경우 데이터 시작일 설정
        if finish_yn and record_date is not None and record_date != '':
            open_date = record_date

        db_min_date, db_max_date = self.get_minmax_date_chart_table(table_name, type)

        # 차트 테이블이 존재하지 않으면 테이블 생성
        if db_min_date is None:
            self.create_db_chart_table(table_name, type)

        finish = False
        min_date = chart_datas['Date'].min()
        max_date = chart_datas['Date'].max()
        query = "DELETE FROM " + table_name + " WHERE Date BETWEEN " + min_date + " AND " + max_date

        # 무조건 조회 데이터는 update 함
        if type == 'KO':
            self.kospi_cur.execute(query)
            self.kospi_db.commit()

            chart_datas.to_sql(table_name, self.kospi_db, index=False, if_exists='append')

            # DB_INFO 테이블에 일봉차트 정보 UPDATE
            self.update_date_db_info('코스피', table_name)
        else:
            self.kosdaq_cur.execute(query)
            self.kosdaq_db.commit()

            chart_datas.to_sql(table_name, self.kosdaq_db, index=False, if_exists='append')

            # DB_INFO 테이블에 일봉차트 정보 UPDATE
            self.update_date_db_info('코스닥', table_name)

        if finish_yn and db_max_date is not None and db_min_date is not None and open_date is not None:
            if min_date <= db_max_date and db_min_date <= open_date:
                finish = True

        # 모든 데이터를 받은 경우, CHART_NOTE에 데이터기록일 저장
        if self.kiwoom.remained_data == False:
            self.update_chart_record_date(code, type, min_date, finish=1)
            finish = True
        else:
            if finish_yn == False:
                self.update_chart_record_date(code, type, min_date, finish=0)
                finish = False

        return finish

    def update_chart_data(self, index, code, type):
        today = datetime.datetime.now().strftime('%Y%m%d')

        self.kiwoom.set_input_value("종목코드", code)
        self.kiwoom.set_input_value("기준일자", today)
        self.kiwoom.set_input_value("수정주가구분", 1)
        self.kiwoom.comm_rq_data("opt10081_req", "opt10081", 0, "0101")

        start_time = time.time()

        chart_datas = DataFrame.from_dict(self.kiwoom.opt10081_output)

        if chart_datas.empty:
            is_finished = True
        else:
            is_finished = self.save_db_stocks_chart_datas(code, type, chart_datas)

        end_time = time.time()
        diff_time = end_time - start_time
        print("실행시간 ---------------> " + str(diff_time))
        if diff_time < TR_REQ_TIME_INTERVAL_1000:
            self.sleep_time((TR_REQ_TIME_INTERVAL_1000 - diff_time) * 1000)

        if is_finished:
            return

        while self.kiwoom.remained_data:
            self.kiwoom.set_input_value("종목코드", code)
            self.kiwoom.set_input_value("기준일자", today)
            self.kiwoom.set_input_value("수정주가구분", 1)
            self.kiwoom.comm_rq_data("opt10081_req", "opt10081", 2, "0101")

            start_time = time.time()

            chart_datas = DataFrame.from_dict(self.kiwoom.opt10081_output)

            if chart_datas.empty:
                is_finished = True
            else:
                is_finished = self.save_db_stocks_chart_datas(code, type, chart_datas)

            end_time = time.time()
            diff_time = end_time - start_time
            print("실행시간 ---------------> " + str(diff_time))
            if diff_time < TR_REQ_TIME_INTERVAL_1000:
                self.sleep_time((TR_REQ_TIME_INTERVAL_1000 - diff_time) * 1000)

            if is_finished:
                break

    def check_latest_data(self, latest_date):
        now = self.get_now_day()

        temp_date = datetime.datetime.strptime(latest_date, '%Y%m%d')
        str_date = temp_date.strftime('%Y-%m-%d') + ' ' + str(CLOSE_TIME) + ':00:00'
        saved_date = datetime.datetime.strptime(str_date, '%Y-%m-%d %H:%M:%S')

        diff_date = now - saved_date

        if diff_date.days > 0:
            return False

        if now.weekday() == saved_date.weekday():
            if now.hour < OPEN_TIME and saved_date.hour < OPEN_TIME:
                return True
            if now.hour >= CLOSE_TIME and saved_date.hour >= CLOSE_TIME:
                return True
        elif now.weekday() - saved_date.weekday() > 0:
            if now.hour < OPEN_TIME and saved_date.hour >= CLOSE_TIME:
                return True
        else:
            return True

        return False

    def search_buy_recommand(self, update_yn):
        # 파라메터 설정
        (start_index, end_index) = self.set_condition_parameter()

        # 전체 시뮬레이션 결과 DataFrame Clear
        self.clear_sim_result()

        # 저장된 시뮬레이션 결과 이용여부를 물음
        use_result = self.saved_sim_result_use_yn()

        start_cnt = 0
        total_cnt = self.stockTable.rowCount()

        # 종목별 기본정보 DB 저장
        for index in range(start_cnt, total_cnt):
            print("{} / {}".format(index + 1, total_cnt))

            # 해당 ROW 선택
            self.stockTable.selectRow(index)

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            if update_yn:
                # 해당 Cell의 색상으로 update 필요여부를 판단함
                bg = self.stockTable.item(index, stockTable_column['기준일']).background().color()

                print("Index={}, R:{}, G:{}, B:{}".format(index, bg.red(), bg.green(), bg.blue()))
                if bg.blue() == 0:
                    self.update_chart_data(index, code, type)
                    self.stockTable.item(index, stockTable_column['기준일']).setBackground(QtGui.QColor(255, 255, 255))
                    date = self.get_db_date_db_info(code, type)
                    self.stockTable.item(index, stockTable_column['기준일']).setText(date)

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
            sim_datas = self.chart_df.loc[start_index:end_index]

            if len(sim_datas) == 0:
                continue

            if update_yn and self.check_latest_data(sim_datas.index[-1]) != True:
                continue

            # 매수 조건1 실행
            self.sim_buy_condition1(code, type, sim_datas.index[-1], sim_datas.iloc[-1], False)

            # 시뮬레이션 잔고 설정
            if len(sim_datas.index) > 0:
                self.add_remain_df(sim_datas.index[-1])

        QtWidgets.QMessageBox.about(self, "매수 검색완료", "매수 검색이  완료되었습니다. 결과를 확인하세요!")

    def search_buyOne_recommand(self, update_yn):
        # 파라메터 설정
        (start_index, end_index) = self.set_condition_parameter()
        self.save_sim_resultPushButton.setEnabled(False)

        selected_rows = self.stockTable.selectedIndexes()
        if len(selected_rows) != 0:
            index = self.stockTable.selectedIndexes()[0].row()

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            if update_yn:
                # 해당 Cell의 색상으로 update 필요여부를 판단함
                bg = self.stockTable.item(index, stockTable_column['기준일']).background().color()

                print("Index={}, R:{}, G:{}, B:{}".format(index, bg.red(), bg.green(), bg.blue()))
                if bg.blue() == 0:
                    self.update_chart_data(index, code, type)
                    self.stockTable.item(index, stockTable_column['기준일']).setBackground(QtGui.QColor(255, 255, 255))
                    date = self.get_db_date_db_info(code, type)
                    self.stockTable.item(index, stockTable_column['기준일']).setText(date)

            # 시뮬레이션 결과 DataFrame Clear
            if len(self.sim_df.index) > 0:
                self.sim_df.drop(self.sim_df.index, inplace=True)

            if len(self.temp_df.index) > 0:
                self.temp_df.drop(self.temp_df.index, inplace=True)

            # 시뮬레이션 대상 데이타
            sim_datas = self.chart_df.loc[start_index:end_index]

            if len(sim_datas) == 0:
                QtWidgets.QMessageBox.warning(self, "매수 검색완료", "해당범위의 데이터가 없습니다. 결과를 확인하세요!")
                return

            if update_yn and self.check_latest_data(sim_datas.index[-1]) != True:
                QtWidgets.QMessageBox.warning(self, "매수 검색완료", "최신 데이터가 아닙니다. 결과를 확인하세요!")
                return

            # 매수 조건1 실행
            self.sim_buy_condition1(code, type, sim_datas.index[-1], sim_datas.iloc[-1], False)

            # 시뮬레이션 잔고 설정
            if len(sim_datas.index) > 0:
                self.add_remain_df(sim_datas.index[-1])

        QtWidgets.QMessageBox.about(self, "매수 검색완료", "매수 검색이  완료되었습니다. 결과를 확인하세요!")

    def search_sell_condition1(self, code, type, index_date, sim_data):
        # 당일 시작가과 종가 구하기
        day_open = sim_data['Open']
        day_close = sim_data['Close']

        # MFI 값이 80 이상이면 매도
        if self.sell_cond1 == 'MFI14(80이상)':
            mfi_value = self.MFI(index_date, 14)
            if mfi_value == None or mfi_value < 80:
                return
        # RSI 값이 시그널 이상이면 매도
        elif self.sell_cond1 == 'RSI14(70이상)':
            rsi_value, rsi_signal = self.RSI2(index_date, 14, 6)
            if rsi_value == None or rsi_value < 70:
                return
        # Envelope 값이 +10 이상이면 매도
        elif self.sell_cond1 == 'Envelope20(+10)':
            low_value, high_value = self.Envelope(index_date, 20, 10)
            if high_value == None or high_value > day_close:
                return
        else:
            pass

        # MFI 값이 80 이상이면 매도
        if self.sell_cond2 == 'MFI14(80이상)':
            mfi_value = self.MFI(index_date, 14)
            if mfi_value == None or mfi_value < 80:
                return
        # RSI 값이 시그널 이상이면 매도
        elif self.sell_cond2 == 'RSI14(70이상)':
            rsi_value, rsi_signal = self.RSI2(index_date, 14, 6)
            if rsi_value == None or rsi_value < 70:
                return
        # Envelope 값이 +10 이상이면 매도
        elif self.sell_cond2 == 'Envelope20(+10)':
            low_value, high_value = self.Envelope(index_date, 20, 10)
            if high_value == None or high_value > day_close:
                return
        else:
            pass

        # 매수에 따른 예수금 감소
        sell_amount = int(self.deposit / day_close)
        buy_price = day_close
        buy_value = sell_amount * buy_price
        buy_charge = int((buy_value * CHARGE_RATE) / 10) * 10
        self.deposit = self.deposit - buy_value

        self.add_sell_sim_df(index_date, sell_amount, day_close, buy_charge, buy_value, buy_price)

    def search_sell_recommand(self, update_yn):
        # 파라메터 설정
        (start_index, end_index) = self.set_condition_parameter()

        # 전체 시뮬레이션 결과 DataFrame Clear
        self.clear_sim_result()

        # 저장된 시뮬레이션 결과 이용여부를 물음
        use_result = self.saved_sim_result_use_yn()

        start_cnt = 0
        total_cnt = self.stockTable.rowCount()

        # 종목별 기본정보 DB 저장
        for index in range(start_cnt, total_cnt):
            print("{} / {}".format(index + 1, total_cnt))

            # 해당 ROW 선택
            self.stockTable.selectRow(index)

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            if update_yn:
                # 해당 Cell의 색상으로 update 필요여부를 판단함
                bg = self.stockTable.item(index, stockTable_column['기준일']).background().color()

                print("Index={}, R:{}, G:{}, B:{}".format(index, bg.red(), bg.green(), bg.blue()))
                if bg.blue() == 0:
                    self.update_chart_data(index, code, type)
                    self.stockTable.item(index, stockTable_column['기준일']).setBackground(QtGui.QColor(255, 255, 255))
                    date = self.get_db_date_db_info(code, type)
                    self.stockTable.item(index, stockTable_column['기준일']).setText(date)

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
            sim_datas = self.chart_df.loc[start_index:end_index]

            if len(sim_datas) == 0:
                continue

            if update_yn and self.check_latest_data(sim_datas.index[-1]) != True:
                continue

            # 매도 검색실행
            self.search_sell_condition1(code, type, sim_datas.index[-1], sim_datas.iloc[-1])

        QtWidgets.QMessageBox.about(self, "매도 검색완료", "매도 검색이 완료되었습니다. 결과를 확인하세요!")

    def search_sellOne_recommand(self, update_yn):
        # 파라메터 설정
        (start_index, end_index) = self.set_condition_parameter()
        self.save_sim_resultPushButton.setEnabled(False)

        selected_rows = self.stockTable.selectedIndexes()
        if len(selected_rows) != 0:
            index = self.stockTable.selectedIndexes()[0].row()

            code = self.stockTable.item(index, stockTable_column['종목코드']).text()
            type = self.stockTable.item(index, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            if update_yn:
                # 해당 Cell의 색상으로 update 필요여부를 판단함
                bg = self.stockTable.item(index, stockTable_column['기준일']).background().color()

                print("Index={}, R:{}, G:{}, B:{}".format(index, bg.red(), bg.green(), bg.blue()))
                if bg.blue() == 0:
                    self.update_chart_data(index, code, type)
                    self.stockTable.item(index, stockTable_column['기준일']).setBackground(QtGui.QColor(255, 255, 255))
                    date = self.get_db_date_db_info(code, type)
                    self.stockTable.item(index, stockTable_column['기준일']).setText(date)

            # 시뮬레이션 결과 DataFrame Clear
            if len(self.sim_df.index) > 0:
                self.sim_df.drop(self.sim_df.index, inplace=True)

            if len(self.temp_df.index) > 0:
                self.temp_df.drop(self.temp_df.index, inplace=True)

            # 시뮬레이션 대상 데이타
            sim_datas = self.chart_df.loc[start_index:end_index]

            if len(sim_datas) == 0:
                QtWidgets.QMessageBox.warning(self, "매도 검색완료", "해당범위의 데이터가 없습니다. 결과를 확인하세요!")
                return

            if update_yn and self.check_latest_data(sim_datas.index[-1]) != True:
                QtWidgets.QMessageBox.warning(self, "매도 검색완료", "최신 데이터가 아닙니다. 결과를 확인하세요!")
                return

            # 매도 검색실행
            self.search_sell_condition1(code, type, sim_datas.index[-1], sim_datas.iloc[-1])

        QtWidgets.QMessageBox.about(self, "매도 검색완료", "매도 검색이 완료되었습니다. 결과를 확인하세요!")

    def save_stocks_info(self):
        first_kospi, first_kosdaq = self.check_daily_updated_db_info(table_name='STOCKS_INFO')

        # 이미 갱신된 주식기본정보를 가진 종목의 INFO필드 UP으로 변경
        query = "SELECT 종목코드 as Code FROM STOCKS_INFO"

        data1 = []
        data2 = []

        if first_kospi >= 1:  # 데이터 존재
            self.kospi_cur.execute(query)
            data1 = self.kospi_cur.fetchall()

        if first_kosdaq >= 1:  # 데이터 존재
            self.kosdaq_cur.execute(query)
            data2 = self.kosdaq_cur.fetchall()

        datas = []
        if first_kospi >= 1 and first_kosdaq >= 1:
            datas = data1 + data2
        elif first_kospi >= 1 and first_kosdaq == 0:
            datas = data1
        elif first_kospi == 0 and first_kosdaq >= 1:
            datas = data2

        if len(datas) != 0:
            datas_df = DataFrame(datas, columns=['Code'])
            datas = datas_df.values

        start_cnt = 0
        total_cnt = self.stockTable.rowCount()

        # 종목별 기본정보 DB 저장
        for i in range(start_cnt, total_cnt):
            print("{} / {}".format(i + 1, total_cnt))

            # 해당 ROW 선택
            self.stockTable.selectRow(i)

            code = self.stockTable.item(i, stockTable_column['종목코드']).text()
            type = self.stockTable.item(i, stockTable_column['구분']).text()  # KO:코스피, KQ:코스닥

            if type == 'KO':
                if first_kospi != 2:
                    need_update = True
                else:
                    need_update = False
            else:
                if first_kosdaq != 2:
                    need_update = True
                else:
                    need_update = False

            if code in datas:
                if need_update == False:
                    continue

            self.kiwoom.set_input_value("종목코드", code)
            self.kiwoom.comm_rq_data("opt10001_req", "opt10001", 0, "0114")

            start_time = time.time()

            stock_info = DataFrame.from_dict(self.kiwoom.opt10001_output)

            code = stock_info['종목코드']
            query = "DELETE FROM STOCKS_INFO WHERE 종목코드 = '" + code[0] + "'"

            # 코스피 주식기본정보 저장
            if type == 'KO':
                if first_kospi == 0:
                    stock_info.to_sql('STOCKS_INFO', self.kospi_db, index=False, if_exists='replace')
                else:
                    self.kospi_cur.execute(query)
                    self.kospi_db.commit()

                    stock_info.to_sql('STOCKS_INFO', self.kospi_db, index=False, if_exists='append')

                # DB_INFO 테이블에 STOCKS_INFO 정보 UPDATE
                self.update_date_db_info('코스피', 'STOCKS_INFO')

            # 코스닥 주식기본정보 저장
            else:
                if first_kosdaq == 0:
                    stock_info.to_sql('STOCKS_INFO', self.kosdaq_db, index=False, if_exists='replace')
                else:
                    self.kosdaq_cur.execute(query)
                    self.kosdaq_db.commit()

                    stock_info.to_sql('STOCKS_INFO', self.kosdaq_db, index=False, if_exists='append')

                # DB_INFO 테이블에 STOCKS_INFO 정보 UPDATE
                self.update_date_db_info('코스닥', 'STOCKS_INFO')

            # 종목 차트데이로 로딩
            self.draw_stock_chart('DAY')
            self.sleep_time(DELAY_TIME * 1000)

            # Cell 색상 변경
            self.stockTable.item(i, stockTable_column['구분']).setBackground(QtGui.QColor(255, 255, 255))

            end_time = time.time()
            self.sleep_time((TR_REQ_TIME_INTERVAL_1000 - (end_time - start_time)) * 1000)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    myWindow = SimulationTrading()
    myWindow.show()
    sys.exit(app.exec_())
