import time
import sqlite3
import pandas as pd
from pandas import DataFrame

from PyQt5 import uic
from PyQt5 import QtCore, QtWidgets

from Kiwoom import *
from LookupTable import *

main_form = uic.loadUiType("get_marketinfo_mini.ui")[0]
start_dialog = uic.loadUiType("init_marketinfo_dialog_mini.ui")[0]

marketTable_column = {'종목코드': 0, '종목명': 1, '상장일': 2, '감리구분': 3, '종목상태': 4, '구분': 5, '기본': 6, '일봉': 7}
upjongTable_column = {'업종코드': 0, '업종명': 1, '구분': 2, '일봉': 3}

OPEN_TIME = 9       # 장 시작시간(09시)
CLOSE_TIME = 16     # 장 마감시간(16시)


class ProgressDelegate(QtWidgets.QStyledItemDelegate):

    def paint(self, painter, option, index):
        progress = index.data(QtCore.Qt.UserRole+1000)
        opt = QtWidgets.QStyleOptionProgressBar()
        opt.rect = option.rect

        opt.minimum = 0
        opt.maximum = 100
        opt.progress = progress
        opt.text = "{}%".format(progress)
        opt.textVisible = True
        QtWidgets.QApplication.style().drawControl(QtWidgets.QStyle.CE_ProgressBar, opt, painter)


class MyDialog(QDialog, start_dialog):
    def __init__(self):
        QDialog.__init__(self)
        self.setupUi(self)


class GetMarketInfo(QMainWindow, main_form):
    def __init__(self):
        super().__init__()
        self.setupUi(self)

        # Event Handler
        self.save_stocks_infoPushButton.clicked.connect(self.save_stocks_info)
        self.save_stocks_day_chartPushButton.clicked.connect(lambda:self.save_stocks_chart_datas('DAY'))
        self.save_upjong_day_chartPushButton.clicked.connect(lambda:self.save_upjong_chart_datas('DAY'))
        self.save_allPushButton.clicked.connect(self.save_all_datas)

        self.Connect_OpenAPI.triggered.connect(self.kiwoom_connect)
        self.Disconnect_OpenAPI.triggered.connect( self.kiwoom_disconnect)
        self.actionExit.triggered.connect(self.kiwoom_disconnect)

        # 키움증권 연결
        self.kiwoom_connect()
        self.kiwoom.OnEventConnect.connect(self.event_connect)

        # DB Connect 설정
        self.kospi_db = sqlite3.connect("./datas/kospi.db")
        self.kosdaq_db = sqlite3.connect("./datas/kosdaq.db")
        self.kospi_cur = self.kospi_db.cursor()
        self.kosdaq_cur = self.kosdaq_db.cursor()

        # 화면 초기화
        self.show_start_dialog()

        self.save_db_kospi_market_info()
        self.dialog.marketInfoProgressBar.setValue(20)
        qApp.processEvents()

        self.save_db_kosdaq_market_info()
        self.dialog.marketInfoProgressBar.setValue(40)
        qApp.processEvents()

        self.create_market_table()
        self.dialog.marketInfoProgressBar.setValue(60)
        qApp.processEvents()

        self.save_db_upjong_code()
        self.dialog.marketInfoProgressBar.setValue(80)
        qApp.processEvents()

        self.create_upjong_table()
        self.dialog.marketInfoProgressBar.setValue(100)
        qApp.processEvents()

        # 종목별 차트데이터 시작일자 기록용 테이블 생성
        self.create_db_chart_note_table()

        # 업종별 차트데이터 시작일자 기록용 테이블 생성
        self.create_db_upjong_note_table()

        self.mark_stocks_info_in_market_table()
        self.mark_stocks_chart_in_market_table('DAY')
        self.mark_upjong_chart_in_upjong_table('DAY')

        self.dialog.close()

        if self.kiwoom.get_connect_state() == 0:
            self.statusBar.showMessage("서버연결 안됨!")
        else:
            self.statusBar.showMessage("서버 연결 중....")


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

    # 초기화 다이알로그창 표출
    def show_start_dialog(self):
        self.dialog = MyDialog()
        self.dialog.show()
        qApp.processEvents()

    def sleep_time(self, millisecond):
        loop = QEventLoop()
        QTimer.singleShot(millisecond, loop.quit)
        loop.exec_()

    # 주말을 뺀 최근 날짜를 리턴함
    def get_now_day(self):
        now = datetime.datetime.now()

        if now.weekday() == 5:      # 오늘이 토요일인 경우
            now = now - datetime.timedelta(days=1)
        elif now.weekday() == 6:    # 오늘이 일요일인 경우
            now = now - datetime.timedelta(days=2)
        else:
            return now

        date = now.strftime('%Y-%m-%d')
        date = date + ' ' + str(CLOSE_TIME) + ':00:00'

        now = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

        return now


    def check_daily_updated_db_info(self, table_name):
        first_kospi = 0             # 0: KOSPI DB에 해당 TABLE이 존재하지 않음
        first_kosdaq = 0            # 0: KOSDAQ DB에 해당 TABLE이 존재하지 않음

        now = self.get_now_day()
        saved_date_ko = self.get_table_saved_date(table_name, "KO")
        saved_date_kq = self.get_table_saved_date(table_name, "KQ")

        # 코스피 주식기본정보 갱신여부 체크
        if saved_date_ko is not None:
            diff_date = now - saved_date_ko
            if diff_date.days > 0:
                first_kospi = 1         # 1: KOSPI DB에 해당 TABLE이 존재하나 당일 데이터가 아님
            else:
                if diff_date.days < 0:
                    first_kospi = 2
                else:
                    if saved_date_ko.hour >= OPEN_TIME and saved_date_ko.hour < CLOSE_TIME:
                        first_kospi = 1     # 1: KOSPI DB에 해당 TABLE이 존재하나 당일 데이터가 아님
                    else:
                        first_kospi = 2     # 2: KOSPI DB에 해당 TABLE이 존재하며 당일 데이터임

        # 코스닥 주식기본정보 갱신여부 체크
        if saved_date_kq is not None:
            diff_date = now - saved_date_kq
            if diff_date.days > 0:
                first_kosdaq = 1         # 1: KOSPI DB에 해당 TABLE이 존재하나 당일 데이터가 아님
            else:
                if diff_date.days < 0:
                    first_kosdaq = 2
                else:
                    if saved_date_kq.hour >= OPEN_TIME and saved_date_kq.hour < CLOSE_TIME:
                        first_kosdaq = 1     # 1: KOSPI DB에 해당 TABLE이 존재하나 당일 데이터가 아님
                    else:
                        first_kosdaq = 2     # 2: KOSPI DB에 해당 TABLE이 존재하며 당일 데이터임

        return first_kospi, first_kosdaq    # 0:데이터없음, 1: 당일데이터 아님, 2: 당일데이터임


    def get_market_info_by_codes(self, codes):
        names = []; dates = []; cnts = []; prices = []; constructs = []; states = []

        for i, code in enumerate(codes):
            names.append(self.kiwoom.get_master_code_name(code))
            dates.append(self.kiwoom.get_master_listed_stock_date(code))
            cnts.append(self.kiwoom.get_master_listed_stock_cnt(code))
            prices.append(self.kiwoom.get_master_last_price(code))
            constructs.append(self.kiwoom.get_master_construction(code))
            states.append(self.kiwoom.get_master_stock_state(code))
        return DataFrame({'Code':codes, 'Name': names, 'StockDate':dates, 'StockCnt':cnts, 'LastPrice':prices,
                          'Construction':constructs, 'StockState':states})


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


    def create_market_table(self):
        kospi_df = pd.read_sql("SELECT * FROM MARKET_INFO", self.kospi_db)
        kosdaq_df = pd.read_sql("SELECT * FROM MARKET_INFO", self.kosdaq_db)

        kospi_df['type'] = 'KO'
        kosdaq_df['type'] = 'KQ'

        kospi_cnt = len(kospi_df.index)
        kosdaq_cnt = len(kosdaq_df.index)

        # 종목명과 코드로 정렬
        kospi_df = kospi_df.sort_values(by=['Name', 'Code'])
        kosdaq_df = kosdaq_df.sort_values(by=['Name', 'Code'])

        df = pd.concat([kospi_df, kosdaq_df])
        total_cnt = len(df.index)

        # 종목별 갯수 표출
        self.kospicntLabel.setText(str(format(kospi_cnt, ',')))
        self.kosdaqcntLabel.setText(str(format(kosdaq_cnt, ',')))
        self.totalcntLabel.setText(str(format(total_cnt, ',')))

        # 종목리스트에 표출될 전체 ROW수 설정
        self.marketTable.setRowCount(total_cnt)

        # Progress Bar 생성
        delegate = ProgressDelegate(self.marketTable)
        self.marketTable.setItemDelegateForColumn(marketTable_column['일봉'], delegate)

        # 종목리스트 출력
        for i in range(total_cnt):
            for j in range(8):
                if j == 0 or j == 5 or j == 7:      # 0:종목코드, 5:감리구분, 7:구분
                    item = QTableWidgetItem(df.iloc[i, j])
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
                elif j == 1 or j == 6:              # 1:종목명, 6:종목상태
                    item = QTableWidgetItem(df.iloc[i, j])
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
                elif j == 2:                        # 2:상장일
                    date = datetime.datetime.strptime(df.iloc[i, j], '%Y%m%d')
                    item = QTableWidgetItem(date.strftime('%Y-%m-%d'))
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
                elif j == 3 or j == 4:              # 3:상장주식수, 4:전일가
                    pass
                    #item = QTableWidgetItem(format(int(df.iloc[i, j]),','))
                    #item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)

                if j >= 3 :
                    self.marketTable.setItem(i, j-2, item)
                else:
                    self.marketTable.setItem(i, j, item)

            item = QTableWidgetItem('')
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
            self.marketTable.setItem(i, marketTable_column['기본'], item)

            progress = QTableWidgetItem()
            progress.setData(QtCore.Qt.UserRole+1000, 0)
            self.marketTable.setItem(i, marketTable_column['일봉'], progress)

        self.marketTable.setColumnWidth(marketTable_column['종목코드'], 70)
        self.marketTable.setColumnWidth(marketTable_column['종목명'], 150)
        self.marketTable.setColumnWidth(marketTable_column['상장일'], 100)
        self.marketTable.setColumnWidth(marketTable_column['감리구분'], 70)
        self.marketTable.setColumnWidth(marketTable_column['종목상태'], 210)
        self.marketTable.setColumnWidth(marketTable_column['구분'], 50)
        self.marketTable.setColumnWidth(marketTable_column['기본'], 50)
        self.marketTable.setColumnWidth(marketTable_column['일봉'], 110)

        self.marketTable.resizeRowsToContents()


    def create_upjong_table(self):
        kospi_df = pd.read_sql("SELECT * FROM UPJONG_CODE", self.kospi_db)
        kosdaq_df = pd.read_sql("SELECT * FROM UPJONG_CODE", self.kosdaq_db)

        kospi_df['type'] = 'KO'
        kosdaq_df['type'] = 'KQ'

        df = pd.concat([kospi_df, kosdaq_df])
        total_cnt = len(df.index)

        # 종목리스트에 표출될 전체 ROW수 설정
        self.upjongTable.setRowCount(total_cnt)

        # Progress Bar 생성
        delegate = ProgressDelegate(self.upjongTable)
        self.upjongTable.setItemDelegateForColumn(upjongTable_column['일봉'], delegate)

        # 업종리스트 출력
        for i in range(total_cnt):
            for j in range(upjongTable_column['구분']+1):
                item = QTableWidgetItem(df.iloc[i, j])

                if j == upjongTable_column['업종코드'] or j == upjongTable_column['구분']:
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
                else:
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)

                self.upjongTable.setItem(i, j, item)

            progress = QTableWidgetItem()
            progress.setData(QtCore.Qt.UserRole+1000, 0)
            self.upjongTable.setItem(i, upjongTable_column['일봉'], progress)

        self.upjongTable.setColumnWidth(upjongTable_column['업종코드'], 70)
        self.upjongTable.setColumnWidth(upjongTable_column['업종명'], 150)
        self.upjongTable.setColumnWidth(upjongTable_column['구분'], 50)
        self.upjongTable.setColumnWidth(upjongTable_column['일봉'], 110)

        self.upjongTable.resizeRowsToContents()


    def mark_stocks_info_in_market_table(self):
        first_kospi, first_kosdaq = self.check_daily_updated_db_info(table_name='STOCKS_INFO')

        # 이미 갱신된 주식기본정보를 가진 종목의 INFO필드 UP으로 변경
        query = "SELECT * FROM STOCKS_INFO"

        if first_kospi >= 1:        # 데이터 존재
            self.kospi_cur.execute(query)
            data1 = self.kospi_cur.fetchall()

        if first_kosdaq >= 1:       # 데이터 존재
            self.kosdaq_cur.execute(query)
            data2 = self.kosdaq_cur.fetchall()

        datas = []
        if first_kospi >= 1 and first_kosdaq >= 1:
            datas = data1 + data2
        elif first_kospi >= 1  and first_kosdaq == 0:
            datas = data1
        elif first_kospi == 0 and first_kosdaq >= 1 :
            datas = data2

        total_cnt = self.marketTable.rowCount()
        step = int(total_cnt / 100)
        progress = 0

        if len(datas) > 0:
            for i in range(total_cnt):
                code = self.marketTable.item(i, marketTable_column['종목코드']).text()
                type = self.marketTable.item(i, marketTable_column['구분']).text()        # KO:코스피, KQ:코스닥

                item = QTableWidgetItem('X')
                for out in datas:
                    if code in out[0]:
                        if (type == 'KO' and first_kospi == 2) or (type == 'KQ' and first_kosdaq == 2):
                            item = QTableWidgetItem('UP')
                        elif (type == 'KO' and first_kospi == 1) or (type == 'KQ' and first_kosdaq == 1):
                            item = QTableWidgetItem('O')
                        break

                item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
                self.marketTable.setItem(i, marketTable_column['기본'], item)

                if ((i+1) % step) == 0:
                    progress += 1
                    self.dialog.stocksInfoProgressBar.setValue(progress)
                    qApp.processEvents()

            self.dialog.stocksInfoProgressBar.setValue(100)
            qApp.processEvents()


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


    def get_stock_open_last_date(self, code, type, chart_type):
        if chart_type == 'DAY':
            query = "select OpenDate, LastDate from ( select A.Code, OpenDate, LastDate " \
                    "from (select TA.Code as Code, case when TA.StockDate < TB.DayRecordDate then TB.DayRecordDate " \
                    "else TA.StockDate end as OpenDate from MARKET_INFO TA " \
                    "left outer join CHART_NOTE TB on TA.CODE = TB.CODE ) A " \
                    "left outer join (select substr(TABLE_NAME, 2, 6) as Code, DATE as LastDate from DB_INFO " \
                    "where TABLE_NAME like 'D%') B on A.code = B.code ) where Code = '" + code + "'"
        else:
            query = "select OpenDate, LastDate from ( select A.Code, OpenDate, LastDate " \
                    "from (select TA.Code as Code, case when TA.StockDate < TB.MinRecordDate then TB.MinRecordDate " \
                    "else TA.StockDate end as OpenDate from MARKET_INFO TA " \
                    "left outer join CHART_NOTE TB on TA.CODE = TB.CODE ) A " \
                    "left outer join (select substr(TABLE_NAME, 2, 6) as Code, DATE as LastDate from DB_INFO " \
                    "where TABLE_NAME like 'S%') B on A.code = B.code ) where Code = '" + code + "'"

        if type == 'KO':
            # 코스피 종목별 데이터 저장일 로드
            self.kospi_cur.execute(query)
            dates = self.kospi_cur.fetchone()
        else:
            # 코스닥 종목별 데이터 저장일 로드
            self.kosdaq_cur.execute(query)
            dates = self.kosdaq_cur.fetchone()

        if dates[0] is not None:
            if len(dates[0]) > 8:
                open_date = datetime.datetime.strptime(dates[0], '%Y%m%d%H%M%S')
            else:
                open_date = datetime.datetime.strptime(dates[0], '%Y%m%d')
        else:
            open_date = None

        if dates[1] is not None:
            last_date = datetime.datetime.strptime(dates[1], '%Y-%m-%d %H:%M:%S')
        else:
            last_date = None

        return open_date, last_date


    def save_stocks_info(self):
        first_kospi, first_kosdaq = self.check_daily_updated_db_info(table_name='STOCKS_INFO')

        selected_rows = self.marketTable.selectedIndexes()
        if len(selected_rows) != 0:
            start_cnt = self.marketTable.selectedIndexes()[0].row()
        else:
            start_cnt = 0

        total_cnt = self.marketTable.rowCount()

        # 종목별 기본정보 DB 저장
        for i in range(start_cnt, total_cnt):
            code = self.marketTable.item(i, marketTable_column['종목코드']).text()
            type = self.marketTable.item(i, marketTable_column['구분']).text()            # KO:코스피, KQ:코스닥
            info = self.marketTable.item(i, marketTable_column['기본']).text()

            if info != 'UP':
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
                if type == 'KQ':
                    if first_kosdaq == 0:
                        stock_info.to_sql('STOCKS_INFO', self.kosdaq_db, index=False, if_exists='replace')
                    else:
                        self.kosdaq_cur.execute(query)
                        self.kosdaq_db.commit()

                        stock_info.to_sql('STOCKS_INFO', self.kosdaq_db, index=False, if_exists='append')

                    # DB_INFO 테이블에 STOCKS_INFO 정보 UPDATE
                    self.update_date_db_info('코스닥', 'STOCKS_INFO')

                item = QTableWidgetItem('UP')
                item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)

                self.marketTable.setItem(i, marketTable_column['기본'], item)
                self.marketTable.selectRow(i)

                end_time = time.time()
                self.sleep_time((TR_REQ_TIME_INTERVAL_1000 - (end_time-start_time)) * 1000)

            self.savedcntLabel.setText(str(format(i+1, ',')))


    def save_db_upjong_code(self):
        # 코스피 업종코드 DB 저장
        ret = self.kiwoom.get_upjong_code(0)
        upjong_code = DataFrame.from_dict(ret)

        upjong_code.to_sql('UPJONG_CODE', self.kospi_db, index=False, if_exists='replace')

        # DB_INFO 테이블에 UPJONG_CODE 정보 UPDATE
        self.update_date_db_info('코스피', 'UPJONG_CODE')

        # 코스닥 업종코드 DB 저장
        ret = self.kiwoom.get_upjong_code(1)
        upjong_code = DataFrame.from_dict(ret)

        upjong_code.to_sql('UPJONG_CODE', self.kosdaq_db, index=False, if_exists='replace')

        # DB_INFO 테이블에 UPJONG_CODE 정보 UPDATE
        self.update_date_db_info('코스닥', 'UPJONG_CODE')


    def create_db_chart_note_table(self):
        # 해당 종목의 일봉 테이블 생성
        query = "CREATE TABLE IF NOT EXISTS CHART_NOTE(Code TEXT PRIMARY KEY, DayRecordDate TEXT, MinRecordDate TEXT, DayFinish INTEGER)"

        self.kospi_cur.execute(query)
        self.kospi_db.commit()

        self.kosdaq_cur.execute(query)
        self.kosdaq_db.commit()


    def create_db_upjong_note_table(self):
        # 해당 종목의 일봉 테이블 생성
        query = "CREATE TABLE IF NOT EXISTS UPJONG_NOTE(Code TEXT PRIMARY KEY, DayRecordDate TEXT, MinRecordDate TEXT)"

        self.kospi_cur.execute(query)
        self.kospi_db.commit()

        self.kosdaq_cur.execute(query)
        self.kosdaq_db.commit()


    def drop_db_stocks_chart(self, code, type, chart_type):
        if chart_type == 'DAY':
            table_name = 'D'+code           # S:분봉, D:일봉, W:주봉, M:월봉
        else:
            table_name = 'S'+code           # S:분봉, D:일봉, W:주봉, M:월봉

        query = "DROP TABLE " + table_name

        if type == 'KO':
            self.kospi_cur.execute(query)
            self.kospi_db.commit()
        else:
            self.kosdaq_cur.execute(query)
            self.kosdaq_db.commit()


    def get_stock_open_date(self, code, type):
        query = "SELECT StockDate FROM MARKET_INFO WHERE Code='"+code+"'"

        if type == 'KO':
            self.kospi_cur.execute(query)
            open_date = self.kospi_cur.fetchone()
        else:
            self.kosdaq_cur.execute(query)
            open_date = self.kosdaq_cur.fetchone()

        if open_date[0] < '19850104':           # 최초 증권데이터 생성일
            stock_date = '19850104'
        else:
            stock_date = open_date[0]

        return stock_date


    def get_chart_record_date(self, code, type, chart_type):
        record_date = None
        finish_yn = None

        # 종목코드에 해당하는 테이블이 존재유무 판단
        query1 = "SELECT * FROM sqlite_master WHERE type='table' AND name='CHART_NOTE'"

        # 해당 종목의 기록일 조회
        if chart_type == 'DAY':
            query2 = "SELECT DayRecordDate, DayFinish FROM CHART_NOTE WHERE Code='"+code+"'"
        elif chart_type == 'MIN':
            query2 = "SELECT MinRecordDate FROM CHART_NOTE WHERE Code='"+code+"'"

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
        query1 = "SELECT * FROM sqlite_master WHERE type='table' AND name='"+table_name+"'"
        # 해당 종목코드에 저장된 최대/최소 날짜 조회
        query2 = "SELECT min(Date), max(Date) FROM "+table_name

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


    def update_chart_record_date(self, code, type, date, chart_type, finish):
        if chart_type == 'DAY':
            if date < '19850104':
                date = '19850104'

            if finish is None:
                query1 = "INSERT OR IGNORE INTO CHART_NOTE(Code, DayRecordDate)VALUES('{}', '{}');".format(code, date)
                query2 = "UPDATE CHART_NOTE SET DayRecordDate = '{}' WHERE Code = '{}';".format(date, code)
            else:
                query1 = "INSERT OR IGNORE INTO CHART_NOTE(Code, DayRecordDate, DayFinish)VALUES('{}', '{}', '{}');".format(code, date, finish)
                query2 = "UPDATE CHART_NOTE SET DayRecordDate = '{}', DayFinish = '{}' WHERE Code = '{}';".format(date, finish, code)

        elif chart_type == 'MIN':
            query1 = "INSERT OR IGNORE INTO CHART_NOTE(Code, MinRecordDate)VALUES('{}', '{}');".format(code, date)
            query2 = "UPDATE CHART_NOTE SET MinRecordDate = '{}' WHERE Code = '{}';".format(date, code)

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


    def update_progressbar_stocks_chart(self, row, code, type, db_min_date, db_max_date, chart_type):
        now = self.get_now_day()

        if chart_type == 'DAY':
            table_name = 'D' + code
        else:
            table_name = 'S' + code

        saved_date = self.get_table_saved_date(table_name, type)

        diff_date = now - saved_date

        # 현재시간과 데이터 저장시간이 하루도 지나지 않았으면 데이터 저장시간이 장중 이후인지 확인
        if diff_date.days == 0 and saved_date.hour >= OPEN_TIME and saved_date.hour < CLOSE_TIME:
            # 장중이면 업데이트할 데이터가 존재함
            now = now + datetime.timedelta(days=1)
        # 데이터 저장시간이 장마감 이후이고 현재시간이 자정과 장시작 이전인지 확인
        elif diff_date.days == 0 and now.hour >= 0 and now.hour < OPEN_TIME:
            # 장마감 이후이면 업데이트할 데이터가 없음
            now = now - datetime.timedelta(days=1)

        # 주식상장일 조회
        if chart_type == 'DAY':
            today = now.strftime('%Y%m%d')
            open_date = self.get_stock_open_date(code, type)
        else:
            today = now.strftime('%Y%m%d%H%M%S')
            # 분봉데이터는 최대 224일(영업일 기준 160일)까지만 존재
            final_date = now - datetime.timedelta(days=224)
            open_date = final_date.strftime('%Y%m%d%H%M%S')

        (record_date, finish_yn) = self.get_chart_record_date(code, type, chart_type)

        # 주식상장일과 차트 데이터가 틀린경우에는 상장일을 차트 데이터로 치환
        if finish_yn and record_date is not None and record_date != '':
            open_date = record_date

        if db_min_date < open_date:
            print("주식상장일({}) 보다 데이터보관일({}) 확인!!!!!".format(open_date, db_min_date))
#            self.update_chart_record_date(code, type, db_min_date, chart_type, finish=0)
#            open_date = db_min_date


        if chart_type == 'DAY':
            # 일봉차트에 저장될 전체 요일 계산
            complete_date = datetime.datetime.strptime(today, '%Y%m%d').date() - \
                            datetime.datetime.strptime(open_date, '%Y%m%d').date()

            # 저장된 일봉차트 요일 계산
            total_saved_date = datetime.datetime.strptime(db_max_date, '%Y%m%d').date() - \
                               datetime.datetime.strptime(db_min_date, '%Y%m%d').date()
        else:
            # 분봉차트에 저장될 전체 요일 계산
            complete_date = datetime.datetime.strptime(today, '%Y%m%d%H%M%S').date() - \
                            datetime.datetime.strptime(open_date, '%Y%m%d%H%M%S').date()

            # 저장된 분봉차트 요일 계산
            total_saved_date = datetime.datetime.strptime(db_max_date, '%Y%m%d%H%M%S').date() - \
                               datetime.datetime.strptime(db_min_date, '%Y%m%d%H%M%S').date()

        if today == open_date:
            if today == db_max_date and saved_date.hour >= CLOSE_TIME:
                per = 100
            else:
                per = 99
        else:
            if finish_yn:
                per = int((total_saved_date.days / complete_date.days) * 100)
            else:
                per = 99

        if chart_type == 'DAY':
            progress_item = self.marketTable.item(row, marketTable_column['일봉'])
            progress_item.setData(QtCore.Qt.UserRole + 1000, per)
            self.marketTable.setItem(row, marketTable_column['일봉'], progress_item)
        else:
            progress_item = self.marketTable.item(row, marketTable_column['분봉'])
            progress_item.setData(QtCore.Qt.UserRole + 1000, per)
            self.marketTable.setItem(row, marketTable_column['분봉'], progress_item)


    def save_db_stocks_chart_datas(self, row, code, type, chart_datas, chart_type):
        # 종목별 주식상장일 조회
        if chart_type == 'DAY':
            table_name = 'D'+code           # S:분봉, D:일봉, W:주봉, M:월봉
            open_date = self.get_stock_open_date(code, type)
        else:
            table_name = 'S' + code  # S:분봉, D:일봉, W:주봉, M:월봉
            open_date = None

        # CHART_NOTE에 기록된 종목별 데이터 시작일 조회
        (record_date, finish_yn) = self.get_chart_record_date(code, type, chart_type)

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
            self.update_chart_record_date(code, type, min_date, chart_type, finish=1)
            finish = True
        else:
            if finish_yn == False:
                self.update_chart_record_date(code, type, min_date, chart_type, finish=0)
                finish = False


        # Progress Bar Update
        db_min_date, db_max_date = self.get_minmax_date_chart_table(table_name, type)
        self.update_progressbar_stocks_chart(row, code, type, db_min_date, db_max_date, chart_type)

        return finish


    def calc_analyze_stocks_ma_chart(self, code, type, chart_type):
        if chart_type == 'DAY':
            table_name = 'D' + code
        else:
            table_name = 'S' + code

        ma_table_name = 'MA_' + table_name

        # 종목코드에 해당하는 테이블이 존재유무 판단
        query1 = "SELECT * FROM sqlite_master WHERE type='table' AND name='" + table_name + "'"

        # 해당 종목코드에 저장된 최대/최소 날짜 조회
        query2 = "SELECT Date, Open, High, Low, Close, Volume FROM " + table_name + \
                 " WHERE Volume != 0 ORDER BY Date"

        result_query = None

        if type == 'KO':
            self.kospi_cur.execute(query1)
            table_yn = self.kospi_cur.fetchone()

            if table_yn is not None:
                self.kospi_cur.execute(query2)
                result_query = self.kospi_cur.fetchall()
        else:
            self.kosdaq_cur.execute(query1)
            table_yn = self.kosdaq_cur.fetchone()

            if table_yn is not None:
                self.kosdaq_cur.execute(query2)
                result_query = self.kosdaq_cur.fetchall()

        if result_query is not None:
            df = DataFrame(result_query, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df = df.set_index('Date')

            # 주가 이동평균선 계산
            ma5   = df['Close'].abs().rolling(window=5).mean()
            ma10  = df['Close'].abs().rolling(window=10).mean()
            ma20  = df['Close'].abs().rolling(window=20).mean()
            ma60  = df['Close'].abs().rolling(window=60).mean()
            ma120 = df['Close'].abs().rolling(window=120).mean()

            # 주가 이동평균선 필드 추가
            df.insert(len(df.columns), "MA5", ma5)
            df.insert(len(df.columns), "MA10", ma10)
            df.insert(len(df.columns), "MA20", ma20)
            df.insert(len(df.columns), "MA60", ma60)
            df.insert(len(df.columns), "MA120", ma120)

            # 거래량 이동평균선 계산
            vma5   = df['Volume'].abs().rolling(window=5).mean()
            vma10  = df['Volume'].abs().rolling(window=10).mean()
            vma20  = df['Volume'].abs().rolling(window=20).mean()
            vma60  = df['Volume'].abs().rolling(window=60).mean()
            vma120 = df['Volume'].abs().rolling(window=120).mean()

            # 거래량 이동평균선 필드 추가
            df.insert(len(df.columns), "VMA5", vma5)
            df.insert(len(df.columns), "VMA10", vma10)
            df.insert(len(df.columns), "VMA20", vma20)
            df.insert(len(df.columns), "VMA60", vma60)
            df.insert(len(df.columns), "VMA120", vma120)

            if type == 'KO':
                df.to_sql(ma_table_name, self.kospi_analyze_db, if_exists='replace')
            else:
                df.to_sql(ma_table_name, self.kosdaq_analyze_db, if_exists='replace')


    def save_stocks_chart_datas(self, chart_type):
        start_time = 0
        end_time = 0

        selected_rows = self.marketTable.selectedIndexes()
        if len(selected_rows) != 0:
            start_cnt = self.marketTable.selectedIndexes()[0].row()
        else:
            start_cnt = 0
        total_cnt = self.marketTable.rowCount()

        # 종목별 기본정보 DB 저장
        for i in range(start_cnt, total_cnt):
            code = self.marketTable.item(i, marketTable_column['종목코드']).text()
            type = self.marketTable.item(i, marketTable_column['구분']).text()            # KO:코스피, KQ:코스닥

            if chart_type == 'DAY':
                item = self.marketTable.item(i, marketTable_column['일봉'])
            else:
                item = self.marketTable.item(i, marketTable_column['분봉'])

            info = item.data(QtCore.Qt.UserRole + 1000)
            self.savedcntLabel.setText(str(format(i + 1, ',')))

            if info != 100:
                self.marketTable.selectRow(i)
                diff_time = end_time - start_time
                print("실행시간 ---------------> " + str(diff_time))
                if diff_time < TR_REQ_TIME_INTERVAL_1000:
                    self.sleep_time((TR_REQ_TIME_INTERVAL_1000-diff_time)*1000)

                today = datetime.datetime.now().strftime('%Y%m%d')

                self.kiwoom.set_input_value("종목코드", code)

                if chart_type == 'DAY':
                    self.kiwoom.set_input_value("기준일자", today)
                    self.kiwoom.set_input_value("수정주가구분", 1)
                    self.kiwoom.comm_rq_data("opt10081_req", "opt10081", 0, "0101")
                else:
                    self.kiwoom.set_input_value("틱범위", 1)
                    self.kiwoom.set_input_value("수정주가구분", 1)
                    self.kiwoom.comm_rq_data("opt10080_req", "opt10080", 0, "0101")

                start_time = time.time()

                if chart_type == 'DAY':
                    chart_datas = DataFrame.from_dict(self.kiwoom.opt10081_output)
                else:
                    chart_datas = DataFrame.from_dict(self.kiwoom.opt10080_output)

                if chart_datas.empty:
                    is_finished = True
                else:
                    is_finished = self.save_db_stocks_chart_datas(i, code, type, chart_datas, chart_type)

                if is_finished:
#                    self.calc_analyze_stocks_ma_chart(code, type, chart_type)
                    end_time = time.time()
                    continue
                else:
                    end_time = time.time()

                while self.kiwoom.remained_data:
                    diff_time = end_time - start_time
                    print("실행시간 ---------------> " + str(diff_time))
                    if diff_time < TR_REQ_TIME_INTERVAL_1000:
                        self.sleep_time((TR_REQ_TIME_INTERVAL_1000-diff_time)*1000)

                    self.kiwoom.set_input_value("종목코드", code)

                    if chart_type == 'DAY':
                        self.kiwoom.set_input_value("기준일자", today)
                        self.kiwoom.set_input_value("수정주가구분", 1)
                        self.kiwoom.comm_rq_data("opt10081_req", "opt10081", 2, "0101")
                    else:
                        self.kiwoom.set_input_value("틱범윈", 1)
                        self.kiwoom.set_input_value("수정주가구분", 1)
                        self.kiwoom.comm_rq_data("opt10080_req", "opt10080", 2, "0101")

                    start_time = time.time()

                    if chart_type == 'DAY':
                        chart_datas = DataFrame.from_dict(self.kiwoom.opt10081_output)
                    else:
                        chart_datas = DataFrame.from_dict(self.kiwoom.opt10080_output)

                    if chart_datas.empty:
                        is_finished = True
                    else:
                        is_finished = self.save_db_stocks_chart_datas(i, code, type, chart_datas, chart_type)

                    if is_finished:
#                        self.calc_analyze_stocks_ma_chart(code, type, chart_type)
                        end_time = time.time()
                        break
                    else:
                        end_time = time.time()


    def bug_fix_min_chart_record_date(self, code, type, date):
        # CHART_NOTE에 저장된 DayRecordDate 로딩
#        query1 = "SELECT DayRecordDate FROM CHART_NOTE WHERE Code = '" + code + "'"

#        if type == 'KO':
#            self.kospi_cur.execute(query1)
#            day_record_date = self.kospi_cur.fetchone()
#        else:
#            self.kosdaq_cur.execute(query1)
#            day_record_date = self.kosdaq_cur.fetchone()

        # 종목별 DB 저장일 조회
        table_name = 'D' + code  # S:분봉, D:일봉, W:주봉, M:월봉
        db_min_date, db_max_date = self.get_minmax_date_chart_table(table_name, type)

        # CHART_NOTE에 DayRecordDate 삭제
        query2 = "UPDATE CHART_NOTE SET DayRecordDate = '"+ db_min_date +"' WHERE Code = '" + code + "';"

        if date < '19850104':
            date = '19850104'

        if db_min_date == date:
            if type == 'KO':
                self.kospi_cur.execute(query2)
                self.kospi_db.commit()
            if type == 'KQ':
                self.kosdaq_cur.execute(query2)
                self.kosdaq_db.commit()


    def mark_progressbar_stocks_chart(self, row, code, type, db_open_date, db_last_date, db_finish_yn, chart_type):
        now = self.get_now_day()
        diff_date = now - db_last_date

        # 현재시간과 데이터 저장시간이 하루도 지나지 않았으면 데이터 저장시간이 장중 이후인지 확인
        if diff_date.days == 0 and now.hour >= OPEN_TIME and now.hour < CLOSE_TIME:
            # 장중이면 업데이트할 데이터가 존재함
            now = now + datetime.timedelta(days=1)
        elif diff_date.days == 0 and db_last_date.hour < CLOSE_TIME:
            # 장중이면 업데이트할 데이터가 존재함
            now = now + datetime.timedelta(days=1)
        else:
            pass

        if chart_type == 'DAY':
#            self.bug_fix_min_chart_record_date(code, type, datetime.datetime.strftime(db_open_date, '%Y%m%d'))

            # 일봉차트에 저장될 전체 요일 계산
            complete_date = now - db_open_date

        else:
            # 분봉데이터는 최대 224일(영업일 기준 160일)까지만 존재
            final_date = now - datetime.timedelta(days=224)
            if db_open_date < final_date:
                db_open_date = final_date

            # 분봉차트에 저장될 전체 요일 계산
            complete_date = now - db_open_date

        # 저장된 일봉차트 요일 계산
        saved_date = db_last_date - db_open_date

        if diff_date.days <= 0 and complete_date.days == 0:
            per = 100
        elif db_finish_yn != True:
            per = 0
        else:
            per = int((saved_date.days / complete_date.days) * 100)

        # 일봉 데이터 최초발생일 저장
        if chart_type == 'DAY':
            progress_item = self.marketTable.item(row, marketTable_column['일봉'])
            progress_item.setData(QtCore.Qt.UserRole + 1000, per)
            self.marketTable.setItem(row, marketTable_column['일봉'], progress_item)
        else:
            progress_item = self.marketTable.item(row, marketTable_column['분봉'])
            progress_item.setData(QtCore.Qt.UserRole + 1000, per)
            self.marketTable.setItem(row, marketTable_column['분봉'], progress_item)


    def mark_stocks_chart_in_market_table(self, chart_type):
        if chart_type == 'DAY':
            query = "select A.Code, OpenDate, LastDate, DayFinish " \
                    "from (select TA.Code as Code, case when TA.StockDate < TB.DayRecordDate then TB.DayRecordDate " \
                    "else TA.StockDate end as OpenDate, TB.DayFinish from MARKET_INFO TA " \
                    "left outer join CHART_NOTE TB on TA.CODE = TB.CODE ) A " \
                    "left outer join (select substr(TABLE_NAME, 2, 7) as Code, DATE as LastDate from DB_INFO " \
                    "where TABLE_NAME like 'D%') B on A.code = B.code"
        else:
            query = "select A.Code, OpenDate, LastDate, DayFinish " \
                    "from (select TA.Code as Code, case when TA.StockDate < TB.MinRecordDate then TB.MinRecordDate " \
                    "else TA.StockDate end as OpenDate, TB.DayFinish from MARKET_INFO TA " \
                    "left outer join CHART_NOTE TB on TA.CODE = TB.CODE ) A " \
                    "left outer join (select substr(TABLE_NAME, 2, 7) as Code, DATE as LastDate from DB_INFO " \
                    "where TABLE_NAME like 'S%') B on A.code = B.code"

        # 코스피 종목별 데이터 저장일 로드
        self.kospi_cur.execute(query)
        kospi_date = self.kospi_cur.fetchall()

        kospi_df = DataFrame(kospi_date, columns=['Code', 'OpenDate', 'LastDate', 'DayFinish'])
        kospi_df = kospi_df.set_index('Code')

        # 코스닥 종목별 데이터 저장일 로드
        self.kosdaq_cur.execute(query)
        kosdaq_date = self.kosdaq_cur.fetchall()

        kosdaq_df = DataFrame(kosdaq_date, columns=['Code', 'OpenDate', 'LastDate', 'DayFinish'])
        kosdaq_df = kosdaq_df.set_index('Code')

        total_cnt = self.marketTable.rowCount()

        step = int(total_cnt / 100)
        progress = 0

        for i in range(total_cnt):
            code = self.marketTable.item(i, marketTable_column['종목코드']).text()
            type = self.marketTable.item(i, marketTable_column['구분']).text()            # KO:코스피, KQ:코스닥

            if type == 'KO':
                dates = kospi_df.loc[code]
            else:
                dates = kosdaq_df.loc[code]

            db_open_date = None
            db_last_date = None
            db_finish_yn = None

            if dates['OpenDate'] is not None and dates['OpenDate'] != '':
                if chart_type == 'DAY':
                    db_open_date = datetime.datetime.strptime(dates['OpenDate'], '%Y%m%d')
                else:
                    if len(dates['OpenDate']) > 8:
                        db_open_date = datetime.datetime.strptime(dates['OpenDate'], '%Y%m%d%H%M%S')
                    else:
                        db_open_date = datetime.datetime.strptime(dates['OpenDate'], '%Y%m%d')
                db_finish_yn = dates['DayFinish']

            if dates['LastDate'] is not None and dates['LastDate']  != '':
                db_last_date = datetime.datetime.strptime(dates['LastDate'], '%Y-%m-%d %H:%M:%S')

            if db_open_date is None or db_last_date is None:
                continue

            # Progress Bar Update
            self.mark_progressbar_stocks_chart(i, code, type, db_open_date, db_last_date, db_finish_yn, chart_type)

            if ((i+1) % step) == 0:
                progress += 1
                if chart_type == 'DAY':
                    self.dialog.stocksDayChartProgressBar.setValue(progress)
                else:
                    self.dialog.stocksMinChartProgressBar.setValue(progress)

                qApp.processEvents()

        if chart_type == 'DAY':
            self.dialog.stocksDayChartProgressBar.setValue(100)
        else:
            self.dialog.stocksMinChartProgressBar.setValue(100)

        qApp.processEvents()


    def create_db_upjong_table(self, table_name, type):
        # 해당 종목의 차트 테이블 생성
        query = "CREATE TABLE IF NOT EXISTS " + table_name + \
                "(Date TEXT PRIMARY KEY, Open INTEGER, High INTEGER, Low INTEGER, Close INTEGER, Volume INTEGER, Value INTEGER)"

        if type == 'KO':
            self.kospi_cur.execute(query)
            self.kospi_db.commit()
        else:
            self.kosdaq_cur.execute(query)
            self.kosdaq_db.commit()


    def get_upjong_record_date(self, code, type, chart_type):
        record_date = None

        # 종목코드에 해당하는 테이블이 존재유무 판단
        query1 = "SELECT * FROM sqlite_master WHERE type='table' AND name='UPJONG_NOTE'"

        # 해당 종목의 기록일 조회
        if chart_type == 'DAY':
            query2 = "SELECT DayRecordDate FROM UPJONG_NOTE WHERE Code='"+code+"'"
        else:
            query2 = "SELECT MinRecordDate FROM UPJONG_NOTE WHERE Code='"+code+"'"

        if type == 'KO':
            self.kospi_cur.execute(query1)
            table_yn = self.kospi_cur.fetchone()

            if table_yn is not None:
                self.kospi_cur.execute(query2)
                kospi_info = self.kospi_cur.fetchone()

                if kospi_info is not None:
                    record_date = kospi_info[0]
        else:
            self.kosdaq_cur.execute(query1)
            table_yn = self.kosdaq_cur.fetchone()

            if table_yn is not None:
                self.kosdaq_cur.execute(query2)
                kosdaq_info = self.kosdaq_cur.fetchone()

                if kosdaq_info is not None:
                    record_date = kosdaq_info[0]

        return record_date


    def update_upjong_record_date(self, code, type, date, chart_type):
        if chart_type == 'DAY':
            query1 = "INSERT OR IGNORE INTO UPJONG_NOTE(Code, DayRecordDate)VALUES('" + code + "', '" + date + "');"
            query2 = "UPDATE UPJONG_NOTE SET DayRecordDate = '"+ date + "' WHERE Code = '" + code + "';"
        elif chart_type == 'MIN':
            query1 = "INSERT OR IGNORE INTO UPJONG_NOTE(Code, MinRecordDate)VALUES('" + code + "', '" + date + "');"
            query2 = "UPDATE UPJONG_NOTE SET MinRecordDate = '" + date + "' WHERE Code = '" + code + "';"

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


    def update_progressbar_upjong_chart(self, row, code, type, db_min_date, db_max_date, chart_type):
        now = self.get_now_day()

        if chart_type == 'DAY':
            table_name = 'KD' + code
        else:
            table_name = 'KS' + code

        saved_date = self.get_table_saved_date(table_name, type)

        diff_date = now - saved_date

        # 현재시간과 데이터 저장시간이 하루도 지나지 않았으면 데이터 저장시간이 장중 이후인지 확인
        if diff_date.days == 0 and saved_date.hour >= OPEN_TIME and saved_date.hour < CLOSE_TIME:
            # 장중이면 업데이트할 데이터가 존재함
            now = now + datetime.timedelta(days=1)
        # 데이터 저장시간이 장마감 이후이고 현재시간이 자정과 장시작 이전인지 확인
        elif diff_date.days == 0 and now.hour >= 0 and now.hour < OPEN_TIME:
            # 장마감 이후이면 업데이트할 데이터가 없음
            now = now - datetime.timedelta(days=1)

        # 주식상장일 조회
        if chart_type == 'DAY':
            today = now.strftime('%Y%m%d')
            open_date = '19850104'
        else:
            today = now.strftime('%Y%m%d%H%M%S')
            # 분봉데이터는 최대 224일(영업일 기준 160일)까지만 존재
            final_date = now - datetime.timedelta(days=224)
            open_date = final_date.strftime('%Y%m%d%H%M%S')

        # UPJONG_NOTE에 저장된 데이터 기록일을 읽음
        record_date = self.get_upjong_record_date(code, type, chart_type)

        # UPJONG_NOTE에 저장된 데이터 기록이 있는 경우, 데이터 시작로 지정
        if record_date is not None and record_date != '':
            open_date = record_date

        # DB에 저장된 차트 데이터가 데이터 시작일 보다 작은면 UPJONG_NOTE의 데이터 기록일 갱신
        if db_min_date < open_date:
            self.update_upjong_record_date(code, type, db_min_date, chart_type)
            open_date = db_min_date

        if chart_type == 'DAY':
            # 차트로 저장 되어야할 전체 요일 계산
            complete_date = datetime.datetime.strptime(today, '%Y%m%d').date() - \
                            datetime.datetime.strptime(open_date, '%Y%m%d').date()

            # DB에 저장된 차트 데이터의 요일 계산
            total_saved_date = datetime.datetime.strptime(db_max_date, '%Y%m%d').date() - \
                               datetime.datetime.strptime(db_min_date, '%Y%m%d').date()
        else:
            # 분봉차트에 저장될 전체 요일 계산
            complete_date = datetime.datetime.strptime(today, '%Y%m%d%H%M%S').date() - \
                            datetime.datetime.strptime(open_date, '%Y%m%d%H%M%S').date()

            # 저장된 일봉차트 요일 계산
            total_saved_date = datetime.datetime.strptime(db_max_date, '%Y%m%d%H%M%S').date() - \
                               datetime.datetime.strptime(db_min_date, '%Y%m%d%H%M%S').date()


        # 업종 상장일이 당일인 경우 업무종료 이후이면 100%, 업무 중이면 99%로 설정
        if today == open_date:
            if today == db_max_date and saved_date.hour >= CLOSE_TIME:
                per = 100
            else:
                per = 99
        else:
            per = int((total_saved_date.days / complete_date.days) * 100)

        if chart_type == 'DAY':
            progress_item = self.upjongTable.item(row, upjongTable_column['일봉'])
            progress_item.setData(QtCore.Qt.UserRole + 1000, per)
            self.upjongTable.setItem(row, upjongTable_column['일봉'], progress_item)
        else:
            progress_item = self.upjongTable.item(row, upjongTable_column['분봉'])
            progress_item.setData(QtCore.Qt.UserRole + 1000, per)
            self.upjongTable.setItem(row, upjongTable_column['분봉'], progress_item)


    def save_db_upjong_chart_datas(self, row, code, type, chart_datas, chart_type):
        # 주식상장일 조회
        open_date = None

        # UPJONG_NOTE에 기록된 최초 데이터 기록일 조회
        if chart_type == 'DAY':
            table_name = 'KD' + code  # S:분봉, D:일봉, W:주봉, M:월봉, KS:업종분봉, KD:업종일봉
        else:
            table_name = 'KS' + code  # S:분봉, D:일봉, W:주봉, M:월봉, KS:업종분봉, KD:업종일봉

        record_date = self.get_upjong_record_date(code, type, chart_type)

        # 최초 데이터 기록일을 기준으로 함
        if record_date is not None and record_date != '':
            open_date = record_date

        # 테이블에 저장된 최대/최소일 조회
        db_min_date, db_max_date = self.get_minmax_date_chart_table(table_name, type)

        # 일봉차트 테이블이 존배하지 않으면 테이블 생성
        if db_min_date is None:
            self.create_db_upjong_table(table_name, type)

        finish = False
        min_date = chart_datas['Date'].min()
        max_date = chart_datas['Date'].max()
        query = "DELETE FROM " + table_name + " WHERE Date BETWEEN " + min_date + " AND " + max_date

        # 무조건 조회 데이터는 update 함
        if type == 'KO':
            self.kospi_cur.execute(query)
            self.kospi_db.commit()

            chart_datas.to_sql(table_name, self.kospi_db, index=False, if_exists='append')

            # DB_INFO 테이블에 차트 정보 UPDATE
            self.update_date_db_info('코스피', table_name)
        else:
            self.kosdaq_cur.execute(query)
            self.kosdaq_db.commit()

            chart_datas.to_sql(table_name, self.kosdaq_db, index=False, if_exists='append')

            # DB_INFO 테이블에 차트 정보 UPDATE
            self.update_date_db_info('코스닥', table_name)

        if db_max_date is not None and db_min_date is not None and open_date is not None:
            if min_date <= db_max_date and db_min_date <= open_date:
                finish = True

        # 종목 오픈일과 차트 데이터 기록일이 다를 경우 UPJONG_NOTE에 차트 데이터 기록일 저장
        if self.kiwoom.remained_data == False:
            self.update_upjong_record_date(code, type, min_date, chart_type)
            finish = True

        # Progress Bar Update
        db_min_date, db_max_date = self.get_minmax_date_chart_table(table_name, type)
        self.update_progressbar_upjong_chart(row, code, type, db_min_date, db_max_date, chart_type)

        return finish


    def calc_analyze_upjong_ma_chart(self, code, type, chart_type):
        if chart_type == 'DAY':
            table_name = 'KD' + code
            # 해당 종목코드에 저장된 최대/최소 날짜 조회
            query2 = "SELECT Date, Open, High, Low, Close, Volume, Value FROM " + table_name + " ORDER BY Date"
        else:
            table_name = 'KS' + code
            # 해당 종목코드에 저장된 최대/최소 날짜 조회
            query2 = "SELECT Date, Open, High, Low, Close, Volume FROM " + table_name + \
                     " WHERE Volume != 0 ORDER BY Date"

        # 종목코드에 해당하는 테이블이 존재유무 판단
        query1 = "SELECT * FROM sqlite_master WHERE type='table' AND name='" + table_name + "'"

        ma_table_name = 'MA_' + table_name

        result_query = None

        if type == 'KO':
            self.kospi_cur.execute(query1)
            table_yn = self.kospi_cur.fetchone()

            if table_yn is not None:
                self.kospi_cur.execute(query2)
                result_query = self.kospi_cur.fetchall()
        else:
            self.kosdaq_cur.execute(query1)
            table_yn = self.kosdaq_cur.fetchone()

            if table_yn is not None:
                self.kosdaq_cur.execute(query2)
                result_query = self.kosdaq_cur.fetchall()

        if result_query is not None:
            if chart_type == 'DAY':
                df = DataFrame(result_query, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Value'])
            else:
                df = DataFrame(result_query, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

            df = df.set_index('Date')

            # 주가 이동평균선 계산
            ma5   = df['Close'].abs().rolling(window=5).mean()
            ma10  = df['Close'].abs().rolling(window=10).mean()
            ma20  = df['Close'].abs().rolling(window=20).mean()
            ma60  = df['Close'].abs().rolling(window=60).mean()
            ma120 = df['Close'].abs().rolling(window=120).mean()

            # 주가 이동평균선 필드 추가
            df.insert(len(df.columns), "MA5", ma5)
            df.insert(len(df.columns), "MA10", ma10)
            df.insert(len(df.columns), "MA20", ma20)
            df.insert(len(df.columns), "MA60", ma60)
            df.insert(len(df.columns), "MA120", ma120)

            # 거래량 이동평균선 계산
            vma5   = df['Volume'].abs().rolling(window=5).mean()
            vma10  = df['Volume'].abs().rolling(window=10).mean()
            vma20  = df['Volume'].abs().rolling(window=20).mean()
            vma60  = df['Volume'].abs().rolling(window=60).mean()
            vma120 = df['Volume'].abs().rolling(window=120).mean()

            # 거래량 이동평균선 필드 추가
            df.insert(len(df.columns), "VMA5", vma5)
            df.insert(len(df.columns), "VMA10", vma10)
            df.insert(len(df.columns), "VMA20", vma20)
            df.insert(len(df.columns), "VMA60", vma60)
            df.insert(len(df.columns), "VMA120", vma120)

            if chart_type == 'DAY':
                # 거래대금 이동평균선 계산
                vama5 = df['Value'].abs().rolling(window=5).mean()
                vama10 = df['Value'].abs().rolling(window=10).mean()
                vama20 = df['Value'].abs().rolling(window=20).mean()
                vama60 = df['Value'].abs().rolling(window=60).mean()
                vama120 = df['Value'].abs().rolling(window=120).mean()

                # 거래량 이동평균선 필드 추가
                df.insert(len(df.columns), "VAMA5", vama5)
                df.insert(len(df.columns), "VAMA10", vama10)
                df.insert(len(df.columns), "VAMA20", vama20)
                df.insert(len(df.columns), "VAMA60", vama60)
                df.insert(len(df.columns), "VAMA120", vama120)

            if type == 'KO':
                df.to_sql(ma_table_name, self.kospi_analyze_db, if_exists='replace')
            else:
                df.to_sql(ma_table_name, self.kosdaq_analyze_db, if_exists='replace')


    def save_upjong_chart_datas(self, chart_type):
        start_time = 0
        end_time = 0

        selected_rows = self.upjongTable.selectedIndexes()
        if len(selected_rows) != 0:
            start_cnt = self.upjongTable.selectedIndexes()[0].row()
        else:
            start_cnt = 0

        total_cnt = self.upjongTable.rowCount()

        # 종목별 기본정보 DB 저장
        for i in range(start_cnt, total_cnt):
            code = self.upjongTable.item(i, upjongTable_column['업종코드']).text()
            type = self.upjongTable.item(i, upjongTable_column['구분']).text()            # KO:코스피, KQ:코스닥

            if chart_type == 'DAY':
                item = self.upjongTable.item(i, upjongTable_column['일봉'])
            else:
                item = self.upjongTable.item(i, upjongTable_column['분봉'])

            info = item.data(QtCore.Qt.UserRole + 1000)

            if info != 100:
                self.upjongTable.selectRow(i)

                today = datetime.datetime.now().strftime('%Y%m%d')

                diff_time = end_time - start_time
                print("실행시간 ---------------> " + str(diff_time))
                if diff_time < TR_REQ_TIME_INTERVAL_1000:
                    self.sleep_time((TR_REQ_TIME_INTERVAL_1000-diff_time)*1000)

                self.kiwoom.set_input_value("업종코드", code)

                if chart_type == 'DAY':
                    self.kiwoom.set_input_value("기준일자", today)
                    self.kiwoom.comm_rq_data("opt20006_req", "opt20006", 0, "0101")
                else:
                    self.kiwoom.set_input_value("틱범위", 1)
                    self.kiwoom.comm_rq_data("opt20005_req", "opt20005", 0, "0101")

                start_time = time.time()

                if chart_type == 'DAY':
                    chart_datas = DataFrame.from_dict(self.kiwoom.opt20006_output)
                else:
                    chart_datas = DataFrame.from_dict(self.kiwoom.opt20005_output)

                if chart_datas.empty:
                    is_finished = True
                else:
                    is_finished = self.save_db_upjong_chart_datas(i, code, type, chart_datas, chart_type)

                if is_finished:
#                    self.calc_analyze_upjong_ma_chart(code, type, chart_type)
                    end_time = time.time()
                    continue
                else:
                    end_time = time.time()

                while self.kiwoom.remained_data:
                    diff_time = end_time - start_time
                    print("실행시간 ---------------> " + str(diff_time))
                    if diff_time < TR_REQ_TIME_INTERVAL_1000:
                        self.sleep_time((TR_REQ_TIME_INTERVAL_1000 - diff_time) * 1000)

                    self.kiwoom.set_input_value("업종코드", code)

                    if chart_type == 'DAY':
                        self.kiwoom.set_input_value("기준일자", today)
                        self.kiwoom.comm_rq_data("opt20006_req", "opt20006", 2, "0101")
                    else:
                        self.kiwoom.set_input_value("틱범위", 1)
                        self.kiwoom.comm_rq_data("opt20005_req", "opt20005", 2, "0101")

                    start_time = time.time()

                    if chart_type == 'DAY':
                        chart_datas = DataFrame.from_dict(self.kiwoom.opt20006_output)
                    else:
                        chart_datas = DataFrame.from_dict(self.kiwoom.opt20005_output)

                    if chart_datas.empty:
                        is_finished = True
                    else:
                        is_finished = self.save_db_upjong_chart_datas(i, code, type, chart_datas, chart_type)

                    if is_finished:
#                        self.calc_analyze_upjong_ma_chart(code, type, chart_type)
                        end_time = time.time()
                        break
                    else:
                        end_time = time.time()


    def mark_progressbar_upjong_chart(self, row, code, type, db_open_date, db_last_date, chart_type):
        now = self.get_now_day()
        diff_date = now - db_last_date

        # 현재시간과 데이터 저장시간이 하루도 지나지 않았으면 데이터 저장시간이 장중 이후인지 확인
        if diff_date.days == 0 and db_last_date.hour >= OPEN_TIME and db_last_date.hour < CLOSE_TIME:
            # 장중이면 업데이트할 데이터가 존재함
            now = now + datetime.timedelta(days=1)

        if chart_type == 'DAY':
            # 일봉차트에 저장될 전체 요일 계산
            complete_date = now - db_open_date
        else:
            # 분봉데이터는 최대 224일(영업일 기준 160일)까지만 존재
            final_date = now - datetime.timedelta(days=224)
            if db_open_date < final_date:
                db_open_date = final_date

            # 분봉차트에 저장될 전체 요일 계산
            complete_date = now - db_open_date

        # 저장된 일봉차트 요일 계산
        saved_date = db_last_date - db_open_date

        if diff_date.days == 0 and complete_date.days == 0:
            per = 100
        else:
            per = int((saved_date.days / complete_date.days) * 100)

        # 일봉 데이터 최초발생일 저장
        if chart_type == 'DAY':
            progress_item = self.upjongTable.item(row, upjongTable_column['일봉'])
            progress_item.setData(QtCore.Qt.UserRole + 1000, per)
            self.upjongTable.setItem(row, upjongTable_column['일봉'], progress_item)
        else:
            progress_item = self.upjongTable.item(row, upjongTable_column['분봉'])
            progress_item.setData(QtCore.Qt.UserRole + 1000, per)
            self.upjongTable.setItem(row, upjongTable_column['분봉'], progress_item)


    def mark_upjong_chart_in_upjong_table(self, chart_type):
        query = "SELECT * FROM UPJONG_NOTE"

        # 코스피 업종별 데이터 저장일 로드
        self.kospi_cur.execute(query)
        kospi_date = self.kospi_cur.fetchall()

        kospi_note = DataFrame(kospi_date, columns=['Code', 'DayRecordDate', 'MinRecordDate'])
        kospi_note = kospi_note.set_index('Code')

        # 코스닥 업목별 데이터 저장일 로드
        self.kosdaq_cur.execute(query)
        kosdaq_date = self.kosdaq_cur.fetchall()

        kosdaq_note = DataFrame(kosdaq_date, columns=['Code', 'DayRecordDate', 'MinRecordDate'])
        kosdaq_note = kosdaq_note.set_index('Code')

        if chart_type == 'DAY':
            query = "select substr(TABLE_NAME, 3, 3) as Code, DATE from DB_INFO where TABLE_NAME like 'KD%'"
        else:
            query = "select substr(TABLE_NAME, 3, 3) as Code, DATE from DB_INFO where TABLE_NAME like 'KS%'"

        # 코스피 DB_INFO 업종데이블 저장일 로드
        self.kospi_cur.execute(query)
        kospi_date = self.kospi_cur.fetchall()

        kospi_info = DataFrame(kospi_date, columns=['Code', 'DATE'])
        kospi_info = kospi_info.set_index('Code')

        # 코스닥 DB_INFO 업종데이블 저장일 로드
        self.kosdaq_cur.execute(query)
        kosdaq_date = self.kosdaq_cur.fetchall()

        kosdaq_info = DataFrame(kosdaq_date, columns=['Code', 'DATE'])
        kosdaq_info = kosdaq_info.set_index('Code')

        total_cnt = self.upjongTable.rowCount()

        step = int(total_cnt / 10)
        progress = 0

        for i in range(total_cnt):
            code = self.upjongTable.item(i, upjongTable_column['업종코드']).text()
            type = self.upjongTable.item(i, upjongTable_column['구분']).text()            # KO:코스피, KQ:코스닥

            if type == 'KO':
                if code in kospi_note.index:
                    note_date = kospi_note.loc[code]
                else:
                    continue

                if code in kospi_info.index:
                    info_date = kospi_info.loc[code]
                else:
                    continue
            else:
                if code in kosdaq_note.index:
                    note_date = kosdaq_note.loc[code]
                else:
                    continue

                if code in kosdaq_info.index:
                    info_date = kosdaq_info.loc[code]
                else:
                    continue

            db_open_date = None
            db_last_date = None

            if chart_type == 'DAY':
                if note_date['DayRecordDate'] is not None and note_date['DayRecordDate'] != '':
                    db_open_date = datetime.datetime.strptime(note_date['DayRecordDate'], '%Y%m%d')
            else:
                if note_date['MinRecordDate'] is not None and note_date['MinRecordDate'] != '':
                    db_open_date = datetime.datetime.strptime(note_date['MinRecordDate'], '%Y%m%d%H%M%S')

            if info_date['DATE'] is not None and info_date['DATE'] != '':
                db_last_date = datetime.datetime.strptime(info_date['DATE'], '%Y-%m-%d %H:%M:%S')

            if db_open_date is None or db_last_date is None:
                continue

            # Progress Bar Update
            self.mark_progressbar_upjong_chart(i, code, type, db_open_date, db_last_date, chart_type)

            if ((i+1) % step) == 0:
                progress += 10
                if chart_type == 'DAY':
                    self.dialog.upjongDayChartProgressBar.setValue(progress)
                else:
                    self.dialog.upjongMinChartProgressBar.setValue(progress)

                qApp.processEvents()

        if chart_type == 'DAY':
            self.dialog.upjongDayChartProgressBar.setValue(100)
        else:
            self.dialog.upjongMinChartProgressBar.setValue(100)

        qApp.processEvents()


    def save_all_datas(self):
        self.marketTable.selectRow(0)
        self.save_stocks_info()
        self.marketTable.selectRow(0)
        self.save_stocks_chart_datas('DAY')
        self.upjongTable.selectRow(0)
        self.save_upjong_chart_datas('DAY')

        QtWidgets.QMessageBox.about(self, "작업완료", "모든 주식데이터를 DB에 저장하였습니다.")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    myWindow = GetMarketInfo()
    myWindow.show()
    sys.exit(app.exec_())