import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
from pandas import DataFrame
import datetime

# Kiwoom Class --------------------------------------------------------------------------------------------------------
TR_REQ_TIME_INTERVAL = 0.3
TR_REQ_TIME_INTERVAL_1000 = 3.7


class Kiwoom(QAxWidget):
    def __init__(self):
        super().__init__()
        self._create_kiwoom_instance()
        self._set_signal_slots()

    def _create_kiwoom_instance(self):
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

    # 사용자 정의 유틸리티 함수 ---------------------------------------------------------------------------------------
    @staticmethod
    def num_format(data):            # 천의 자리마다 콤마 추가
        strip_data = data.lstrip('-0')

        if strip_data == '':
            strip_data = '0'

        format_data = format(int(strip_data), ',d')
        if data.startswith('-'):
            format_data = '-' + format_data

        return format_data

    @staticmethod
    def per_format(data):           # 백분율(%) 포맷 변경
        strip_data = data.lstrip('-0')

        if strip_data == '':
            strip_data = '0'

        if strip_data.startswith('.'):
            strip_data = '0' + strip_data

        if data.startswith('-'):
            strip_data = '-' + strip_data

        return strip_data

    @staticmethod
    def abs_format(data):            # 천의 자리마다 콤마 추가
        strip_data = data.lstrip('+-0')

        if strip_data == '':
            strip_data = '0'

        format_data = format(int(strip_data), ',d')

        return format_data

    @staticmethod
    def log(data):                      # 로그 출력
        now = datetime.datetime.now()
        nowDatetime = now.strftime('%Y-%m-%d %H:%M:%S')
        print("[{}] OpenAPI {}".format(nowDatetime, data))


    # 로그인 버전 처리 ------------------------------------------------------------------------------------------------
    def comm_connect(self):
        self.dynamicCall("CommConnect()")
        Kiwoom.log("Kiwoom CommConnect....")
        Kiwoom.log("Start login event loop.....")
        self.login_event_loop = QEventLoop()
        self.login_event_loop.exec_()

    def comm_terminate(self):
        self.dynamicCall("CommTerminate()")
        Kiwoom.log("Kiwoom CommTerminate....")

    def get_connect_state(self):
        ret = self.dynamicCall("GetConnectState()")
        return ret

    def get_login_info(self, tag):
        ret = self.dynamicCall("GetLoginInfo(QString)", tag)
        Kiwoom.log("GetLoginInfo [{}]={}".format(tag, ret))
        return ret


    # 조회와 실시간 데이터 처리 ---------------------------------------------------------------------------------------
    def comm_rq_data(self, rqname, trcode, next, screen_no):
        self.dynamicCall("CommRqData(QString, QString, int, QString)", rqname, trcode, next, screen_no)
        Kiwoom.log("CommRqData [RQName]={} [TrCode]={} [PrevNext]={} [ScreenNo]={}".
                   format(rqname, trcode, next, screen_no))
        Kiwoom.log("Start tr event loop....")
        self.tr_event_loop = QEventLoop()
        self.tr_event_loop.exec_()

    def set_input_value(self, id, value):
        self.dynamicCall("SetInputValue(QString, QString)", id, value)
        Kiwoom.log("SetInputValue [ID]={} [Value]={}".format(id, value))

    def _get_comm_data(self, trcode, record_name, index, item_name):
        ret = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode,
                               record_name, index, item_name)
#        Kiwoom.log("GetCommData [TrCode]={} [RecordName]={} [Index]={} [ItemName]={}".
#                   format(trcode, record_name, index, item_name))
#        Kiwoom.log("            [Return Value]={}".format(ret.strip()))
        return ret.strip()

    def _get_repeat_cnt(self, trcode, record_name):
        ret = self.dynamicCall("GetRepeatCnt(QString, QString)", trcode, record_name)
        Kiwoom.log("GetRepeatCnt [TrCode]={} [RecordName]={}".format(trcode, record_name))
        Kiwoom.log("             [Return Value]={}".format(ret))
        return ret

    def _get_comm_dataEx(self, trcode, record_name):
        ret = self.dynamicCall("GetCommDataEx(QString, QString)", trcode, record_name)
        #        Kiwoom.log("GetCommData [TrCode]={} [RecordName]={} [Index]={} [ItemName]={}".
        #                   format(trcode, record_name, index, item_name))
        #        Kiwoom.log("            [Return Value]={}".format(ret.strip()))
        return ret

    # 조회과 잔고처리 -------------------------------------------------------------------------------------------------
    def send_order(self, rqname, screen_no, acc_no, order_type, code, quantity, price, hoga_gb, order_no):
        ret = self.dynamicCall("SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                               [rqname, screen_no, acc_no, order_type, code, quantity, price, hoga_gb, order_no])
        Kiwoom.log("SendOrder [RQName]={} [ScreenNo]={} [AccNo]={} [OrderType]={} [Code]={} [Qty]={} [Price]={} [HogaGb]={} [OrderNo]={}".
                   format(rqname, screen_no, acc_no, order_type, code, quantity, price, hoga_gb, order_no))

        if ret == 0:
            Kiwoom.log("SendOrder Success!!!!")
        else:
            Kiwoom.log("SendOrder ERROR={}".format(ret))

    def get_chejan_data(self, fid):
        ret = self.dynamicCall("GetChejanData(int)", fid)
        Kiwoom.log("GetChejanData [Fid:{}]={}".format(fid, ret))
        return ret


    # 기타함수 (종목정보관련 함수)-------------------------------------------------------------------------------------
    def get_code_list_by_market(self, market):
        list = self.dynamicCall("GetCodeListByMarket(QString)", market)
        Kiwoom.log("GetCodeListByMarket [{}]".format(market))
        list = list.split(';')
#        Kiwoom.log("                    [Return Value]={}".format(list[:-1]))
        return list[:-1]

    def get_master_code_name(self, code):
        rtn = self.dynamicCall("GetMasterCodeName(QString)", code)
        Kiwoom.log("GetMasterCodeName [Code:{}]={}".format(code, rtn))
        return rtn

    def get_master_listed_stock_cnt(self, code):
        rtn = self.dynamicCall("GetMasterListedStockCnt(QString)", code)
#        Kiwoom.log("GetMasterListedStockCnt [Code:{}]={}".format(code, rtn))
        return rtn

    def get_master_construction(self, code):
        rtn = self.dynamicCall("GetMasterConstruction(QString)", code)
#        Kiwoom.log("GetMasterConstruction [Code:{}]={}".format(code, rtn))
        return rtn

    def get_master_listed_stock_date(self, code):
        rtn = self.dynamicCall("GetMasterListedStockDate(QString)", code)
#        Kiwoom.log("GetMasterListedStockDate [Code:{}]={}".format(code, rtn))
        return rtn

    def get_master_last_price(self, code):
        rtn = self.dynamicCall("GetMasterLastPrice(QString)", code)
#        Kiwoom.log("GetMasterLastPrice [Code:{}]={}".format(code, rtn))
        return rtn

    def get_master_stock_state(self, code):
        rtn = self.dynamicCall("GetMasterStockState(QString)", code)
#        Kiwoom.log("GetMasterStockState [Code:{}]={}".format(code, rtn))
        return rtn


    # 기타함수 (특수 함수)---------------------------------------------------------------------------------------------
    def get_server_gubun(self):
        ret = self.dynamicCall("KOA_Functions(QString, QString)", "GetServerGubun", "")
        Kiwoom.log("KOA_Functions [GetServerGubun]={}".format(ret))
        return ret

    def get_upjong_code(self, market):
        ret = self.dynamicCall("KOA_Functions(QString, QString)", "GetUpjongCode", market)
        Kiwoom.log("KOA_Functions [Market]={}".format(market))
#        Kiwoom.log("KOA_Functions [Market]={} [GetUpjongCode]={}".format(market, ret))

        upjong_code = {'Code': [], 'Name': []}

        codes = ret.split('|')
        for code in codes[:-1]:
            code_list = code.split(',')
            upjong_code['Code'].append(code_list[1])
            upjong_code['Name'].append(code_list[2])

        return upjong_code


    # 이벤트 핸들러 ---------------------------------------------------------------------------------------------------
    def _set_signal_slots(self):
        self.OnEventConnect.connect(self._event_connect)
        self.OnReceiveTrData.connect(self._receive_tr_data)
        self.OnReceiveChejanData.connect(self._receive_chejan_data)

    def _event_connect(self, err_code):
        self.login_event_loop.exit()
        Kiwoom.log("Exit login event loop!!!!")

        if err_code == 0:
            Kiwoom.log("Kiwoom Connected!!!!")
        else:
            Kiwoom.log("Kiwoom Disconnected!!!!")

    def _receive_tr_data(self, screen_no, rqname, trcode, record_name, next, unused1, unused2, unused3, unused4):
        if next == '2':
            self.remained_data = True
        else:
            self.remained_data = False

        Kiwoom.log("OnReceiveTrData [ScrNo]={} [RQName]={} [TrCode]={} [RecordName]={} [PrevNext]={}".
                   format(screen_no, rqname, trcode, record_name, next))

        if rqname == "opt10001_req":
            self._opt10001(rqname, trcode, record_name)

        elif rqname == "opt10080_req":
            self._opt10080(rqname, trcode, record_name)

        elif rqname == "opt10081_req":
            self._opt10081(rqname, trcode, record_name)

        elif rqname == "opt20005_req":
            self._opt20005(rqname, trcode, record_name)

        elif rqname == "opt20006_req":
            self._opt20006(rqname, trcode, record_name)

        elif rqname == "opw00001_req":
            self._opw00001(rqname, trcode, record_name)

        elif rqname == "opw00018_req":
            self._opw00018(rqname, trcode, record_name)

        try:
            if rqname != "send_order_req":
                self.tr_event_loop.exit()
                Kiwoom.log("Exit tr event loop!!!!")

        except AttributeError:
            Kiwoom.log("OnReceiveTrData Exception Error={}".format(AttributeError))
            pass

    def _receive_chejan_data(self, gubun, item_cnt, fid_list):
        print(gubun)
        print(self.get_chejan_data(9203))
        print(self.get_chejan_data(302))
        print(self.get_chejan_data(900))
        print(self.get_chejan_data(901))
    #------------------------------------------------------------------------------------------------------------------

    # 주식기본정보요청
    def _opt10001(self, rqname, trcode, record_name):
        Kiwoom.log("opt10001 [RQName]={} [TrCode]={} [RecordName]={}".format(rqname, trcode, record_name))
        code = self._get_comm_data(trcode, record_name, 0, "종목코드")
        name = self._get_comm_data(trcode, record_name, 0, "종목명")
        settl_date = self._get_comm_data(trcode, record_name, 0, "결산월")
        face_value = self._get_comm_data(trcode, record_name, 0, "액면가")
        capital = self._get_comm_data(trcode, record_name, 0, "자본금")
        stock = self._get_comm_data(trcode, record_name, 0, "상장주식")
        credit_ratio = self._get_comm_data(trcode, record_name, 0, "신용비율")
        max_year = self._get_comm_data(trcode, record_name, 0, "연중최고")
        min_year = self._get_comm_data(trcode, record_name, 0, "연중최저")
        market_capital = self._get_comm_data(trcode, record_name, 0, "시가총액")
        market_capital_weight = self._get_comm_data(trcode, record_name, 0, "시가총액비중")
        foreign_exhaust_ratio = self._get_comm_data(trcode, record_name, 0, "외인소진률")
        substitute_price = self._get_comm_data(trcode, record_name, 0, "대용가")
        per = self._get_comm_data(trcode, record_name, 0, "PER")
        eps = self._get_comm_data(trcode, record_name, 0, "EPS")
        roe = self._get_comm_data(trcode, record_name, 0, "ROE")
        pbr = self._get_comm_data(trcode, record_name, 0, "PBR")
        ev = self._get_comm_data(trcode, record_name, 0, "EV")
        bps = self._get_comm_data(trcode, record_name, 0, "BPS")
        sale = self._get_comm_data(trcode, record_name, 0, "매출액")
        business_profit = self._get_comm_data(trcode, record_name, 0, "영업이익")
        net_profit = self._get_comm_data(trcode, record_name, 0, "당기순이익")
        max_250 = self._get_comm_data(trcode, record_name, 0, "250최고")
        min_250 = self._get_comm_data(trcode, record_name, 0, "250최저")
        open = self._get_comm_data(trcode, record_name, 0, "시가")
        high = self._get_comm_data(trcode, record_name, 0, "고가")
        low = self._get_comm_data(trcode, record_name, 0, "저가")
        upper_limit = self._get_comm_data(trcode, record_name, 0, "상한가")
        lower_limit = self._get_comm_data(trcode, record_name, 0, "하한가")
        base = self._get_comm_data(trcode, record_name, 0, "기준가")
        predict = self._get_comm_data(trcode, record_name, 0, "예상체결가")
        predict_num = self._get_comm_data(trcode, record_name, 0, "예상체결수량")
        max_date_250 = self._get_comm_data(trcode, record_name, 0, "250최고가일")
        max_ratio_250 = self._get_comm_data(trcode, record_name, 0, "250최고가대비율")
        min_date_250 = self._get_comm_data(trcode, record_name, 0, "250최저가일")
        min_ratio_250 = self._get_comm_data(trcode, record_name, 0, "250최저가대비율")
        current = self._get_comm_data(trcode, record_name, 0, "현재가")
        sign = self._get_comm_data(trcode, record_name, 0, "대비기호")
        before_ratio = self._get_comm_data(trcode, record_name, 0, "전일대비")
        fluctuate_ratio = self._get_comm_data(trcode, record_name, 0, "등락율")
        volume = self._get_comm_data(trcode, record_name, 0, "거래량")
        volume_ratio = self._get_comm_data(trcode, record_name, 0, "거래대비")
        face_value_unit = self._get_comm_data(trcode, record_name, 0, "액면가단위")
        capital_stock = self._get_comm_data(trcode, record_name, 0, "유통주식")
        capital_stock_ratio = self._get_comm_data(trcode, record_name, 0, "유통비율")

        self.opt10001_output = {"종목코드":[code], "종목명":[name], "결산월":[settl_date], "액면가":[face_value],
                                "자본금":[capital], "상장주식":[stock], "신용비율":[credit_ratio], "연중최고":[max_year],
                                "연중최저":[min_year], "시가총액":[market_capital], "시가총액비중":[market_capital_weight],
                                "외인소진률":[foreign_exhaust_ratio], "대용가":[substitute_price], "PER":[per], "EPS":[eps],
                                "ROE":[roe], "PBR":[pbr], "EV":[ev], "BPS":[bps], "매출액":[sale], "영업이익":[business_profit],
                                "당기순이익":[net_profit], "250최고":[max_250], "250최저":[min_250], "시가":[open],
                                "고가":[high], "저가":[low], "상한가":[upper_limit], "하한가":[lower_limit], "기준가":[base],
                                "예상체결가":[predict], "예상체결수량":[predict_num], "250최고가일":[max_date_250],
                                "250최고가대비율":[max_ratio_250], "250최저가일":[min_date_250],
                                "250최저가대비율":[min_ratio_250], "현재가":[current], "대비기호":[sign],
                                "전일대비":[before_ratio], "등락율":[fluctuate_ratio], "거래량":[volume],
                                "거래대비":[volume_ratio], "액면가단위":[face_value_unit], "유통주식":[capital_stock],
                                "유통비율":[capital_stock_ratio]}

        Kiwoom.log("opt10001 [종목코드]={} [종목명]={} ....".format(code, name))

    # 주식분봉차트 조회요청
    def _opt10080(self, rqname, trcode, record_name):
        Kiwoom.log("opt10080 [RQName]={} [TrCode]={} [RecordName]={}".format(rqname, trcode, record_name))

        self.opt10080_output = {'Date':[], 'Open':[], 'High':[], 'Low':[], 'Close':[], 'Volume':[], 'Gubun':[]}
        opt10080_Ex_column = ['현재가', '거래량', '체결시간', '시가', '고가', '저가', '수정주가구분', '수정비율',
                              '대업종구분', '소업종구분', '종목정보', '수정주가이벤트', '전일종가']

        datas = self._get_comm_dataEx(trcode, record_name)
        datas = DataFrame(datas, columns=opt10080_Ex_column)

        if datas.empty:
            Kiwoom.log("opt10080 [종목코드]=NO DATA [데이터수]=NO DATA [데이터 종료일]=NO DATA")
        else:
            self.opt10080_output['Date'] = list(datas['체결시간'])
            self.opt10080_output['Open'] = list(datas['시가'].astype('int'))
            self.opt10080_output['High'] = list(datas['고가'].astype('int'))
            self.opt10080_output['Low'] = list(datas['저가'].astype('int'))
            self.opt10080_output['Close'] = list(datas['현재가'].astype('int'))
            self.opt10080_output['Volume'] = list(datas['거래량'].astype('int'))
            self.opt10080_output['Gubun'] = list(datas['수정주가구분'])

            code = self._get_comm_data(trcode, record_name, 0, "종목코드")
            Kiwoom.log("opt10080 [종목코드]={} [데이터수]={} [데이터 종료일]={}".format(code, datas['체결시간'].count(), datas['체결시간'].min()))

    # 주식일봉차트 조회요청
    def _opt10081(self, rqname, trcode, record_name):
        Kiwoom.log("opt10081 [RQName]={} [TrCode]={} [RecordName]={}".format(rqname, trcode, record_name))

        self.opt10081_output = {'Date':[], 'Open':[], 'High':[], 'Low':[], 'Close':[], 'Volume':[], 'Gubun':[]}
        opt10081_Ex_column = ['종목코드', '현재가', '거래량', '거래대금', '일자', '시가', '고가', '저가',
                              '수정주가구분', '수정비율', '대업종구분', '소업종구분', '종목정보', '수정주가이벤트',
                              '전일종가']

        datas = self._get_comm_dataEx(trcode, record_name)
        datas = DataFrame(datas, columns=opt10081_Ex_column)

        if datas.empty:
            Kiwoom.log("opt10081 [종목코드]=NO DATA [데이터수]=NO DATA [데이터 종료일]=NO DATA")
        else:
            self.opt10081_output['Date'] = list(datas['일자'])
            self.opt10081_output['Open'] = list(datas['시가'].astype('int'))
            self.opt10081_output['High'] = list(datas['고가'].astype('int'))
            self.opt10081_output['Low'] = list(datas['저가'].astype('int'))
            self.opt10081_output['Close'] = list(datas['현재가'].astype('int'))
            self.opt10081_output['Volume'] = list(datas['거래량'].astype('int'))
            self.opt10081_output['Gubun'] = list(datas['수정주가구분'])

            code = self._get_comm_data(trcode, record_name, 0, "종목코드")
            Kiwoom.log("opt10081 [종목코드]={} [데이터수]={} [데이터 종료일]={}".format(code, datas['일자'].count(), datas['일자'].min()))

    # 업종분봉차트 조회요청
    def _opt20005(self, rqname, trcode, record_name):
        Kiwoom.log("opt20005 [RQName]={} [TrCode]={} [RecordName]={}".format(rqname, trcode, record_name))

        self.opt20005_output = {'Date':[], 'Open':[], 'High':[], 'Low':[], 'Close':[], 'Volume':[]}
        opt20005_Ex_column = ['현재가', '거래량', '체결시간', '시가', '고가', '저가', '대업종구분', '소업종구분',
                              '종목정보', '전일종가']

        datas = self._get_comm_dataEx(trcode, record_name)
        datas = DataFrame(datas, columns=opt20005_Ex_column)

        if datas.empty:
            Kiwoom.log("opt20005 [업종코드]=NO DATA [데이터수]=NO DATA [데이터 종료일]=NO DATA")
        else:
            self.opt20005_output['Date'] = list(datas['체결시간'])
            self.opt20005_output['Open'] = list(datas['시가'].astype('int'))
            self.opt20005_output['High'] = list(datas['고가'].astype('int'))
            self.opt20005_output['Low'] = list(datas['저가'].astype('int'))
            self.opt20005_output['Close'] = list(datas['현재가'].astype('int'))
            self.opt20005_output['Volume'] = list(datas['거래량'].astype('int'))

            code = self._get_comm_data(trcode, record_name, 0, "업종코드")
            Kiwoom.log("opt20005 [업종코드]={} [데이터수]={} [데이터 종료일]={}".format(code, datas['체결시간'].count(), datas['체결시간'].min()))

    # 업종일봉차트 조회요청
    def _opt20006(self, rqname, trcode, record_name):
        Kiwoom.log("opt20006 [RQName]={} [TrCode]={} [RecordName]={}".format(rqname, trcode, record_name))

        self.opt20006_output = {'Date':[], 'Open':[], 'High':[], 'Low':[], 'Close':[], 'Volume':[], 'Value':[]}
        opt20006_Ex_column = ['현재가', '거래량', '일자', '시가', '고가', '저가', '거래대금',
                              '대업종구분', '소업종구분', '종목정보', '전일종가']

        datas = self._get_comm_dataEx(trcode, record_name)
        datas = DataFrame(datas, columns=opt20006_Ex_column)

        if datas.empty:
            Kiwoom.log("opt20006 [업종코드]=NO DATA [데이터수]=NO DATA [데이터 종료일]=NO DATA")
        else:
            self.opt20006_output['Date'] = list(datas['일자'])
            self.opt20006_output['Open'] = list(datas['시가'].astype('int'))
            self.opt20006_output['High'] = list(datas['고가'].astype('int'))
            self.opt20006_output['Low'] = list(datas['저가'].astype('int'))
            self.opt20006_output['Close'] = list(datas['현재가'].astype('int'))
            self.opt20006_output['Volume'] = list(datas['거래량'].astype('int'))
            self.opt20006_output['Value'] = list(datas['거래대금'].astype('int'))

            code = self._get_comm_data(trcode, record_name, 0, "업종코드")
            Kiwoom.log("opt20006 [업종코드]={} [데이터수]={} [데이터 종료일]={}".format(code, datas['일자'].count(), datas['일자'].min()))

    # 예수금 상세현황 요청
    def _opw00001(self, rqname, trcode, record_name):
        d2_deposit = self._get_comm_data(trcode, record_name, 0, "d+2추정예수금")
        self.d2_deposit = Kiwoom.num_format(d2_deposit)

    # 계좌 평가잔고내역 저장용 인스턴스 변수 생성
    def reset_opw00018_output(self):
        self.opw00018_output = {'single': [], 'multi': []}

    # 계좌 평가잔고내역 요청
    def _opw00018(self, rqname, trcode, record_name):
        # single data
        total_purchase_price = self._get_comm_data(trcode, record_name, 0, "총매입금액")
        total_eval_price = self._get_comm_data(trcode, record_name, 0, "총평가금액")
        total_eval_profit_loss_price = self._get_comm_data(trcode, record_name, 0, "총평가손익금액")
        total_earning_rate = self._get_comm_data(trcode, record_name, 0, "총수익률(%)")
        estimated_deposit = self._get_comm_data(trcode, record_name, 0, "추정예탁자산")

        self.opw00018_output['single'].append(Kiwoom.num_format(total_purchase_price))
        self.opw00018_output['single'].append(Kiwoom.num_format(total_eval_price))
        self.opw00018_output['single'].append(Kiwoom.num_format(total_eval_profit_loss_price))

        if float(total_earning_rate) > 0.0:
            if self.get_server_gubun():
                total_earning_rate = float(total_earning_rate) / 100
                total_earning_rate = str(total_earning_rate)
        total_earning_rate = Kiwoom.per_format2(total_earning_rate)

        self.opw00018_output['single'].append(total_earning_rate)
        self.opw00018_output['single'].append(Kiwoom.num_format(estimated_deposit))

        # multi data
        rows = self._get_repeat_cnt(trcode, rqname)
        for i in range(rows):
            name = self._get_comm_data(trcode, record_name, i, "종목명")
            quantity = self._get_comm_data(trcode, record_name, i, "보유수량")
            purchase_price = self._get_comm_data(trcode, record_name, i, "매입가")
            current_price = self._get_comm_data(trcode, record_name, i, "현재가")
            eval_profit_loss_price = self._get_comm_data(trcode, record_name, i, "평가손익")
            earning_rate = self._get_comm_data(trcode, record_name, i, "수익률(%)")

            quantity = Kiwoom.num_format(quantity)
            purchase_price = Kiwoom.num_format(purchase_price)
            current_price = Kiwoom.num_format(current_price)
            eval_profit_loss_price = Kiwoom.num_format(eval_profit_loss_price)
            earning_rate = Kiwoom.per_format2(earning_rate)

            self.opw00018_output['multi'].append([name, quantity, purchase_price, current_price,
                                                  eval_profit_loss_price, earning_rate])