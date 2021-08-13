import sys
import numpy as np
import pandas as pd
import sqlite3

# tensorflow와 tf.keras를 임포트합니다
from tensorflow import keras

#TRAIN_START_DATE = '20150615'               # Training Data 중 Y값의 시작일(상하한 30% 조정일)
TRAIN_START_DATE = '19850104'               # Training Data 중 Y값의 시작일(증권전산화일)
#TRAIN_START_DATE = '20080101'               # 10년치 데이터 학습
MIN_TRAIN_DAY = 240                         # 최소 학습데이터가 필요일수(240=1년)
TEST_PERIOD = 20                            # 테스트 기간
SEQ_LENGTH = 20                             # 현재까지 DATA_DIM = 5, SEQ_LENGTH는 20이 적당함

class StockDataset:
    def __init__(self, predict_start_date, predict_end_date=None, seq_length=SEQ_LENGTH):
        self.kospi_db = sqlite3.connect("./datas/kospi.db")
        self.kosdaq_db = sqlite3.connect("./datas/kosdaq.db")
        self.kospi_cur = self.kospi_db.cursor()
        self.kosdaq_cur = self.kosdaq_db.cursor()
        self.price_set = None
        self.result_set = None
        self.start_iloc = None
        self.end_iloc = None
        self.last_iloc = None

        self.data_dim = None
        self.seq_len = seq_length
        self.start_date = predict_start_date
        self.end_date = predict_end_date

    def convert_rate_X(self, np_datas):
        np_x = np.array([[1.0 for i in range(len(np_datas[0]))]])                 # 첫번째 데이터는 증감이 1임
        for i in range(1, len(np_datas)):
            a = np_datas[i]
            b = np_datas[i-1]
            rate = a/b
            np_x = np.append(np_x, [rate], axis=0)
        return np_x

    def convert_rate_Y(self, np_datas):
        np_y = np.array([1])                                                    # 첫번째 데이터는 증감이 1임
        for i in range(1, len(np_datas)) :
            rate = np_datas[i]/np_datas[i-1]
            np_y = np.append(np_y, [rate], axis=0)
        return np_y

    def convert_scale_X(self, np_datas):
        np_x = self.convert_rate_X(np_datas)
#        np_x = np_datas.astype(float)

        # Normalization(Min Max Scale)
        for i in range(len(np_x[0])):
            min = np.min(np_x[:,i])
            max = np.max(np_x[:,i])
            np_x[:,i] = (np_x[:,i] - min) / (max - min)
        return np_x

    def convert_scale_Y(self, np_datas):
        np_y = self.convert_rate_Y(np_datas)
#        np_y = np_datas.astype(float)

        # Normalization(Min Max Scale)
        min = np.min(np_y)
        max = np.max(np_y)
        np_y = (np_y - min) / (max - min)
        return np_y

    def convert_binary_X(self, np_datas):
        np_x = np.array([[1 for i in range(len(np_datas[0]))]])                 # 첫번째 데이터는 증감이 1임
        for i in range(1, len(np_datas)):
            a = np_datas[i]
            b = np_datas[i-1]
            rate = a/b
            binary = np.array([])
            for j in range(len(rate)):
                if rate[j] > 1:
                    binary = np.append(binary, [1], axis=0)
                else:
                    binary = np.append(binary, [0], axis=0)
            np_x = np.append(np_x, [binary], axis=0)
        return np_x.astype(int)

    def convert_binary_Y(self, np_datas):
        np_y = np.array([1])  # 첫번째 데이터는 증감이 1임
        for i in range(1, len(np_datas)):
            rate = np_datas[i] / np_datas[i - 1]
            if rate > 1:
                np_y = np.append(np_y, [1], axis=0)
            else:
                np_y = np.append(np_y, [0], axis=0)
        return np_y

    def make_dataset(self, df_datas, df_label):
        df_x = df_datas[:-2]                        # 예측일 수 만큼 데이터셋에서 제외
        np_x = df_x.values

        # 전일대비 증감으로 데이터 변환
#        np_x = self.convert_rate_X(np_x)
#        np_x = self.convert_scale_X(np_x)
        np_x = self.convert_binary_X(np_x)
        np_x = np_x[1:]                             # 첫날의 전일대비 데이터는 무조건 1이므로 제외

        # squence_size 단위로 데이터 생성
        train_x = []
        for i in range(len(np_x)-(self.seq_len-1)) :
            train_x.append(np_x[i:i+self.seq_len])

        train_x = np.array(train_x)                 # 학습용 X 데이터 완성

        np_y = df_label.values

        #전일대비 증감으로 데이터 변환
#        np_y = self.convert_rate_Y(np_y)
#        np_y = self.convert_scale_Y(np_y)
        np_y = self.convert_binary_Y(np_y)
        np_y = np_y[1:]

        train_y1 = np_y[self.seq_len:-1]
        train_y2 = np_y[self.seq_len+1:]

        return (train_x, train_y1, train_y2)


    def load_train_data(self, code, market_type, chart_type):
        if chart_type == 'DAY':
            table_name = 'D' + code
        else:
            table_name = 'S' + code

        if market_type == 'KO':
            chart_df = pd.read_sql("SELECT * from " + table_name + " WHERE Volume != 0 ORDER BY Date", con=self.kospi_db)
        else:
            chart_df = pd.read_sql("SELECT * from " + table_name + " WHERE Volume != 0 ORDER BY Date", con=self.kosdaq_db)

        chart_df = chart_df.dropna(axis=0)                  # None 데이터가 있는 필드 제거
        chart_df = chart_df.set_index('Date')

        # 차트데이터 검증
        open_index = chart_df.index[0]
        last_index = chart_df.index[-1]

        # Predict 시작일 검증
        if open_index > self.start_date or last_index < self.start_date:
            print("주가예측 요청일({})일이 종목상장일({}) 보다 작거나, 저장된 마지막 주식데이터일({}) 보다 큽니다.".format(self.start_date, open_index, last_index))
            return (None, None, None), (None, None, None)

        # 종목상장일이 등락폭를 30%로 조정한 날짜(2015.06.15) 이전이면 등록폭 조정일을 기준으로 학습시작
        if open_index < TRAIN_START_DATE:
            temp = chart_df.loc[TRAIN_START_DATE:]
            start_date = temp.index[0]
            self.start_iloc = chart_df.index.get_loc(start_date)
        else:
            self.start_iloc = chart_df.index.get_loc(open_index)

        # Predict 데이터의 시작 위치
        try:
            predict_iloc = chart_df.index.get_loc(self.start_date)
        except KeyError:
            print("주가예측 요청일({})에 해당하는 주가 데이터가 없습니다.".format(self.start_date))
            return (None, None, None), (None, None, None)

        # Predict 데이터의 위치가 최소 트레이닝 데이터 보다 커야함
        if (predict_iloc - self.start_iloc) < MIN_TRAIN_DAY:
            print("최소 학습일수({}) 보다 주식데이터가 작습니다.".format(MIN_TRAIN_DAY))
            return (None, None, None), (None, None, None)

        # Predict 종료일 검증
        if self.end_date is not None:
            if self.start_date > self.end_date or last_index < self.end_date:
                print("주가 변동률 예측 종료일({})이 시작일({}) 보다 작거나, 저장된 마지막 주식데이터일({}) 보다 큽니다.".format(self.end_date, self.start_date, last_index))
                return (None, None, None), (None, None, None)

            try:
                self.end_iloc = chart_df.index.get_loc(self.end_date)
            except KeyError:
                print("주가 예측 종료일({})에 해당하는 주가 데이터가 없습니다.".format(self.end_date))
                return (None, None, None), (None, None, None)

            # 머신러닝을 위한 샘플 데이터 생성
            sample_df = chart_df.iloc[self.start_iloc:self.end_iloc+1]
        else:
            # 머신러닝을 위한 샘플 데이터 생성
            sample_df = chart_df.iloc[self.start_iloc:]

#        self.price_set = sample_df[['Open', 'Low', 'High', 'Close']]
#        self.price_set = sample_df[['Open', 'Close', 'Volume']]
        self.price_set = sample_df[['Open', 'Low', 'High', 'Close', 'Volume']]               # 현재까지 수익률이 가장 높음
#        self.price_set = sample_df[['Close', 'Volume']]

        self.data_dim = len(list(self.price_set.columns))

        # 머신러닝을 위한 결과 데이터 생성
        if self.end_date is not None:
            self.result_set = chart_df['Close'].iloc[self.start_iloc:self.end_iloc+1]
            self.last_iloc = self.price_set.index.get_loc(self.end_date)
        else:
            self.result_set = chart_df['Close'].iloc[self.start_iloc:]
            self.last_iloc = self.price_set.index.get_loc(last_index)

        # Predict 데이터 시작 위치
        predict_iloc = self.price_set.index.get_loc(self.start_date)
        
        # Training 데이터 종료 위치(Test 데이터 확보)
        self.end_iloc = predict_iloc - TEST_PERIOD

        # Training 데이터셋 생성
        training_datas = self.price_set.iloc[:self.end_iloc]
        training_label = self.result_set.iloc[:self.end_iloc]

        (train_x, train_y1, train_y2) = self.make_dataset(training_datas, training_label)

        # Test 데이터 생성(전일대비 증감을 계산하기 위해 1일과 D+1일을 감안한 1일, 총 2일이 필요)
        self.start_iloc = self.end_iloc - (self.seq_len + 2)
        self.end_iloc = predict_iloc

        test_datas = self.price_set.iloc[self.start_iloc:self.end_iloc]
        test_label = self.result_set.iloc[self.start_iloc:self.end_iloc]
        (test_x, test_y1, test_y2) = self.make_dataset(test_datas, test_label)

        return (train_x, train_y1, train_y2), (test_x, test_y1, test_y2)


    def load_train_group_data(self, codes, market_type, chart_type):
        first = True

        for code in codes:
            (train_x, train_y1, train_y2), (test_x, test_y1, test_y2) = self.load_train_data(code, market_type, chart_type)

            if train_x is None:
                continue

            if first:
                train_datas = train_x
                train_label1 = train_y1
                train_label2 = train_y2

                test_datas = test_x
                test_label1 = test_y1
                test_label2 = test_y2

                first = False
            else:
                train_datas = np.append(train_datas, train_x, axis=0)
                train_label1 = np.append(train_label1, train_y1)
                train_label2 = np.append(train_label2, train_y2)

                test_datas = np.append(test_datas, test_x, axis=0)
                test_label1 = np.append(test_label1, test_y1)
                test_label2 = np.append(test_label2, test_y2)

        return (train_datas, train_label1, train_label2), (test_datas, test_label1, test_label2)


    def load_next_step_data(self):
        # 모든 데이터를 읽었으면 None를 리턴함
        if self.end_iloc > self.last_iloc:
            return (None, None, None, None, None, None, None)

        # Test 데이터 생성(전일대비 증감을 계산하기 위해 1일과 D+1일을 감안한 1일, 총 2일이 필요)
        self.start_iloc = self.end_iloc - (self.seq_len + 2)
        self.end_iloc += 1

        test_datas = self.price_set.iloc[self.start_iloc:self.end_iloc]
        test_label = self.result_set.iloc[self.start_iloc:self.end_iloc]
        date1 = test_label.index[-2]                # D+1 날짜
        close1 = test_label.loc[date1]
        date2 = test_label.index[-1]                # D+2 날짜
        close2 = test_label.loc[date2]
        (test_x, test_y1, test_y2) = self.make_dataset(test_datas, test_label)

        return (test_x, test_y1, test_y2, date1, close1, date2, close2)
