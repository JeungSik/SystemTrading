from __future__ import absolute_import, division, print_function, unicode_literals, unicode_literals

# tensorflow와 tf.keras를 임포트합니다
import tensorflow as tf
from tensorflow import keras

# 헬퍼(helper) 라이브러리를 임포트합니다
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import sqlite3
import datetime

from sklearn.preprocessing import MinMaxScaler

import sys

print("Tensorflow Version = {}".format(tf.__version__))

TRAIN_Y_DATE = '20150615'                   # Training Data 중 Y값의 시작일
PREDICT_Y_DATE = '20190102'                 # Predict Data 중 Y값의 시작일
MIN_TRAIN_DAY = 240                         # 최소 학습데이터가 필요일수(240=1년)
TEST_PERIOD = 20                            # 테스트 기간

class stock_market_dataset:
    def __init__(self):
        self.kospi_analyze_db = sqlite3.connect("./datas/kospi_analyze.db")
        self.kosdaq_analyze_db = sqlite3.connect("./datas/kosdaq_analyze.db")
        self.kospi_analyze_cur = self.kospi_analyze_db.cursor()
        self.kosdaq_analyze_cur = self.kosdaq_analyze_db.cursor()
        self.price_set = None
        self.result_set = None
        self.start_iloc = None
        self.end_iloc = None
        self.last_iloc = None
        self.seq_len = None

    def convert_X(self, np_datas):
        np_x = [[1.0 for i in range(len(np_datas[0]))]]                 # 첫번째 데이터는 증감이 1임
        for i in range(1, len(np_datas)) :
            a = np_datas[i]
            b = np_datas[i-1]
            rate = a/b
            np_x.append(rate)
        return np.array(np_x)

    def convert_Y(self, np_datas):
        np_y = [1.0]                                                    # 첫번째 데이터는 증감이 1임
        for i in range(1, len(np_datas)) :
            rate = np_datas[i]/np_datas[i-1]
            np_y.append(rate)
        return np.array(np_y)

    def make_dataset(self, df_datas, df_label):
        df_x = df_datas[:-2]          # 예측일 수 만큼 데이터셋에서 제외
        np_x = df_x.values

        # 전일대비 증감으로 데이터 변환
        np_x = self.convert_X(np_x)
        np_x = np_x[1:]                             # 첫날의 전일대비 데이터는 무조건 1이므로 제외

        # Normalization
#        np_x = (np_x - np_x.min()) / (np_x.max() - np_x.min())

        # squence_size 단위로 데이터 생성
        train_x = []
        for i in range(len(np_x)-(self.seq_len-1)) :
            train_x.append(np_x[i:i+self.seq_len])

        train_x = np.array(train_x)                 # 학습용 X 데이터 완성

        np_y = df_label.values
        #전일대비 증감으로 데이터 변환
        np_y = self.convert_Y(np_y)
        np_y = np_y[1:]

        # Normalization
#        np_y = (np_y - np_y.min()) / (np_y.max() - np_y.min())

        train_y1 = np_y[self.seq_len:-1]
        train_y2 = np_y[self.seq_len+1:]

        return (train_x, train_y1, train_y2)

    def load_train_data(self, code, type, chart_type, squence_size):
        self.seq_len = squence_size

        if chart_type == 'DAY':
            table_name = 'MA_D' + code
        else:
            table_name = 'MA_S' + code

        if type == 'KO':
            chart_df = pd.read_sql("SELECT * from " + table_name + " WHERE Volume != 0", con=self.kospi_analyze_db)
        else:
            chart_df = pd.read_sql("SELECT * from " + table_name + " WHERE Volume != 0", con=self.kosdaq_analyze_db)

        chart_df = chart_df.dropna(axis=0)
        chart_df = chart_df.set_index('Date')

        # 차트데이터 검증
        open_index = chart_df.index[0]
        last_index = chart_df.index[-1]

        # Predict 요청일 검증
        if open_index > PREDICT_Y_DATE or last_index < PREDICT_Y_DATE:
            print("주가예측 요청일("+PREDICT_Y_DATE+")일이 종목상장일("+open_index+") 보다 작거나, 저장된 마지막 주식데이터일("+last_index+") 보다 큽니다.")
            return (None, None, None), (None, None, None), (None, None, None)

        # Predict 데이터의 시작 위치
        predict_iloc = chart_df.index.get_loc(PREDICT_Y_DATE)

        # Predict 데이터의 위치가 최소 트레이닝 데이터 보다 커야함
        if predict_iloc is None or predict_iloc < MIN_TRAIN_DAY:
            print("주가예측 요청일("+PREDICT_Y_DATE+")에 데이터가 없거나, 최소 학습 요청일수("+MIN_TRAIN_DAY+") 보다 주식데이터가 작습니다.")
            return (None, None, None), (None, None, None), (None, None, None)

        # 종목상장일이 등락폭를 30%로 조정한 날짜(2015.06.15) 이전이면 등록폭 조정일을 기준으로 학습시작
        if open_index < TRAIN_Y_DATE:
            self.start_iloc = chart_df.index.get_loc(TRAIN_Y_DATE)
        else:
            self.start_iloc = chart_df.index.get_loc(open_index)

        # 머신러닝을 위한 샘플 데이터 생성
        sample_df = chart_df.iloc[self.start_iloc:]

#        price_set = sample_df[['Open', 'Low', 'High', 'Close']]
#        price_set = sample_df[['Close', 'MA5', 'MA10', 'MA20', 'MA60', 'MA120']]
#        price_set = sample_df[['Open', 'Low', 'High', 'Close', 'MA5', 'MA10', 'MA20', 'MA60', 'MA120']]
        self.price_set = sample_df[['Open', 'Low', 'High', 'Close', 'Volume']]               # 현재까지 수익률이 가장 높음
#        price_set = sample_df[['Close', 'Volume']]
#        self.price_set = sample_df[['Volume', 'VMA5', 'VMA10', 'VMA20', 'VMA60', 'VMA120']]  # 이 데이터도 어느정도 높음
#        price_set = sample_df[['Close', 'MA5', 'MA10', 'MA20', 'MA60', 'MA120', 'Volume', 'VMA5', 'VMA10', 'VMA20', 'VMA60', 'VMA120']]
#        price_set = sample_df[['Close', 'MA20', 'Volume', 'VMA20']]

        # 머신러닝을 위한 결과 데이터 생성
        self.result_set = chart_df['Close'].iloc[self.start_iloc:]

        # 머신러닝을 위한 샘플 데이터의 마지막 위치
        self.last_iloc = self.price_set.index.get_loc(last_index)

        # Predict 데이터 시작 위치 
        predict_iloc = self.price_set.index.get_loc(PREDICT_Y_DATE)
        
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


    def load_train_group_data(self, codes, type, chart_type, squence_size):
        first = True

        for code in codes:
            (train_x, train_y1, train_y2), (test_x, test_y1, test_y2) = self.load_train_data(code, type, chart_type, squence_size)

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


    def get_remain_len(self):
        return len(self.result_set.iloc[self.end_iloc:])


    def load_next_step_data(self):
        # 모든 데이터를 읽었으면 None를 리턴함
        if self.end_iloc >= self.last_iloc:
            return (None, None, None)

        # Test 데이터 생성(전일대비 증감을 계산하기 위해 1일과 D+1일을 감안한 1일, 총 2일이 필요)
        self.start_iloc = self.end_iloc - (self.seq_len + 2)
        self.end_iloc += 1

        test_datas = self.price_set.iloc[self.start_iloc:self.end_iloc]
        test_label = self.result_set.iloc[self.start_iloc:self.end_iloc]
        (test_x, test_y1, test_y2) = self.make_dataset(test_datas, test_label)

        return (test_x, test_y1, test_y2)


#-----------------------------------------------------------------------------------------------------------------------
DATA_DIM = 5
SEQ_LENGTH = 20             # 현재까지 DATA_DIM = 5, SEQ_LENGTH는 20이 적당함
OUTPUT_DIM = 1
EPOCH = 2
BATCH_SIZE = 1

def create_model():
    model = keras.models.Sequential([
                keras.layers.LSTM(units=64, batch_input_shape=(1, SEQ_LENGTH, DATA_DIM), return_sequences=True, kernel_initializer='he_uniform'),
                keras.layers.Dropout(0.2),
                #    keras.layers.LSTM(units=64, return_sequences=True),
                #    keras.layers.Dropout(0.2),
                keras.layers.LSTM(units=256, return_sequences=False, kernel_initializer='he_uniform'),
                #    keras.layers.Dropout(0.2),
                #   keras.layers.Dense(64, activation='softmax', kernel_initializer='he_uniform'),
                keras.layers.Dense(OUTPUT_DIM, activation='relu', kernel_initializer='he_uniform')
            ])
    # model.set_weights(model.get_weights())
    # model.compile(optimizer='rmsprop', loss='mse')
    model.compile(optimizer='adam', loss='mse')
    return model
#-----------------------------------------------------------------------------------------------------------------------

# 시가총액 TOP50
LISTS = ['005930', '000660', '005935', '005380', '035420', '012330', '051910', '068270', '055550', '051900',
         '017670', '005490', '207940', '000270', '105560', '006400', '028260', '015760', '096770', '018260']
#         '032830', '034730', '033780', '003550', '036570', '010950', '035720', '000810', '086790', '066570',
#         '009540', '011170', '316140', '251270', '010130', '090430', '024110', '009150', '030200', '035250',
#         '018880', '021240', '032640', '086280', '267250', '002790', '004020', '034220', '010140', '000720']
CODE = '051910'
logs = []
stock_market = stock_market_dataset()

# LISTS의 종목전체 트레이닝 및 CODE 종목 예측
#(train_x, train_y1, train_y2), (test_x_t, test_y1_t, test_y2_t) = stock_market.load_train_group_data(LISTS, 'KO', 'DAY', SEQ_LENGTH)
#(train_x_t, train_y1_t, train_y2_t), (test_x, test_y1, test_y2) = stock_market.load_train_data(CODE, 'KO', 'DAY', SEQ_LENGTH)

# CODE 종목 트레이닝 및 예측
(train_x, train_y1, train_y2), (test_x, test_y1, test_y2) = stock_market.load_train_data(CODE, 'KO', 'DAY', SEQ_LENGTH)

if train_x is None:
    print("[{}]는 학습할 데이터가 없습니다.".format(code))
    sys.exit(1)

model = keras.models.load_model('./models/rnn_top20_190102.h5')

scale_label = test_y2.reshape(-1,1)
min_max_scaler = MinMaxScaler(feature_range=(scale_label.min(), scale_label.max()))

scale_min = scale_label.min()
scale_max = scale_label.max()

# Test 데이터로 학습한 결과로 예측
predicts = model.predict(test_x)
#predicts = min_max_scaler.fit_transform(predicts)
predicts = (predicts - scale_min) / (scale_max - scale_min)


predict_sum = 0
real_sum = 0
y_bias = (predicts - scale_label).mean()
#y_bias = 0

for i in range(len(scale_label)):
    predict_y, real_y = predicts[i], scale_label[i]

    # 전일대비 증감이므로 1보다 작은 하락, 1보다 크면 상승임
    if (predict_y - y_bias) > 1:
        predict_sum += ((predict_y - y_bias) - 1)       # 상승률을 누적시킴
        real_sum += (real_y - 1)                        # 정답의 상승률을 누적시킴
        print("[index={}] : {} ({})".format(i, ((predict_y - y_bias) - 1), (real_y - 1)))

print("------------------------------------------------------------")
print("[{}][TEST 전체 수익] : {} ({})".format(CODE, predict_sum, real_sum))
print("------------------------------------------------------------")
logs.append("------------------------------------------------------------")
logs.append("[{}][TEST 전체 수익] : {} ({})".format(CODE, predict_sum, real_sum))
logs.append("------------------------------------------------------------")

model.fit(test_x, test_y2, batch_size=BATCH_SIZE, epochs=EPOCH)

month = 0
total_predict_sum = 0
total_real_sum = 0

while(True):
    predict_sum = 0
    real_sum = 0
    predict_ys = []
    real_ys = []

    month += 1

    for i in range(TEST_PERIOD):
        (test_x, test_y1, test_y2) = stock_market.load_next_step_data()

        if test_x is not None:
            predict_y = model.predict(test_x)

            real_ys = np.append(real_ys, test_y2)
            predict_ys = np.append(predict_ys, predict_y)

            scale_y = (predict_y - scale_min) / (scale_max - scale_min)

            if (scale_y - y_bias) > 1:
                predict_sum += ((scale_y - y_bias) - 1)     # 상승률을 누적시킴
                real_sum += (test_y2 - 1)                    # 정답의 상승률을 누적시킴
                print("[index={}] : {} ({})".format(i, ((scale_y - y_bias) - 1), (test_y2 - 1)))

            model.fit(test_x, test_y2, batch_size=BATCH_SIZE, epochs=EPOCH)
        else:
            break

    print("------------------------------------------------------------")
    print("[{}차][PREDICT 전체 수익] : {} ({})".format(month, predict_sum, real_sum))
    print("------------------------------------------------------------")
    logs.append("[{}차][PREDICT 전체 수익] : {} ({})".format(month, predict_sum, real_sum))

    total_predict_sum += predict_sum
    total_real_sum += real_sum

    scale_min = real_ys.min()
    scale_max = real_ys.max()
    y_bias = (predict_ys - real_ys).mean()

    if test_x is None:
        logs.append("------------------------------------------------------------")
        logs.append("[TOTAL 수익] : {} ({})".format(total_predict_sum, total_real_sum))
        logs.append("------------------------------------------------------------")

        for log in logs:
            print(log)
        sys.exit(1)

