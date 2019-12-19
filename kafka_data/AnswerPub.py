from confluent_kafka import Producer, KafkaException
import sys
import traceback


# 用來接收error訊息
def error_cb(err):
    print('Error: %s' % err)


# 基本參數
KAFKA_BROKER_URL = '18.217.149.82:9092'   # 設定要連接的Kafka群
WORKSHOP_ID = 'db104'                       # 修改成你/妳的班級編號
STUDENT_ID = int('30')                            # 修改成你/妳的學生編號

# 主程式進入點
if __name__ == '__main__':
    # 步驟1. 設定要連線到Kafka集群的相關設定
    props = {
        'bootstrap.servers': KAFKA_BROKER_URL,  # Kafka集群在那裡? (置換成要連接的Kafka集群)
        'error_cb': error_cb                    # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)

    # 步驟3. 指定想要發佈訊息的topic名稱
    topicName = WORKSHOP_ID.lower() + ".test"  # 答案繳交的Topic
    try:
        STUDENT_ID = str(STUDENT_ID)
        print('Start sending messages...')
        # 步驟4.產生要發佈到Kafka的訊息
        #      - 參數#1: topicName, 參數#2: msgValue, 參數#3: msgKey
        producer.produce(topicName, '1', STUDENT_ID+'|01')  # 第 1題   4
        producer.produce(topicName, '3', STUDENT_ID+'|02')  # 第 2題
        producer.produce(topicName, '4', STUDENT_ID+'|03')  # 第 3題
        producer.produce(topicName, '3', STUDENT_ID+'|04')  # 第 4題
        producer.produce(topicName, '3', STUDENT_ID+'|05')  # 第 5題   1

        producer.produce(topicName, '4', STUDENT_ID+'|06')  # 第 6題
        producer.produce(topicName, '2', STUDENT_ID+'|07')  # 第 7題
        producer.produce(topicName, '1', STUDENT_ID+'|08')  # 第 8題
        producer.produce(topicName, '3', STUDENT_ID+'|09')  # 第 9題
        producer.produce(topicName, '2', STUDENT_ID+'|10')  # 第10題

        producer.produce(topicName, '2', STUDENT_ID+'|11')  # 第11題
        producer.produce(topicName, '3', STUDENT_ID+'|12')  # 第12題
        producer.produce(topicName, '5', STUDENT_ID+'|13')  # 第13題
        producer.produce(topicName, '1', STUDENT_ID+'|14')  # 第14題
        producer.produce(topicName, '4', STUDENT_ID+'|15')  # 第15題

        producer.produce(topicName, '3', STUDENT_ID+'|16')  # 第16題   4
        producer.produce(topicName, '1', STUDENT_ID+'|17')  # 第17題
        producer.produce(topicName, '2', STUDENT_ID+'|18')  # 第18題
        producer.produce(topicName, '1', STUDENT_ID+'|19')  # 第19題   2
        producer.produce(topicName, '2', STUDENT_ID+'|20')  # 第20題

        producer.produce(topicName, '3', STUDENT_ID+'|21')  # 第21題
        producer.produce(topicName, '1', STUDENT_ID+'|22')  # 第22題   5
        producer.produce(topicName, '1', STUDENT_ID+'|23')  # 第23題
        producer.produce(topicName, '2', STUDENT_ID+'|24')  # 第24題
        producer.produce(topicName, '2', STUDENT_ID+'|25')  # 第25題
        
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(producer))
    except KafkaException as e:
        traceback.print_exc(file=sys.stdout)

    # 步驟5. 確認所在Buffer的訊息都己經送出去給Kafka了
    producer.flush()
    print("Message sending completed!")

