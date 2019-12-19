#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from confluent_kafka import Producer
import sys
import time
import json
from model.Employee import Employee

'''
* 示範: 如何將資料包成物件並轉換成JSON字串送入Kafka
'''


# 用來接收從Producer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)


# 回傳現在的時間(epoch in millis)
def epoch_now_mills():
    return int(round(time.time() * 1000))


# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        # 為了不讓打印訊息拖慢速度, 我們每1萬打印一筆record Metadata來看
        if int(msg.key()) % 10000 == 0:
            sys.stderr.write('%% Message delivered to topic:[{}]\n'.format(msg.topic()))


# 主程式進入點
if __name__ == '__main__':
    # 步驟1. 設定要連線到Kafka集群的相關設定
    props = {
        # Kafka集群在那裡?
        'bootstrap.servers': 'localhost:9092',           # <-- 置換成要連接的Kafka集群
        'error_cb': error_cb                           # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)
    # 步驟3. 指定想要發佈訊息的topic名稱
    topicName = 'ak03.json'
    msgCount = 100000  # 10萬筆

    try:
        print('Start sending messages ...')
        time_start = int(round(time.time() * 1000))
        # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])
        '''
        / ** 示範: 如何將資料包成物件並轉換成JSON字串送入Kafka **
        // 由於一般應用場景的資料都是包括了很多的資料欄位及不同的資料型別。通常都會使用一個類別物件來做為資料傳遞的容器。
        // 因此如何把一個Data Transfer Object (DTO)序列化送進Kafka是相當關鍵的步驟
        '''
        for i in range(msgCount):
            fakeNumber = str(i)
            # 讓我們產生假的Employee資料
            employee = Employee(id_='emp_id_'+fakeNumber,
                                first_name='fn_'+fakeNumber,
                                last_name='ln_'+fakeNumber,
                                dept_id='dept_id_'+str(i % 10),
                                hire_date=epoch_now_mills(),
                                wage=float(i),
                                sex=True)
            # 轉換成JSON字串
            employeeJson = json.dumps(employee.__dict__)
            # 送出訊息
            producer.produce(topicName, key=str(i), value=employeeJson, callback=delivery_callback)
            producer.poll(0)  # 呼叫poll來讓client程式去檢查內部的Buffer, 並觸發callback
            if i % 10000 == 0:
                print('Send {} messages'.format(i))
        time_spend = int(round(time.time() * 1000)) - time_start
        print('Send        : ' + str(msgCount) + ' messages to Kafka')
        print('Total spend : ' + str(time_spend) + ' millis-seconds')
        print('Throughput : ' + str(msgCount/time_spend * 1000) + ' msg/sec')
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                         .format(len(producer)))
    except Exception as e:
        print(e)

    # 步驟5. 確認所有在Buffer裡的訊息都己經送出去給Kafka了
    producer.flush(10)
    print('Message sending completed!')
