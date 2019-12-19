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

'''
* 示範: Sync
'''


# 用來接收從Producer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)
    

# 主程式進入點
if __name__ == '__main__':
    # 步驟1. 設定要連線到Kafka集群的相關設定
    props = {
        # Kafka集群在那裡?
        'bootstrap.servers': 'localhost:9092',    # <-- 置換成要連接的Kafka集群
        'error_cb': error_cb                        # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)

    # 步驟3. 指定想要發佈訊息的topic名稱
    topicName = 'ak03.fire_and_forget'
    msgCount = 10000  # 1萬筆

    try:
        print('Start sending messages ...')

        time_start = int(round(time.time() * 1000))

        # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])

        # ** 示範: Synchronous Send **
        # librdkafka(C++)的produce都是用async實作,
        # 所以使用confluent_kafka的client作sync發佈的話，
        # 只能採produce()搭配flush()實作。
        # 由於這種方法是一筆一筆的送。因此這種方法的throughput最差
        # (通常不會用Kafka來做這種事)

        for i in range(msgCount):
            producer.produce(topicName, key=str(i), value='msg_{}'.format(i))
            producer.flush()   # <-- 每次produce()後就呼叫flush()，就實作了Sync Producer的功能
            if i % 1000 == 0:
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
