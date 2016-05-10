from __future__ import absolute_import

from celery import shared_task
from celery.task import Task

from pykafka import KafkaClient

from kafka import KafkaConsumer
import re

import redis

#######################################################################################################################

REDIS_MASTER = 'salest-master-server'
REDIS_MASTER_PORT = 6300

REDIS_SLAVE = 'salest-master-server'
REDIS_SLAVE_PORT = 6310

KEY_EXPIRE_TIME = 60 * 60 * 24

REDIS_KEY_PREFIX = "today_timebase_data_of"

def update_transaction(key,value,opt_key_expire_t=KEY_EXPIRE_TIME):
    
    write_conn_pool = redis.ConnectionPool(host=REDIS_MASTER,port=REDIS_MASTER_PORT, db=0)
    write_rds = redis.Redis(connection_pool=write_conn_pool)
    
    query_result = write_rds.get(key)
    if query_result == None:
        query_result = str(0)
    
    acc_value = int(query_result) + int(value)
    write_rds.setex(key, acc_value, KEY_EXPIRE_TIME)
    return

#######################################################################################################################


last_msg_uuid = None

@shared_task
def run_kafka_consumer():
    consumer = KafkaConsumer(bootstrap_servers='salest-master-server:9092', auto_offset_reset='earliest')
    consumer.subscribe(['tr-events'])

    for message in consumer:
        regex_message = re.sub('[^0-9a-zA-Z-:,]+', ' ', message.value)
        message_fields  = regex_message.split(' ')
        
        tr_record = message_fields[len(message_fields)-1]
        uuid = message_fields[len(message_fields)-2]
        
        tr_record =  re.sub('[^0-9-:,]','',tr_record)
        
        if last_msg_uuid != uuid:
            last_msg_uuid = uuid
            print uuid, tr_record

@shared_task
class KafkaConsumerTask(Task):
     
    client = KafkaClient(hosts="salest-master-server:9092")
    topic = client.topics['tr-events']
    consumer = topic.get_simple_consumer()

    last_msg_uuid = None
    
    def run(self, **kwargs):
        
        #print "KafkaConsumerTask Started"
        
        for message in self.consumer:
            if message is not None:
                regex_message = re.sub('[^0-9a-zA-Z-:,]+', ' ', message.value)
                message_fields  = regex_message.split(' ')
        
                tr_record = message_fields[len(message_fields)-1]
                uuid = message_fields[len(message_fields)-2]
        
                tr_record =  re.sub('[^0-9-:,]','',tr_record)
        
                if self.last_msg_uuid != uuid:
                    self.last_msg_uuid = uuid
                    #print uuid, tr_record
                    
                    #2016-05-09-19,21:56:01,127,1,6500
                    record_fields = tr_record.split(',')
                    
                    date = (record_fields[0].rsplit('-',1))[0]
                    sales_amount = record_fields[4]
                    time_slot = (record_fields[1].split(':'))[0]
                    
                    print date, sales_amount , time_slot
                    update_transaction("{0}:{1}:{2}".format(REDIS_KEY_PREFIX,date,time_slot), sales_amount)
                    
