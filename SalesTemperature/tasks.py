from __future__ import absolute_import

from celery import shared_task
from celery.task import Task

from pykafka import KafkaClient

from kafka import KafkaConsumer
import re

import redis
import threading

import time

consumerThread = None

#######################################################################################################################

REDIS_MASTER = 'salest-master-server'
REDIS_MASTER_PORT = 6300

REDIS_SLAVE = 'salest-master-server'
REDIS_SLAVE_PORT = 6310

KEY_EXPIRE_TIME = 60 * 60 * 24

REDIS_KEY_PREFIX = "today_timebase_data_of"

def update_transaction(key,value,tr_seq, opt_key_expire_t=KEY_EXPIRE_TIME):
    
    print "update_transaction",key,value

    write_conn_pool = redis.ConnectionPool(host=REDIS_MASTER,port=REDIS_MASTER_PORT, db=0)
    write_rds = redis.Redis(connection_pool=write_conn_pool)

    dict_to_store = {}
    dict_to_read = {}
    
    key_list = write_rds.hkeys(key)
    
    # Read from cache
    if len(key_list) > 0:
        value_list = write_rds.hmget(key,['last_tr_seq','total_amount'])
            
        for sub_key,value in zip(key_list,value_list):
            dict_to_read[sub_key] = value
        
        # Write to cache
        if int(tr_seq) > int(dict_to_read['last_tr_seq']):
            acc_value = int(dict_to_read['total_amount']) + int(value)

            dict_to_store['last_tr_seq'] = tr_seq
            dict_to_store['total_amount'] = acc_value
            
            #write_rds.delete(keys)
            write_rds.hmset(key, dict_to_store)
            write_rds.expire(key, KEY_EXPIRE_TIME)
            
            print "Write Key : ", key, str(dict_to_store)
        
    else :
        dict_to_store['last_tr_seq'] = tr_seq
        dict_to_store['total_amount'] = value

        write_rds.hmset(key, dict_to_store)
        write_rds.expire(key, KEY_EXPIRE_TIME)
    
        print "Write Key : ", str(dict_to_store)
    
    return

#######################################################################################################################


last_msg_uuid = None
                    
class KafkaConsumerThread(threading.Thread):
    
    client = KafkaClient(hosts="salest-master-server:9092")
    topic = client.topics['tr-events']
    consumer = topic.get_simple_consumer()
    
    last_msg_uuid = None
        
    def run(self):
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
                    tr_seq = (record_fields[0].split('-'))[3]
                    
                    sales_amount = record_fields[4]
                    time_slot = (record_fields[1].split(':'))[0]
                    
                    #print date, sales_amount , time_slot
                    update_transaction("{0}:{1}:{2}".format(REDIS_KEY_PREFIX,date,time_slot), sales_amount, tr_seq)
           
        
        
@shared_task
class KafkaConsumerTask(Task): 
    KafkaConsumerThread().start()

    def run(self, **kwargs):
        print "KafkaConsumerTask.run do nothing..."


        
       