import redis

REDIS_MASTER = 'salest-master-server'
REDIS_MASTER_PORT = 6300

REDIS_SLAVE = 'salest-master-server'
REDIS_SLAVE_PORT = 6310

KEY_EXPIRE_TIME = 60 * 60 * 24

def write_transaction(key,value,opt_key_expire_t=KEY_EXPIRE_TIME):
    
    write_conn_pool = redis.ConnectionPool(host=REDIS_MASTER,port=REDIS_MASTER_PORT, db=0)
    write_rds = redis.Redis(connection_pool=write_conn_pool)
    
    if write_rds.exists(key) is False:
        write_rds.setex(key, value, KEY_EXPIRE_TIME)
        
    return 

def read_transaction(key):
    
    read_conn_pool = redis.ConnectionPool(host=REDIS_SLAVE,port=REDIS_SLAVE_PORT, db=0)
    read_rds = redis.Redis(connection_pool=read_conn_pool)

    query_result = read_rds.get(key)
    return query_result


def write_dict_transaction(key,dict_value,opt_key_expire_t=KEY_EXPIRE_TIME):
    
    write_conn_pool = redis.ConnectionPool(host=REDIS_MASTER,port=REDIS_MASTER_PORT, db=0)
    write_rds = redis.Redis(connection_pool=write_conn_pool)
    
    if write_rds.exists(key) is False:
        write_rds.hmset(key, dict_value)
        write_rds.expire(key, KEY_EXPIRE_TIME)
        
    return 


def read_dict_transaction(key,subkeyList='*'):
    
    read_conn_pool = redis.ConnectionPool(host=REDIS_SLAVE,port=REDIS_SLAVE_PORT, db=0)
    read_rds = redis.Redis(connection_pool=read_conn_pool)

    if read_rds.exists(key) is True:
        
        if subkeyList == '*':
            key_list = read_rds.hkeys(key)
            value_list = read_rds.hmget(key,key_list)
            
            query_result = {}
            for key,value in zip(key_list,value_list):
                query_result[key] = value
            
        else :
            query_result = read_rds.hmget(key,subkeyList)
            
        return query_result
    
    return None


def get_transaction_incr_counter(date):
    
    conn_pool = redis.ConnectionPool(host=REDIS_MASTER,port=REDIS_MASTER_PORT, db=0)
    rds = redis.Redis(connection_pool=conn_pool)
    
    key = "tr_counter:" + date
    if rds.exists(key):
        rds.incr(key,1)
        return rds.get(key)
    else:
        rds.setex(key, 1, KEY_EXPIRE_TIME)
        return rds.get(key)
    