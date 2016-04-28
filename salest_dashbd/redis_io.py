import redis

REDIS_MASTER = 'salest-master-server'
REDIS_MASTER_PORT = 6300

REDIS_SLAVE = 'salest-master-server'
REDIS_SLAVE_PORT = 6310

KEY_EXPIRE_TIME = 60

def write_transaction(key,value):
    
    write_conn_pool = redis.ConnectionPool(host=REDIS_MASTER,port=REDIS_MASTER_PORT, db=0)
    write_rds = redis.Redis(connection_pool=write_conn_pool)
    
    write_rds.setex(key, value, KEY_EXPIRE_TIME)
    return 


def read_transaction(key):
    
    read_conn_pool = redis.ConnectionPool(host=REDIS_SLAVE,port=REDIS_SLAVE_PORT, db=0)
    read_rds = redis.Redis(connection_pool=read_conn_pool)

    query_result = read_rds.get(key)
    return query_result

