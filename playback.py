import json
import redis

if __name__ == '__main__':
    store = redis.Redis()
    
    while store.llen('stream'):
        key = store.pop('stream')

        if store.exists(key):
            tweet = json.loads(store.get(key))
            store.delete(key)
        
            print '%(screen_name)20s - %(text)s' % tweet
