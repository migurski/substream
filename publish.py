import time
import json
import math
import redis
import random

delay = 10
rate = 2

if __name__ == '__main__':

    while True:
        start = time.time()
        
        tweets = []
        
        store = redis.Redis()
        total = store.llen('stream')
        count = int(delay * rate)
        skip = (total / count) - 1
        
        print 'total', total, 'count', count, 'skip', skip
        
        for j in range(count):
            for k in range(skip):
                # skip a bunch to make the corrent count
                key = store.pop('stream')
        
                if store.exists(key):
                    store.delete(key)

            key = store.pop('stream')
    
            if store.exists(key):
                tweet = json.loads(store.get(key))
                store.delete(key)
                tweets.append(tweet)

        for tweet in tweets:
            print '%(screen_name)20s - %(text)s' % tweet
        
        store.disconnect()
        
        time.sleep(start + delay - time.time())
