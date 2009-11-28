import re
import time
import json
import math
import redis
import random

delay = 10
rate = 2

if __name__ == '__main__':

    prev = None
    curr = 0
    
    # from http://daringfireball.net/2009/11/liberal_regex_for_matching_urls
    urls = re.compile(r'\b(([\w-]+://?|www[.])[^\s()<>]+(?:\([\w\d]+\)|([^\!\'\#\%\&\'\(\)\*\+\,-\.\/\:\;\<\=\>\?\@\[\/\]\^\_\{\|\}\~\s]|/)))')

    while True:
        next = curr + 1
        
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
            
            if urls.search(tweet['text']):
                print ' ' * 22, ', '.join([group[0] for group in urls.findall(tweet['text'])])
        
        store.disconnect()
        
        print '-' * 20, prev, '<--', curr, '-->', next
        
        time.sleep(start + delay - time.time())
        
        prev, curr = curr, next
