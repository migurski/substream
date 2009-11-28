import pdb
import sys
import re
import json
import redis
import socket
import base64
import urllib
import urlparse
import optparse

def queue_tweet(store, tweet):
    """
    """
    store.set('tweet-%(id)s' % tweet, json.dumps(tweet))
    store.expire('tweet-%(id)s' % tweet, 300)
    store.push('stream', 'tweet-%(id)s' % tweet)

def stream_lines(username, password, filter):
    """ Connect to Twitter firehose, and yield each line of the HTTP response.
    """
    if filter:
        address = 'http://stream.twitter.com/1/statuses/filter.json?track=' + urllib.quote(filter)
    else:
        address = 'http://stream.twitter.com/1/statuses/sample.json'

    scheme, host, path, p, query, f = urlparse.urlparse(address)
    credentials = base64.b64encode(username + ':' + password)
    
    twitter = socket.create_connection((host, 80), 600)
    twitter.send('GET %(path)s?%(query)s HTTP/1.1\n' % locals())
    twitter.send('Authorization: Basic %(credentials)s\n' % locals())
    twitter.send('Host: %(host)s\n\n' % locals())
    
    # the tail end of the previous chunk
    data = ''

    while True:
        chunk = data + twitter.recv(1024)
        lines = re.split(r'[\r\n]+', chunk)
        
        for line in lines[:-1]:
            yield line

        data = lines[-1]
        

parser = optparse.OptionParser(usage="""stream.py [options]
""")

parser.add_option('-u', '--username', dest='username',
                  help='Twitter stream username',
                  action='store')

parser.add_option('-p', '--password', dest='password',
                  help='Twitter stream password',
                  action='store')

parser.add_option('-f', '--filter', dest='filter',
                  help='Optional filter keyword',
                  action='store')

if __name__ == '__main__':
    
    options, args = parser.parse_args()
    
    store = redis.Redis()
    firehose = stream_lines(options.username, options.password, options.filter)
    
    for line in firehose:
        if line.startswith('{'):
            try:
                tweet, message = {}, json.loads(line)
                
                if message.has_key('delete'):
                    continue
                
                if message.has_key('limit'):
                    print '*' * 60, 'skipped', message['limit']['track']
                    continue
                
                tweet['id'] = str(message['id'])
                tweet['text'] = message['text'].replace('&lt;', '<').replace('&gt;', '>')
                tweet['location'] = message['user']['location']
                tweet['screen_name'] = message['user']['screen_name']

                print '%(screen_name)20s - %(text)s' % tweet

            except Exception, e:
                pdb.set_trace()

            else:
                queue_tweet(store, tweet)
