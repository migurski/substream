import pdb
import sys
import json
import redis
import urllib
import urllib2
import optparse
import xml.sax.handler
import xml.sax

class TweetHandler(xml.sax.handler.ContentHandler):
    """ Handles these:
    
        <?xml version="1.0" encoding="UTF-8"?>
        <status>
          <created_at>Fri Nov 27 02:25:15 +0000 2009</created_at>
          <id>6100056160</id>
          <text>AHHHH fuck! I just remembered that I tore my lace leggings last night at the club! FUCK! Them bitches was hot! lol oh well, time 4 new ones</text>
          <source>web</source>
          <truncated>false</truncated>
          <in_reply_to_status_id></in_reply_to_status_id>
          <in_reply_to_user_id></in_reply_to_user_id>
          <favorited>false</favorited>
          <in_reply_to_screen_name></in_reply_to_screen_name>
          <user>
            <id>30522437</id>
            <name>Dani Nicole </name>
            <screen_name>dani_PYTscorpio</screen_name>
            <location>Ft Wth moving 2 ATL June 2010</location>
            <description>the P.Y.T.  shawty hold the work for me, shawty hold the gun for me, shawty take daddy dick, shawty nvr run frm it ;p</description>
            <profile_image_url>http://a1.twimg.com/profile_images/543663876/n25317261_32815056_3406_normal.jpg</profile_image_url>
            <url>http://www.myspace.com/sexyassgirlonthecomputer</url>
            <protected>false</protected>
            <followers_count>96</followers_count>
            <profile_background_color>642D8B</profile_background_color>
            <profile_text_color>3D1957</profile_text_color>
            <profile_link_color>FF0000</profile_link_color>
            <profile_sidebar_fill_color>7AC3EE</profile_sidebar_fill_color>
            <profile_sidebar_border_color>65B0DA</profile_sidebar_border_color>
            <friends_count>135</friends_count>
            <created_at>Sat Apr 11 20:57:07 +0000 2009</created_at>
            <favourites_count>2</favourites_count>
            <utc_offset>-21600</utc_offset>
            <time_zone>Central Time (US &amp; Canada)</time_zone>
            <profile_background_image_url>http://a3.twimg.com/profile_background_images/53199957/dani_8.jpg</profile_background_image_url>
            <profile_background_tile>true</profile_background_tile>
            <statuses_count>1224</statuses_count>
            <notifications></notifications>
            <geo_enabled>false</geo_enabled>
            <verified>false</verified>
            <following></following>
          </user>
          <geo/>
        </status>
    """
    def startDocument(self):
        """ Initialize a bunch of variables
        """
        self.stack = []
        self.chars = None
        self.tweet = {}

    def startElement(self, name, attrs):
        self.stack.append(name)
        self.chars = []

    def characters(self, chars):
        self.chars.append(chars)

    def endElement(self, name):
        self.stack.pop()
        
        if len(self.stack):
            if self.stack[-1] == 'limit':
                if name == 'track':
                    print '*' * 80, ''.join(self.chars)
            if self.stack[-1] == 'status':
                if name == 'id':
                    self.tweet['id'] = ''.join(self.chars)
                if name == 'text':
                    self.tweet['text'] = ''.join(self.chars).replace('&lt;', '<').replace('&gt;', '>').replace('\r', ' ').replace('\n', ' ')
            if self.stack[-1] == 'user':
                if name == 'location':
                    self.tweet['location'] = ''.join(self.chars)
                if name == 'screen_name':
                    self.tweet['screen_name'] = ''.join(self.chars)

    def endDocument(self):
        if 'id' in self.tweet and 'screen_name' in self.tweet and  'text' in self.tweet:
            print '%(screen_name)20s - %(text)s' % self.tweet
        else:
            self.tweet = False

def queue_tweet(store, tweet):
    """
    """
    store.set('tweet-%(id)s' % tweet, json.dumps(tweet))
    store.expire('tweet-%(id)s' % tweet, 300)
    store.push('stream', 'tweet-%(id)s' % tweet)

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
    
    if options.filter:
        address = 'http://stream.twitter.com/1/statuses/filter.xml?track=' + urllib.quote(options.filter)
    else:
        address = 'http://stream.twitter.com/1/statuses/sample.xml'

    # so much boilerplate
    manager = urllib2.HTTPPasswordMgr()
    manager.add_password('Firehose', 'http://stream.twitter.com', options.username, options.password)
    handler = urllib2.HTTPBasicAuthHandler(manager)
    opener = urllib2.build_opener(handler)
    request = urllib2.Request(address)
    firehose = opener.open(request)
    handler = TweetHandler()
    
    store = redis.Redis()
    
    while True:
        # read tweets line-by-line, breaking on lines that start with an XML header
        line = firehose.readline()

        if line.startswith('<?xml'):
            # at the beginning of a tweet, collect each line of XML into a list
            lines = [line]
            
            while True:
                # read a tweet line-by-line, breaking when the next tweet starts
                line = firehose.readline()
                
                if line.startswith('<?xml'):
                    # when the end has been overshot, parse what we've got
                    try:
                        xml.sax.parseString(''.join(lines).strip(), handler)
                        if handler.tweet:
                            queue_tweet(store, handler.tweet)
                    except Exception, e:
                        raise
                        pdb.set_trace()

                    # move on to the next tweet
                    break

                else:
                    # add a line to the current tweet
                    lines.append(line)
