import pdb
import sys
import re
import json
import redis
import socket
import base64
import urllib
import select
import pprint
import urlparse
import optparse

parser = optparse.OptionParser(usage="""digest.py [options]
""")

parser.add_option('-i', '--inhaler', dest='inhaler',
                  help='Redist host:port for inhaler heartbeat',
                  action='store')

parser.add_option('-e', '--exhaler', dest='exhaler',
                  help='Redist host:port for exhaler heartbeat',
                  action='store')

if __name__ == '__main__':
    
    options, args = parser.parse_args()
    
    store = redis.Redis()

    words_pat = re.compile(r"#?\w+(?:'(?:t)(?:re))?", re.UNICODE)
    
    # from http://daringfireball.net/2009/11/liberal_regex_for_matching_urls
    urls_pat = re.compile(r'\b((?:[\w-]+://?|www[.])[^\s()<>]+(?:\([\w\d]+\)|(?:[^\!\'\#\%\&\'\(\)\*\+\,-\.\/\:\;\<\=\>\?\@\[\/\]\^\_\{\|\}\~\s]|/)))')
    
    while True:
        try:
            raw = store.pop('intake')
            tweet = json.loads(raw)
            
            # {u'created_at': u'Tue Dec 01 06:07:49 +0000 2009',
            #  u'favorited': False,
            #  u'geo': None,
            #  u'id': 6227375503,
            #  u'in_reply_to_screen_name': u'specialN',
            #  u'in_reply_to_status_id': 6226988284,
            #  u'in_reply_to_user_id': 44899614,
            #  u'source': u'web',
            #  u'text': u'@specialN ah, das klingt doch gut?!? na, dann hoffe ich mal das beste!!!',
            #  u'truncated': False,
            #  u'user': {u'created_at': u'Thu May 21 15:09:38 +0000 2009',
            #            u'description': u'Riding an HP Velotechnik Speedmachine Recumbent Bike - Constantly in Transit',
            #            u'favourites_count': 0,
            #            u'followers_count': 108,
            #            u'following': None,
            #            u'friends_count': 80,
            #            u'geo_enabled': False,
            #            u'id': 41603785,
            #            u'location': u'Hamburg, Germany',
            #            u'name': u'Lars Reisberg',
            #            u'notifications': None,
            #            u'profile_background_color': u'ff9d00',
            #            u'profile_background_image_url': u'http://a3.twimg.com/profile_background_images/47783583/twitter_profile.jpg',
            #            u'profile_background_tile': True,
            #            u'profile_image_url': u'http://a3.twimg.com/profile_images/488346363/lars_reisberg_normal.jpg',
            #            u'profile_link_color': u'06aeea',
            #            u'profile_sidebar_border_color': u'000000',
            #            u'profile_sidebar_fill_color': u'c7c7c7',
            #            u'profile_text_color': u'000000',
            #            u'protected': False,
            #            u'screen_name': u'FastTransit',
            #            u'statuses_count': 1759,
            #            u'time_zone': u'Berlin',
            #            u'url': u'http://www.speedmachineadventures.blogspot.com',
            #            u'utc_offset': 3600,
            #            u'verified': False}}
            
            tweet['text'] = tweet['text'].replace('&lt;', '<').replace('&gt;', '>')
            
            text = tweet['text']
            print text
            
            tokens = []
            
            for part in urls_pat.split(text):
                if urls_pat.match(part):
                    tokens.append(part)
                else:
                    tokens += words_pat.findall(part)
            
            print tokens

            break
        except:
            raise # pdb.set_trace()
