import redis
from textblob import TextBlob
#r = redis.Redis()
import logging
import redis

logging.basicConfig()
r = redis.StrictRedis(host="localhost", port=6379, charset="utf-8", decode_responses=True)
#print(r.hgetall("showid:s3"))

#for i in r.keys():
#    print(r.hgetall())

#d = r.keys()
a = []
for i in range(1,7000):
    a.append(r.hgetall(("showid:s"+str(i)+"")))
l = []
for i in a:
    l.append(i.get("description"))
#print(l)

#print(type(l))

pos=0
neutral=0
negative=0

for entry in filter(None, l):
    if TextBlob(entry).polarity > 0:
        pos = pos + 1
    if TextBlob(entry).polarity == 0:
        neutral = neutral + 1
    if TextBlob(entry).polarity < 0:
        negative = negative + 1
print("User story 1")        
print("Positive  : {:.2f}".format(pos / 7787 * 100),"%")
print("Neutral   : {:.2f}".format(neutral /  7787* 100),"%")
print("Negative  : {:.2f}".format(negative / 7787 * 100),"%")


#userstory2 

print("please enter show id")
s = input()

print("please enter your age")
age = int(input())

print("please enter what you want to watch")
mov = input()
print(mov)

#key = r.keys()
#print(key)
print("User story 2")   
if(r.hget(s,"title") == mov ):
        print("Series exists in db")
else:
    print("series not present in database")        
if(age < 18):
    print("Cant watch") if r.hget(s,"rating") == 'UR' else print("enjoy live streaming......") 




#r.hincrby("showid:s1", "views", 1)
print("views ------------")
#print(r.hget("showid:s1", "views"))

import logging
import redis

logging.basicConfig()

class Error(Exception):
   """Raised when two users finished watching the series"""

def buymovie(r: redis.Redis, showid: int) -> None:
    with r.pipeline() as pipe:
        error_count = 0
        while True:
            try:
                pipe.watch(showid)
                nleft: bytes = r.hget(showid, "views")
                if nleft > "0":
                    pipe.multi()
                    pipe.hincrby(showid, "views", -1)
                    pipe.hincrby(showid, "watched", 1)
                    pipe.execute()
                    break
                else:
                   
                    pipe.unwatch()
                    raise Error(
                        f"views exceeded, you are not qualified for next year subscription!"
                    )
            except redis.WatchError:
                logging.warning(
                    "WatchError #%d: %s; retrying",
                    error_count, showid
                )
    return None


print(r.hmget("showid:s2", "views", "watched"))

for _ in range(100):
    buymovie(r, "showid:s64")

buymovie(r, "showid:s64")

