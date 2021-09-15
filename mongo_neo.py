import pymongo
from neo4j import GraphDatabase
data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687",auth=("neo4j","123"))
session = data_base_connection.session()
import pandas as pd
import seaborn as sns
import warnings
import matplotlib.pyplot as plt
plt.show()
warnings.simplefilter(action="ignore", category=FutureWarning)

atlasclient  = pymongo.MongoClient('mongodb+srv://vinay:1234@series.gyzgu.mongodb.net/Series?retryWrites=true&w=majority')
#print(atlasclient)

mydb = atlasclient.tvshows.tvshows

query1 = [
  {
    "$search": {
      "index":"autocomplete",
      "autocomplete": {
         "path": "name",
        "query": "Narco"
      }
    }
  },
  {
    "$limit": 10
  },
  {
    "$project": {
      "_id": 0,
      "name": 1
    }
  }
]

result = list(mydb.aggregate(query1))
names = []
for i in result:
    names.append(i['name'])
names = set(names)   
names = list(names) 
print("Autocomplete results of Narc")
print(names)


execution_commands = []
for i,val in enumerate(names):
    
    query1 = "MATCH (:Movie {title: '"+str(val)+"'})--(person:Person) RETURN person"
    execution_commands.append(query1)

#print(execution_commands)
for i,val in enumerate(execution_commands):
    print("crew of "+names[i]+"pulled from neo4j------>")
    p = session.run(val)

    for r in p:
      print(r[0]['name'])


###Query

q1 = """Match (t:Type)<-[:TYPED_AS]-(m:Movie)-[:WHERE]->(c:Country) where t.type = "TV Show" return t.type,m.rating, c.name,m.duration"""            
p = session.run(q1)

df = pd.DataFrame(p)
df.columns = ['type','rating','country', 'season']
Ind_data = df[df.country == 'India']
print(Ind_data)
World = df[df.country!='India']

df['Country_type'] = df.apply(lambda x : "India" if 'India' == x['country'] else "Rest Of the World", axis=1)
#print(df[df['Country_type']=='India'])
print(df)

#Ind_data['season'].value_counts().plt(kind='bar')

sns.countplot(Ind_data['season'], color='gray')
#plt.show()

sns.countplot(World['season'], color='gray')
#plt.show()

######Query 
q = "match (m:Movie{release_year :'2019',listed_in :'Anime Series, International TV Shows'})-[:TYPED_AS]->(t:Type{type:'TV Show'}) return m"

p = session.run(q)
print("------------------------------------Tv shows listed in anime and international tv shows ----------------------------")
for r in p:
    print("%s" % (r[0]["title"]))

#prodution houses information extracted from mongodb 
title = 'KENGAN ASHURA'
q2 = "find({'name':'Kengan Ashura'})"
result = list(mydb.q2)
print(result)
