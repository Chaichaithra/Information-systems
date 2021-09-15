from neo4j import GraphDatabase
data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687",auth=("neo4j","123"))
session = data_base_connection.session()
import matplotlib.pyplot as plt
import pandas as pd

#Query1
#Recommendations using link prediction algorithm 

query1 = """
MATCH (a:Movie {title:'13 Reasons Why'} )-[*2]-(b:Movie)
WHERE a <> b AND a.title < b.title
WITH DISTINCT a,b
RETURN a.title as title, b.title as recommendation, gds.alpha.linkprediction.adamicAdar(a, b) AS score
ORDER BY score DESC
LIMIT 10
"""
p = session.run(query1)
for r in p:
    print(r[1])

#Query2
#Shortest path algorithm 
query2 = """ MATCH (cs:Person { name: 'Brad Anderson' }),(ms:Person { name:'Brad Pitt' }), p = shortestPath((cs)-[:ACTED_IN|DIRECTED*]-(ms))
    WHERE length(p)> 1 
    RETURN p """

p = session.run(query2)
for r in p:
    print(r)

#Query 3 
#10 years line graph 
query3 = """ 
MATCH (y:Year {value: 2012})-[:NEXT*0..10]->(y2:Year)<-[:CREATED_ON]-(f:Movie)-[r:WHERE]->(c:Country)
RETURN y2.value as year,c.name as country,count(r) as count
ORDER BY year DESC, count DESC """

p = session.run(query3)
df = pd.DataFrame(p)
df.drop(df[df[1] == 'nan'],axis = 0,inplace = True)
filter=df[1][:10]
temp= df.loc[df[1].isin(filter)]
temp=temp.set_index([0, 1])
temp=temp.unstack(level=-1)
temp.fillna(0,inplace=True)
temp.plot(figsize=(12, 12)).legend(bbox_to_anchor=(1, 1))
plt.show()  

#Query 4
query_similarity = """ MATCH (m:Movie)-[:IN_CATEGORY]->(cat:Category)
WITH {item:id(cat), categories: collect(id(m))} as userData
WITH collect(userData) AS data
CALL gds.alpha.similarity.overlap.stream({data: data})
YIELD item1, item2, count1, count2, intersection, similarity
RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to,
       count1, count2, intersection, similarity
ORDER BY similarity DESC LIMIT 5"""

p = session.run(query_similarity)

for r in p:
    print(r[0],"-----",r[1],"-----",r[5])