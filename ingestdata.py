import pandas as pd
from neo4j import GraphDatabase
data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687",auth=("neo4j","123"))
session = data_base_connection.session()

netflix = pd.read_csv("C:/Users/chait/Desktop/IS/netflix_titles.csv")

netflix["date_added"] = pd.to_datetime(netflix['date_added'])
netflix['year'] = netflix['date_added'].dt.year
netflix['month'] = netflix['date_added'].dt.month
netflix['day'] = netflix['date_added'].dt.day

netflix['description'] = netflix['description'].str.replace(r"[\']", r"")
netflix['description'] = netflix['description'].str.replace(r"[\"]", r"")
netflix['cast'] = netflix['cast'].str.replace(r"[\']", r"")
netflix['title'] = netflix['title'].str.replace(r"[\']", r"")
netflix['listed_in'] = netflix['listed_in'].str.replace(r"[\']", r"")
netflix['director'] = netflix['director'].str.replace(r"[\']", r"")

execution_commands = []

for index, row in netflix.iterrows():
    neo4j_create_statement = "Create(m:Movie{id:'"+str(row['show_id'])+"',title:'"+str(row['title'])+"'}) set m.director= '"+str(row['director'])+"', m.country = '"+str(row['country'])+"',m.date_str = '"+str(row['date_added'])+"', m.release_year = '"+str(row['release_year'])+"',m.rating = '"+str(row['rating'])+"',m.duration = '"+str(row['duration'])+"',m.listed_in = '"+str(row['listed_in'])+"',m.description = '"+str(row['description'])+"',m.cast='"+str(row['cast'])+"',m.year = '"+str(row['year'])+"',m.month = '"+str(row['month'])+"',m.day = '"+str(row['day'])+"',m.type = '"+str(row['type'])+"'"
    execution_commands.append(neo4j_create_statement)
for i in execution_commands:
    #print(i)
    session.run(i)
#    print(neo4j_create_statement)

neo4j_person_statement = "MATCH (m:Movie) WHERE m.cast IS NOT NULL WITH m UNWIND split(m.cast, ',') AS actor MERGE (p:Person {name: trim(actor)}) MERGE (p)-[r:ACTED_IN]->(m)"
session.run(neo4j_person_statement)

neo4j_category_statement = "MATCH (m:Movie) WHERE m.listed_in IS NOT NULL WITH m UNWIND split(m.listed_in, ',') AS category MERGE (c:Category {name: trim(category)}) MERGE (m)-[r:IN_CATEGORY]->(c)"

session.run(neo4j_category_statement)
neo4j_type_statement = "MATCH (m:Movie) WHERE m.type IS NOT NULL WITH m MERGE (t:Type {type: m.type}) MERGE (m)-[r:TYPED_AS]->(t)"

session.run(neo4j_type_statement)

neo4j_director_statement = "MATCH (m:Movie) WHERE m.director IS NOT NULL WITH m MERGE (d:Person {name: m.director}) MERGE (d)-[r:DIRECTED]->(m)"
session.run(neo4j_director_statement)

neo4j_countries_statement = "MATCH (m:Movie) WHERE m.country IS NOT NULL MERGE (c:Country {name: trim(m.country)}) MERGE (m)-[:WHERE]->(c)"
session.run(neo4j_countries_statement)

neo4j_delete_statement = "MATCH(m:Movie) SET m.country = null, m.category = null,m.type = null , m.director = null , m.cast =null"
session.run(neo4j_delete_statement)

year = "CREATE INDEX ON :Year(value)"
session.run(year)

create_years = "WITH range(2012, 2019) AS years, range(1,12) AS months FOREACH(year IN years | CREATE (y:Year {value: year}))"

session.run(create_years)

years_connect = "MATCH (year:Year) WITH year ORDER BY year.value WITH collect(year) AS years FOREACH(i in RANGE(0, size(years)-2) | FOREACH(year1 in [years[i]] | FOREACH(year2 in [years[i+1]] | MERGE (year1)-[:NEXT]->(year2))))"

session.run(years_connect)

""" request = "MATCH (m:Movie) WITH m, m.year AS y MATCH (year:Year {value: y}) MERGE (m)-[:CREATED_ON]->(year) RETURN m,year"
session.run(request) """

request = "MATCH (m:Movie) WITH m  MATCH (y:Year {value:toInteger(m.release_year)}) create (m)-[:CREATED_ON]->(y) "

session.run(request) 

#creating releatiosnship  between years and movies
