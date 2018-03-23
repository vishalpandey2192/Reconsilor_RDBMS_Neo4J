from neomodel import (config, StructuredNode, StringProperty, IntegerProperty,
    UniqueIdProperty, RelationshipTo, RelationshipFrom,db)

config.DATABASE_URL = 'bolt://neo4j:tanveenisgreatvishalisaight@stage-neo4j.clearlinkdata.com:7687'

class Country(StructuredNode):
    code = StringProperty(unique_index=True, required=True)

    # traverse incoming IS_FROM relation, inflate to Person objects
    inhabitant = RelationshipFrom('Person', 'IS_FROM')


class Person(StructuredNode):
    uid = UniqueIdProperty()
    name = StringProperty(unique_index=True)
    age = IntegerProperty(index=True, default=0)

    # traverse outgoing IS_FROM relations, inflate to Country objects
    country = RelationshipTo(Country, 'IS_FROM')

jim = Person(name='vishal', age=3).save()
jim.age = 4
jim.save() # validation happens here

rohan = Person(name='Rohan', age=3).save()
rohan.age = 4
rohan.save()

india = Country(code='IND').save()
india.save() # validation happens here

germany = Country(code='GE').save()
germany.save()

jim.country.connect(india)

germany.inhabitant.connect(rohan)

results, meta = db.cypher_query('MATCH (n) return n LIMIT 10', '')
print(results)
people = [Person.inflate(row[0]) for row in results]

#jim.delete()
#jim.refresh() # reload properties from neo
#jim.id # neo4j internal id
