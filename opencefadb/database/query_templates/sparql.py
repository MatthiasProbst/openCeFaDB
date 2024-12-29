from gldb.query.rdfstorequery import SparqlQuery

SELECT_FAN_PROPERTIES = SparqlQuery("""
PREFIX schema: <http://schema.org/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>

SELECT ?parameter ?property ?value
WHERE {
  <https://www.wikidata.org/wiki/Q131549102> m4i:hasParameter ?parameter .
  ?parameter a ?type .
  ?parameter ?property ?value .
}
""")

SELECT_ALL = SparqlQuery("SELECT * WHERE {?s ?p ?o}")
