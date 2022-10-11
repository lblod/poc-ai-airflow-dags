from typing import NamedTuple

from docker.types import Mount


# region util_classes
class Default_args(NamedTuple):
    start_date: str = "2019-01-01 00:00:00"
    owner: str = "Airflow"


class Pipeline_args(NamedTuple):
    docker_image: str
    docker_host: str
    auto_remove: bool
    network_mode: str
    mount: list
    sparql_endpoint: str
    load_query: str
    load_taxo_query: str = None


class config(NamedTuple):
    pipeline_args: Pipeline_args
    default_config: Default_args = Default_args()


DEMO_LOAD_QUERY = """
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX soic: <http://rdfs.org/sioc/ns#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>


SELECT ?thing ?text WHERE {
VALUES ?thing {<http://example.com/submissions/10> }
  GRAPH <http://mu.semte.ch/application> {
     ?thing prov:generated ?gen .
?gen dct:hasPart ?part.
?part soic:content ?text.
  }
}
"""


# endregion

def load_bertopic_retrain_conf():
    LOAD_QUERY = """
    PREFIX prov: <http://www.w3.org/ns/prov#> 
    PREFIX dct: <http://purl.org/dc/terms/>  
    PREFIX soic: <http://rdfs.org/sioc/ns#>  
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>  
    
    SELECT DISTINCT ?thing ?part ?text WHERE {
    ?thing a <http://rdf.myexperiment.org/ontologies/base/Submission>; prov:generated/dct:hasPart ?part.    
    ?part soic:content ?text.  
    FILTER NOT EXISTS {  ?thing ext:ingestedBy ext:ml2GrowSmartRegulationsTopicModeler  }}
    """

    LOAD_QUERY = DEMO_LOAD_QUERY

    pipe_arg = Pipeline_args(
        docker_image="lblod/poc-ai-airflow-bertopic:latest",
        docker_host="tcp://docker-socket-proxy:2375",
        auto_remove=True,
        network_mode="bridge",
        mount=[
            Mount(target="/data", source="bertopic-retrain-data"),
            Mount(target="/models", source="airflow_model_store")
        ],
        sparql_endpoint="http://192.168.6.152:8892/sparql",
        load_query=LOAD_QUERY
    )

    return config(
        pipeline_args=pipe_arg
    )


def load_bertopic_transform_conf():
    LOAD_QUERY = """
    PREFIX prov: <http://www.w3.org/ns/prov#> 
    PREFIX dct: <http://purl.org/dc/terms/>  
    PREFIX soic: <http://rdfs.org/sioc/ns#>  
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>  

    SELECT DISTINCT ?thing ?part ?text WHERE {
    ?thing a <http://rdf.myexperiment.org/ontologies/base/Submission>; prov:generated/dct:hasPart ?part.    
    ?part soic:content ?text.  
    FILTER NOT EXISTS {  ?thing ext:ingestedBy ext:ml2GrowSmartRegulationsTopicModeler  } }  LIMIT 500
    """

    LOAD_QUERY = DEMO_LOAD_QUERY

    pipe_arg = Pipeline_args(
        docker_image="lblod/poc-ai-airflow-bertopic:latest",
        docker_host="tcp://docker-socket-proxy:2375",
        auto_remove=True,
        network_mode="bridge",
        mount=[
            Mount(target="/data", source="bertopic-transform-data"),
            Mount(target="/models", source="airflow_model_store")
        ],
        sparql_endpoint="http://192.168.6.152:8892/sparql",
        load_query=LOAD_QUERY
    )

    return config(
        pipeline_args=pipe_arg
    )


def load_ner_config():
    LOAD_QUERY = """
    PREFIX prov: <http://www.w3.org/ns/prov#> 
    PREFIX dct: <http://purl.org/dc/terms/>  
    PREFIX soic: <http://rdfs.org/sioc/ns#>  
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>  

    SELECT DISTINCT ?thing ?part ?text WHERE {
    ?thing a <http://rdf.myexperiment.org/ontologies/base/Submission>; prov:generated/dct:hasPart ?part.    
    ?part soic:content ?text.  }
    """

    LOAD_QUERY = DEMO_LOAD_QUERY

    #    FILTER NOT EXISTS {  ?thing ext:ingestedBy ext:ml2GrowSmartRegulationsTopicModeler  } }  LIMIT 50

    # LOAD_QUERY = """PREFIX prov: <http://www.w3.org/ns/prov#>PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>SELECT ?s ?content WHERE {  ?s a besluit:Besluit ;    prov:value ?content. FILTER (STRLEN(?content) >= 100)} LIMIT 10"""

    pipe_arg = Pipeline_args(
        docker_image="lblod/poc-ai-airflow-ner:latest",
        docker_host="tcp://docker-socket-proxy:2375",
        auto_remove=True,
        network_mode="bridge",
        mount=[
            Mount(target="/data", source="ner-data"),
            Mount(target="/models", source="airflow_model_store")
        ],
        sparql_endpoint="http://192.168.6.152:8892/sparql",  # "https://qa.centrale-vindplaats.lblod.info/sparql"
        load_query=LOAD_QUERY
    )

    return config(
        pipeline_args=pipe_arg
    )


def load_zeroshot_config():
    LOAD_QUERY = """
        PREFIX prov: <http://www.w3.org/ns/prov#> 
        PREFIX dct: <http://purl.org/dc/terms/>  
        PREFIX soic: <http://rdfs.org/sioc/ns#>  
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>  

        SELECT DISTINCT ?thing ?part ?text WHERE {
        ?thing a <http://rdf.myexperiment.org/ontologies/base/Submission>; prov:generated/dct:hasPart ?part.    
        ?part soic:content ?text.  
        FILTER NOT EXISTS {  ?thing ext:ingestedBy ext:ingestedMl2GrowSmartRegulationsBBC  }}
        """

    LOAD_QUERY = DEMO_LOAD_QUERY
    # LOAD_QUERY = """PREFIX prov: <http://www.w3.org/ns/prov#>PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>SELECT ?s ?content WHERE {  ?s a besluit:Besluit ;    prov:value ?content. FILTER (STRLEN(?content) >= 100)} LIMIT 10"""

    LOAD_TAXO_QUERY = """
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    SELECT ?nl WHERE {
        <http://data.lblod.info/ML2GrowClassification> ?o ?taxo.
    ?taxo ext:nl_taxonomy ?nl
    }
    """

    pipe_arg = Pipeline_args(
        docker_image="lblod/poc-ai-airflow-zeroshot:latest",
        docker_host="tcp://docker-socket-proxy:2375",
        auto_remove=True,
        network_mode="bridge",
        mount=[
            Mount(target="/data", source="zeroshot-data"),
            Mount(target="/models", source="airflow_model_store")
        ],
        sparql_endpoint="http://192.168.6.152:8892/sparql",  # "https://qa.centrale-vindplaats.lblod.info/sparql"
        load_query=LOAD_QUERY,
        load_taxo_query=LOAD_TAXO_QUERY
    )

    return config(
        pipeline_args=pipe_arg
    )


def load_embed_config():
    LOAD_QUERY = """
        PREFIX prov: <http://www.w3.org/ns/prov#> 
        PREFIX dct: <http://purl.org/dc/terms/>  
        PREFIX soic: <http://rdfs.org/sioc/ns#>  
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>  

        SELECT DISTINCT ?thing ?part ?text WHERE {
        ?thing a <http://rdf.myexperiment.org/ontologies/base/Submission>; prov:generated/dct:hasPart ?part.    
        ?part soic:content ?text.  
        FILTER NOT EXISTS {  ?thing ext:ingestedBy ext:ml2GrowSmartRegulationsTopicModeler  } } 
        """
    LOAD_QUERY = DEMO_LOAD_QUERY

    # LOAD_QUERY = """PREFIX prov: <http://www.w3.org/ns/prov#>PREFIX besluit:
    # <http://data.vlaanderen.be/ns/besluit#>PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>PREFIX rdfs:
    # <http://www.w3.org/2000/01/rdf-schema#>SELECT ?s ?content WHERE {  ?s a besluit:Besluit ;    prov:value
    # ?content. FILTER (STRLEN(?content) >= 100)} LIMIT 10"""

    pipe_arg = Pipeline_args(
        docker_image="lblod/poc-ai-airflow-embed:latest",
        docker_host="tcp://docker-socket-proxy:2375",
        auto_remove=True,
        network_mode="bridge",
        mount=[
            Mount(target="/data", source="embed-data"),
            Mount(target="/models", source="airflow_model_store")
        ],
        sparql_endpoint="http://192.168.6.152:8892/sparql",  # "https://qa.centrale-vindplaats.lblod.info/sparql"
        load_query=LOAD_QUERY
    )

    return config(
        pipeline_args=pipe_arg
    )
