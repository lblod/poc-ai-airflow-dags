import os
from typing import NamedTuple

import yaml
from docker.types import Mount

CONFIG_PATH = os.getenv("AIRFLOW_DAG_CONFIG_PATH", "/opt/airflow/git/dags/configs")


# region util_classes
class Default_args(NamedTuple):
    """
    Named tuple that contains the default values for Airflow DAGs
    """
    start_date: str = "2019-01-01 00:00:00"
    owner: str = "Airflow"


class Pipeline_args(NamedTuple):
    """
    Extra information that can be configured in the DAG
    """
    docker_image: str
    docker_host: str
    auto_remove: bool
    network_mode: str
    mount: list
    sparql_endpoint: str
    load_query: str
    load_taxo_query: str = None


class Config(NamedTuple):
    """
    A config object that contains the Airflow DAG config, but also the custom configuration for the airflow.
    """
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
    """
    This function loads the config that is used for the bertopic retrain DAG

    :return: a Pipeline_args object, containing relevant information
    """

    # TODO: remove load query (currently ignored... we are using the demo yaml request --> change that later!)
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

    with open(f'{CONFIG_PATH}/bertopic-retrain.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    pipe_arg = Pipeline_args(
        docker_image=config["docker_image"],
        docker_host=config["docker_host"],
        auto_remove=config["auto_remove"],
        network_mode=config["network_mode"],
        mount=[Mount(target=obj["target"], source=obj["source"]) for obj in config["mounts"]],
        sparql_endpoint=config["sparql_endpoint"],
        load_query=config["load_query"]
    )

    return Config(
        pipeline_args=pipe_arg
    )


def load_bertopic_transform_conf():
    """
    This function loads the config that is used for the bertopic transform DAG

    :return: a Pipeline_args object, containing relevant information
    """

    # TODO: remove load query (currently ignored... we are using the demo yaml request --> change that later!)
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

    with open(f'{CONFIG_PATH}/bertopic-transform.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    pipe_arg = Pipeline_args(
        docker_image=config["docker_image"],
        docker_host=config["docker_host"],
        auto_remove=config["auto_remove"],
        network_mode=config["network_mode"],
        mount=[Mount(target=obj["target"], source=obj["source"]) for obj in config["mounts"]],
        sparql_endpoint=config["sparql_endpoint"],
        load_query=config["load_query"]
    )

    return Config(
        pipeline_args=pipe_arg
    )


def load_ner_config():
    """
    This function loads the config that is used for the ner DAG

    :return: a Pipeline_args object, containing relevant information
    """

    # TODO: remove load query (currently ignored... we are using the demo yaml request --> change that later!)
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

    with open(f'{CONFIG_PATH}/ner.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    pipe_arg = Pipeline_args(
        docker_image=config["docker_image"],
        docker_host=config["docker_host"],
        auto_remove=config["auto_remove"],
        network_mode=config["network_mode"],
        mount=[Mount(target=obj["target"], source=obj["source"]) for obj in config["mounts"]],
        sparql_endpoint=config["sparql_endpoint"],
        load_query=config["load_query"]
    )

    return Config(
        pipeline_args=pipe_arg
    )


def load_zeroshot_config():
    """
    This function loads the config that is used for the zeroshot DAG

    :return: a Pipeline_args object, containing relevant information
    """

    # TODO: remove load query (currently ignored... we are using the demo yaml request --> change that later!)
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
    with open(f'{CONFIG_PATH}/zeroshot.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    pipe_arg = Pipeline_args(
        docker_image=config["docker_image"],
        docker_host=config["docker_host"],
        auto_remove=config["auto_remove"],
        network_mode=config["network_mode"],
        mount=[Mount(target=obj["target"], source=obj["source"]) for obj in config["mounts"]],
        sparql_endpoint=config["sparql_endpoint"],
        load_query=config["load_query"],
        load_taxo_query=config["load_taxo_query"]
    )

    return Config(
        pipeline_args=pipe_arg
    )


def load_embed_config():
    """
    This function loads the config that is used for the bertopic embedding DAG

    :return: a Pipeline_args object, containing relevant information
    """

    # TODO: remove load query (currently ignored... we are using the demo yaml request --> change that later!)
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

    with open(f'{CONFIG_PATH}/embed.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    pipe_arg = Pipeline_args(
        docker_image=config["docker_image"],
        docker_host=config["docker_host"],
        auto_remove=config["auto_remove"],
        network_mode=config["network_mode"],
        mount=[Mount(target=obj["target"], source=obj["source"]) for obj in config["mounts"]],
        sparql_endpoint=config["sparql_endpoint"],
        load_query=config["load_query"]
    )

    return Config(
        pipeline_args=pipe_arg
    )
