from typing import NamedTuple
from airflow.models import Variable
from kubernetes.client import models as k8s
import yaml

def get_config_path():
    config_path = Variable.get("CONFIG_PATH", default_var="/opt/airflow/dags/repo/configs")
    print("==="*25, f"\n{config_path}\n", "==="*25)
    return config_path


class DefaultArgs(NamedTuple):
    """
    Named tuple that contains the default values for Airflow DAGs
    """
    start_date: str = "2019-01-01 00:00:00"
    owner: str = "Airflow"


class PipelineArgs(NamedTuple):
    """
    Extra information that can be configured in the DAG
    """
    image: str
    mount: list
    volumes: list
    sparql_endpoint: str
    load_query: str
    load_taxo_query: str = None


class Config(NamedTuple):
    """
    A config object that contains the Airflow DAG config, but also the custom configuration for the airflow.
    """
    pipeline_args: PipelineArgs
    default_config: DefaultArgs = DefaultArgs()


def create_config_from_yaml(file_name: str):
    with open(f'{get_config_path()}/{file_name}') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    pipe_arg = PipelineArgs(
        image=config["image"],
        volumes=[k8s.V1Volume(name=obj["source"], persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=obj["source"])) for obj in config["mounts"]],
        mount=[k8s.V1VolumeMount(mount_path=f"/{obj['target']}", name=obj["source"]) for obj in config["mounts"]],
        sparql_endpoint=Variable.get("SPARQL_ENDPOINT", default_var=config["sparql_endpoint"]),
        load_query=config["load_query"],
        load_taxo_query=config.get("load_taxo_query")
    )

    return Config(
        pipeline_args=pipe_arg
    )


def load_bertopic_retrain_conf():
    """
    This function loads the config that is used for the bertopic retrain DAG

    :return: a Pipeline_args object, containing relevant information
    """

    return create_config_from_yaml("bertopic-retrain.yaml")


def load_bertopic_transform_conf():
    """
    This function loads the config that is used for the bertopic transform DAG

    :return: a Pipeline_args object, containing relevant information
    """

    return create_config_from_yaml("bertopic-transform.yaml")


def load_ner_config():
    """
    This function loads the config that is used for the ner DAG

    :return: a Pipeline_args object, containing relevant information
    """

    return create_config_from_yaml("ner.yaml")


def load_zeroshot_config():
    """
    This function loads the config that is used for the zeroshot DAG

    :return: a Pipeline_args object, containing relevant information
    """

    return create_config_from_yaml("zeroshot.yaml")


def load_embed_config():
    """
    This function loads the config that is used for the bertopic embedding DAG

    :return: a Pipeline_args object, containing relevant information
    """

    return create_config_from_yaml("embed.yaml")
