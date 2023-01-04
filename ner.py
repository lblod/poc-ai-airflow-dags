from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from utils.utils import load_ner_config

cfg = load_ner_config()

with DAG(dag_id='ner', schedule_interval='0 0 * * *', default_args=cfg.default_config._asdict(), catchup=False) as dag:
    task_load = KubernetesPodOperator(
        namespace="abb",
        name="ner-load",
        image=cfg.pipeline_args.image,
        task_id="load",
        cmds=[
            "python3",
            "load.py",
            cfg.pipeline_args.sparql_endpoint,
            cfg.pipeline_args.load_query
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    task_ner = KubernetesPodOperator(
        namespace="abb",
        name="ner-transform-ner",
        image=cfg.pipeline_args.image,
        task_id="ner",
        cmds=[
            "python3",
            "ner.py"
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    task_save = KubernetesPodOperator(
        namespace="abb",
        name="ner-save",
        image=cfg.pipeline_args.image,
        task_id="save",
        cmds=[
            "python3",
            "save.py",
            cfg.pipeline_args.sparql_endpoint
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    task_load >> task_ner >> task_save
