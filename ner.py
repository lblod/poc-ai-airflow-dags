from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from utils import load_ner_config

cfg = load_ner_config()

with DAG(dag_id='ner', schedule_interval='0 0 * * *', default_args=cfg.default_config._asdict(), catchup=False) as dag:
    task_load = KubernetesPodOperator(
        name="ner-load",
        image=cfg.pipeline_args.image,
        task_id="load",
        cmds=[
            "python3",
            "load.py",
            cfg.pipeline_args.sparql_endpoint,
            cfg.pipeline_args.load_query
        ],
        volume_mounts=cfg.pipeline_args.mount
    )

    task_ner = KubernetesPodOperator(
        name="ner-transform-ner",
        image=cfg.pipeline_args.image,
        task_id="ner",
        cmds=[
            "python3",
            "ner.py"
        ],
        volume_mounts=cfg.pipeline_args.mount
    )

    task_save = KubernetesPodOperator(
        name="ner-save",
        image=cfg.pipeline_args.image,
        task_id="save",
        cmds=[
            "python3",
            "save.py",
            cfg.pipeline_args.sparql_endpoint
        ],
        volume_mounts=cfg.pipeline_args.mount
    )

    task_load >> task_ner >> task_save
