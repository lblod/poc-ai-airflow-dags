from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from utils import load_bertopic_transform_conf


cfg = load_bertopic_transform_conf()

with DAG(dag_id='bertopic-transform', schedule_interval='0 0 * * *', default_args=cfg.default_config._asdict(), catchup=False) as dag:

    task_load = KubernetesPodOperator(
        name="abb-embed-bertopic-load",
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

    task_transform = KubernetesPodOperator(
        name="abb-embed-bertopic-transform",
        image=cfg.pipeline_args.image,
        task_id="transform",
        cmds=[
            "python3",
            "transform.py"
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    task_save = KubernetesPodOperator(
        name="abb-embed-bertopic-save",
        image=cfg.pipeline_args.image,
        task_id="save",
        cmds=[
            "python3",
            "save_transform.py",
            cfg.pipeline_args.sparql_endpoint
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    task_load >> task_transform >> task_save
