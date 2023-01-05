from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from utils import load_bertopic_retrain_conf

cfg = load_bertopic_retrain_conf()

with DAG(dag_id='bertopic-retrain', schedule_interval='0 0 1 * *', default_args=cfg.default_config._asdict(), catchup=False) as dag:

    task_load = KubernetesPodOperator(
        namespace="abb",
        name="abb-bertopic-load",
        image=cfg.pipeline_args.image,
        task_id="load",
        cmds=[
            "python3",
            "load.py",
            cfg.pipeline_args.sparql_endpoint,
            cfg.pipeline_args.load_query
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount,
        image_pull_policy="Always"
    )

    task_retrain_and_save = KubernetesPodOperator(
        namespace="abb",
        name="abb-bertopic-retrain",
        image=cfg.pipeline_args.image,
        task_id="retrain",
        cmds=[
            "python3",
            "retrain.py"
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    task_restart_api = KubernetesPodOperator(
        namespace="abb",
        name="abb-bertopic-restart-api",
        image=cfg.pipeline_args.image,
        task_id="restart_api",
        cmds=[
            "python3",
            "restart_api.py"
        ]
    )

    task_transform = KubernetesPodOperator(
        namespace="abb",
        name="abb-bertopic-transform",
        image=cfg.pipeline_args.image,
        task_id="transform",
        cmds=[
            "python3",
            "transform.py"
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    task_save_topics = KubernetesPodOperator(
        namespace="abb",
        name="abb-bertopic-save_topics",
        image=cfg.pipeline_args.image,
        task_id="save_topics",
        cmds=[
            "python3",
            "save_topics.py",
            cfg.pipeline_args.sparql_endpoint,
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    task_save_transform = KubernetesPodOperator(
        namespace="abb",
        name="abb-bertopic-save_transform",
        image=cfg.pipeline_args.image,
        task_id="save_documents",
        cmds=[
            "python3",
            "save_transform.py",
            cfg.pipeline_args.sparql_endpoint,
        ],
        volumes=cfg.pipeline_args.volumes,
        volume_mounts=cfg.pipeline_args.mount
    )

    task_load >> task_retrain_and_save >> task_restart_api >> task_transform >> [task_save_transform, task_save_topics]
