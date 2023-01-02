from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from utils import load_bertopic_retrain_conf

cfg = load_bertopic_retrain_conf()

with DAG(dag_id='bertopic-retrain', schedule_interval='0 0 1 * *', default_args=cfg.default_config._asdict(), catchup=False) as dag:

    task_load = KubernetesPodOperator(
        name="abb-bertopic-load",
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

    task_retrain_and_save = KubernetesPodOperator(
        name="abb-bertopic-retrain",
        image=cfg.pipeline_args.image,
        task_id="retrain",
        cmds=[
            "python3",
            "retrain.py"
        ],
        volume_mounts=cfg.pipeline_args.mount
    )

    restart_operator = BashOperator(
        task_id='reboot-topic-api',
        bash_command= 'curl -X POST "http://docker-socket-proxy:2375/containers/lblod-bertopic-api/restart"',

    )

    task_transform = KubernetesPodOperator(
        name="abb-bertopic-transform",
        image=cfg.pipeline_args.image,
        task_id="transform",
        cmds=[
            "python3",
            "transform.py"
        ],
        volume_mounts=cfg.pipeline_args.mount
    )

    task_save_topics = KubernetesPodOperator(
        name="abb-bertopic-save_topics",
        image=cfg.pipeline_args.image,
        task_id="save_topics",
        cmds=[
            "python3",
            "save_topics.py",
            cfg.pipeline_args.sparql_endpoint,
        ],
        volume_mounts=cfg.pipeline_args.mount
    )

    task_save_transform = KubernetesPodOperator(
        name="abb-bertopic-save_transform",
        image=cfg.pipeline_args.image,
        task_id="save_documents",
        cmds=[
            "python3",
            "save_transform.py",
            cfg.pipeline_args.sparql_endpoint,
        ],
        volume_mounts=cfg.pipeline_args.mount
    )

    task_load >> task_retrain_and_save >> restart_operator >> task_transform >> [task_save_transform, task_save_topics]
