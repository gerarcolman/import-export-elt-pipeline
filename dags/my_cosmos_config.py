from cosmos import ProfileConfig, ProjectConfig
from cosmos.config import ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode
from pathlib import Path

from pendulum import datetime

DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name='import_export',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/include/dbt/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path="/usr/local/airflow/include/dbt/"
)

DBT_EXECUTION_CONFIG = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt"
)

DBT_RENDER_CONFIG = RenderConfig(
    load_method=LoadMode.DBT_LS,
    select=['path:models', 'path:seeds']
)