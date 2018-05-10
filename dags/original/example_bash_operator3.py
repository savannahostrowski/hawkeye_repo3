# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_params = [
    {
        "label": "Configurations",
        "order": 1,
        "params": [
            {
                "order": 1,
                "controlType": "dropdown",
                "label": "Sample Dropdown param",
                "key": "PARAM_DROPDOWN",
                "value": "drop down value",
                "required": True,
                "options": [{"key": "item1", "value": "value1"},
                            {"key": "item2", "value": "value2"}
                            ],
                "help": "this message will be displayed below the form item"
            },
            {
                "order": 2,
                "controlType": "textbox",
                "key": "SOFTWARE_VERSION",
                "value": "2.7.2",
                "required": True,
                "label": "Software Version",
                "help": "software version. ex: 0.0.1, 0.0.1-SNAPSHOT, 0.0.1-RELEASE. "
                        "See https://github.geo.apple.com/maps-osm/osm-integrity"
            },
            {
                "order": 3,
                "controlType": "checkbox",
                "key": "CHECK_BOX",
                "value": False,
                "required": True,
                "label": "Check This Out",
                "help": "this is a sample check box"
            },
        ]
    }
]

args = {
    'owner': 'airflow',
    'start_date': datetime(2016, 6, 29),
}

dag = DAG(
    dag_id='example_bash_operator3',
    default_args=args,
    input_params=default_params,
    dagrun_timeout=timedelta(minutes=60))

task1 = BashOperator(
    task_id='task1',
    bash_command="""
    echo "drop down value is $PARAM_DROPDOWN"
    echo "this should also work {{ params.PARAM_DROPDOWN }}"
    """,
    dag=dag)

task2 = BashOperator(
    task_id='task2', bash_command='echo 2', dag=dag)

task3 = BashOperator(
    task_id='task3', bash_command='echo 3', dag=dag)

task4 = BashOperator(
    task_id='task4', bash_command='echo 4', dag=dag)

# === build DAG relations ===

task1 >> task2
task1 >> task3
task2 >> task4
task3 >> task4

if __name__ == "__main__":
    dag.cli()
