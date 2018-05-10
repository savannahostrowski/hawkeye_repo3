
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.artifact.TaskArtifactDependency import TaskArtifactDependency
from osmflow.plugins.operators.report_operator import ReportOperator

# default system environment variables.
# Only keys in this dictionary will be available to tasks.
default_env = {
    'DATABASE_USER': 'osm',
    'DATABASE_PWD': 'osm',
}
# variables here will be available in web UI for user to overwrite.
default_params = {
    # If True, any TaskArtifactDependency reference missing from runtime params will
    # be provided with default artifacts from the database
    # If False, an AirflowException will be raised and the task will fail.
    'RESOLVE_UNMET_DEPENDENCIES': 'False'
}

# DAG default arguments
default_args = {
    'owner': 'pat',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 2),
    'email': ['maps-osm@apple.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'env': default_env
}

dag = DAG('artifacts_management_example3',
          schedule_interval='@once',
          params=default_params,
          default_args=default_args)

CYM_atlas = TaskArtifactDependency(type='atlas',
                                   tag_filter={
                                       "country": "CYM",
                                       "state": "validated"
                                   },
                                   reference='cym_atlas',
                                   required_local=True
                                   )


corrections_config = TaskArtifactDependency(type='config',
                                            tag_filter={
                                                "state": "validated",
                                                "component": "integrity",
                                                "subcomponent": "corrections"
                                            },
                                            reference="corrections_config",
                                            required_local=True)

# Dependencies for task atlas_counts
# required_local=True will trigger hooks to attempt to parse any resolved artifact URIs
# and download supported artifacts locally
log4j_properties = TaskArtifactDependency(type='config',
                                          tag_filter={
                                              "state": "validated",
                                              "component": "atlas",
                                              "subcomponent": "log4j"
                                          },
                                          reference='log4j_properties',
                                          required_local=True
                                          )

atlas = TaskArtifactDependency(type='atlas',
                               tag_filter={
                                   "state": "validated"
                               },
                               reference="atlas",
                               required_local=True)

atlas_jar = TaskArtifactDependency(type='software',
                                   tag_filter={
                                       "component": "atlas",
                                       "classifier": "shaded",
                                       "state": "validated"
                                   },
                                   reference='atlas_jar',
                                   required_local=True)

# ws output holder
feature_counts_output = "{{ ws }}/featureCounts/output.txt"

# Use artifacts.get_workspace_uri(ref) to refer to the location of the locally downloaded
# remote artifact

atlas_counts = BashOperator(
    task_id='atlas_counts',
    artifact_dependencies=[atlas_jar, atlas, log4j_properties],
    bash_command='java -cp {{ artifacts.get_workspace_uri("atlas_jar") }}'
                 ' -Dlog4j.configuration=file:{{ artifacts.get_workspace_uri("log4j_properties") }} '
                 ' org.openstreetmap.atlas.utilities.runtime.FlexibleCommand featureCounts'
                 ' -input={{ artifacts.get_workspace_uri("atlas") }}'
                 ' -output=' + feature_counts_output,
    dag=dag
)

# Use the defined output string above to report the artifact produced upstream.
# This can also be achieved by registering the artifact, currently only available in the
# Python Operator
report_atlas_counts_output = ReportOperator(
    task_id='report_atlas_counts',
    dag=dag,
    payload=[{
        'type': 'log',
        'uri': feature_counts_output,
        'checksum': "undefined"
    }])

# Dependency for task echo_atlas_counts uses the artifact reported in
# report_atlas_counts_output
# use_upstream=True flag here causes deferred validation, and searches for artifacts
# produced by upstream tasks in the same dag_run, that comply with the tag filter.
txt_file = TaskArtifactDependency(type='log',
                                  tag_filter={
                                      "state": "new"
                                  },
                                  reference='output_text',
                                  use_upstream=True)
# Use artifacts.get_uri(ref) to refer to the original uri. This ca
# n be local or remote!
echo_atlas_counts = BashOperator(
    task_id='echo_atlas_counts',
    artifact_dependencies=txt_file,
    bash_command='cat {{ artifacts.get_uri("output_text") }} ',
    dag=dag
)

# set the dependencies
atlas_counts >> report_atlas_counts_output >> echo_atlas_counts
