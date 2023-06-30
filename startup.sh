source env/bin/activate
pipreqs --savepath=requirements.in && pip-compile

export PYTHONPATH=data2bots_elt/common:$PYTHONPATH
export PYTHONPATH=data2bots_elt/dags:$PYTHONPATH
export PYTHONPATH=data2bots_elt/dbt-dag:$PYTHONPATH
export PYTHONPATH=data2bots_elt/pipeline:$PYTHONPATH
