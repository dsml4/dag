daghome=$(pwd)/daghome
mkdir -p $daghome
DAGSTER_HOME=$daghome dagit -f dag.py
