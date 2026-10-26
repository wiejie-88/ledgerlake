"""Skip test packages whose runtime isn't installed: Spark and Airflow tests run inside the Airflow container."""

import importlib.util

collect_ignore = [
    package
    for package, module in (("spark", "pyspark"), ("dags", "airflow"))
    if importlib.util.find_spec(module) is None
]
