"""Skip the Spark test package where PySpark isn't installed (it runs inside the Airflow container)."""

import importlib.util

collect_ignore = [] if importlib.util.find_spec("pyspark") else ["spark"]
