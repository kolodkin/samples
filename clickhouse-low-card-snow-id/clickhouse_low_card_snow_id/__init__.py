from .lowcardinality_benchmark import main as lowcardinality_main
from .snowflake_id_benchmark import main as snowflake_main


def main():
    lowcardinality_main()
    snowflake_main()
