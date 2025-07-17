from utils.helpers import pretty_all_jsons

from pipelines import (
    stripe_to_file,
    sqlite_to_file,
    file_to_bq,
    file_to_postgres,
)

import time

if __name__ == '__main__':

    # Stripe
    stripe_to_file.run_stripe_to_file_pipeline()

    # sqlite
    sqlite_to_file.run_sqlite_to_file_pipeline()

    # File to BQ
    file_to_postgres.run_file_to_bq_pipeline()

    pretty_all_jsons()