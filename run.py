from utils.helpers import pretty_all_jsons

from pipelines import (
    stripe_to_file,
    sqlite_to_file,
    file_to_postgres,
)

if __name__ == '__main__':

    # sqlite
    sqlite_to_file.run_sqlite_to_file_pipeline()

    # Stripe
    stripe_to_file.run_stripe_to_file_pipeline()

    # File to BQ
    file_to_postgres.run_file_to_postgress_pipeline()

    pretty_all_jsons()