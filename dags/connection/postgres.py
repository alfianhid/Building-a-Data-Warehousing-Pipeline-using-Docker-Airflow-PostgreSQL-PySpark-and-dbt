import os
from logger.logger import logger
# from dotenv import dotenv_values
from sqlalchemy import create_engine


# Set environment config
# CONFIG = dotenv_values(".env")
# if not CONFIG:
#     CONFIG = os.environ
    

# Create PostgreSQL connection
@logger
def connect_to_db():
    print("Connecting to database...")
    connection_uri = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        # CONFIG["POSTGRES_USER"],
        # CONFIG["POSTGRES_PASSWORD"],
        # CONFIG["POSTGRES_HOST"],
        # CONFIG["POSTGRES_PORT"],
        # CONFIG["POSTGRES_DB"],
        os.getenv("POSTGRES_USER"),
        os.getenv("POSTGRES_PASSWORD"),
        os.getenv("POSTGRES_HOST"),
        os.getenv("POSTGRES_PORT"),
        os.getenv("POSTGRES_DB"),
    )

    engine = create_engine(connection_uri, pool_pre_ping=True)
    engine.connect()
    return engine