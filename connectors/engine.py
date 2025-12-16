from .configs.auth_to_db import *
from sqlalchemy import create_engine

ch_engine = create_engine(
    url = ConnectCH.url
)