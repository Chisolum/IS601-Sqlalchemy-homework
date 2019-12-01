from sqlalchemy import create_engine

engine = create_engine('sqlite:Sqlite-Data/sqlite3.db')
engine.connect()