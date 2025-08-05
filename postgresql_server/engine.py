from sqlalchemy import create_engine


engine = create_engine('postgresql+psycopg2://manuel94:mypassword@localhost:5432/td_db')  # Uncomment for SQLite
