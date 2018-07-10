from flask import current_app, g
from flask.cli import with_appcontext
from flask_sqlalchemy import SQLAlchemy


def getDatabaseConnection(databaseString):
    """Attempt connection to the database"""
    
    sqlsession = None

    try:
        sqlengine = sqlalchemy.create_engine(databaseString)
        SQLSession = sessionmaker(bind=sqlengine)
        sqlsession = SQLSession()

        print("Connection to " + databaseString + " successfull")
        logging.info("Connection to " + databaseString + " successfull")
    except Exception as e:
        logging.error("Error in connection to the database")
        logging.error(traceback.format_exc())
        print("Error in connection to the database")

    return sqlsession


def get_db():
    if 'db' not in g:

        #connectionString = current_app.config['SQLALCHEMY_DATABASE_URI']
        #g.db = getDatabaseConnection(connectionString)

        g.db = SQLAlchemy(current_app)

        '''
        g.db = sqlite3.connect(
            current_app.config['DATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row
        '''

    return g.db


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()


def init_app(app):
    app.teardown_appcontext(close_db)
    g.db = SQLAlchemy(app)
    #app.cli.add_command(get_db)