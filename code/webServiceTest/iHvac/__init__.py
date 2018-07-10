import os
import logging
from flask import Flask
#from .hvacIssueDBMapping import *
#from . import db
from . import hvacServices
from flask_sqlalchemy import SQLAlchemy
from . import global_s

def create_app(test_config=None):

    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        #DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
        SQLALCHEMY_DATABASE_URI="mysql+mysqldb://ihvac:ihvac@169.236.181.40:3306/HVACIssues",
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    #set the logger config
    logging.basicConfig(filename='hvacIssue.log', level=logging.WARNING,\
    format='%(levelname)s:%(threadName)s:%(asctime)s:%(filename)s:%(funcName)s:%(message)s', datefmt='%m/%d/%Y %H:%M:%S')

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    #db.init_app(app)
    app.register_blueprint(hvacServices.bp)

    global_s.dbConnection = SQLAlchemy(app)

    return app








