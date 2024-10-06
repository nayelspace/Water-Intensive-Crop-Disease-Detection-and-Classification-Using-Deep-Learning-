from flask import Flask, render_template, flash, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from .config import Config
import logging
from google.cloud import storage
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from google.cloud.sql.connector import Connector, IPTypes
import pymysql
import google.cloud.logging

db = SQLAlchemy() 

# Initialize login manager outside the create_app function to be accessible across the app
login_manager = LoginManager()

# Set up Google Cloud Logging
def setup_logging():
    client = google.cloud.logging.Client()
    client.setup_logging()
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    """
    Initializes a connection pool for a Cloud SQL instance of MySQL.

    Uses the Cloud SQL Python Connector package.
    """
    connector = Connector(IPTypes.PUBLIC)

    def getconn() -> pymysql.connections.Connection:
        return connector.connect(
            Config.DB_CONN_NAME,
            "pymysql",
            user=Config.DB_USER,
            password=Config.DB_PASS,
            db=Config.DB_NAME
        )

    pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn,
    )
    return pool

def create_app():
    app = Flask(__name__)
    app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024
    app.config.from_object(Config)

    # Configure logging
    setup_logging()

    # Use the Cloud SQL Connector to establish a connection engine
    #engine = connect_with_connector()

    # Set the SQLALCHEMY_DATABASE_URI
    #app.config['SQLALCHEMY_DATABASE_URI'] = str(engine.url)

    # Initialize extensions with the app instance
    db.init_app(app)

    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'

    # Use the Cloud SQL Connector to establish a connection pool
    app.config['SQLALCHEMY_DATABASE_URI'] = Config.SQLALCHEMY_DATABASE_URI

    # Import models to ensure they are known to SQLAlchemy
    from .models import User, Field, Batch, Image

    # User loader callback for Flask-Login
    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))
    
    @login_manager.unauthorized_handler
    def unauthorized_callback():
        flash('Please log in to access this page.')
        return redirect(url_for('auth.login'))

    # Error handlers
    @app.errorhandler(404)
    def not_found_error(error):
        return render_template('404.html'), 404

    @app.errorhandler(500)
    def internal_error(error):
        db.session.rollback()
        return render_template('500.html'), 500

    # Register blueprints for different parts of the app
    from .auth import auth as auth_blueprint
    app.register_blueprint(auth_blueprint, url_prefix='/auth')

    from .main import main as main_blueprint
    app.register_blueprint(main_blueprint, url_prefix='/main')

    return app

app = create_app()

if __name__ == '__main__':
    app.run()