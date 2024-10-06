from . import db
from flask_login import UserMixin
from datetime import datetime

class User(UserMixin, db.Model):
    __tablename__ = 'User'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)
    datetime = db.Column(db.DateTime, default=datetime.utcnow)
    fields = db.relationship('Field', backref='user', lazy=True)
    batches = db.relationship('Batch', backref='user', lazy=True)

class Field(db.Model):
    __tablename__ = 'Field'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    code = db.Column(db.String(50))
    datetime = db.Column(db.DateTime, default=datetime.utcnow)
    user_id = db.Column(db.Integer, db.ForeignKey('User.id'), nullable=False)
    batches = db.relationship('Batch', backref='field', lazy=True)

class Batch(db.Model):
    __tablename__ = 'Batch'
    id = db.Column(db.Integer, primary_key=True)
    img_qty = db.Column(db.Integer, nullable=False)
    x_grid = db.Column(db.Integer, nullable=False)
    y_grid = db.Column(db.Integer, nullable=False)
    date_taken = db.Column(db.Date)
    datetime = db.Column(db.DateTime, default=datetime.utcnow)
    user_id = db.Column(db.Integer, db.ForeignKey('User.id'), nullable=False)
    field_id = db.Column(db.Integer, db.ForeignKey('Field.id'), nullable=False)
    images = db.relationship('Image', backref='batch', lazy=True)

class Image(db.Model):
    __tablename__ = 'Image'
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(100), nullable=False)
    path = db.Column(db.String(200), nullable=False)
    label = db.Column(db.String(100), nullable=False, default='no')
    healthy = db.Column(db.Float, nullable=True)
    rice_blast = db.Column(db.Float, nullable=True)
    brown_spot = db.Column(db.Float, nullable=True)
    order = db.Column(db.Integer)
    date_taken = db.Column(db.Date)
    datetime = db.Column(db.DateTime, default=datetime.utcnow)
    batch_id = db.Column(db.Integer, db.ForeignKey('Batch.id'), nullable=False)