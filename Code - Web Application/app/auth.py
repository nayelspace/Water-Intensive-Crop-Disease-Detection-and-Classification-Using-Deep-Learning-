from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_user, logout_user, login_required
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy.exc import IntegrityError
from .models import User
from . import db
import logging

logger = logging.getLogger(__name__)
auth = Blueprint('auth', __name__)

@auth.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        user = User.query.filter_by(email=email).first()
        
        if user and check_password_hash(user.password, password):
            login_user(user)
            return redirect(url_for('main.index'))
        else:
            flash('Invalid login credentials')
    return render_template('login.html')

@auth.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        name = request.form.get('name')
        email = request.form.get('email')
        password = request.form.get('password')

        try:
            if User.query.filter_by(email=email).first():
                flash('Email already exists.')
                return redirect(url_for('auth.register'))

            hashed_password = generate_password_hash(password, method='pbkdf2:sha256')
            new_user = User(name=name, email=email, password=hashed_password)
            db.session.add(new_user)
            db.session.commit()

            flash('Registration successful! Please login.')
            return redirect(url_for('auth.login'))
        except IntegrityError:
            db.session.rollback()
            flash('A user with this email already exists.')
        except Exception as e:
            db.session.rollback()
            logger.error(f"Failed to register user {email}: {e}")
            flash('An unexpected error occurred during registration. Please try again.')
    return render_template('register.html')

@auth.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login')) 
