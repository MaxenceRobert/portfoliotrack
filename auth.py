from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from database import create_user, get_user_by_email, get_user_by_id

# ── Objet User requis par Flask-Login ───────────────────────────────────────
class User(UserMixin):
    def __init__(self, id, email):
        self.id    = id
        self.email = email

def get_user_object(user_id):
    """Charge un User depuis la DB — utilisé par Flask-Login automatiquement."""
    row = get_user_by_id(user_id)
    if row:
        return User(row['id'], row['email'])
    return None

# ── Blueprint = mini-module Flask pour grouper les routes auth ──────────────
auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        email    = request.form['email'].strip().lower()
        password = request.form['password']
        confirm  = request.form['confirm_password']

        if password != confirm:
            flash('Les mots de passe ne correspondent pas.', 'error')
            return redirect(url_for('auth.register'))

        if len(password) < 8:
            flash('Le mot de passe doit faire au moins 8 caractères.', 'error')
            return redirect(url_for('auth.register'))

        hashed = generate_password_hash(password)
        success = create_user(email, hashed)

        if not success:
            flash('Cet email est déjà utilisé.', 'error')
            return redirect(url_for('auth.register'))

        flash('Compte créé ! Tu peux te connecter.', 'success')
        return redirect(url_for('auth.login'))

    return render_template('register.html')


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email    = request.form['email'].strip().lower()
        password = request.form['password']
        row      = get_user_by_email(email)

        if not row or not check_password_hash(row['password_hash'], password):
            flash('Email ou mot de passe incorrect.', 'error')
            return redirect(url_for('auth.login'))

        user = User(row['id'], row['email'])
        login_user(user)
        return redirect(url_for('main.dashboard'))

    return render_template('login.html')


@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))