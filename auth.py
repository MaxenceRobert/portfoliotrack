import os
import secrets
import resend
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from database import create_user, get_user_by_email, get_user_by_id, create_email_token, get_email_token, verify_user_email

resend.api_key = os.environ.get('RESEND_API_KEY')

class User(UserMixin):
    def __init__(self, id, email):
        self.id    = id
        self.email = email

def get_user_object(user_id):
    row = get_user_by_id(user_id)
    if row:
        return User(row['id'], row['email'])
    return None

def send_verification_email(email, token):
    verification_url = f"https://getportfoliotrack.com/verify-email/{token}"
    resend.Emails.send({
        "from": "PortfolioTrack <noreply@getportfoliotrack.com>",
        "to": email,
        "subject": "Vérifiez votre adresse email — PortfolioTrack",
        "html": f"""
        <div style="font-family: sans-serif; max-width: 500px; margin: 0 auto; padding: 2rem;">
            <h2 style="color: #5B5FED;">Bienvenue sur PortfolioTrack 👋</h2>
            <p>Clique sur le bouton ci-dessous pour confirmer ton adresse email et activer ton compte.</p>
            <a href="{verification_url}"
               style="display:inline-block; margin-top:1rem; padding: 0.75rem 1.5rem;
                      background:#5B5FED; color:white; border-radius:8px;
                      text-decoration:none; font-weight:600;">
                Vérifier mon email
            </a>
            <p style="margin-top:1.5rem; color:#9CA3AF; font-size:0.85rem;">
                Si tu n'as pas créé de compte, ignore cet email.
            </p>
        </div>
        """
    })

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

        hashed  = generate_password_hash(password)
        success = create_user(email, hashed)
        if not success:
            flash('Cet email est déjà utilisé.', 'error')
            return redirect(url_for('auth.register'))

        user = get_user_by_email(email)
        token = secrets.token_urlsafe(32)
        create_email_token(user['id'], token)

        try:
            send_verification_email(email, token)
            flash('Compte créé ! Vérifie ta boîte mail pour activer ton compte.', 'success')
        except Exception:
            flash('Compte créé mais l\'email de vérification n\'a pas pu être envoyé. Contacte le support.', 'error')

        return redirect(url_for('auth.login'))

    return render_template('register.html')

@auth_bp.route('/verify-email/<token>')
def verify_email(token):
    row = get_email_token(token)
    if not row:
        flash('Lien de vérification invalide ou expiré.', 'error')
        return redirect(url_for('auth.login'))
    verify_user_email(row['user_id'])
    flash('Email vérifié ✓ Tu peux maintenant te connecter.', 'success')
    return redirect(url_for('auth.login'))

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email    = request.form['email'].strip().lower()
        password = request.form['password']
        row      = get_user_by_email(email)

        if not row or not check_password_hash(row['password_hash'], password):
            flash('Email ou mot de passe incorrect.', 'error')
            return redirect(url_for('auth.login'))

        if not row.get('email_verified'):
            flash('Vérifie ton email avant de te connecter. Vérifie tes spams.', 'error')
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