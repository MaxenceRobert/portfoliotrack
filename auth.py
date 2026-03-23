import os
import secrets
import datetime
import resend
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from database import (
    create_user, get_user_by_email, get_user_by_id,
    create_email_token, get_email_token, verify_user_email,
    create_reset_token, get_reset_token, invalidate_reset_token,
    update_user_password,
)

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

def send_reset_email(email, token):
    reset_url = f"https://getportfoliotrack.com/reset-password/{token}"
    resend.Emails.send({
        "from": "PortfolioTrack <noreply@getportfoliotrack.com>",
        "to": email,
        "subject": "Réinitialisation de ton mot de passe — PortfolioTrack",
        "html": f"""
        <div style="font-family: sans-serif; max-width: 500px; margin: 0 auto; padding: 2rem;">
            <h2 style="color: #5B5FED;">Réinitialisation de mot de passe</h2>
            <p>Tu as demandé à réinitialiser ton mot de passe. Clique sur le bouton ci-dessous.
               Ce lien est valable <strong>1 heure</strong>.</p>
            <a href="{reset_url}"
               style="display:inline-block; margin-top:1rem; padding: 0.75rem 1.5rem;
                      background:#5B5FED; color:white; border-radius:8px;
                      text-decoration:none; font-weight:600;">
                Réinitialiser mon mot de passe
            </a>
            <p style="margin-top:1.5rem; color:#9CA3AF; font-size:0.85rem;">
                Si tu n'as pas demandé cette réinitialisation, ignore cet email.
                Ton mot de passe ne sera pas modifié.
            </p>
        </div>
        """
    })

@auth_bp.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        email = request.form['email'].strip().lower()
        user  = get_user_by_email(email)
        # Always show the same message to avoid email enumeration
        if user:
            token      = secrets.token_urlsafe(32)
            expires_at = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
            create_reset_token(user['id'], token, expires_at)
            try:
                send_reset_email(email, token)
            except Exception:
                pass  # Fail silently — don't leak whether email exists
        flash(
            'Si cet email est associé à un compte, un lien de réinitialisation a été envoyé.',
            'success'
        )
        return redirect(url_for('auth.forgot_password'))
    return render_template('forgot_password.html')

@auth_bp.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    row = get_reset_token(token)

    if not row:
        flash('Lien invalide ou déjà utilisé.', 'error')
        return redirect(url_for('auth.login'))

    # Check expiry
    expires_at = row['expires_at']
    if isinstance(expires_at, str):
        expires_at = datetime.datetime.fromisoformat(expires_at)
    elif hasattr(expires_at, 'tzinfo') and expires_at.tzinfo:
        expires_at = expires_at.replace(tzinfo=None)

    if datetime.datetime.utcnow() > expires_at:
        flash('Ce lien a expiré. Demande un nouveau lien.', 'error')
        return redirect(url_for('auth.forgot_password'))

    used = row['used']
    if used is True or used == 1:
        flash('Ce lien a déjà été utilisé.', 'error')
        return redirect(url_for('auth.login'))

    if request.method == 'POST':
        password = request.form['password']
        confirm  = request.form['confirm_password']

        if password != confirm:
            flash('Les mots de passe ne correspondent pas.', 'error')
            return redirect(url_for('auth.reset_password', token=token))
        if len(password) < 8:
            flash('Le mot de passe doit faire au moins 8 caractères.', 'error')
            return redirect(url_for('auth.reset_password', token=token))

        new_hash = generate_password_hash(password)
        update_user_password(row['user_id'], new_hash)
        invalidate_reset_token(token)
        flash('Mot de passe mis à jour. Tu peux te connecter.', 'success')
        return redirect(url_for('auth.login'))

    return render_template('reset_password.html', token=token)