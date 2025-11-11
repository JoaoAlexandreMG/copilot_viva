import os
from flask import Flask, render_template, redirect, url_for, request, session
from db.database import get_session, init_db
from models.models import Asset, Outlet, SmartDevice, User
from datetime import datetime
from routes.app.users import users_bp
from routes.app.outlet import outlets_bp
from routes.app.assets import assets_bp
from routes.app.smartdevices import smart_devices_bp
from routes.app.google_accounts import google_accounts_bp
from routes.portal.dashboard import dashboard_bp
from routes.portal.users import users_bp as portal_users_bp
from routes.portal.outlets import outlets_bp as portal_outlets_bp
from routes.portal.assets import assets_bp as portal_assets_bp
from routes.portal.smartdevices import smartdevices_bp as portal_smartdevices_bp
from routes.portal.tracking import tracking_bp as portal_tracking_bp
from routes.portal.auth import auth_bp as portal_auth_bp
from utils.google_accounts import create_google_accounts_for_all_clients
from utils.excel_to_db import insert_or_update_users_from_excel, insert_or_update_outlets_from_excel, insert_or_update_assets_from_excel, insert_or_update_smartdevices_from_excel, insert_or_update_health_events_from_excel
from sqlalchemy import func

app = Flask(__name__, template_folder="templates", static_folder="static", static_url_path="/static")
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key")

# Context processor to make user info available in templates
@app.context_processor
def inject_user():
    """Inject user information into template context"""
    user_data = {}
    if "user" in session:
        user = session["user"]
        user_data["user"] = user  # Add full user object
        user_data["user_name"] = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        user_data["user_email"] = user.get("email", "")
    return user_data

# Middleware to handle _method override for PUT/DELETE via HTML forms
@app.before_request
def handle_method_override():
    """
    Allow HTML forms to override HTTP method using _method parameter.
    This is useful for supporting PUT and DELETE via HTML forms.
    """
    if request.method == "POST":
        method_override = request.form.get("_method", "").upper()
        if method_override in ["PUT", "DELETE", "PATCH"]:
            request.environ["REQUEST_METHOD"] = method_override

# Registrar blueprints - eles já têm seus próprios url_prefix
# App blueprints (legacy routes)
app.register_blueprint(users_bp)
app.register_blueprint(outlets_bp)
app.register_blueprint(assets_bp)
app.register_blueprint(smart_devices_bp)
app.register_blueprint(google_accounts_bp)

# Portal blueprints (new portal routes)
app.register_blueprint(dashboard_bp)
app.register_blueprint(portal_users_bp)
app.register_blueprint(portal_outlets_bp)
app.register_blueprint(portal_assets_bp)
app.register_blueprint(portal_smartdevices_bp)
app.register_blueprint(portal_tracking_bp)
app.register_blueprint(portal_auth_bp)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return render_template("login.html")
    try:
        upn = request.form.get("upn", "").strip()
        user_country = request.form.get("country", "").strip().lower()
        check_only = request.form.get("check_only") == "true"
        destination = request.form.get("destination", "assets")
        
        # Capturar dados de localização (quando disponíveis - mobile)
        latitude = request.form.get("latitude")
        longitude = request.form.get("longitude")
        gps_accuracy = request.form.get("gps_accuracy")
        country_code = request.form.get("country_code")
        user_ip = request.form.get("user_ip")
        device_info = request.form.get("device_info")

        print(f"[DEBUG] Login attempt - UPN: {upn}, Country: '{user_country}', Check: {check_only}, Dest: {destination}")
        print(f"[DEBUG] Location data - GPS: {latitude},{longitude}, Country: {country_code}, IP: {user_ip}")

        db_session = get_session()

        # Find user by UPN (case-insensitive)
        user = db_session.query(User).filter(User.upn.ilike(upn)).first()

        if not user:
            print(f"[ERROR] User not found: {upn}")
            return render_template("login.html",
                                 error="Usuário não encontrado ou inativo",
                                 upn=upn)

        if not user.client:
            print(f"[ERROR] User has no client: {upn}")
            return render_template("login.html",
                                 error="Usuário não tem client associado",
                                 upn=upn)

        # If this is just a check request, return success (no error means user is valid)
        if check_only:
            print(f"[DEBUG] Check-only request successful for: {upn}")
            return "", 200

        # Use user's country from DB if not provided via form
        if not user_country and user.country:
            user_country = user.country.lower()
            print(f"[DEBUG] Using user's DB country: {user_country}")

        print(f"[DEBUG] Creating session for user: {upn}, country: {user_country}")

        # Store user info in session with location data
        session_data = {
            "upn": user.upn,
            "user_country": user_country,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "client": user.client,
            "email": user.email,
            "role": user.role
        }
        
        # Adicionar dados de localização se disponíveis (mobile)
        if latitude and longitude:
            session_data.update({
                "latitude": float(latitude),
                "longitude": float(longitude),
                "gps_accuracy": float(gps_accuracy) if gps_accuracy else None,
                "has_gps": True
            })
            print(f"[INFO] GPS captured for {upn}: {latitude}, {longitude} (accuracy: {gps_accuracy}m)")
        else:
            session_data["has_gps"] = False
            print(f"[INFO] No GPS for {upn} (desktop or GPS denied)")
            
        # Adicionar outros dados de localização
        if country_code:
            session_data["country_code"] = country_code
        if user_ip:
            session_data["user_ip"] = user_ip
        if device_info:
            try:
                import json
                session_data["device_info"] = json.loads(device_info)
            except:
                pass

        session["user"] = session_data

        # Update last login
        user.last_login_on = datetime.now()
        db_session.commit()

        print(f"[DEBUG] Session created successfully for {upn}")
        print(f"[DEBUG] Redirecting to destination: {destination}")

        # Redirect based on destination
        if destination == "portal":
            print(f"[DEBUG] Redirecting to portal dashboard")
            return redirect(url_for("dashboard.render_dashboard"))
        else:
            # Default to assets
            print(f"[DEBUG] Redirecting to assets")
            return redirect(url_for("assets.list_assets"))

    except Exception as e:
        print(f"[ERROR] Login error: {str(e)}")
        import traceback
        traceback.print_exc()
        return render_template("login.html",
                             error="Erro interno do servidor",
                             upn=upn)

@app.route("/logout", methods=["POST"])
def logout():
    """
    Logout user - clear session
    """
    session.clear()
    return redirect(url_for("index"))

if __name__ == "__main__":
    init_db()
    db_session = get_session()
    created, total = create_google_accounts_for_all_clients(db_session)
    app.run(debug=True, host='0.0.0.0', port=5000)
