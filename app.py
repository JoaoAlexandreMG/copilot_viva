import os
from flask import Flask, render_template, redirect, url_for, request, session
from db.database import get_session, init_db
from models.models import User
from datetime import datetime
from routes.app.users import users_bp
from routes.app.outlet import outlets_bp
from routes.app.assets import assets_bp
from routes.app.smartdevices import smart_devices_bp
from routes.portal.dashboard import dashboard_bp
from routes.inventory import inventory_bp
from routes.portal.users import users_bp as portal_users_bp
from routes.portal.outlets import outlets_bp as portal_outlets_bp
from routes.portal.assets import assets_bp as portal_assets_bp
from routes.portal.smartdevices import smartdevices_bp as portal_smartdevices_bp
from routes.portal.tracking import tracking_bp as portal_tracking_bp, SIMPLE_TRACKING_AUTHORIZED_CLIENTS
from routes.portal.alerts import alerts_bp as portal_alerts_bp
from utils.vision_accounts import create_accounts_for_all_clients
from swagger import register_swagger

app = Flask(__name__, template_folder="templates", static_folder="static", static_url_path="/static")
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key")

# Context processor to make user info available in templates
@app.context_processor
def inject_user():
    """Inject user information into template context"""
    user_data = {}
    client_code = None

    if "user" in session:
        user = session["user"]
        user_data["user"] = user  # Add full user object
        user_data["user_name"] = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        user_data["user_email"] = user.get("email", "")
        client_code = user.get("client")

    # Add function to check if client has access to simple tracking
    user_data["is_simple_tracking_authorized"] = client_code in SIMPLE_TRACKING_AUTHORIZED_CLIENTS

    return user_data


# Keep a safe 'theme' variable available in templates to avoid UndefinedError.
# We intentionally do not inject any default colors here to preserve templates' own fallbacks.
@app.context_processor
def inject_theme():
    """Inject site theme variables into templates with Viva AI brand colors (can be overridden via environment variables)."""
    theme = {
        'primary': os.getenv('THEME_PRIMARY', '#00C3FF'),  # Azul Elétrico
        'primary_dark': os.getenv('THEME_PRIMARY_DARK', '#4C3AFF'),  # Roxo Inteligência
        'accent': os.getenv('THEME_ACCENT', '#00F5B5'),  # Verde Ação
        'secondary': os.getenv('THEME_SECONDARY', '#008CFF'),  # Azul Neon
        'success': os.getenv('THEME_SUCCESS', '#00F5B5'),  # Verde Ação
        'danger': os.getenv('THEME_DANGER', '#FFC93C'),  # Amarelo Atenção
        'warning': os.getenv('THEME_WARNING', '#FFC93C'),  # Amarelo Atenção
        'light': os.getenv('THEME_LIGHT', '#F5F5F7'),
        'darker': os.getenv('THEME_DARKER', '#0A1F44'),  # Azul Profundo
        'border_color': os.getenv('THEME_BORDER', '#D0D0D0'),
        'bg_light': os.getenv('THEME_BG_LIGHT', '#f5f5f5'),
        'text_dark': os.getenv('THEME_TEXT_DARK', '#1C1C1E'),
        'text_gray': os.getenv('THEME_TEXT_GRAY', '#8E8E93'),
        'text_tertiary': os.getenv('THEME_TEXT_TERTIARY', '#C7C7CC'),
        'spacing_xs': os.getenv('THEME_SPACING_XS', '0.5rem'),
        'spacing_sm': os.getenv('THEME_SPACING_SM', '0.75rem'),
        'spacing_md': os.getenv('THEME_SPACING_MD', '1rem'),
        'spacing_lg': os.getenv('THEME_SPACING_LG', '1.25rem'),
        'spacing_xl': os.getenv('THEME_SPACING_XL', '1.5rem'),
        'radius_sm': os.getenv('THEME_RADIUS_SM', '8px'),
        'radius_md': os.getenv('THEME_RADIUS_MD', '12px'),
        'radius_lg': os.getenv('THEME_RADIUS_LG', '16px'),
        'radius': os.getenv('THEME_RADIUS', '12px')
    }
    return {'theme': theme}

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

# Portal blueprints (new portal routes)
app.register_blueprint(dashboard_bp)
app.register_blueprint(portal_users_bp)
app.register_blueprint(portal_outlets_bp)
app.register_blueprint(portal_assets_bp)
app.register_blueprint(portal_smartdevices_bp)
app.register_blueprint(portal_tracking_bp)
app.register_blueprint(portal_alerts_bp)
app.register_blueprint(inventory_bp)

# Register Swagger documentation
register_swagger(app)

# Initialize database and create accounts when app starts
init_db()
db_session = get_session()
created, total = create_accounts_for_all_clients(db_session)
print(f"[INFO] Database initialized. Created {created} accounts out of {total} total.")

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return render_template("login.html")
    try:
        upn = request.form.get("upn", "").strip()
        user_country = request.form.get("country", "").strip().lower()
        check_only = request.form.get("check_only") == "true"
        destination = request.form.get("destination", "assets")
        # Sanitizar o destination para evitar '/Inventory' ou espaços
        if destination:
            destination = destination.strip().lower().lstrip('/')
    
        latitude = request.form.get("latitude")
        longitude = request.form.get("longitude")
        country_code = request.form.get("country_code")
        user_ip = request.form.get("user_ip")

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
                "has_gps": True
            })
            print(f"[INFO] GPS captured for {upn}: {latitude}, {longitude}")
        else:
            session_data["has_gps"] = False
            print(f"[INFO] No GPS for {upn} (desktop or GPS denied)")
            
        # Adicionar outros dados de localização
        if country_code:
            session_data["country_code"] = country_code
        if user_ip:
            session_data["user_ip"] = user_ip

        session["user"] = session_data

        # Update last login
        user.last_login_on = datetime.now()
        db_session.commit()

        # Redirect based on destination
        # Check `inventory` explicitly before `portal` to permit custom inventory redirect
        if destination == "inventory":
            return redirect(url_for("inventory.render_inventory_dashboard"))

        if destination == "portal":
            return redirect(url_for("dashboard.render_dashboard"))
        else:
            # Se cliente tem acesso ao simple tracking, redireciona para lá
            if user.client in SIMPLE_TRACKING_AUTHORIZED_CLIENTS:
                return redirect(url_for("portal_tracking.render_simple_tracking"))
            else:
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
    created, total = create_accounts_for_all_clients(db_session)
    app.run(debug=False, host='0.0.0.0', port=5000)
