import os
from flask import Flask, render_template, redirect, url_for, request, session
from werkzeug.middleware.proxy_fix import ProxyFix
from db.database import get_session, init_db, Session
from models.models import User
from datetime import datetime
import time
from sqlalchemy.exc import TimeoutError, OperationalError
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
from routes.portal.tracking import (
    tracking_bp as portal_tracking_bp,
    SIMPLE_TRACKING_AUTHORIZED_CLIENTS,
)
from routes.portal.alerts import alerts_bp as portal_alerts_bp
from routes.admin import admin_bp
from routes.admin_dashboard import admin_dashboard_bp
from utils.vision_accounts import create_accounts_for_all_clients

# Clients autorizados para usar a seção de Inventário (case-insensitive)
INVENTORY_AUTHORIZED_CLIENTS = {c.lower() for c in ("Redbull",)}

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static",
)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key")

# ADICIONAR ESTE BLOCO LOGO APÓS A CRIAÇÃO DO APP
@app.teardown_appcontext
def shutdown_session(exception=None):
    """
    Garante que a sessão do banco seja removida ao final de cada request.
    Isso previne 'Idle' e 'Idle in Transaction'.
    """
    Session.remove()

# Fix for running behind a proxy (HTTPS -> HTTP)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)


# Context processor to make user info available in templates
@app.context_processor
def inject_user():
    """Inject user information into template context"""
    user_data = {
        "is_inventory_authorized": False,
        "is_simple_tracking_authorized": False,
        "is_inventory_client_admin": False,
        "is_inventory_technician": False,
    }
    client_code = None
    role = None

    if "user" in session:
        user = session["user"]
        user_data["user"] = user  # Add full user object
        user_data["user_name"] = (
            f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        )
        user_data["user_email"] = user.get("email", "")
        client_code = user.get("client")
        role = user.get("role")

    # Add function to check if client has access to simple tracking
    user_data["is_simple_tracking_authorized"] = (
        client_code in SIMPLE_TRACKING_AUTHORIZED_CLIENTS
    )
    # Flag para controle de acesso ao Inventário (usado em templates)
    user_data["is_inventory_authorized"] = (
        isinstance(client_code, str)
        and client_code.lower() in INVENTORY_AUTHORIZED_CLIENTS
    )
    # Flags de role específicas para Inventário
    user_data["is_inventory_client_admin"] = role == "Client Admin_Inventory"
    user_data["is_inventory_technician"] = role == "Technician_inventory"

    return user_data


# Keep a safe 'theme' variable available in templates to avoid UndefinedError.
# We intentionally do not inject any default colors here to preserve templates' own fallbacks.
@app.context_processor
def inject_theme():
    """Inject site theme variables into templates with Viva AI brand colors (can be overridden via environment variables)."""
    theme = {
        "primary": os.getenv("THEME_PRIMARY", "#00C3FF"),  # Azul Elétrico
        "primary_dark": os.getenv("THEME_PRIMARY_DARK", "#4C3AFF"),  # Roxo Inteligência
        "accent": os.getenv("THEME_ACCENT", "#00F5B5"),  # Verde Ação
        "secondary": os.getenv("THEME_SECONDARY", "#008CFF"),  # Azul Neon
        "success": os.getenv("THEME_SUCCESS", "#00F5B5"),  # Verde Ação
        "danger": os.getenv("THEME_DANGER", "#FFC93C"),  # Amarelo Atenção
        "warning": os.getenv("THEME_WARNING", "#FFC93C"),  # Amarelo Atenção
        "light": os.getenv("THEME_LIGHT", "#F5F5F7"),
        "darker": os.getenv("THEME_DARKER", "#0A1F44"),  # Azul Profundo
        "border_color": os.getenv("THEME_BORDER", "#D0D0D0"),
        "bg_light": os.getenv("THEME_BG_LIGHT", "#f5f5f5"),
        "text_dark": os.getenv("THEME_TEXT_DARK", "#1C1C1E"),
        "text_gray": os.getenv("THEME_TEXT_GRAY", "#8E8E93"),
        "text_tertiary": os.getenv("THEME_TEXT_TERTIARY", "#C7C7CC"),
        "spacing_xs": os.getenv("THEME_SPACING_XS", "0.5rem"),
        "spacing_sm": os.getenv("THEME_SPACING_SM", "0.75rem"),
        "spacing_md": os.getenv("THEME_SPACING_MD", "1rem"),
        "spacing_lg": os.getenv("THEME_SPACING_LG", "1.25rem"),
        "spacing_xl": os.getenv("THEME_SPACING_XL", "1.5rem"),
        "radius_sm": os.getenv("THEME_RADIUS_SM", "8px"),
        "radius_md": os.getenv("THEME_RADIUS_MD", "12px"),
        "radius_lg": os.getenv("THEME_RADIUS_LG", "16px"),
        "radius": os.getenv("THEME_RADIUS", "12px"),
    }
    return {"theme": theme}


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


# Middleware para restringir rotas de acordo com roles de Inventário
@app.before_request
def enforce_inventory_role_restrictions():
    """
    Garante que:
    - Usuários com role 'Client Admin_Inventory' só acessem rotas /inventory/...;
    - Usuários com role 'Technician_inventory' só acessem /inventory/operation
      (e endpoints auxiliares com esse prefixo).
    """
    # Ignora requests de static, login e logout
    path = request.path or ""
    if path.startswith("/static"):
        return
    if path == "/" or path.startswith("/api-docs"):
        return
    if path == "/logout":
        return

    user = session.get("user")
    if not user:
        return

    role = user.get("role")

    # Client Admin_Inventory: apenas rotas de inventário (qualquer /inventory/...)
    if role == "Client Admin_Inventory":
        if not path.startswith("/inventory"):
            return redirect(url_for("inventory.render_inventory_list"))

    # Technician_inventory: apenas operação de inventário (/inventory/operation...)
    if role == "Technician_inventory":
        # Permite apenas operação e seus endpoints auxiliares
        allowed_prefixes = (
            "/inventory/operation",
            "/inventory/check-asset",
            # Adicione outros endpoints auxiliares necessários aqui
        )
        if not any(path.startswith(prefix) for prefix in allowed_prefixes):
            return redirect(url_for("inventory.render_inventory_operation"))


# Helper function for database retry logic with exponential backoff
def retry_db_operation(operation, max_retries=3):
    """
    Executa uma operação de banco com retry automático
    Args:
        operation: função que executa a operação no banco
        max_retries: número máximo de tentativas
    Returns:
        Resultado da operação ou None se todas as tentativas falharem
    """
    for attempt in range(max_retries):
        try:
            return operation()
        except (TimeoutError, OperationalError) as e:
            if attempt < max_retries - 1:
                # Backoff exponencial: 0.5s, 1s, 2s
                wait_time = 0.5 * (2**attempt)
                print(f"[WARN] DB attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                print(f"[WARN] Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                # Última tentativa falhou, re-raise a exceção
                print(
                    f"[ERROR] DB operation failed after {max_retries} attempts: {str(e)}"
                )
                raise


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

# Admin blueprints (API endpoints)
app.register_blueprint(admin_bp)
app.register_blueprint(admin_dashboard_bp)

# Register Swagger documentation
register_swagger(app)


# Health check endpoint (CRÍTICO para uptime - usado por load balancer)
@app.route("/health", methods=["GET"])
def health():
    """Endpoint de health check para monitoramento de uptime"""
    from health_check import HealthCheck

    checks = HealthCheck.full_check()

    # Return 200 se status geral for ok
    status_code = 200 if checks["overall"] == "healthy" else 503
    return checks, status_code


# Initialize database and create accounts when app starts
init_db()
db_session = get_session()
created, total = create_accounts_for_all_clients(db_session)
print(f"[INFO] Database initialized. Created {created} accounts out of {total} total.")


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return render_template("login.html")

    db_session = None
    upn = ""
    try:
        upn = request.form.get("upn", "").strip()
        user_country = request.form.get("country", "").strip().lower()
        check_only = request.form.get("check_only") == "true"
        destination = request.form.get("destination", "assets")
        # Sanitizar o destination para evitar '/Inventory' ou espaços
        if destination:
            destination = destination.strip().lower().lstrip("/")

        latitude = request.form.get("latitude")
        longitude = request.form.get("longitude")
        country_code = request.form.get("country_code")
        user_ip = request.form.get("user_ip")

        db_session = get_session()

        # Função auxiliar para buscar usuário com retry
        def fetch_user():
            db_session.commit()  # Ensure any pending transactions are committed
            return db_session.query(User).filter(User.upn.ilike(upn)).first()

        # Executar busca de usuário com retry automático
        user = retry_db_operation(fetch_user, max_retries=3)

        # Force refresh the user object from database
        if user:
            db_session.refresh(user)

        print(f"[DEBUG] Searching for UPN: '{upn}'")
        print(f"[DEBUG] User found: {user is not None}")
        if user:
            print(
                f"[DEBUG] User details - UPN: '{user.upn}', Client: '{user.client}', Role: '{user.role}'"
            )

            # Double-check with raw SQL to verify database content
            from sqlalchemy import text

            def fetch_raw_result():
                return db_session.execute(
                    text(
                        "SELECT upn, client, role FROM users WHERE LOWER(upn) = LOWER(:upn)"
                    ),
                    {"upn": upn},
                ).fetchone()

            raw_result = retry_db_operation(fetch_raw_result, max_retries=3)

            if raw_result:
                print(
                    f"[DEBUG] Raw SQL result - UPN: '{raw_result[0]}', Client: '{raw_result[1]}', Role: '{raw_result[2]}'"
                )
                # If raw SQL shows a client but ORM doesn't, use the raw result
                if raw_result[1] and not user.client:
                    print(
                        f"[DEBUG] ORM/Raw discrepancy detected! Using raw SQL client value: '{raw_result[1]}'"
                    )
                    user.client = raw_result[1]
                    db_session.commit()
            else:
                print("[DEBUG] No raw SQL result found")

        if not user:
            print(f"[ERROR] User not found: {upn}")
            return render_template(
                "login.html", error="Usuário não encontrado ou inativo", upn=upn
            )

        if not user.client:
            print(f"[ERROR] User has no client: {upn}")
            return render_template(
                "login.html", error="Usuário não tem client associado", upn=upn
            )

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
            "role": user.role,
        }

        # Adicionar dados de localização se disponíveis (mobile)
        if latitude and longitude:
            session_data.update(
                {
                    "latitude": float(latitude),
                    "longitude": float(longitude),
                    "has_gps": True,
                }
            )
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
        def update_last_login():
            user.last_login_on = datetime.now()
            db_session.commit()

        retry_db_operation(update_last_login, max_retries=3)

        # Técnicos de inventário: ao logar, abrir diretamente a tela de operação
        # já com o modal de adicionar asset aberto. Isso não afeta outros roles.
        if user.role == "Technician_inventory":
            return redirect(url_for("inventory.render_inventory_operation", open="add"))

        # Redirect based on destination
        # Check `inventory` explicitly before `portal` to permit custom inventory redirect
        if destination == "inventory":
            # Só permite ir para Inventário se o client estiver autorizado.
            if user.client and user.client.lower() in INVENTORY_AUTHORIZED_CLIENTS:
                return redirect(
                    url_for("inventory.render_inventory_operation", open="add")
                )
            # Se não estiver autorizado, cai no fluxo normal abaixo (portal/tracking/assets)

        if destination == "portal":
            return redirect(url_for("dashboard.render_dashboard"))
        else:
            # Se cliente tem acesso ao simple tracking, redireciona para lá
            if user.client in SIMPLE_TRACKING_AUTHORIZED_CLIENTS:
                return redirect(url_for("portal_tracking.render_simple_tracking"))
            else:
                return redirect(url_for("assets.list_assets"))

    except (TimeoutError, OperationalError) as e:
        # Erros de banco de dados - retornar 503 Service Unavailable
        print(f"[ERROR] Database timeout/unavailable: {str(e)}")
        return (
            render_template(
                "login.html",
                error="Serviço temporariamente indisponível. Por favor tente novamente em alguns segundos.",
                upn=upn,
            ),
            503,
        )

    except Exception as e:
        print(f"[ERROR] Login error: {str(e)}")
        import traceback

        traceback.print_exc()
        return render_template("login.html", error="Erro interno do servidor", upn=upn)

    finally:
        # Sempre fechar a sessão para liberar a conexão do pool
        if db_session:
            try:
                db_session.close()
            except Exception as e:
                print(f"[WARN] Error closing DB session: {str(e)}")


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
    app.run(debug=False, host="0.0.0.0", port=5001)
