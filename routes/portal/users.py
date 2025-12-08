from flask import (
    Blueprint,
    render_template,
    request,
    session,
    redirect,
    url_for,
    flash,
    jsonify,
)
from models.models import User
from db.database import get_session
from datetime import datetime
from sqlalchemy import or_, func
from .decorators import require_authentication


class Pagination:
    """Lightweight pagination helper compatible with template expectations."""

    def __init__(self, items, page, per_page, total_items):
        self.items = items
        self.page = page
        self.per_page = per_page
        self.total = total_items

        # Precompute pages to avoid repeated math operations
        self.pages = (
            (self.total + self.per_page - 1) // self.per_page if self.total else 0
        )

    @property
    def has_prev(self):
        return self.page > 1

    @property
    def prev_num(self):
        return self.page - 1 if self.has_prev else None

    @property
    def has_next(self):
        return self.page < self.pages

    @property
    def next_num(self):
        return self.page + 1 if self.has_next else None

    def iter_pages(self, left_edge=2, left_current=2, right_current=2, right_edge=2):
        """Yield page numbers following Flask-SQLAlchemy style."""
        if not self.pages:
            return

        last = 0
        for num in range(1, self.pages + 1):
            in_left_edge = num <= left_edge
            in_left_current = self.page - num <= left_current and self.page >= num
            in_right_current = num - self.page <= right_current and num >= self.page
            in_right_edge = num > self.pages - right_edge

            if in_left_edge or in_left_current or in_right_current or in_right_edge:
                if last + 1 != num:
                    yield None
                yield num
                last = num


users_bp = Blueprint("portal_users", __name__, url_prefix="/portal_associacao/users")


@users_bp.route("/", methods=["GET"])
@require_authentication
def list_and_create_users():
    """
    List all users with pagination (filtered by user client)
    """
    try:
        db_session = get_session()

        # Get user client from session
        user_client = session.get("user", {}).get("client")
        if not user_client:
            flash("Client não encontrado na sessão", "error")
            return redirect(url_for("dashboard.render_dashboard"))

        # Get page number from query parameter
        page = request.args.get("page", 1, type=int) or 1
        per_page = 10

        # Query users filtered by client and build pagination object manually
        base_query = (
            db_session.query(User)
            .filter(User.client == user_client)
            .order_by(User.first_name.asc(), User.last_name.asc())
        )
        total_users = base_query.count()

        # Clamp page number within valid bounds
        total_pages = (total_users + per_page - 1) // per_page if total_users else 0
        if page < 1:
            page = 1
        if total_users == 0:
            page = 1
        elif total_pages and page > total_pages:
            page = total_pages

        results = (
            base_query.offset((page - 1) * per_page).limit(per_page).all()
            if total_users
            else []
        )
        users_pagination = Pagination(results, page, per_page, total_users)

        return render_template(
            "portal/users/users.html", users=users_pagination, page_type="list"
        )

    except Exception as e:
        print(f"[ERROR] Error listing users: {str(e)}")
        flash("Erro ao listar usuários", "error")
        return redirect(url_for("dashboard.render_dashboard"))


@users_bp.route("/", methods=["POST"])
@require_authentication
def create_user():
    """
    Create a new user
    """
    try:
        user = session.get("user")
        if not user or not user.get("client"):
            flash("Sessão inválida. Por favor, faça login novamente.", "error")
            return redirect(url_for("auth.login"))
        
        db_session = get_session()

        # Validate required fields
        required_fields = {
            "upn": "UPN",
            "user_name": "Nome de Usuário",
            "first_name": "Nome",
            "last_name": "Sobrenome",
            "role": "Função",
        }

        missing_fields = []
        for field, label in required_fields.items():
            if not request.form.get(field, "").strip():
                missing_fields.append(label)

        if missing_fields:
            flash(f"Campos obrigatórios: {', '.join(missing_fields)}", "error")
            return redirect(url_for("portal_users.list_and_create_users"))

        # Ensure UPN is provided
        upn = request.form.get("upn", "").strip()

        # Check if UPN already exists
        existing_user = db_session.query(User).filter(User.upn == upn).first()
        if existing_user:
            flash("Já existe um usuário com este UPN.", "error")
            return redirect(url_for("portal_users.list_and_create_users"))

        # Create new user
        new_user = User(
            first_name=request.form.get("first_name", ""),
            last_name=request.form.get("last_name", ""),
            user_name=request.form.get("user_name", ""),
            email=request.form.get("email", ""),
            upn=upn,
            phone=request.form.get("phone"),
            role=request.form.get("role"),
            reporting_manager=request.form.get("reporting_manager"),
            country=request.form.get("country"),
            responsible_country=request.form.get("responsible_country"),
            sales_organization=request.form.get("sales_organization"),
            sales_office=request.form.get("sales_office"),
            sales_group=request.form.get("sales_group"),
            sales_territory=request.form.get("sales_territory"),
            teleselling_territory=request.form.get("teleselling_territory"),
            bd_territory_name=request.form.get("bd_territory_name"),
            ca_territory_name=request.form.get("ca_territory_name"),
            mc_territory_name=request.form.get("mc_territory_name"),
            p1_territory_name=request.form.get("p1_territory_name"),
            p2_territory_name=request.form.get("p2_territory_name"),
            p3_territory_name=request.form.get("p3_territory_name"),
            p4_territory_name=request.form.get("p4_territory_name"),
            p5_territory_name=request.form.get("p5_territory_name"),
            ncb_territory_name=request.form.get("ncb_territory_name"),
            preferred_notification_type=request.form.get("preferred_notification_type"),
            client=user.get("client"),
            is_active=True,
            created_on=datetime.now(),
            created_by=user.get("upn", "system"),
        )

        db_session.add(new_user)
        db_session.commit()

        flash(f"Usuário {new_user.first_name} criado com sucesso!", "success")
        return redirect(url_for("portal_users.list_and_create_users"))

    except Exception as e:
        db_session.rollback()
        print(f"[ERROR] Error creating user: {str(e)}")
        flash(f"Erro ao criar usuário: {str(e)}", "error")
        return redirect(url_for("portal_users.list_and_create_users"))


@users_bp.route("/<string:upn>", methods=["GET"])
@require_authentication
def get_user_details(upn):
    """
    Get user details by ID (JSON endpoint for modal view/edit)
    """
    try:
        db_session = get_session()
        user_obj = db_session.query(User).filter(User.upn == upn).first()

        if not user_obj:
            return jsonify({"error": "User not found"}), 404

        return jsonify(user_obj.to_dict())

    except Exception as e:
        print(f"[ERROR] Error fetching user: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@users_bp.route("/<string:upn>", methods=["PUT", "POST", "DELETE"])
@require_authentication
def manage_user(upn):
    """
    Manage user by ID (Update or Delete)
    Handles PUT, DELETE and POST with _method override
    """
    try:
        print(
            f"[DEBUG] manage_user called for {upn}. Method: {request.method}, Form: {dict(request.form)}"
        )

        # Check for DELETE method override
        is_delete = False
        if request.method == "DELETE":
            is_delete = True
        elif request.method == "POST" and request.form.get("_method") == "DELETE":
            is_delete = True

        db_session = get_session()
        user_obj = db_session.query(User).filter(User.upn == upn).first()

        if not user_obj:
            print(f"[ERROR] User not found: {upn}")
            flash("Usuário não encontrado", "error")
            return redirect(url_for("portal_users.list_and_create_users"))

        if is_delete:
            # DELETE LOGIC
            user_name = f"{user_obj.first_name} {user_obj.last_name}"
            print(f"[DEBUG] Deleting user: {user_name} (UPN: {upn})")

            db_session.delete(user_obj)
            db_session.commit()

            print(f"[SUCCESS] User {user_name} deleted successfully")
            flash(f"Usuário {user_name} deletado com sucesso!", "success")
            return redirect(url_for("portal_users.list_and_create_users"))

        else:
            # UPDATE LOGIC
            user = session.get("user")
            if not user or not user.get("client"):
                flash("Sessão inválida. Por favor, faça login novamente.", "error")
                return redirect(url_for("auth.login"))

            # Update all fields
            user_obj.first_name = request.form.get("first_name", user_obj.first_name)
            user_obj.last_name = request.form.get("last_name", user_obj.last_name)
            user_obj.user_name = request.form.get("user_name", user_obj.user_name)
            user_obj.email = request.form.get("email", user_obj.email)
            user_obj.phone = request.form.get("phone", user_obj.phone)
            user_obj.role = request.form.get("role", user_obj.role)
            user_obj.reporting_manager = request.form.get(
                "reporting_manager", user_obj.reporting_manager
            )
            user_obj.country = request.form.get("country", user_obj.country)
            user_obj.responsible_country = request.form.get(
                "responsible_country", user_obj.responsible_country
            )
            user_obj.sales_organization = request.form.get(
                "sales_organization", user_obj.sales_organization
            )
            user_obj.sales_office = request.form.get(
                "sales_office", user_obj.sales_office
            )
            user_obj.sales_group = request.form.get("sales_group", user_obj.sales_group)
            user_obj.sales_territory = request.form.get(
                "sales_territory", user_obj.sales_territory
            )
            user_obj.teleselling_territory = request.form.get(
                "teleselling_territory", user_obj.teleselling_territory
            )
            user_obj.bd_territory_name = request.form.get(
                "bd_territory_name", user_obj.bd_territory_name
            )
            user_obj.ca_territory_name = request.form.get(
                "ca_territory_name", user_obj.ca_territory_name
            )
            user_obj.mc_territory_name = request.form.get(
                "mc_territory_name", user_obj.mc_territory_name
            )
            user_obj.p1_territory_name = request.form.get(
                "p1_territory_name", user_obj.p1_territory_name
            )
            user_obj.p2_territory_name = request.form.get(
                "p2_territory_name", user_obj.p2_territory_name
            )
            user_obj.p3_territory_name = request.form.get(
                "p3_territory_name", user_obj.p3_territory_name
            )
            user_obj.p4_territory_name = request.form.get(
                "p4_territory_name", user_obj.p4_territory_name
            )
            user_obj.p5_territory_name = request.form.get(
                "p5_territory_name", user_obj.p5_territory_name
            )
            user_obj.ncb_territory_name = request.form.get(
                "ncb_territory_name", user_obj.ncb_territory_name
            )
            user_obj.preferred_notification_type = request.form.get(
                "preferred_notification_type", user_obj.preferred_notification_type
            )
            user_obj.client = user.get("client")

            new_upn = request.form.get("upn", user_obj.upn)
            user_obj.upn = new_upn
            user_obj.modified_on = datetime.now()
            user_obj.modified_by = user.get("upn", "system")

            db_session.commit()
            flash(f"Usuário {user_obj.first_name} atualizado com sucesso!", "success")
            return redirect(url_for("portal_users.list_and_create_users"))

    except Exception as e:
        db_session.rollback()
        print(f"[ERROR] Error managing user: {str(e)}")
        import traceback

        traceback.print_exc()
        flash(f"Erro ao processar usuário: {str(e)}", "error")
        return redirect(url_for("portal_users.list_and_create_users"))


@users_bp.route("/search", methods=["GET"])
@require_authentication
def search_users():
    """
    Search users by name, email, or UPN (JSON endpoint)
    Supports multi-word searches - all words must match somewhere
    """
    try:
        query = request.args.get("q", "").strip()

        # Validate input
        if not query or len(query) < 2:
            return jsonify([])

        # Prevent overly long queries
        if len(query) > 100:
            query = query[:100]

        db_session = get_session()
        user_client = session.get("user", {}).get("client")

        if not user_client:
            print(f"[SEARCH] ERROR: No client in session")
            return jsonify([])

        print(f"[SEARCH] Query: '{query}' | Client: {user_client}")

        # Split query into parts for multi-word search
        query_parts = query.split()

        # Start with base query filtered by client
        base_query = db_session.query(User).filter(User.client == user_client)

        # Crie uma "coluna virtual" concatenando First + Last name
        # coalesce transforma NULL em string vazia '' para evitar que a concatenação resulte em NULL
        full_name = func.concat(
            func.coalesce(User.first_name, ""), " ", func.coalesce(User.last_name, "")
        )

        # For each word in the query, it must match in at least one field
        for part in query_parts:
            search_pattern = f"%{part}%"
            part_filter = or_(
                User.upn.ilike(search_pattern),
                User.email.ilike(search_pattern),
                User.first_name.ilike(search_pattern),
                User.last_name.ilike(search_pattern),
                User.user_name.ilike(search_pattern),
                # Adiciona a busca na concatenação do nome completo
                full_name.ilike(search_pattern),
            )
            base_query = base_query.filter(part_filter)

        # Execute query
        users = (
            base_query.order_by(User.first_name.asc(), User.last_name.asc())
            .limit(50)
            .all()
        )

        print(f"[SEARCH] Found {len(users)} users")
        if users:
            print(f"[SEARCH] First result: {users[0].first_name} {users[0].last_name}")

        return jsonify([user_obj.to_dict() for user_obj in users])

    except Exception as e:
        print(f"[ERROR] Error searching users: {str(e)}")
        import traceback

        traceback.print_exc()
        return jsonify([]), 500
