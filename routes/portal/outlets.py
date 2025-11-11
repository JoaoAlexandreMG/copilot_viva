from flask import Blueprint, render_template, request, session, redirect, url_for, flash, jsonify
from models.models import Outlet
from db.database import get_session
from datetime import datetime
from .decorators import require_authentication


class Pagination:
    """Simple pagination helper to mimic Flask-SQLAlchemy paginate API."""

    def __init__(self, items, page, per_page, total_items):
        self.items = items
        self.page = page
        self.per_page = per_page
        self.total = total_items
        self.pages = (self.total + self.per_page - 1) // self.per_page if self.total else 0

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
        if not self.pages:
            return

        last = 0
        for num in range(1, self.pages + 1):
            if (
                num <= left_edge
                or (self.page - left_current <= num <= self.page + right_current)
                or num > self.pages - right_edge
            ):
                if last + 1 != num:
                    yield None
                yield num
                last = num

outlets_bp = Blueprint("portal_outlets", __name__, url_prefix="/portal_associacao/outlets")


@outlets_bp.route("/", methods=["GET"])
@require_authentication
def get_outlets():
    """
    List all outlets (alias for list_and_create_outlets)
    """
    return list_and_create_outlets()


@outlets_bp.route("", methods=["GET"])
@require_authentication
def list_and_create_outlets():
    """
    List all outlets with pagination (filtered by user client)
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

        base_query = db_session.query(Outlet).filter(Outlet.client == user_client).order_by(Outlet.name.asc())
        total_outlets = base_query.count()

        total_pages = (total_outlets + per_page - 1) // per_page if total_outlets else 0
        if page < 1:
            page = 1
        if total_outlets == 0:
            page = 1
        elif total_pages and page > total_pages:
            page = total_pages

        results = base_query.offset((page - 1) * per_page).limit(per_page).all() if total_outlets else []
        outlets_pagination = Pagination(results, page, per_page, total_outlets)

        return render_template(
            "portal/outlets/outlets.html",
            outlets=outlets_pagination,
            page_type="list"
        )

    except Exception as e:
        print(f"[ERROR] Error listing outlets: {str(e)}")
        flash("Erro ao listar outlets", "error")
        return redirect(url_for("dashboard.render_dashboard"))


@outlets_bp.route("", methods=["POST"])
@require_authentication
def create_outlet():
    """
    Create a new outlet
    """
    try:
        user = session.get("user")
        db_session = get_session()

        def form_bool(field_name, default=False):
            value = request.form.get(field_name)
            if value is None:
                return default
            return str(value).lower() in {"1", "true", "on", "yes"}

        # Create new outlet
        new_outlet = Outlet(
            name=request.form.get("name", ""),
            code=request.form.get("code", ""),
            outlet_type=request.form.get("outlet_type"),
            country=request.form.get("country"),
            state=request.form.get("state"),
            city=request.form.get("city"),
            street=request.form.get("street"),
            address_2=request.form.get("address_2"),
            latitude=request.form.get("latitude"),
            longitude=request.form.get("longitude"),
            client=request.form.get("client"),
            is_key_outlet=form_bool("is_key_outlet"),
            is_smart=form_bool("is_smart"),
            is_active=form_bool("is_active", default=True),
            created_on=datetime.now(),
            created_by=user.get("upn", "system")
        )

        db_session.add(new_outlet)
        db_session.commit()

        flash(f"Outlet {new_outlet.name} criado com sucesso!", "success")
        return redirect(url_for("portal_outlets.list_and_create_outlets"))

    except Exception as e:
        db_session.rollback()
        print(f"[ERROR] Error creating outlet: {str(e)}")
        flash(f"Erro ao criar outlet: {str(e)}", "error")
        return redirect(url_for("portal_outlets.list_and_create_outlets"))


@outlets_bp.route("/search", methods=["GET"])
@require_authentication
def search_outlets():
    """
    Search outlets by name or code (JSON endpoint)
    """
    try:
        query = request.args.get("q", "").strip()

        if not query:
            return jsonify([])

        db_session = get_session()

        # Search by name or code
        search_pattern = f"%{query}%"
        outlets = db_session.query(Outlet).filter(
            (Outlet.name.ilike(search_pattern)) |
            (Outlet.code.ilike(search_pattern))
        ).limit(20).all()

        return jsonify([outlet.to_dict() for outlet in outlets])

    except Exception as e:
        print(f"[ERROR] Error searching outlets: {str(e)}")
        return jsonify([]), 500


@outlets_bp.route("/<string:outlet_code>", methods=["GET"])
@require_authentication
def get_outlet_details(outlet_code):
    """
    Get outlet details by code (JSON endpoint for modal view/edit)
    """
    try:
        db_session = get_session()
        outlet = db_session.query(Outlet).filter(Outlet.code == outlet_code).first()

        if not outlet:
            return jsonify({"error": "Outlet not found"}), 404

        return jsonify(outlet.to_dict())

    except Exception as e:
        print(f"[ERROR] Error fetching outlet: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@outlets_bp.route("/<string:outlet_code>", methods=["PUT", "POST"])
@require_authentication
def update_outlet(outlet_code):
    """
    Update outlet by code
    """
    try:
        user = session.get("user")
        db_session = get_session()
        outlet = db_session.query(Outlet).filter(Outlet.code == outlet_code).first()

        if not outlet:
            flash("Outlet não encontrado", "error")
            return redirect(url_for("portal_outlets.list_and_create_outlets"))

        # Update all fields
        outlet.name = request.form.get("name", outlet.name)
        outlet.code = request.form.get("code", outlet.code)
        outlet.outlet_type = request.form.get("outlet_type", outlet.outlet_type)
        outlet.country = request.form.get("country", outlet.country)
        outlet.state = request.form.get("state", outlet.state)
        outlet.city = request.form.get("city", outlet.city)
        outlet.street = request.form.get("street", outlet.street)
        outlet.address_2 = request.form.get("address_2", outlet.address_2)
        outlet.latitude = request.form.get("latitude", outlet.latitude)
        outlet.longitude = request.form.get("longitude", outlet.longitude)
        outlet.client = request.form.get("client", outlet.client)
        def form_bool(field_name, default=None):
            value = request.form.get(field_name)
            if value is None:
                return default
            return str(value).lower() in {"1", "true", "on", "yes"}

        bool_value = form_bool("is_active")
        if bool_value is not None:
            outlet.is_active = bool_value

        key_outlet_value = form_bool("is_key_outlet")
        if key_outlet_value is not None:
            outlet.is_key_outlet = key_outlet_value

        smart_value = form_bool("is_smart")
        if smart_value is not None:
            outlet.is_smart = smart_value
        outlet.modified_on = datetime.now()
        outlet.modified_by = user.get("upn", "system")

        db_session.commit()
        flash(f"Outlet {outlet.name} atualizado com sucesso!", "success")
        return redirect(url_for("portal_outlets.list_and_create_outlets"))

    except Exception as e:
        db_session.rollback()
        print(f"[ERROR] Error updating outlet: {str(e)}")
        flash(f"Erro ao atualizar outlet: {str(e)}", "error")
        return redirect(url_for("portal_outlets.list_and_create_outlets"))


@outlets_bp.route("/<string:outlet_code>", methods=["DELETE"])
@require_authentication
def delete_outlet(outlet_code):
    """
    Delete outlet by code
    """
    try:
        user = session.get("user")
        db_session = get_session()
        outlet = db_session.query(Outlet).filter(Outlet.code == outlet_code).first()

        if not outlet:
            flash("Outlet não encontrado", "error")
            return redirect(url_for("portal_outlets.list_and_create_outlets"))

        outlet_name = outlet.name
        db_session.delete(outlet)
        db_session.commit()

        flash(f"Outlet {outlet_name} deletado com sucesso!", "success")
        return redirect(url_for("portal_outlets.list_and_create_outlets"))

    except Exception as e:
        db_session.rollback()
        print(f"[ERROR] Error deleting outlet: {str(e)}")
        flash(f"Erro ao deletar outlet: {str(e)}", "error")
        return redirect(url_for("portal_outlets.list_and_create_outlets"))

