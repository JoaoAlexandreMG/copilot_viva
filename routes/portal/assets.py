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
from models.models import Asset
from db.database import get_session
from datetime import datetime
from .decorators import require_authentication

assets_bp = Blueprint("portal_assets", __name__, url_prefix="/portal_associacao/assets")


class Pagination:
    """Lightweight pagination helper compatible with template expectations."""

    def __init__(self, items, page, per_page, total_items):
        self.items = items
        self.page = page
        self.per_page = per_page
        self.total = total_items
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


@assets_bp.route("/", methods=["GET"])
@require_authentication
def list_and_create_assets():
    """
    List all assets with pagination (filtered by user client)
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

        base_query = (
            db_session.query(Asset)
            .filter(Asset.client == user_client)
            .order_by(Asset.bottler_equipment_number.asc())
        )
        total_assets = base_query.count()

        total_pages = (total_assets + per_page - 1) // per_page if total_assets else 0
        if page < 1:
            page = 1
        if total_assets == 0:
            page = 1
        elif total_pages and page > total_pages:
            page = total_pages

        results = (
            base_query.offset((page - 1) * per_page).limit(per_page).all()
            if total_assets
            else []
        )
        assets_pagination = Pagination(results, page, per_page, total_assets)

        return render_template(
            "portal/assets/assets.html", assets=assets_pagination, page_type="list"
        )

    except Exception as e:
        print(f"[ERROR] Error listing assets: {str(e)}")
        flash("Erro ao listar assets", "error")
        return redirect(url_for("dashboard.render_dashboard"))


@assets_bp.route("/", methods=["POST"])
@require_authentication
def create_asset():
    """
    Create a new asset
    """
    try:
        user = session.get("user")
        if not user or not user.get("client"):
            flash("Sessão inválida. Por favor, faça login novamente.", "error")
            return redirect(url_for("auth.login"))
        
        db_session = get_session()

        # Validate required field
        oem_serial_number = request.form.get("oem_serial_number")
        if not oem_serial_number:
            flash("O campo 'OEM Serial Number' é obrigatório.", "error")
            return redirect(url_for("portal_assets.list_and_create_assets"))

        # Check for duplicate OEM serial number
        existing_asset = (
            db_session.query(Asset)
            .filter(Asset.oem_serial_number == oem_serial_number)
            .first()
        )
        if existing_asset:
            flash(
                f"Já existe um asset com o OEM Serial Number '{oem_serial_number}'.",
                "error",
            )
            return redirect(url_for("portal_assets.list_and_create_assets"))

        def form_bool(name):
            return str(request.form.get(name, "false")).lower() in {
                "1",
                "true",
                "on",
                "yes",
            }

        # Create new asset
        new_asset = Asset(
            bottler_equipment_number=request.form.get("bottler_equipment_number", ""),
            asset_type=request.form.get("asset_type"),
            oem_serial_number=oem_serial_number,
            technical_id=request.form.get("technical_id"),
            category=request.form.get("category"),
            outlet=request.form.get("outlet"),
            outlet_code=request.form.get("outlet_code"),
            outlet_type=request.form.get("outlet_type"),
            store_location=request.form.get("store_location"),
            client=user.get("client"),
            sub_client=request.form.get("sub_client"),
            trade_channel=request.form.get("trade_channel"),
            sales_organization=request.form.get("sales_organization"),
            sales_office=request.form.get("sales_office"),
            city=request.form.get("city"),
            state=request.form.get("state"),
            country=request.form.get("country"),
            time_zone=request.form.get("time_zone"),
            latitude=request.form.get("latitude"),
            longitude=request.form.get("longitude"),
            is_smart=form_bool("is_smart"),
            is_missing=form_bool("is_missing"),
            is_competition=form_bool("is_competition"),
            is_factory_asset=form_bool("is_factory_asset"),
            is_vision=form_bool("is_vision"),
            prime_position=form_bool("prime_position"),
            created_on=datetime.now(),
            created_by=user.get("upn", "system"),
        )

        db_session.add(new_asset)
        db_session.commit()

        flash(
            f"Asset {new_asset.bottler_equipment_number} criado com sucesso!", "success"
        )
        return redirect(url_for("portal_assets.list_and_create_assets"))

    except Exception as e:
        db_session.rollback()
        print(f"[ERROR] Error creating asset: {str(e)}")
        flash(f"Erro ao criar asset: {str(e)}", "error")
        return redirect(url_for("portal_assets.list_and_create_assets"))


@assets_bp.route("/search", methods=["GET"])
@require_authentication
def search_assets():
    """
    Search assets by equipment number or serial (JSON endpoint)
    """
    try:
        query = request.args.get("q", "").strip()

        if not query:
            return jsonify([])

        db_session = get_session()

        # Search by bottler_equipment_number or oem_serial_number
        search_pattern = f"%{query}%"
        assets = (
            db_session.query(Asset)
            .filter(
                (Asset.bottler_equipment_number.ilike(search_pattern))
                | (Asset.oem_serial_number.ilike(search_pattern))
            )
            .limit(20)
            .all()
        )

        return jsonify([asset.to_dict() for asset in assets])

    except Exception as e:
        print(f"[ERROR] Error searching assets: {str(e)}")
        return jsonify([]), 500


@assets_bp.route("/<string:oem_serial>", methods=["GET"])
@require_authentication
def get_asset_details(oem_serial):
    """
    Get asset details by OEM serial (JSON endpoint for modal view/edit)
    """
    try:
        db_session = get_session()
        asset = (
            db_session.query(Asset)
            .filter(Asset.oem_serial_number == oem_serial)
            .first()
        )

        if not asset:
            return jsonify({"error": "Asset not found"}), 404

        return jsonify(asset.to_dict())

    except Exception as e:
        print(f"[ERROR] Error fetching asset: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@assets_bp.route("/<string:oem_serial>", methods=["PUT", "POST", "DELETE"])
@require_authentication
def manage_asset(oem_serial):
    """
    Manage asset by OEM serial (Update or Delete)
    Handles PUT, DELETE and POST with _method override
    """
    try:
        # Check for DELETE method override
        is_delete = False
        if request.method == "DELETE":
            is_delete = True
        elif request.method == "POST" and request.form.get("_method") == "DELETE":
            is_delete = True

        db_session = get_session()
        asset = (
            db_session.query(Asset)
            .filter(Asset.oem_serial_number == oem_serial)
            .first()
        )

        if not asset:
            flash("Asset não encontrado", "error")
            return redirect(url_for("portal_assets.list_and_create_assets"))

        if is_delete:
            # DELETE LOGIC
            asset_name = (
                asset.bottler_equipment_number or f"Asset #{asset.oem_serial_number}"
            )
            db_session.delete(asset)
            db_session.commit()

            flash(f"Asset {asset_name} deletado com sucesso!", "success")
            return redirect(url_for("portal_assets.list_and_create_assets"))

        else:
            # UPDATE LOGIC
            user = session.get("user")

            def form_bool(name):
                return str(request.form.get(name, "false")).lower() in {
                    "1",
                    "true",
                    "on",
                    "yes",
                }

            # Update all fields
            asset.bottler_equipment_number = request.form.get(
                "bottler_equipment_number", asset.bottler_equipment_number
            )
            asset.asset_type = request.form.get("asset_type", asset.asset_type)
            asset.oem_serial_number = request.form.get(
                "oem_serial_number", asset.oem_serial_number
            )
            asset.technical_id = request.form.get("technical_id", asset.technical_id)
            asset.category = request.form.get("category", asset.category)
            asset.outlet = request.form.get("outlet", asset.outlet)
            asset.outlet_code = request.form.get("outlet_code", asset.outlet_code)
            asset.outlet_type = request.form.get("outlet_type", asset.outlet_type)
            asset.store_location = request.form.get(
                "store_location", asset.store_location
            )
            asset.client = user.get("client")
            asset.sub_client = request.form.get("sub_client", asset.sub_client)
            asset.trade_channel = request.form.get("trade_channel", asset.trade_channel)
            asset.sales_organization = request.form.get(
                "sales_organization", asset.sales_organization
            )
            asset.sales_office = request.form.get("sales_office", asset.sales_office)
            asset.city = request.form.get("city", asset.city)
            asset.state = request.form.get("state", asset.state)
            asset.country = request.form.get("country", asset.country)
            asset.time_zone = request.form.get("time_zone", asset.time_zone)

            lat = request.form.get("latitude")
            lon = request.form.get("longitude")
            if lat is not None and lat != "":
                asset.latitude = lat
            if lon is not None and lon != "":
                asset.longitude = lon

            asset.is_smart = form_bool("is_smart")
            asset.is_missing = form_bool("is_missing")
            asset.is_competition = form_bool("is_competition")
            asset.is_factory_asset = form_bool("is_factory_asset")
            asset.is_vision = form_bool("is_vision")
            asset.prime_position = form_bool("prime_position")

            asset.modified_on = datetime.now()
            asset.modified_by = user.get("upn", "system")

            db_session.commit()
            flash(
                f"Asset {asset.bottler_equipment_number} atualizado com sucesso!",
                "success",
            )
            return redirect(url_for("portal_assets.list_and_create_assets"))

    except Exception as e:
        db_session.rollback()
        print(f"[ERROR] Error managing asset: {str(e)}")
        flash(f"Erro ao processar asset: {str(e)}", "error")
        return redirect(url_for("portal_assets.list_and_create_assets"))
