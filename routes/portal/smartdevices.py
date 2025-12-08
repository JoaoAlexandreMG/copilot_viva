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
import re
from sqlalchemy import func
from models.models import SmartDevice
from db.database import get_session
from datetime import datetime
from .decorators import require_authentication


def normalize_mac_no_sep(mac):
    """Normalize MAC address by stripping separators, returning 12 uppercase hex digits or None if invalid."""
    if not mac:
        return None
    cleaned = re.sub(r"[^0-9A-Fa-f]", "", mac)
    cleaned = cleaned.strip().upper()
    if len(cleaned) != 12:
        return None
    return cleaned


class Pagination:
    """Minimal pagination helper mirroring Flask-SQLAlchemy paginate API."""

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


smartdevices_bp = Blueprint(
    "portal_smartdevices", __name__, url_prefix="/portal_associacao/smartdevices"
)


@smartdevices_bp.route("/", methods=["GET"])
@require_authentication
def list_and_create_smartdevices():
    """
    List all smart devices with pagination (filtered by user client)
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
            db_session.query(SmartDevice)
            .filter(SmartDevice.client == user_client)
            .order_by(SmartDevice.serial_number.asc())
        )
        total_devices = base_query.count()

        total_pages = (total_devices + per_page - 1) // per_page if total_devices else 0
        if page < 1:
            page = 1
        if total_devices == 0:
            page = 1
        elif total_pages and page > total_pages:
            page = total_pages

        results = (
            base_query.offset((page - 1) * per_page).limit(per_page).all()
            if total_devices
            else []
        )
        smartdevices_pagination = Pagination(results, page, per_page, total_devices)

        return render_template(
            "portal/smartdevices/smartdevices.html",
            smartdevices=smartdevices_pagination,
            page_type="list",
        )

    except Exception as e:
        print(f"[ERROR] Error listing smart devices: {str(e)}")
        flash("Erro ao listar dispositivos inteligentes", "error")
        return redirect(url_for("dashboard.render_dashboard"))


@smartdevices_bp.route("/", methods=["POST"])
@require_authentication
def create_smartdevice():
    """
    Create a new smart device
    """
    try:
        user = session.get("user")
        if not user or not user.get("client"):
            flash("Sessão inválida. Por favor, faça login novamente.", "error")
            return redirect(url_for("auth.login"))
        
        db_session = get_session()

        def form_bool(name, default=False):
            value = request.form.get(name)
            if value is None:
                return default
            return str(value).lower() in {"1", "true", "on", "yes"}

        def get_date_field(name):
            value = request.form.get(name)
            if not value or value.strip() == "":
                return None
            return value

        # Validate mac_address
        mac_address = request.form.get("mac_address")
        if not mac_address:
            flash("O campo 'MAC Address' é obrigatório.", "error")
            return redirect(url_for("portal_smartdevices.list_and_create_smartdevices"))
        # Normalize and validate MAC address format
        cleaned_mac = normalize_mac_no_sep(mac_address)
        if not cleaned_mac:
            flash(
                "O 'MAC Address' deve estar no formato XX:XX:XX:XX:XX:XX ou XX-XX-XX-XX-XX-XX.",
                "error",
            )
            return redirect(url_for("portal_smartdevices.list_and_create_smartdevices"))
        # Check if mac_address is unique (compare normalized value)
        existing_device = (
            db_session.query(SmartDevice)
            .filter(
                func.upper(
                    func.replace(
                        func.replace(SmartDevice.mac_address, ":", ""), "-", ""
                    )
                )
                == cleaned_mac
            )
            .first()
        )
        if existing_device:
            flash("Já existe um dispositivo com este 'MAC Address'.", "error")
            return redirect(url_for("portal_smartdevices.list_and_create_smartdevices"))

        # Create new smart device
        normalized_mac_colon = ":".join(
            [cleaned_mac[i : i + 2] for i in range(0, 12, 2)]
        )
        new_device = SmartDevice(
            serial_number=request.form.get("serial_number", ""),
            mac_address=normalized_mac_colon,
            linked_with_asset=request.form.get("linked_with_asset"),
            outlet=request.form.get("outlet"),
            city=request.form.get("city"),
            state=request.form.get("state"),
            country=request.form.get("country"),
            client=user.get("client"),
            outlet_code=request.form.get("outlet_code"),
            last_ping=get_date_field("last_ping"),
            is_online=form_bool("is_online"),
            is_missing=form_bool("is_missing"),
            is_sd_gateway=form_bool("is_sd_gateway"),
            created_on=datetime.now(),
            created_by=user.get("upn", "system"),
        )

        db_session.add(new_device)
        db_session.commit()

        flash(f"Dispositivo {new_device.serial_number} criado com sucesso!", "success")
        return redirect(url_for("portal_smartdevices.list_and_create_smartdevices"))

    except Exception as e:
        db_session.rollback()
        print(f"[ERROR] Error creating smart device: {str(e)}")
        flash(f"Erro ao criar dispositivo: {str(e)}", "error")
        return redirect(url_for("portal_smartdevices.list_and_create_smartdevices"))


@smartdevices_bp.route("/search", methods=["GET"])
@require_authentication
def search_smartdevices():
    """
    Search smart devices by serial number or MAC address (JSON endpoint)
    """
    try:
        query = request.args.get("q", "").strip()

        if not query:
            return jsonify([])

        db_session = get_session()

        # Search by serial_number or mac_address
        search_pattern = f"%{query}%"
        devices = (
            db_session.query(SmartDevice)
            .filter(
                (SmartDevice.mac_address.ilike(search_pattern))
                | (SmartDevice.serial_number.ilike(search_pattern))
                | (SmartDevice.outlet.ilike(search_pattern))
                | (SmartDevice.outlet_code.ilike(search_pattern))
                | (SmartDevice.city.ilike(search_pattern))
            )
            .limit(20)
            .all()
        )

        return jsonify([device.to_dict() for device in devices])

    except Exception as e:
        print(f"[ERROR] Error searching smart devices: {str(e)}")
        return jsonify([]), 500


@smartdevices_bp.route("/<string:mac_address>", methods=["GET"])
@require_authentication
def get_smartdevice_details(mac_address):
    """
    Get smart device details by MAC address (JSON endpoint for modal view/edit)
    """
    try:
        db_session = get_session()
        device = (
            db_session.query(SmartDevice)
            .filter(SmartDevice.mac_address == mac_address)
            .first()
        )

        if not device:
            return jsonify({"error": "Device not found"}), 404

        return jsonify(device.to_dict())

    except Exception as e:
        print(f"[ERROR] Error fetching smart device: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@smartdevices_bp.route("/<string:mac_address>", methods=["PUT", "POST", "DELETE"])
@require_authentication
def manage_smartdevice(mac_address):
    """
    Manage smart device by MAC address (Update or Delete)
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
        device = (
            db_session.query(SmartDevice)
            .filter(SmartDevice.mac_address == mac_address)
            .first()
        )

        if not device:
            flash("Dispositivo não encontrado", "error")
            return redirect(url_for("portal_smartdevices.list_and_create_smartdevices"))

        if is_delete:
            # DELETE LOGIC
            device_name = device.serial_number or f"Device {device.mac_address}"
            db_session.delete(device)
            db_session.commit()

            flash(f"Dispositivo {device_name} deletado com sucesso!", "success")
            return redirect(url_for("portal_smartdevices.list_and_create_smartdevices"))

        else:
            # UPDATE LOGIC
            user = session.get("user")
            if not user or not user.get("client"):
                flash("Sessão inválida. Por favor, faça login novamente.", "error")
                return redirect(url_for("auth.login"))

            def form_bool(name, default=None):
                value = request.form.get(name)
                if value is None:
                    return default
                return str(value).lower() in {"1", "true", "on", "yes"}

            def get_date_field(name, current_value):
                if name not in request.form:
                    return current_value
                value = request.form.get(name)
                if not value or str(value).strip() == "":
                    return None
                return value

            # Update key fields
            device.serial_number = request.form.get(
                "serial_number", device.serial_number
            )
            new_mac = request.form.get("mac_address", device.mac_address)
            new_mac_clean = normalize_mac_no_sep(new_mac) if new_mac else None

            if (
                new_mac
                and new_mac_clean
                and new_mac_clean != normalize_mac_no_sep(device.mac_address)
            ):
                if not new_mac_clean:
                    flash(
                        "O 'MAC Address' deve estar no formato XX:XX:XX:XX:XX:XX ou XX-XX-XX-XX-XX-XX.",
                        "error",
                    )
                    return redirect(
                        url_for("portal_smartdevices.list_and_create_smartdevices")
                    )

                # Check uniqueness (normalized)
                existing = (
                    db_session.query(SmartDevice)
                    .filter(
                        func.upper(
                            func.replace(
                                func.replace(SmartDevice.mac_address, ":", ""), "-", ""
                            )
                        )
                        == new_mac_clean
                    )
                    .first()
                )
                if existing:
                    flash("Já existe um dispositivo com este 'MAC Address'.", "error")
                    return redirect(
                        url_for("portal_smartdevices.list_and_create_smartdevices")
                    )

            if new_mac and new_mac_clean:
                device.mac_address = ":".join(
                    [new_mac_clean[i : i + 2] for i in range(0, 12, 2)]
                )
            else:
                device.mac_address = device.mac_address
            device.linked_with_asset = request.form.get(
                "linked_with_asset", device.linked_with_asset
            )
            device.outlet = request.form.get("outlet", device.outlet)
            device.outlet_code = request.form.get("outlet_code", device.outlet_code)
            device.city = request.form.get("city", device.city)
            device.state = request.form.get("state", device.state)
            device.country = request.form.get("country", device.country)
            device.client = user.get("client")
            device.last_ping = get_date_field("last_ping", device.last_ping)

            bool_online = form_bool("is_online")
            if bool_online is not None:
                device.is_online = bool_online

            bool_missing = form_bool("is_missing")
            if bool_missing is not None:
                device.is_missing = bool_missing

            bool_gateway = form_bool("is_sd_gateway")
            if bool_gateway is not None:
                device.is_sd_gateway = bool_gateway

            device.modified_on = datetime.now()
            device.modified_by = user.get("upn", "system")

            db_session.commit()
            flash(
                f"Dispositivo {device.serial_number} atualizado com sucesso!", "success"
            )
            return redirect(url_for("portal_smartdevices.list_and_create_smartdevices"))

    except Exception as e:
        db_session.rollback()
        print(f"[ERROR] Error managing smart device: {str(e)}")
        flash(f"Erro ao processar dispositivo: {str(e)}", "error")
        return redirect(url_for("portal_smartdevices.list_and_create_smartdevices"))
