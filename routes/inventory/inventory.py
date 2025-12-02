from flask import Blueprint, render_template, session, redirect, url_for, jsonify, request
from routes.portal.decorators import require_authentication
from db.database import get_session
from models.models import AssetsInventory

inventory_bp = Blueprint("inventory", __name__, url_prefix="/inventory")


def _serialize_inventory_asset(asset: AssetsInventory) -> dict:
    data = asset.to_dict()
    # Normaliza campos de data para strings ISO legíveis no front
    for date_field in ("created_at", ):
        if data.get(date_field):
            data[date_field] = data[date_field].isoformat()
    return data


@inventory_bp.route('/', methods=['GET'])
@require_authentication
def render_inventory_index():
    """Render the inventory index page."""
    try:
        user = session.get('user')
        if not user:
            return redirect(url_for('index'))

        # Redirect /inventory/ to the list page to keep URLs consistent
        return redirect(url_for('inventory.render_inventory_list'))

    except Exception as e:
        print(f"[ERROR] Error rendering inventory index: {e}")
        return redirect(url_for('index'))


    # Inventory list route (user-facing list under /inventory/list)

@inventory_bp.route('/list', methods=['GET'])
@require_authentication
def render_inventory_list():
    """Render the main inventory dashboard page."""
    try:
        user = session.get('user')
        if not user:
            return redirect(url_for('index'))
        db_session = get_session()
        assets = db_session.query(AssetsInventory).filter(AssetsInventory.is_deleted.is_(False)).all()
        return render_template('inventory/list.html', user=user, assets_inventory=assets)
    except Exception as e:
        print(f"[ERROR] Error rendering inventory dashboard: {e}")
        return redirect(url_for('inventory.render_inventory_index'))


@inventory_bp.route('/dashboard', methods=['GET'])
@require_authentication
def legacy_inventory_dashboard_redirect():
    """Compat redirect from old /dashboard path to /list."""
    return redirect(url_for('inventory.render_inventory_list'))


@inventory_bp.route('/operation', methods=['GET'])
@require_authentication
def render_inventory_operation():
    """Render the inventory operations page."""
    try:
        user = session.get('user')
        if not user:
            return redirect(url_for('index'))
        db_session = get_session()
        # Carrega todos os ativos não deletados para exibir no mapa
        assets = db_session.query(AssetsInventory).filter(AssetsInventory.is_deleted.is_(False)).all()
        operation_assets = [_serialize_inventory_asset(asset) for asset in assets]

        return render_template('inventory/operation.html', user=user, operation_assets=operation_assets)
    except Exception as e:
        print(f"[ERROR] Error rendering inventory operation: {e}")
        return redirect(url_for('inventory.render_inventory_index'))


@inventory_bp.route('/operation/data', methods=['GET'])
@require_authentication
def get_inventory_operation_data():
    """JSON endpoint com os assets para o mapa de operações."""
    try:
        db_session = get_session()
        assets = db_session.query(AssetsInventory).filter(AssetsInventory.is_deleted.is_(False)).all()
        return jsonify([_serialize_inventory_asset(asset) for asset in assets])
    except Exception as e:
        print(f"[ERROR] Error fetching inventory operation data: {e}")
        return jsonify({"error": "Erro ao buscar dados de inventário"}), 500


@inventory_bp.route('/operation/create', methods=['POST'])
@require_authentication
def create_inventory_asset():
    """Cria um novo asset no inventário via JSON ou form."""
    try:
        payload = request.get_json(silent=True) or request.form

        serial_number = (payload.get('serial_number') or '').strip()
        if not serial_number:
            return jsonify({"error": "O campo serial_number é obrigatório."}), 400

        asset_type = (payload.get('asset_type') or '').strip() or None
        material = (payload.get('material') or '').strip() or None
        outlet_name = (payload.get('outlet_name') or '').strip() or None
        street = (payload.get('street') or '').strip() or None
        city = (payload.get('city') or '').strip() or None
        notes = (payload.get('notes') or '').strip() or None

        def to_float(value):
            try:
                return float(value) if value not in (None, '') else None
            except (TypeError, ValueError):
                return None

        last_latitude = to_float(payload.get('last_latitude'))
        last_longitude = to_float(payload.get('last_longitude'))

        db_session = get_session()

        # Evita duplicidade simples pelo serial_number quando não deletado
        existing = db_session.query(AssetsInventory).filter(
            AssetsInventory.serial_number == serial_number,
            AssetsInventory.is_deleted.is_(False)
        ).first()
        if existing:
            return jsonify({"error": "Já existe um asset com este serial_number."}), 409

        user = session.get('user') or {}
        created_by = user.get('upn') or user.get('email') or 'system'

        asset = AssetsInventory(
            serial_number=serial_number,
            asset_type=asset_type,
            material=material,
            outlet_name=outlet_name,
            street=street,
            city=city,
            notes=notes,
            last_latitude=last_latitude,
            last_longitude=last_longitude,
            created_by_user=created_by
        )

        db_session.add(asset)
        db_session.commit()

        return jsonify({"asset": _serialize_inventory_asset(asset)}), 201
    except Exception as e:
        print(f"[ERROR] Error creating inventory asset: {e}")
        return jsonify({"error": "Erro ao criar asset"}), 500
