from flask import Blueprint, render_template, session, redirect, url_for, jsonify
from routes.portal.decorators import require_authentication
from db.database import get_session
from models.models import AssetsInventory

inventory_bp = Blueprint("inventory", __name__, url_prefix="/inventory")


def _serialize_inventory_asset(asset: AssetsInventory) -> dict:
    data = asset.to_dict()
    # Normaliza campos de data para strings ISO legíveis no front
    for date_field in ("last_visit_at", "created_at"):
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

        # Redirect /inventory/ to the dashboard page to keep URLs consistent
        return redirect(url_for('inventory.render_inventory_dashboard'))

    except Exception as e:
        print(f"[ERROR] Error rendering inventory index: {e}")
        return redirect(url_for('index'))


    # Inventory dashboard route (user-facing dashboard under /inventory/dashboard)

@inventory_bp.route('/dashboard', methods=['GET'])
@require_authentication
def render_inventory_dashboard():
    """Render the main inventory dashboard page."""
    try:
        user = session.get('user')
        if not user:
            return redirect(url_for('index'))
        return render_template('inventory/dashboard.html', user=user)
    except Exception as e:
        print(f"[ERROR] Error rendering inventory dashboard: {e}")
        return redirect(url_for('inventory.render_inventory_index'))


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
