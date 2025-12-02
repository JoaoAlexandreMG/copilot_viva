from flask import Blueprint, render_template, session, redirect, url_for
from routes.portal.decorators import require_authentication

inventory_bp = Blueprint("inventory", __name__, url_prefix="/inventory")


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
        return render_template('inventory/operation.html', user=user)
    except Exception as e:
        print(f"[ERROR] Error rendering inventory operation: {e}")
        return redirect(url_for('inventory.render_inventory_index'))
