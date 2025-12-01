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

        # For now, just render a simple inventory page
        # You can extend this template with actual inventory logic later
        return render_template('inventory/index.html', user=user)

    except Exception as e:
        print(f"[ERROR] Error rendering inventory index: {e}")
        return redirect(url_for('index'))
