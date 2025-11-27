from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify
from models.models import Alert
from .decorators import require_authentication
from db.database import get_session
from datetime import datetime, timedelta, timezone
from sqlalchemy import text

alerts_bp = Blueprint("portal_alerts", __name__, url_prefix="/portal_associacao")

@alerts_bp.route("/alerts", methods=["GET"])
@require_authentication
def render_alerts():
    """
    Render alerts page with period and advanced filtering
    Query params:
    - period: número de dias (7 ou 30, padrão 30)
    - alert_type: filtro por tipo de alerta
    - city: filtro por cidade
    - outlet: filtro por outlet
    - serial: filtro por serial do asset
    """
    try:
        user = session.get("user")
        if not user:
            return redirect(url_for("index"))

        # Get period from query params, default to 30
        period = request.args.get('period', 30, type=int)

        # Validate period
        if period not in [7, 30]:
            period = 30

        # Get filters from query params
        alert_type_filter = request.args.get('alert_type', '').strip()
        city_filter = request.args.get('city', '').strip()
        outlet_filter = request.args.get('outlet', '').strip()
        serial_filter = request.args.get('serial', '').strip()

        # Get client from session
        client = user.get("client")
        if not client:
            return redirect(url_for("index"))

        # Load alerts with filters
        alerts_data = get_alerts_for_period(
            client,
            days=period,
            alert_type=alert_type_filter if alert_type_filter else None,
            city=city_filter if city_filter else None,
            outlet=outlet_filter if outlet_filter else None,
            serial=serial_filter if serial_filter else None
        )

        return render_template(
            "portal/alerts.html",
            alerts=alerts_data['alerts'],
            period=period,
            total_alerts=alerts_data['total'],
            alert_types=alerts_data['alert_types'],
            cities=alerts_data['cities'],
            outlets=alerts_data['outlets'],
            serials=alerts_data['serials'],
            selected_alert_type=alert_type_filter,
            selected_city=city_filter,
            selected_outlet=outlet_filter,
            selected_serial=serial_filter,
            page_type="list"
        )

    except Exception as e:
        print(f"[ERROR] Error rendering alerts: {str(e)}")
        return redirect(url_for("index"))

@alerts_bp.route("/api/alerts", methods=["GET"])
@require_authentication
def get_alerts_api():
    """
    API endpoint to return alerts with period filtering
    Query params:
    - period: número de dias (7 ou 30, padrão 30)
    """
    try:
        user = session.get("user")
        if not user:
            return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401

        # Get period from query params, default to 30
        period = request.args.get('period', 30, type=int)

        # Validate period
        if period not in [7, 30]:
            period = 30

        client = user.get("client")
        if not client:
            return jsonify({'status': 'error', 'message': 'No client in session'}), 400

        alerts_data = get_alerts_for_period(client, days=period)

        return jsonify({
            'status': 'ok',
            'data': {
                'alerts': [alert.to_dict() if hasattr(alert, 'to_dict') else dict(alert) for alert in alerts_data['alerts']],
                'total': alerts_data['total'],
                'period': period
            }
        }), 200

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

def get_alerts_for_period(client, days=30, alert_type=None, city=None, outlet=None, serial=None):
    """
    Get alerts for a client within a specified period with optional filters.
    Filter options in dropdowns are dynamically updated based on the filtered results.

    Args:
        client: Client name/code
        days: Number of days to filter (7 or 30)
        alert_type: Filter by alert type (optional)
        city: Filter by outlet city (optional)
        outlet: Filter by outlet (optional)
        serial: Filter by asset serial number (optional)

    Returns:
        Dictionary with 'alerts' list, 'total' count, and dynamic filter options
    """
    db_session = get_session()

    try:
        # Calculate period start
        now = datetime.now(timezone.utc)
        period_start = now - timedelta(days=days)

        # Build base query with all applied filters
        query = db_session.query(Alert).filter(
            Alert.client == client,
            Alert.alert_at >= period_start
        )

        # Apply optional filters
        if alert_type:
            query = query.filter(Alert.alert_type == alert_type)
        if city:
            query = query.filter(Alert.outlet_city.ilike(f'%{city}%'))
        if outlet:
            query = query.filter(Alert.outlet.ilike(f'%{outlet}%'))
        if serial:
            query = query.filter(Alert.asset_serial_number.ilike(f'%{serial}%'))

        # Order by alert_at descending (newest first)
        alerts = query.order_by(Alert.alert_at.desc()).all()

        # Extract unique values from filtered alerts for dynamic filter options
        # This ensures that only available options are shown in each dropdown
        alert_types = sorted(list(set([a.alert_type for a in alerts if a.alert_type])))
        cities = sorted(list(set([a.outlet_city for a in alerts if a.outlet_city])))
        outlets = sorted(list(set([a.outlet for a in alerts if a.outlet])))
        serials = sorted(list(set([a.asset_serial_number for a in alerts if a.asset_serial_number])))

        total = len(alerts)

        return {
            'alerts': alerts,
            'total': total,
            'alert_types': alert_types,
            'cities': cities,
            'outlets': outlets,
            'serials': serials
        }

    finally:
        db_session.close()
