from flask import Blueprint, render_template, request, session, redirect, url_for, jsonify
from models.models import DoorEvent, AlertsDefinition
from db.database import get_session
from .decorators import require_authentication
from sqlalchemy import text
import re
import unicodedata
from datetime import datetime, timedelta

tracking_bp = Blueprint("portal_tracking", __name__, url_prefix="/portal_associacao/tracking")


def remover_special_caracteres(text):
    """
    Remove special characters and accents from a string, leaving only alphanumeric characters and spaces.
    Returns lowercase text without accents.
    """
    if not text:
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove accents
    text = ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )
    
    # Remove special characters
    return re.sub(r'[^a-z0-9\s]', '', text)

def find_address_by_lat_long(lat, lon):
    """
    Find city, state, and country by latitude and longitude using a geocoding service
    """
    try:
        import requests

        GEOCODING_API_URL = "https://nominatim.openstreetmap.org/reverse"
        params = {
            "lat": lat,
            "lon": lon,
            "format": "json"
        }
        response = requests.get(GEOCODING_API_URL, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            address = data.get("address", {})
            city = address.get("city", address.get("town", address.get("village", "")))
            state = address.get("state", "")
            country = address.get("country", "")
            return city, state, country
        else:
            print(f"[ERROR] Geocoding API error: {response.status_code}")
            return None, None, None

    except Exception as e:
        print(f"[ERROR] Error in find_address_by_lat_long: {str(e)}")
        return None, None, None
    
def check_gps_displacement_alert(db_session, client_code, asset, movement_event):
    """
    Check and process GPS displacement alert for an asset.
    Updates the asset's city, state, and country if displacement exceeds the threshold.
    """
    # Find definition for GPS displacement alert
    alert_definition = db_session.query(AlertsDefinition).filter(
        AlertsDefinition.client == client_code,
        AlertsDefinition.type == "GPS Displacement",
        AlertsDefinition.asset_serial_number == asset.oem_serial_number
    ).first()
    if not alert_definition:
        alert_definition = db_session.query(AlertsDefinition).filter(
            AlertsDefinition.client == client_code,
            AlertsDefinition.type == "GPS Displacement",
            AlertsDefinition.outlet == asset.outlet
        ).first()
    if not alert_definition:
        alert_definition = db_session.query(AlertsDefinition).filter(
            AlertsDefinition.client == client_code,
            AlertsDefinition.type == "GPS Displacement",
            AlertsDefinition.sales_organization == asset.sales_organization
        ).first()

    if alert_definition:
        displacement_limit = alert_definition.gps_displacement_threshold
        if asset.displacement_meter > displacement_limit:
            city, state, country = find_address_by_lat_long(movement_event.latitude, movement_event.longitude)
            return city, state, country

    return None, None, None

def calculate_door_event_statistics(db_session, asset_serial_number, days=30):
    """
    Calculate door event statistics for morning, afternoon, and night over the last `days` days.

    Args:
        db_session: Database session object.
        asset_serial_number: Serial number of the asset.
        days: Number of days to look back for door events.

    Returns:
        A dictionary containing averages for morning, afternoon, and night.
    """
    time_periods = ["Morning", "Afternoon", "Night"]
    door_event_stats = {}

    for period in time_periods:
        door_events = db_session.query(DoorEvent).filter(
            DoorEvent.time_of_day == period,
            DoorEvent.asset_serial_number == asset_serial_number,
            DoorEvent.open_event_time >= (datetime.utcnow() - timedelta(days=days))
        ).all()

        door_count = sum(event.door_count for event in door_events)

        unique_dates = set()
        for door_event in door_events:
            date_only = door_event.open_event_time.date()
            unique_dates.add(date_only)
        average = door_count / len(unique_dates) if unique_dates else 0

        door_event_stats[period] = {
            "average": average
        }

    return door_event_stats

@tracking_bp.route("/", methods=["GET"])
@require_authentication
def render_tracking():
    if request.method == "GET":
        try:
            return render_template("portal/tracking/tracking.html")
        except Exception as e:
            print(f"[ERROR] Error rendering tracking page: {str(e)}")
            return redirect(url_for("dashboard.render_dashboard"))

    return render_template("portal/tracking/tracking.html")


@tracking_bp.route("/devices", methods=["GET"])
@require_authentication
def get_assets_optimized():
    """
    Rota ultra-rápida que consome a MATERIALIZED VIEW com suporte a múltiplos filtros.
    Parâmetros de busca suportados:
    - bottler_equipment_number: Número do equipamento
    - oem_serial_number: Número de série OEM
    - outlet: Nome do outlet
    - sub_trade_channel: Canal de comércio
    - city: Cidade
    - state: Estado
    - country: País
    - is_online: Status online (true/false)
    - is_missing: Status perdido (true/false)
    """
    db_session = get_session()
    client_code = session.get("user", {}).get("client")

    try:
        # 1. Construir query SQL com filtros dinâmicos
        where_clauses = ["client = :client"]
        params = {'client': client_code}
        
        # Coletar parâmetros de filtro
        bottler_equipment_number = request.args.get('bottler_equipment_number', '').strip()
        oem_serial_number = request.args.get('oem_serial_number', '').strip()
        outlet = request.args.get('outlet', '').strip()
        sub_trade_channel = request.args.get('sub_trade_channel', '').strip()
        city = request.args.get('city', '').strip()
        state = request.args.get('state', '').strip()
        country = request.args.get('country', '').strip()
        is_online = request.args.get('is_online', '').strip().lower()
        is_missing = request.args.get('is_missing', '').strip().lower()
        
        # Adicionar filtros de string (case-insensitive)
        if bottler_equipment_number:
            where_clauses.append("LOWER(bottler_equipment_number) LIKE LOWER(:bottler_equipment_number)")
            params['bottler_equipment_number'] = f"%{bottler_equipment_number}%"
        
        if oem_serial_number:
            where_clauses.append("LOWER(oem_serial_number) LIKE LOWER(:oem_serial_number)")
            params['oem_serial_number'] = f"%{oem_serial_number}%"
        
        if outlet:
            where_clauses.append("LOWER(outlet) LIKE LOWER(:outlet)")
            params['outlet'] = f"%{outlet}%"
        
        if sub_trade_channel:
            where_clauses.append("LOWER(sub_trade_channel) LIKE LOWER(:sub_trade_channel)")
            params['sub_trade_channel'] = f"%{sub_trade_channel}%"
        
        if city:
            where_clauses.append("LOWER(city) LIKE LOWER(:city)")
            params['city'] = f"%{city}%"
        
        if state:
            where_clauses.append("LOWER(state) LIKE LOWER(:state)")
            params['state'] = f"%{state}%"
        
        if country:
            where_clauses.append("LOWER(country) LIKE LOWER(:country)")
            params['country'] = f"%{country}%"
        
        # Adicionar filtros booleanos
        if is_online in ('true', '1', 'yes'):
            where_clauses.append("is_online = true")
        elif is_online in ('false', '0', 'no'):
            where_clauses.append("is_online = false")
        
        if is_missing in ('true', '1', 'yes'):
            where_clauses.append("is_missing = true")
        elif is_missing in ('false', '0', 'no'):
            where_clauses.append("is_missing = false")
        
        # Construir a query WHERE
        where_sql = " AND ".join(where_clauses)
        
        # 2. Buscar todos os assets da Materialized View com filtros aplicados
        sql_assets = text(f"SELECT * FROM mv_client_assets_report WHERE {where_sql}")
        result_assets = db_session.execute(sql_assets, params)
        
        data = []
        for row in result_assets:
            # Converte o objeto Row para um dict
            asset_data = dict(row._mapping)
            
            # --- Transformação de Dados Crucial ---
            # O template JS espera 'latitude', 'longitude' e 'has_location'.
            # Nossa view tem 'latest_latitude' e 'latest_longitude'.
            lat = asset_data.pop('latest_latitude', None)
            lon = asset_data.pop('latest_longitude', None)
            
            asset_data['latitude'] = lat
            asset_data['longitude'] = lon
            asset_data['has_location'] = bool(lat and lon)
            
            data.append(asset_data)

        # 3. Buscar subclients distintos para o filtro
        sql_subclients = text("""
            SELECT subclient_code, subclient_name 
            FROM subclients 
            WHERE client = :client 
            AND subclient_code IS NOT NULL 
            AND subclient_name IS NOT NULL
            GROUP BY subclient_code, subclient_name
            ORDER BY subclient_name
        """)
        result_subclients = db_session.execute(sql_subclients, {'client': client_code})
        available_subclients = [dict(row._mapping) for row in result_subclients]

        # 4. Log de filtros aplicados
        active_filters = {
            'bottler_equipment_number': bottler_equipment_number,
            'oem_serial_number': oem_serial_number,
            'outlet': outlet,
            'sub_trade_channel': sub_trade_channel,
            'city': city,
            'state': state,
            'country': country,
            'is_online': is_online,
            'is_missing': is_missing
        }
        active_filters = {k: v for k, v in active_filters.items() if v}
        
        if active_filters:
            print(f"[INFO] Filtros aplicados: {active_filters}")

        # 5. Construir a resposta no formato que o JS espera
        return jsonify({
            "data": data,
            "pagination": {
                "page": 1,
                "pages": 1,
                "total": len(data)
            },
            "available_subclients": available_subclients,
            "active_filters": active_filters,
            "optimized": True # Sinaliza para o JS que estes dados vieram da view
        })

    except Exception as e:
        print(f"[ERROR] Erro em get_assets_optimized: {str(e)}")
        return jsonify({"error": "Erro ao consultar a view materializada", "details": str(e)}), 500
    finally:
        db_session.close()
    