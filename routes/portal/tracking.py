from flask import (
    Blueprint,
    render_template,
    request,
    session,
    redirect,
    url_for,
    jsonify,
)
from models.models import DoorEvent, AlertsDefinition, MovementsFindHub
from db.database import get_session
from .decorators import require_authentication
from sqlalchemy import text
import re
import unicodedata
import os
from datetime import datetime, timedelta
import requests_cache
from retry_requests import retry
import openmeteo_requests

tracking_bp = Blueprint(
    "portal_tracking", __name__, url_prefix="/portal_associacao/tracking"
)

# Clients autorizados para usar a seção de Rastreio Simples
SIMPLE_TRACKING_AUTHORIZED_CLIENTS = {
    "Fogel de Centroamerica, S.A.",
    "Philip Morris Brasil",
}


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
    text = "".join(
        c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn"
    )

    # Remove special characters
    return re.sub(r"[^a-z0-9\s]", "", text)


def find_address_by_lat_long(lat, lon):
    """
    Find city, state, and country by latitude and longitude using a geocoding service
    """
    try:
        import requests

        GEOCODING_API_URL = "https://nominatim.openstreetmap.org/reverse"
        params = {"lat": lat, "lon": lon, "format": "json"}
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
    alert_definition = (
        db_session.query(AlertsDefinition)
        .filter(
            AlertsDefinition.client == client_code,
            AlertsDefinition.type == "GPS Displacement",
            AlertsDefinition.asset_serial_number == asset.oem_serial_number,
        )
        .first()
    )
    if not alert_definition:
        alert_definition = (
            db_session.query(AlertsDefinition)
            .filter(
                AlertsDefinition.client == client_code,
                AlertsDefinition.type == "GPS Displacement",
                AlertsDefinition.outlet == asset.outlet,
            )
            .first()
        )
    if not alert_definition:
        alert_definition = (
            db_session.query(AlertsDefinition)
            .filter(
                AlertsDefinition.client == client_code,
                AlertsDefinition.type == "GPS Displacement",
                AlertsDefinition.sales_organization == asset.sales_organization,
            )
            .first()
        )

    if alert_definition:
        displacement_limit = alert_definition.gps_displacement_threshold
        if asset.displacement_meter > displacement_limit:
            city, state, country = find_address_by_lat_long(
                movement_event.latitude, movement_event.longitude
            )
            return city, state, country

    return None, None, None


def get_ambient_temperature(latitude, longitude):
    """
    Fetch current ambient temperature from Open-Meteo API.
    Returns temperature in Celsius or None if request fails.
    """
    try:
        if latitude is None or longitude is None:
            return None

        # Setup the Open-Meteo API client with cache and retry
        cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        # Get current temperature
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m",
            "timezone": "auto",
        }

        responses = openmeteo.weather_api(
            "https://api.open-meteo.com/v1/forecast", params=params
        )

        if responses and len(responses) > 0:
            response = responses[0]
            current = response.Current()
            temperature = current.Variables(0).Value() if current else None
            return float(temperature) if temperature is not None else None

        return None

    except Exception as e:
        print(f"[WARNING] Erro ao buscar temperatura ambiente do Open-Meteo: {str(e)}")
        return None


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
        door_events = (
            db_session.query(DoorEvent)
            .filter(
                DoorEvent.time_of_day == period,
                DoorEvent.asset_serial_number == asset_serial_number,
                DoorEvent.open_event_time >= (datetime.utcnow() - timedelta(days=days)),
                DoorEvent.door_count < 150,
            )
            .all()
        )

        door_count = sum(event.door_count for event in door_events)

        unique_dates = set()
        for door_event in door_events:
            date_only = door_event.open_event_time.date()
            unique_dates.add(date_only)
        average = door_count / len(unique_dates) if unique_dates else 0

        door_event_stats[period] = {"average": average}

    return door_event_stats


@tracking_bp.route("/", methods=["GET"])
@require_authentication
def render_tracking():
    """
    Renderiza a página de tracking com suporte a filtros via GET parameters.

    Parâmetros GET suportados:
    - bottler_equipment_number: Número do equipamento
    - oem_serial_number: Número de série OEM
    - outlet: Nome do outlet
    - sub_trade_channel: Canal de comércio
    - city: Cidade
    - state: Estado
    - country: País
    - is_online: Status online (true/false)
    - is_missing: Status perdido (true/false)
    - is_active: Status ativo (true/false) - Inativo se sem movimento por 24h
    - temp_is_ok: Status de temperatura (true/false)
    - subclient: Código do subclient
    """
    try:
        # Capturar parâmetros GET para passar ao template
        filters = {
            "bottler_equipment_number": request.args.get(
                "bottler_equipment_number", ""
            ),
            "oem_serial_number": request.args.get("oem_serial_number", ""),
            "outlet": request.args.get("outlet", ""),
            "sub_trade_channel": request.args.get("sub_trade_channel", ""),
            "city": request.args.get("city", ""),
            "state": request.args.get("state", ""),
            "country": request.args.get("country", ""),
            "subclient": request.args.get("subclient", ""),
            "is_online": request.args.get("is_online", ""),
            "is_missing": request.args.get("is_missing", ""),
            "is_active": request.args.get("is_active", ""),
            "temp_is_ok": request.args.get("temp_is_ok", ""),
            "overheated": request.args.get("overheated", ""),
            "battery_is_ok": request.args.get("battery_is_ok", ""),
        }

        google_maps_api_key = os.getenv("GOOGLE_MAPS_API_KEY", "")
        return render_template(
            "portal/tracking/tracking.html",
            filters=filters,
            google_maps_api_key=google_maps_api_key,
        )
    except Exception as e:
        print(f"[ERROR] Error rendering tracking page: {str(e)}")
        return redirect(url_for("dashboard.render_dashboard"))


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
    - is_active: Status ativo (true/false) - Inativo se sem movimento por 24h
    - load_all: Se true, carrega TODOS os assets (sem paginação) para o mapa
    """
    db_session = get_session()
    client_code = request.args.get("client")
    if not client_code:
        client_code = session.get("user", {}).get("client")

    try:
        # 1. Construir query SQL com filtros dinâmicos
        # Remover filtro de last_movement_time para incluir todos os assets
        where_clauses = ["client = :client"]
        params = {"client": client_code}

        # Coletar parâmetros de filtro
        bottler_equipment_number = request.args.get(
            "bottler_equipment_number", ""
        ).strip()
        oem_serial_number = request.args.get("oem_serial_number", "").strip()
        outlet = request.args.get("outlet", "").strip()
        sub_trade_channel = request.args.get("sub_trade_channel", "").strip()
        city = request.args.get("city", "").strip()
        state = request.args.get("state", "").strip()
        country = request.args.get("country", "").strip()
        is_online = request.args.get("is_online", "").strip().lower()
        is_active = request.args.get("is_active", "").strip().lower()
        is_missing = request.args.get("is_missing", "").strip().lower()
        battery_is_ok = request.args.get("battery_is_ok", "").strip().lower()
        temp_is_ok = request.args.get("temp_is_ok", "").strip().lower()
        overheated = request.args.get("overheated", "").strip().lower()
        load_all = request.args.get("load_all", "false").strip().lower() == "true"

        # Adicionar filtros de string (case-insensitive)
        if bottler_equipment_number:
            where_clauses.append(
                "LOWER(bottler_equipment_number) LIKE LOWER(:bottler_equipment_number)"
            )
            params["bottler_equipment_number"] = f"%{bottler_equipment_number}%"

        if oem_serial_number:
            where_clauses.append(
                "LOWER(oem_serial_number) LIKE LOWER(:oem_serial_number)"
            )
            params["oem_serial_number"] = f"%{oem_serial_number}%"

        if outlet:
            where_clauses.append("LOWER(outlet) LIKE LOWER(:outlet)")
            params["outlet"] = f"%{outlet}%"

        if sub_trade_channel:
            where_clauses.append(
                "LOWER(sub_trade_channel) LIKE LOWER(:sub_trade_channel)"
            )
            params["sub_trade_channel"] = f"%{sub_trade_channel}%"

        if city:
            where_clauses.append("LOWER(city) LIKE LOWER(:city)")
            params["city"] = f"%{city}%"

        if state:
            where_clauses.append("LOWER(state) LIKE LOWER(:state)")
            params["state"] = f"%{state}%"

        if country:
            where_clauses.append("LOWER(country) LIKE LOWER(:country)")
            params["country"] = f"%{country}%"

        # Adicionar filtros booleanos
        if is_online in ("true", "1", "yes"):
            where_clauses.append("is_online = true")
        elif is_online in ("false", "0", "no"):
            where_clauses.append("is_online = false")

        # Fetch gps_displacement_threshold once (used for is_missing filter)
        gps_displacement_threshold = 300  # Default value
        if is_missing in ("true", "1", "yes", "false", "0", "no"):
            query = """
            SELECT gps_displacement_threshold
            FROM alerts_definition
            WHERE type='GPS Displacement' and client=:client
            LIMIT 1
            """
            result = db_session.execute(text(query), {"client": client_code}).fetchone()
            gps_displacement_threshold = result[0] if result else 300

        if is_missing in ("true", "1", "yes"):
            where_clauses.append("displacement_meter > :gps_displacement_threshold")
            params["gps_displacement_threshold"] = gps_displacement_threshold
        elif is_missing in ("false", "0", "no"):
            where_clauses.append("displacement_meter <= :gps_displacement_threshold")
            params["gps_displacement_threshold"] = gps_displacement_threshold

        if is_active in ("true", "1", "yes"):
            where_clauses.append("is_active = true")
        elif is_active in ("false", "0", "no"):
            where_clauses.append("is_active = false")
        temp_alert_definition_sql = text(
            """
        SELECT mco.applied_min, mco.applied_max
        FROM mv_client_overview mco
        WHERE mco.client = :client
        """
        )
        temp_alert_definition = db_session.execute(
            temp_alert_definition_sql, {"client": client_code}
        ).fetchone()

        # Definir limites de temperatura (usados no SQL dinâmico)
        temp_min = (
            temp_alert_definition.applied_min
            if temp_alert_definition and temp_alert_definition.applied_min is not None
            else 0
        )
        temp_max = (
            temp_alert_definition.applied_max
            if temp_alert_definition and temp_alert_definition.applied_max is not None
            else 7
        )

        if temp_is_ok in ("true", "1", "yes"):
            where_clauses.append("temperature_c BETWEEN :temp_min AND :temp_max")
            params["temp_min"] = temp_min
            params["temp_max"] = temp_max
        elif temp_is_ok in ("false", "0", "no"):
            where_clauses.append(
                "(temperature_c <:temp_min OR temperature_c >:temp_max) AND temperature_c IS NOT NULL AND temperature_c>=-50 AND temperature_c<=50"
            )
            params["temp_min"] = temp_min
            params["temp_max"] = temp_max

        # Filtro para desligados (temperatura > 100°C)
        if overheated in ("true", "1", "yes"):
            where_clauses.append("temperature_c > :overheated_threshold")
            params["overheated_threshold"] = 100

        if battery_is_ok in ("true", "1", "yes", "good"):
            where_clauses.append("battery > 50")
        elif battery_is_ok in ("false", "0", "no"):
            where_clauses.append("battery <= 50 AND battery IS NOT NULL")
        elif battery_is_ok == "medium":
            where_clauses.append("battery BETWEEN 20 AND 50")
        elif battery_is_ok == "low":
            where_clauses.append("battery < 20")

        # Construir a query WHERE
        where_sql = " AND ".join(where_clauses)

        # 2. Buscar COUNT total de assets para paginação
        sql_count = text(
            f"SELECT COUNT(*) as total FROM mv_smart_device_current_status WHERE {where_sql}"
        )
        total_count = db_session.execute(sql_count, params).scalar() or 0

        # 3. Paginação para a LISTA
        items_per_page = request.args.get("per_page", 3, type=int)
        page = request.args.get("page", 1, type=int)
        list_offset = (page - 1) * items_per_page
        total_pages = (total_count + items_per_page - 1) // items_per_page

        # 4. Buscar os assets da página ATUAL para a LISTA (paginado)
        sql_list = text(
            f"""
            SELECT *,
                COALESCE(
                    NULLIF(CAST(latitude AS FLOAT), 0),
                    NULLIF(CAST(last_known_latitude AS FLOAT), 0),
                    NULLIF(CAST(outlet_latitude AS FLOAT), 0)
                ) as final_latitude,
                COALESCE(
                    NULLIF(CAST(longitude AS FLOAT), 0),
                    NULLIF(CAST(last_known_longitude AS FLOAT), 0),
                    NULLIF(CAST(outlet_longitude AS FLOAT), 0)
                ) as final_longitude
            FROM mv_smart_device_current_status 
            WHERE {where_sql} 
            LIMIT :limit OFFSET :offset
        """
        )
        params["limit"] = items_per_page
        params["offset"] = list_offset
        result_list = db_session.execute(sql_list, params)

        list_data = []
        for row in result_list:
            asset_data = dict(row._mapping)
            # Usar coordenadas calculadas com fallback
            lat = asset_data.pop("final_latitude", None)
            lon = asset_data.pop("final_longitude", None)
            # Guardar coordenadas do outlet antes de remover
            outlet_lat_raw = asset_data.get("outlet_latitude")
            outlet_lng_raw = asset_data.get("outlet_longitude")
            # Converter para float
            try:
                outlet_lat = (
                    float(outlet_lat_raw)
                    if outlet_lat_raw and str(outlet_lat_raw).strip()
                    else None
                )
                if outlet_lat == 0:
                    outlet_lat = None
            except:
                outlet_lat = None
            try:
                outlet_lng = (
                    float(outlet_lng_raw)
                    if outlet_lng_raw and str(outlet_lng_raw).strip()
                    else None
                )
                if outlet_lng == 0:
                    outlet_lng = None
            except:
                outlet_lng = None
            # Remover colunas extras que não queremos expor
            asset_data.pop("latitude", None)
            asset_data.pop("longitude", None)
            asset_data.pop("last_known_latitude", None)
            asset_data.pop("last_known_longitude", None)
            asset_data.pop("outlet_latitude", None)
            asset_data.pop("outlet_longitude", None)
            # Definir coordenadas finais
            asset_data["latitude"] = lat
            asset_data["longitude"] = lon
            asset_data["outlet_lat"] = outlet_lat
            asset_data["outlet_lng"] = outlet_lng
            asset_data["has_location"] = bool(lat and lon)
            asset_data["temp_min"] = temp_min
            asset_data["temp_max"] = temp_max
            list_data.append(asset_data)

        # 5. Se load_all=true, buscar TODOS os assets para o MAPA (sem paginação)
        map_data = []
        if load_all:
            sql_map = text(
                f"""
                SELECT *,
                    COALESCE(
                        NULLIF(CAST(latitude AS FLOAT), 0),
                        NULLIF(CAST(last_known_latitude AS FLOAT), 0),
                        NULLIF(CAST(outlet_latitude AS FLOAT), 0)
                    ) as final_latitude,
                    COALESCE(
                        NULLIF(CAST(longitude AS FLOAT), 0),
                        NULLIF(CAST(last_known_longitude AS FLOAT), 0),
                        NULLIF(CAST(outlet_longitude AS FLOAT), 0)
                    ) as final_longitude
                FROM mv_smart_device_current_status 
                WHERE {where_sql}
            """
            )
            result_map = db_session.execute(sql_map, params)
            for row in result_map:
                asset_data = dict(row._mapping)
                # Usar coordenadas calculadas com fallback
                lat = asset_data.pop("final_latitude", None)
                lon = asset_data.pop("final_longitude", None)
                # Guardar coordenadas do outlet antes de remover
                outlet_lat_raw = asset_data.get("outlet_latitude")
                outlet_lng_raw = asset_data.get("outlet_longitude")
                # Converter para float
                try:
                    outlet_lat = (
                        float(outlet_lat_raw)
                        if outlet_lat_raw and str(outlet_lat_raw).strip()
                        else None
                    )
                    if outlet_lat == 0:
                        outlet_lat = None
                except:
                    outlet_lat = None
                try:
                    outlet_lng = (
                        float(outlet_lng_raw)
                        if outlet_lng_raw and str(outlet_lng_raw).strip()
                        else None
                    )
                    if outlet_lng == 0:
                        outlet_lng = None
                except:
                    outlet_lng = None
                # Remover colunas extras
                asset_data.pop("latitude", None)
                asset_data.pop("longitude", None)
                asset_data.pop("last_known_latitude", None)
                asset_data.pop("last_known_longitude", None)
                asset_data.pop("outlet_latitude", None)
                asset_data.pop("outlet_longitude", None)
                # Definir coordenadas finais
                asset_data["latitude"] = lat
                asset_data["longitude"] = lon
                asset_data["outlet_lat"] = outlet_lat
                asset_data["outlet_lng"] = outlet_lng
                asset_data["has_location"] = bool(lat and lon)
                asset_data["temp_min"] = temp_min
                asset_data["temp_max"] = temp_max
                map_data.append(asset_data)
            print(
                f"[INFO] Carregando TODOS os {len(map_data)} assets para o mapa (load_all=true)"
            )
        else:
            # Se load_all=false, usar apenas os da página para o mapa
            map_data = list_data

        # 5. Buscar subclients distintos para o filtro
        sql_subclients = text(
            """
            SELECT subclient_code, subclient_name 
            FROM subclients 
            WHERE client = :client 
            AND subclient_code IS NOT NULL 
            AND subclient_name IS NOT NULL
            GROUP BY subclient_code, subclient_name
            ORDER BY subclient_name
        """
        )
        result_subclients = db_session.execute(sql_subclients, {"client": client_code})
        available_subclients = [dict(row._mapping) for row in result_subclients]

        # 6. Log de filtros aplicados
        active_filters = {
            "bottler_equipment_number": bottler_equipment_number,
            "oem_serial_number": oem_serial_number,
            "outlet": outlet,
            "sub_trade_channel": sub_trade_channel,
            "city": city,
            "state": state,
            "country": country,
            "is_online": is_online,
            "is_missing": is_missing,
            "is_active": is_active,
            "temp_is_ok": temp_is_ok,
            "overheated": overheated,
            "battery_is_ok": battery_is_ok,
        }
        active_filters = {k: v for k, v in active_filters.items() if v}

        if active_filters:
            print(f"[INFO] Filtros aplicados: {active_filters}")

        # 7. Construir a resposta no formato que o JS espera
        return jsonify(
            {
                "data": list_data,  # Lista paginada para exibição na sidebar
                "map_data": map_data,  # Todos os dados para o mapa (quando load_all=true)
                "pagination": {
                    "page": page,
                    "pages": total_pages,
                    "total": total_count,
                    "per_page": items_per_page,
                    "has_prev": page > 1,
                    "has_next": page < total_pages,
                },
                "available_subclients": available_subclients,
                "active_filters": active_filters,
                "optimized": True,  # Sinaliza para o JS que estes dados vieram da view
            }
        )

    except Exception as e:
        print(f"[ERROR] Erro em get_assets_optimized: {str(e)}")
        return (
            jsonify(
                {"error": "Erro ao consultar a view materializada", "details": str(e)}
            ),
            500,
        )
    finally:
        db_session.close()


@tracking_bp.route("/map-markers", methods=["GET"])
@require_authentication
def get_map_markers():
    """
    Rota otimizada para carregar markers do mapa.
    Retorna apenas os campos necessários para exibição no mapa (leve e rápido).
    Suporta:
    - Limite configurável (max 1000)
    - Filtro por bounding box (viewport do mapa)
    - Mesmos filtros da rota /devices
    """
    db_session = get_session()
    client_code = session.get("user", {}).get("client")

    try:
        # Parâmetros
        limit = min(request.args.get("limit", 500, type=int), 1000)  # Max 1000

        # Bounding box do viewport (opcional)
        min_lat = request.args.get("min_lat", type=float)
        max_lat = request.args.get("max_lat", type=float)
        min_lng = request.args.get("min_lng", type=float)
        max_lng = request.args.get("max_lng", type=float)

        # Construir filtros
        where_clauses = ["client = :client"]
        params = {"client": client_code}

        # Filtros de texto (mesmos da rota /devices)
        bottler_equipment_number = request.args.get(
            "bottler_equipment_number", ""
        ).strip()
        oem_serial_number = request.args.get("oem_serial_number", "").strip()
        outlet = request.args.get("outlet", "").strip()
        city = request.args.get("city", "").strip()
        state = request.args.get("state", "").strip()
        is_online = request.args.get("is_online", "").strip().lower()
        temp_is_ok = request.args.get("temp_is_ok", "").strip().lower()
        battery_is_ok = request.args.get("battery_is_ok", "").strip().lower()
        is_missing = request.args.get("is_missing", "").strip().lower()
        is_active = request.args.get("is_active", "").strip().lower()
        overheated = request.args.get("overheated", "").strip().lower()
        subclient = request.args.get("subclient", "").strip()

        if bottler_equipment_number:
            where_clauses.append(
                "LOWER(bottler_equipment_number) LIKE LOWER(:bottler_equipment_number)"
            )
            params["bottler_equipment_number"] = f"%{bottler_equipment_number}%"

        if oem_serial_number:
            where_clauses.append(
                "LOWER(oem_serial_number) LIKE LOWER(:oem_serial_number)"
            )
            params["oem_serial_number"] = f"%{oem_serial_number}%"

        if outlet:
            where_clauses.append("LOWER(outlet) LIKE LOWER(:outlet)")
            params["outlet"] = f"%{outlet}%"

        if city:
            where_clauses.append("LOWER(city) LIKE LOWER(:city)")
            params["city"] = f"%{city}%"

        if state:
            where_clauses.append("LOWER(state) LIKE LOWER(:state)")
            params["state"] = f"%{state}%"

        if subclient:
            where_clauses.append("sub_client = :subclient")
            params["subclient"] = subclient

        if is_online in ("true", "1", "yes"):
            where_clauses.append("is_online = true")
        elif is_online in ("false", "0", "no"):
            where_clauses.append("is_online = false")

        # Filtro de temperatura
        if temp_is_ok in ("true", "1", "yes", "false", "0", "no"):
            temp_alert_sql = text(
                """
                SELECT mco.applied_min, mco.applied_max
                FROM mv_client_overview mco
                WHERE mco.client = :client
            """
            )
            temp_alert = db_session.execute(
                temp_alert_sql, {"client": client_code}
            ).fetchone()
            temp_min = (
                temp_alert.applied_min if temp_alert and temp_alert.applied_min else 0
            )
            temp_max = (
                temp_alert.applied_max if temp_alert and temp_alert.applied_max else 7
            )

            if temp_is_ok in ("true", "1", "yes"):
                where_clauses.append("temperature_c BETWEEN :temp_min AND :temp_max")
            else:
                where_clauses.append(
                    "(temperature_c < :temp_min OR temperature_c > :temp_max) AND temperature_c IS NOT NULL"
                )
            params["temp_min"] = temp_min
            params["temp_max"] = temp_max

        # Filtro de bateria
        if battery_is_ok in ("good", "medium", "low"):
            if battery_is_ok == "good":
                where_clauses.append("battery > 50")
            elif battery_is_ok == "medium":
                where_clauses.append("battery BETWEEN 20 AND 50")
            elif battery_is_ok == "low":
                where_clauses.append("battery < 20")

        # Filtro para desligados (temperatura > 100°C)
        if overheated in ("true", "1", "yes"):
            where_clauses.append("temperature_c > :overheated_threshold")
            params["overheated_threshold"] = 100

        # Filtro de ausência (deslocamento GPS)
        if is_missing in ("true", "1", "yes", "false", "0", "no"):
            gps_threshold_sql = text(
                """
                SELECT gps_displacement_threshold
                FROM alerts_definition
                WHERE type='GPS Displacement' AND client=:client
                LIMIT 1
            """
            )
            gps_result = db_session.execute(
                gps_threshold_sql, {"client": client_code}
            ).fetchone()
            gps_threshold = gps_result[0] if gps_result else 300
            params["gps_threshold"] = gps_threshold

            if is_missing in ("true", "1", "yes"):
                where_clauses.append(
                    "(displacement_meter > :gps_threshold OR displacement_meter IS NULL)"
                )
            else:
                where_clauses.append(
                    "(displacement_meter <= :gps_threshold AND displacement_meter IS NOT NULL)"
                )

        # Filtro de atividade (últimas 24 horas)
        if is_active in ("true", "1", "yes"):
            where_clauses.append("is_active = true")
        elif is_active in ("false", "0", "no"):
            where_clauses.append("is_active = false")

        # Filtro por bounding box (viewport do mapa)
        if all(v is not None for v in [min_lat, max_lat, min_lng, max_lng]):
            where_clauses.append(
                """
                COALESCE(
                    NULLIF(CAST(latitude AS FLOAT), 0),
                    NULLIF(CAST(last_known_latitude AS FLOAT), 0),
                    NULLIF(CAST(outlet_latitude AS FLOAT), 0)
                ) BETWEEN :min_lat AND :max_lat
                AND COALESCE(
                    NULLIF(CAST(longitude AS FLOAT), 0),
                    NULLIF(CAST(last_known_longitude AS FLOAT), 0),
                    NULLIF(CAST(outlet_longitude AS FLOAT), 0)
                ) BETWEEN :min_lng AND :max_lng
            """
            )
            params["min_lat"] = min_lat
            params["max_lat"] = max_lat
            params["min_lng"] = min_lng
            params["max_lng"] = max_lng

        where_sql = " AND ".join(where_clauses)

        # Query otimizada - apenas campos necessários para o mapa
        sql = text(
            f"""
            SELECT 
                oem_serial_number,
                bottler_equipment_number,
                outlet,
                is_online,
                temperature_c,
                displacement_meter,
                COALESCE(
                    NULLIF(CAST(latitude AS FLOAT), 0),
                    NULLIF(CAST(last_known_latitude AS FLOAT), 0),
                    NULLIF(CAST(outlet_latitude AS FLOAT), 0)
                ) as lat,
                COALESCE(
                    NULLIF(CAST(longitude AS FLOAT), 0),
                    NULLIF(CAST(last_known_longitude AS FLOAT), 0),
                    NULLIF(CAST(outlet_longitude AS FLOAT), 0)
                ) as lng
            FROM mv_smart_device_current_status
            WHERE {where_sql}
            AND (
                (latitude IS NOT NULL AND CAST(latitude AS FLOAT) != 0)
                OR (last_known_latitude IS NOT NULL AND CAST(last_known_latitude AS FLOAT) != 0)
                OR (outlet_latitude IS NOT NULL AND CAST(outlet_latitude AS FLOAT) != 0)
            )
            LIMIT :limit
        """
        )
        params["limit"] = limit

        result = db_session.execute(sql, params)

        # Buscar threshold de GPS para determinar is_missing
        gps_threshold_sql = text(
            """
            SELECT gps_displacement_threshold
            FROM alerts_definition
            WHERE type='GPS Displacement' AND client=:client
            LIMIT 1
        """
        )
        gps_result = db_session.execute(
            gps_threshold_sql, {"client": client_code}
        ).fetchone()
        gps_threshold = gps_result[0] if gps_result else 300

        markers = []
        for row in result:
            if row.lat and row.lng:
                is_missing = (row.displacement_meter or 0) > gps_threshold
                markers.append(
                    {
                        "serial": row.oem_serial_number,
                        "equip": row.bottler_equipment_number or "",
                        "outlet": row.outlet or "",
                        "online": row.is_online or False,
                        "temp": float(row.temperature_c) if row.temperature_c else None,
                        "missing": is_missing,
                        "lat": float(row.lat),
                        "lng": float(row.lng),
                    }
                )

        # Contar total disponível
        count_sql = text(
            f"""
            SELECT COUNT(*) FROM mv_smart_device_current_status
            WHERE {where_sql}
            AND (
                (latitude IS NOT NULL AND CAST(latitude AS FLOAT) != 0)
                OR (last_known_latitude IS NOT NULL AND CAST(last_known_latitude AS FLOAT) != 0)
                OR (outlet_latitude IS NOT NULL AND CAST(outlet_latitude AS FLOAT) != 0)
            )
        """
        )
        # Remover limit do params para count
        count_params = {k: v for k, v in params.items() if k != "limit"}
        total = db_session.execute(count_sql, count_params).scalar() or 0

        return jsonify(
            {
                "markers": markers,
                "count": len(markers),
                "total": total,
                "limited": total > limit,
            }
        )

    except Exception as e:
        print(f"[ERROR] Erro em get_map_markers: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        db_session.close()


@tracking_bp.route("/asset_details/<serial_number>", methods=["GET"])
@require_authentication
def get_asset_details(serial_number):
    """
    Retorna detalhes completos e agregados de um único asset,
    consumindo múltiplas materialized views e views.
    """
    db_session = get_session()
    client_code = session.get("user", {}).get("client")

    try:
        # 1. Dados básicos do asset (mv_smart_device_current_status)
        basic_sql = text(
            """
            SELECT
                client,
                bottler_equipment_number,
                oem_serial_number,
                outlet,
                sub_trade_channel,
                city,
                state,
                country,
                sub_client,
                smart_device_mac,
                temperature_c,
                COALESCE(
                    NULLIF(CAST(latitude AS FLOAT), 0),
                    NULLIF(CAST(last_known_latitude AS FLOAT), 0),
                    NULLIF(CAST(outlet_latitude AS FLOAT), 0)
                ) as latitude,
                COALESCE(
                    NULLIF(CAST(longitude AS FLOAT), 0),
                    NULLIF(CAST(last_known_longitude AS FLOAT), 0),
                    NULLIF(CAST(outlet_longitude AS FLOAT), 0)
                ) as longitude,
                NULLIF(CAST(outlet_latitude AS FLOAT), 0) as outlet_lat,
                NULLIF(CAST(outlet_longitude AS FLOAT), 0) as outlet_lng
            FROM mv_smart_device_current_status
            WHERE client = :client AND oem_serial_number = :serial
        """
        )

        basic_result = db_session.execute(
            basic_sql, {"client": client_code, "serial": serial_number}
        ).fetchone()

        if not basic_result:
            return jsonify({"error": "Asset não encontrado"}), 404

        # 2. Dados de saúde detalhados (health_events)
        health_sql = text(
            """
            SELECT
                battery,
                battery_status,
                temperature_c,
                evaporator_temperature_c,
                condensor_temperature_c,
                ambient_temperature_c,
                max_cabinet_temperature_c,
                min_cabinet_temperature_c,
                avg_power_consumption_watt,
                total_compressor_on_time_percent,
                light,
                light_status,
                cooler_voltage_v,
                max_voltage_v,
                min_voltage_v,
                interval_min,
                event_time,
                asset_type,
                outlet_type
            FROM health_events
            WHERE client = :client AND asset_serial_number = :serial
            ORDER BY event_time DESC
            LIMIT 1
        """
        )

        health_result = db_session.execute(
            health_sql, {"client": client_code, "serial": serial_number}
        ).fetchone()

        # 3. Estatísticas do dashboard (mv_smart_device_current_status) - inclui power/compressor como fallback
        stats_sql = text(
            """
            SELECT
                battery,
                temperature_c,
                avg_power_consumption_watt,
                total_compressor_on_time_percent
            FROM mv_smart_device_current_status
            WHERE client = :client AND oem_serial_number = :serial
        """
        )

        stats_result = db_session.execute(
            stats_sql, {"client": client_code, "serial": serial_number}
        ).fetchone()

        # 4. Combinar todos os dados
        asset_details = dict(basic_result._mapping)

        # Adicionar dados de saúde se disponíveis
        if health_result:
            health_data = dict(health_result._mapping)
            asset_details.update(
                {
                    "battery_level": health_data.get("battery"),
                    "battery_status": health_data.get("battery_status"),
                    "evaporator_temperature_c": health_data.get(
                        "evaporator_temperature_c"
                    ),
                    "condensor_temperature_c": health_data.get(
                        "condensor_temperature_c"
                    ),
                    "ambient_temperature_c": health_data.get("ambient_temperature_c"),
                    "max_cabinet_temperature_c": health_data.get(
                        "max_cabinet_temperature_c"
                    ),
                    "min_cabinet_temperature_c": health_data.get(
                        "min_cabinet_temperature_c"
                    ),
                    "avg_power_consumption_watt": health_data.get(
                        "avg_power_consumption_watt"
                    ),
                    "total_compressor_on_time_percent": health_data.get(
                        "total_compressor_on_time_percent"
                    ),
                    "light_status": health_data.get("light_status"),
                    "cooler_voltage_v": health_data.get("cooler_voltage_v"),
                    "max_voltage_v": health_data.get("max_voltage_v"),
                    "min_voltage_v": health_data.get("min_voltage_v"),
                    "last_health_event": health_data.get("event_time"),
                    "asset_type": health_data.get("asset_type"),
                    "outlet_type": health_data.get("outlet_type"),
                }
            )

        # Adicionar dados das estatísticas se disponíveis (e como fallback para power/compressor)
        if stats_result:
            stats_data = dict(stats_result._mapping)
            asset_details.update(
                {
                    "battery_from_stats": stats_data.get("battery"),
                    "latest_temperature_from_stats": stats_data.get("temperature_c"),
                }
            )

        # 5. Obter consumo e compressor dos últimos 30 dias da MV
        health_avg_sql = text(
            """
            SELECT
                avg_power_consumption_watt as avg_power_30d,
                total_compressor_on_time_percent as avg_compressor_30d
            FROM mv_smart_device_current_status
            WHERE client = :client
            AND oem_serial_number = :serial
        """
        )
        health_avg_result = db_session.execute(
            health_avg_sql, {"client": client_code, "serial": serial_number}
        ).fetchone()

        if health_avg_result:
            asset_details["avg_power_consumption_watt_30d"] = health_avg_result[0] or 0
            asset_details["avg_compressor_percent_30d"] = health_avg_result[1] or 0

        # 6. Calcular estatísticas de porta (últimos 30 dias)
        door_stats = calculate_door_event_statistics(db_session, serial_number, days=30)
        asset_details.update(
            {
                "door_event_average_morning": door_stats.get("Morning", {}).get(
                    "average", 0
                ),
                "door_event_average_afternoon": door_stats.get("Afternoon", {}).get(
                    "average", 0
                ),
                "door_event_average_night": door_stats.get("Night", {}).get(
                    "average", 0
                ),
            }
        )

        # 7. Buscar temperatura do ambiente local via Open-Meteo API
        if asset_details.get("latitude") and asset_details.get("longitude"):
            ambient_temp = get_ambient_temperature(
                asset_details.get("latitude"), asset_details.get("longitude")
            )
            asset_details["ambient_temperature_from_api"] = ambient_temp
        else:
            asset_details["ambient_temperature_from_api"] = None

        # 8. Garantir que nenhum valor None seja enviado
        for key, value in asset_details.items():
            if value is None:
                if any(
                    word in key.lower()
                    for word in [
                        "average",
                        "level",
                        "temperature",
                        "voltage",
                        "power",
                        "percent",
                    ]
                ):
                    asset_details[key] = 0
                elif "time" in key.lower():
                    asset_details[key] = None  # Manter None para timestamps
                else:
                    asset_details[key] = "N/A"

        return jsonify(asset_details)

    except Exception as e:
        print(f"[ERROR] Erro em get_asset_details: {str(e)}")
        return (
            jsonify({"error": "Erro ao buscar detalhes do asset", "details": str(e)}),
            500,
        )
    finally:
        db_session.close()


@tracking_bp.route("/asset-analytics/<serial_number>", methods=["GET"])
@require_authentication
def get_asset_analytics(serial_number):
    """
    Retorna dados analíticos detalhados do asset similar ao dashboard.
    Inclui gráficos de temperatura e porta ao longo do dia, estatísticas de consumo, etc.
    """
    db_session = get_session()
    client_code = session.get("user", {}).get("client")

    try:
        from datetime import datetime, timedelta, timezone

        # Validações
        now = datetime.now(timezone.utc)
        one_day_before = now - timedelta(hours=24)
        thirty_days_ago = now - timedelta(days=30)

        # Obter definições de alerta para temperatura
        temp_alert_definition = (
            db_session.query(AlertsDefinition)
            .filter(
                AlertsDefinition.client == client_code,
                AlertsDefinition.type == "Temperature Alert",
            )
            .first()
        )

        temp_min = (
            temp_alert_definition.temperature_below
            if temp_alert_definition
            and temp_alert_definition.temperature_below is not None
            else 0
        )
        temp_max = (
            temp_alert_definition.temperature_above
            if temp_alert_definition
            and temp_alert_definition.temperature_above is not None
            else 7
        )

        # 1. Dados horários de temperatura e porta para o asset específico
        hourly_sql = text(
            """
            SELECT
                EXTRACT(HOUR FROM he.event_time AT TIME ZONE 'UTC') as hour_value,
                AVG(CAST(he.temperature_c as FLOAT)) as hourly_avg_temp,
                COUNT(de.id) as hourly_door_count
            FROM health_events he
            LEFT JOIN door de ON he.asset_serial_number = de.asset_serial_number 
                AND DATE(de.open_event_time) = DATE(he.event_time)
                AND EXTRACT(HOUR FROM de.open_event_time) = EXTRACT(HOUR FROM he.event_time)
            WHERE he.client = :client 
                AND he.asset_serial_number = :serial
                AND he.event_time >= :one_day_before
            GROUP BY EXTRACT(HOUR FROM he.event_time AT TIME ZONE 'UTC')
            ORDER BY hour_value
        """
        )

        hourly_data = db_session.execute(
            hourly_sql,
            {
                "client": client_code,
                "serial": serial_number,
                "one_day_before": one_day_before,
            },
        ).fetchall()

        # Processar dados horários
        hourly_temp = {}
        hourly_doors = {}

        def _round_preserve_sign(v, decimals=2, min_display=0.01):
            if v is None:
                return 0
            raw = float(v)
            rounded = round(raw, decimals)
            if rounded == 0 and raw < 0:
                return -min_display
            return float(rounded)

        for row in hourly_data:
            hour = int(row.hour_value) if row.hour_value is not None else 0
            hourly_temp[hour] = _round_preserve_sign(row.hourly_avg_temp)
            hourly_doors[hour] = int(
                row.hourly_door_count if row.hourly_door_count else 0
            )

        # Preencher horas vazias
        hourly_labels = [f"{h:02d}:00" for h in range(24)]
        hourly_temp_data = [hourly_temp.get(h, 0) for h in range(24)]
        hourly_door_data = [hourly_doors.get(h, 0) for h in range(24)]

        # 2. Estatísticas agregadas últimos 30 dias
        stats_sql = text(
            """
            WITH LatestHealth AS (
                SELECT battery, temperature_c, avg_power_consumption_watt, total_compressor_on_time_percent,
                       ROW_NUMBER() OVER (ORDER BY event_time DESC) AS rn
                FROM health_events
                WHERE client = :client 
                    AND asset_serial_number = :serial
                    AND event_time >= :thirty_days_ago
            )
            SELECT
                AVG(CAST(temperature_c as FLOAT)) as avg_temperature,
                MAX(CAST(temperature_c as FLOAT)) as max_temperature,
                MIN(CAST(temperature_c as FLOAT)) as min_temperature,
                AVG(CAST(battery as FLOAT)) as avg_battery,
                AVG(CAST(avg_power_consumption_watt as FLOAT)) as avg_power,
                AVG(CAST(total_compressor_on_time_percent as FLOAT)) as avg_compressor_time
            FROM LatestHealth
        """
        )

        stats_result = db_session.execute(
            stats_sql,
            {
                "client": client_code,
                "serial": serial_number,
                "thirty_days_ago": thirty_days_ago,
            },
        ).fetchone()

        # 3. Contagem de eventos de porta por período do dia
        door_period_sql = text(
            """
            SELECT
                CASE 
                    WHEN EXTRACT(HOUR FROM open_event_time) >= 6 AND EXTRACT(HOUR FROM open_event_time) < 12 THEN 'morning'
                    WHEN EXTRACT(HOUR FROM open_event_time) >= 12 AND EXTRACT(HOUR FROM open_event_time) < 18 THEN 'afternoon'
                    ELSE 'night'
                END as period,
                COUNT(*) as count
            FROM door
            WHERE client = :client 
                AND asset_serial_number = :serial
                AND open_event_time >= :thirty_days_ago
            GROUP BY period
        """
        )

        door_period = db_session.execute(
            door_period_sql,
            {
                "client": client_code,
                "serial": serial_number,
                "thirty_days_ago": thirty_days_ago,
            },
        ).fetchall()

        door_periods = {row.period: row.count for row in door_period}

        # 4. Status de temperatura categorizado
        temp_status_sql = text(
            """
            SELECT
                CASE 
                    WHEN CAST(temperature_c as FLOAT) BETWEEN :temp_min AND :temp_max THEN 'ok'
                    WHEN CAST(temperature_c as FLOAT) > :temp_max THEN 'above'
                    ELSE 'below'
                END as status,
                COUNT(*) as count
            FROM health_events
            WHERE client = :client 
                AND asset_serial_number = :serial
                AND event_time >= :thirty_days_ago
            GROUP BY status
        """
        )

        temp_status = db_session.execute(
            temp_status_sql,
            {
                "client": client_code,
                "serial": serial_number,
                "thirty_days_ago": thirty_days_ago,
                "temp_min": temp_min,
                "temp_max": temp_max,
            },
        ).fetchall()

        temp_status_counts = {row.status: row.count for row in temp_status}

        return jsonify(
            {
                "status": "success",
                "hourly_data": {
                    "labels": hourly_labels,
                    "temperature": hourly_temp_data,
                    "doors": hourly_door_data,
                },
                "statistics": {
                    "avg_temperature": (
                        _round_preserve_sign(stats_result.avg_temperature)
                        if stats_result
                        else 0
                    ),
                    "max_temperature": (
                        _round_preserve_sign(stats_result.max_temperature)
                        if stats_result
                        else 0
                    ),
                    "min_temperature": (
                        _round_preserve_sign(stats_result.min_temperature)
                        if stats_result
                        else 0
                    ),
                    "avg_battery": round(
                        (
                            float(stats_result.avg_battery)
                            if stats_result and stats_result.avg_battery
                            else 0
                        ),
                        2,
                    ),
                    "avg_power_consumption": round(
                        (
                            float(stats_result.avg_power)
                            if stats_result and stats_result.avg_power
                            else 0
                        ),
                        2,
                    ),
                    "avg_compressor_time": round(
                        (
                            float(stats_result.avg_compressor_time)
                            if stats_result and stats_result.avg_compressor_time
                            else 0
                        ),
                        2,
                    ),
                },
                "door_periods": {
                    "morning": door_periods.get("morning", 0),
                    "afternoon": door_periods.get("afternoon", 0),
                    "night": door_periods.get("night", 0),
                },
                "temperature_status": {
                    "ok": temp_status_counts.get("ok", 0),
                    "above": temp_status_counts.get("above", 0),
                    "below": temp_status_counts.get("below", 0),
                    "temp_min": temp_min,
                    "temp_max": temp_max,
                },
            }
        )

    except Exception as e:
        print(f"[ERROR] Erro em get_asset_analytics: {str(e)}")
        return jsonify({"error": "Erro ao buscar analytics", "details": str(e)}), 500
    finally:
        db_session.close()


@tracking_bp.route("/asset-hourly-chart/<serial_number>", methods=["GET"])
@require_authentication
def get_asset_hourly_chart(serial_number):
    """
    Retorna a MÉDIA de temperatura e abertura de portas por hora do dia (00:00 a 23:00)
    calculada sobre os últimos 30 dias para um asset específico.
    Usa a mesma lógica do dashboard (mv_client_overview) mas filtrado por asset.
    Retorna 24 pontos de dados (um para cada hora do dia).
    """
    db_session = get_session()
    client_code = session.get("user", {}).get("client")

    try:
        from datetime import datetime, timedelta, timezone

        # Definir intervalo de tempo (últimos 30 dias)
        now = datetime.now(timezone.utc)
        start_date = now - timedelta(days=30)

        # Query de Temperatura - Média por hora do dia (0-23) nos últimos 30 dias
        # Mesma lógica usada pela mv_client_overview
        temp_sql = text(
            """
            SELECT
                EXTRACT(HOUR FROM event_time)::INT as hour,
                ROUND(AVG(CAST(temperature_c AS FLOAT))::numeric, 2) as avg_temp
            FROM health_events
            WHERE client = :client 
                AND asset_serial_number = :serial
                AND event_time >= :start_date
                AND temperature_c IS NOT NULL
                AND temperature_c BETWEEN -50 AND 50
            GROUP BY EXTRACT(HOUR FROM event_time)::INT
            ORDER BY hour
        """
        )

        # Query de Portas - Média de aberturas por hora do dia (0-23) nos últimos 30 dias
        # Calcula: total de aberturas por hora / número total de dias ÚNICOS com eventos (global)
        # Exemplo: 32 eventos em 16 dias diferentes = média de 2 por dia
        door_sql = text(
            """
            WITH total_unique_days AS (
                SELECT COUNT(DISTINCT DATE(open_event_time)) as days_count
                FROM door
                WHERE client = :client 
                    AND asset_serial_number = :serial
                    AND open_event_time >= :start_date
            ),
            hourly_counts AS (
                SELECT 
                    hour_in_day as hour,
                    COUNT(*) as total_count
                FROM door
                WHERE client = :client 
                    AND asset_serial_number = :serial
                    AND open_event_time >= :start_date
                GROUP BY hour_in_day
            )
            SELECT 
                hc.hour,
                ROUND((hc.total_count::FLOAT / GREATEST(tud.days_count, 1))::numeric, 2) as avg_door_count,
                hc.total_count,
                tud.days_count as unique_days
            FROM hourly_counts hc
            CROSS JOIN total_unique_days tud
            ORDER BY hc.hour
        """
        )

        # Executar consultas
        temp_results = db_session.execute(
            temp_sql,
            {"client": client_code, "serial": serial_number, "start_date": start_date},
        ).fetchall()

        door_results = db_session.execute(
            door_sql,
            {"client": client_code, "serial": serial_number, "start_date": start_date},
        ).fetchall()

        # Criar mapas para acesso rápido: { hora: valor }
        temp_map = {}
        for row in temp_results:
            if row.hour is not None:
                temp_map[int(row.hour)] = float(row.avg_temp) if row.avg_temp else None

        door_map = {}
        for row in door_results:
            if row.hour is not None:
                door_map[int(row.hour)] = (
                    float(row.avg_door_count) if row.avg_door_count else 0
                )

        # Preparar arrays finais com 24 pontos (00:00 a 23:00)
        # Formato igual ao dashboard: [{"hour": 0, "avg_temp": X}, ...]
        labels = []
        temperature_data = []
        door_data = []

        # Dados raw para compatibilidade com frontend (igual dashboard)
        raw_temp_data = []
        raw_door_data = []

        for hour in range(24):
            # Label no formato "00:00", "01:00", etc.
            labels.append(f"{hour:02d}:00")

            # Temperatura média para esta hora (ou None se não houver dados)
            temp_val = temp_map.get(hour)
            temperature_data.append(temp_val)
            raw_temp_data.append({"hour": hour, "avg_temp": temp_val})

            # Média de aberturas de porta para esta hora (ou 0 se não houver dados)
            door_val = door_map.get(hour, 0)
            door_data.append(door_val)
            raw_door_data.append({"hour": hour, "avg_door_count": door_val})

        return jsonify(
            {
                "labels": labels,
                "temperature": temperature_data,
                "door_count": door_data,
                "raw_temp_data": raw_temp_data,
                "raw_door_data": raw_door_data,
                "period": "Média dos últimos 30 dias por hora do dia",
            }
        )

    except Exception as e:
        print(f"[ERROR] Erro em get_asset_hourly_chart: {str(e)}")
        import traceback

        traceback.print_exc()
        return (
            jsonify({"error": "Erro ao buscar dados gráficos", "details": str(e)}),
            500,
        )
    finally:
        db_session.close()


# ==================== SIMPLE TRACKING - MOVEMENTS FIND HUB ====================


def is_client_authorized_for_simple_tracking(client_code):
    """Verifica se o cliente tem acesso ao rastreio simples."""
    return client_code in SIMPLE_TRACKING_AUTHORIZED_CLIENTS


@tracking_bp.route("/simple", methods=["GET"])
@require_authentication
def render_simple_tracking():
    """
    Renderiza página simples de rastreio com dados da tabela movements_find_hub.
    Apenas clientes autorizados têm acesso.
    """
    try:
        client_code = session.get("user", {}).get("client")

        # Verificar se cliente tem acesso
        if not is_client_authorized_for_simple_tracking(client_code):
            print(
                f"[WARNING] Cliente não autorizado para rastreio simples: {client_code}"
            )
            return redirect(url_for("dashboard.render_dashboard"))

        return render_template("portal/tracking/simple_tracking.html")
    except Exception as e:
        print(f"[ERROR] Error rendering simple tracking page: {str(e)}")
        return redirect(url_for("dashboard.render_dashboard"))


@tracking_bp.route("/simple/movements", methods=["GET"])
@require_authentication
def get_simple_movements():
    """
    Retorna dados simples de movimentos da tabela movements_find_hub.
    Parâmetros de busca suportados:
    - device_number: Número do smart device
    - days: Número de dias a buscar (padrão: 30)
    - page: Número da página (padrão: 1)
    - limit: Quantidade por página (padrão: 50)
    """
    client_code = session.get("user", {}).get("client")

    # Verificar se cliente tem acesso
    if not is_client_authorized_for_simple_tracking(client_code):
        return jsonify({"error": "Acesso não autorizado"}), 403

    db_session = get_session()

    try:
        # Coletar parâmetros
        device_number = request.args.get("device_number", "").strip()
        days = request.args.get("days", 30, type=int)
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 5, type=int)

        # Construir query base
        query = db_session.query(MovementsFindHub).filter(
            MovementsFindHub.client == client_code
        )

        # Aplicar filtros
        if device_number:
            query = query.filter(
                MovementsFindHub.smart_device_number.ilike(f"%{device_number}%")
            )

        # Filtrar por data
        if days:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            query = query.filter(MovementsFindHub.start_time >= cutoff_date)

        # Ordenar por data descrescente
        query = query.order_by(MovementsFindHub.start_time.desc())

        # Contar total
        total_count = query.count()

        # Paginação
        offset = (page - 1) * limit
        movements = query.offset(offset).limit(limit).all()

        # Serializar dados
        data = []
        for movement in movements:
            data.append(
                {
                    "id": movement.id,
                    "smart_device_number": movement.smart_device_number,
                    "latitude": movement.latitude,
                    "longitude": movement.longitude,
                    "start_time": (
                        movement.start_time.isoformat() if movement.start_time else None
                    ),
                    "accuracy_meter": movement.accuracy_meter,
                }
            )

        # Calcular páginas
        total_pages = (total_count + limit - 1) // limit

        return jsonify(
            {
                "data": data,
                "pagination": {
                    "page": page,
                    "pages": total_pages,
                    "total": total_count,
                    "per_page": limit,
                    "has_prev": page > 1,
                    "has_next": page < total_pages,
                },
            }
        )

    except Exception as e:
        print(f"[ERROR] Erro em get_simple_movements: {str(e)}")
        return jsonify({"error": "Erro ao buscar movimentos", "details": str(e)}), 500
    finally:
        db_session.close()
