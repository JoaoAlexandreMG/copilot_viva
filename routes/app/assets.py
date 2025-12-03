from flask import (
    Blueprint,
    jsonify,
    render_template,
    request,
    session,
    redirect,
    url_for,
)
import requests
from models.models import Asset, HealthEvent, Movement, SubClient
from db.database import get_session
from utils.location import correlate_asset_temperature_with_location, haversine_distance
from sqlalchemy import func
import logging
import math
import re
import unicodedata

logger = logging.getLogger(__name__)

# Blueprint configuration
assets_bp = Blueprint("assets", __name__, url_prefix="/assets")

# ===================================
# UTILITY FUNCTIONS
# ===================================


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


def get_user_country(user_lat, user_lon):
    """
    Get user's country using BigDataCloud API
    """
    try:
        url = f"https://api.bigdatacloud.net/data/reverse-geocode-client?latitude={user_lat}&longitude={user_lon}"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            location_data = response.json()
            country = location_data.get("countryName")
            if country:
                return remover_special_caracteres(country)
    except Exception as e:
        print(f"[WARNING] Error getting country from API: {e}")
    return None


def require_authentication():
    """
    Check if user is authenticated
    """
    user = session.get("user")
    if not user:
        return redirect(url_for("index"))
    return user


def get_user_location():
    """
    Get user location from session
    """
    stored_location = session.get("user_location")
    if stored_location:
        try:
            return float(stored_location.get("lat")), float(stored_location.get("lon"))
        except (TypeError, ValueError):
            pass
    return None, None


def calculate_asset_distance(asset, user_lat, user_lon):
    """
    Calculate distance between user and asset
    """
    if not (user_lat and user_lon and asset.latitude and asset.longitude):
        return None

    try:
        distance = haversine_distance(
            user_lat, user_lon, float(asset.latitude), float(asset.longitude)
        )
        if distance is None or not math.isfinite(distance):
            return None
        return round(distance, 2)
    except (TypeError, ValueError):
        return None


# ===================================
# WEB ROUTES (HTML PAGES)
# ===================================


@assets_bp.route("/", methods=["GET"])
def list_assets():
    """
    LIST ASSETS - Main assets listing page with filtering, sorting and pagination

    Query Parameters:
    - user_lat, user_lon: User location coordinates
    - sub_client: Optional subclient filter
    - search: Text search (serial, outlet, address)
    - page: Page number (default: 1)
    - per_page: Items per page (default: 5)

    Features:
    - Authentication required
    - Uses MATERIALIZED VIEW (mv_asset_current_status) for optimal performance
    - Distance-based sorting (closest assets first) in real-time with user location
    - Subclient filtering
    - Text search filtering
    - Pagination (5 per page)
    """
    try:
        # Authentication check
        user = require_authentication()
        if not isinstance(user, dict):  # It's a redirect response
            return user

        # Get coordinates from request or session
        user_lat = request.args.get("user_lat", type=float)
        user_lon = request.args.get("user_lon", type=float)

        # Persist coordinates in session for real-time location updates
        if user_lat is not None and user_lon is not None:
            session["user_location"] = {"lat": user_lat, "lon": user_lon}
        else:
            user_lat, user_lon = get_user_location()

        # Get filters from query parameters
        sub_client = request.args.get("sub_client")
        page = request.args.get("page", 1, type=int)
        per_page = 5  # Fixed: 5 items per page

        if page < 1:
            page = 1

        db_session = get_session()
        client_code = user["client"]

        # Build query using MATERIALIZED VIEW for comprehensive information
        where_clauses = ["client = :client"]
        params = {"client": client_code}

        # Apply subclient filter if provided
        if sub_client:
            where_clauses.append("sub_client = :sub_client")
            params["sub_client"] = sub_client

        # Build the WHERE clause
        where_sql = " AND ".join(where_clauses)

        # Query from MATERIALIZED VIEW - contains all necessary fields
        # Fields: battery, last_health_time, temperature_c, avg_power_consumption_watt,
        #         total_compressor_on_time_percent, last_movement_time, etc.
        from sqlalchemy import text as sql_text

        sql_query = sql_text(
            f"""
            SELECT * FROM mv_asset_current_status
            WHERE {where_sql}
        """
        )
        result = db_session.execute(sql_query, params)

        # Convert rows to dictionaries
        all_assets = []
        for row in result:
            asset_data = dict(row._mapping)

            # Ensure numeric fields are properly typed
            if asset_data.get("battery") is not None:
                try:
                    asset_data["battery"] = int(float(asset_data["battery"]))
                except (TypeError, ValueError):
                    asset_data["battery"] = None

            if asset_data.get("temperature_c") is not None:
                try:
                    asset_data["temperature_c"] = float(asset_data["temperature_c"])
                except (TypeError, ValueError):
                    asset_data["temperature_c"] = None

            all_assets.append(asset_data)

        # Calculate distances and sort by proximity (in real-time with user location)
        assets_with_distance = []
        for asset in all_assets:
            distance = None
            if (
                user_lat
                and user_lon
                and asset.get("latitude")
                and asset.get("longitude")
            ):
                try:
                    distance = haversine_distance(
                        user_lat,
                        user_lon,
                        float(asset["latitude"]),
                        float(asset["longitude"]),
                    )
                    if distance is None or not math.isfinite(distance):
                        distance = None
                    else:
                        distance = round(distance, 2)
                except (TypeError, ValueError):
                    distance = None

            assets_with_distance.append(
                {
                    "asset": asset,
                    "distance": distance if distance is not None else float("inf"),
                }
            )

        # Sort by distance (closest first) - REAL-TIME based on current user location
        assets_with_distance.sort(key=lambda x: x["distance"])

        # Extract sorted assets and add distance attribute
        sorted_assets = []
        for item in assets_with_distance:
            asset = item["asset"]
            distance_value = item["distance"]
            asset["distance_km"] = (
                distance_value if math.isfinite(distance_value) else None
            )
            sorted_assets.append(asset)

        # Apply pagination
        total_assets = len(sorted_assets)
        total_pages = (
            (total_assets + per_page - 1) // per_page if total_assets > 0 else 1
        )

        if page > total_pages and total_pages > 0:
            page = total_pages

        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_assets = sorted_assets[start_idx:end_idx]

        # Get available subclients for dropdown
        available_subclients = (
            db_session.query(SubClient).filter(SubClient.client == client_code).all()
        )

        # Calculate KPIs from all filtered assets (not just paginated)
        low_battery_count = 0
        high_temp_count = 0
        online_count = 0
        missing_count = 0

        for asset in sorted_assets:
            # Low battery: < 30%
            battery = asset.get("battery")
            if battery is not None and battery < 30:
                low_battery_count += 1

            # High temperature: >= 40°C
            temp = asset.get("temperature_c")
            if temp is not None and temp >= 40:
                high_temp_count += 1

            # Online status
            if asset.get("is_online"):
                online_count += 1

            # Missing status
            if asset.get("is_missing"):
                missing_count += 1

        return render_template(
            "app/assets.html",
            assets=paginated_assets,
            total=total_assets,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
            user=user,
            selected_sub_client=sub_client,
            available_subclients=available_subclients,
            user_lat=user_lat,
            user_lon=user_lon,
            low_battery_count=low_battery_count,
            high_temp_count=high_temp_count,
            online_count=online_count,
            missing_count=missing_count,
        )

    except Exception as e:
        print(f"[ERROR] Error in list_assets: {str(e)}")
        import traceback

        traceback.print_exc()
        return render_template(
            "app/assets.html",
            assets=[],
            total=0,
            page=1,
            per_page=5,
            total_pages=0,
            user={},
        )


@assets_bp.route("/location", methods=["POST"])
def store_user_location():
    """
    STORE LOCATION - Store user location in session for reuse

    Request Body (JSON):
    {
        "latitude": float,
        "longitude": float,
        "country": string (optional),
        "country_code": string (optional)
    }

    Response:
    {
        "status": "stored"
    }
    """
    try:
        payload = request.get_json(silent=True) or {}

        # Store country and country code if provided
        country = payload.get("country")
        country_code = payload.get("country_code")
        if country and country_code:
            session["user_location"] = {
                "country": country,
                "country_code": country_code,
            }
            return jsonify({"status": "stored"}), 200

        # Validate latitude and longitude
        latitude = payload.get("latitude")
        longitude = payload.get("longitude")
        if latitude is None or longitude is None:
            return jsonify({"error": "latitude and longitude required"}), 400

        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except (TypeError, ValueError):
            return jsonify({"error": "invalid coordinate values"}), 400

        # Store coordinates in session
        session["user_location"] = {"lat": latitude, "lon": longitude}
        return jsonify({"status": "stored"}), 200

    except Exception as e:
        logger.error(f"Error in store_user_location: {str(e)}")
        return jsonify({"error": "failed to store location"}), 500


@assets_bp.route("/api/search", methods=["GET"])
def search_assets_api():
    """
    SEARCH ASSETS API - Advanced search and filtering with pagination

    Query Parameters:
    - outlet_code: Filter by outlet code
    - client: Filter by client
    - sub_client: Filter by subclient
    - page: Page number (default: 1)
    - per_page: Items per page (default: 10)
    - outlet: Multiple outlet names
    - city: Multiple cities
    - state: Multiple states
    - country: Multiple countries
    - temperature: Temperature status (good/high)
    - battery: Battery status (good/low)
    - search: Text search on serial, street, city

    Response:
    {
        "assets": [...],
        "page": int,
        "per_page": int,
        "total": int,
        "total_pages": int
    }
    """
    try:
        # Get query parameters
        outlet_code = request.args.get("outlet_code")
        client = request.args.get("client")
        sub_client = request.args.get("sub_client")
        page = request.args.get("page", default=1, type=int)
        per_page = request.args.get("per_page", default=10, type=int)

        # Advanced filter parameters
        outlets = request.args.getlist("outlet")
        cities = request.args.getlist("city")
        states = request.args.getlist("state")
        countries = request.args.getlist("country")
        temperatures = request.args.getlist("temperature")
        batteries = request.args.getlist("battery")
        search = request.args.get("search", "").strip()

        # Get user location for distance calculation
        user_lat, user_lon = get_user_location()

        db_session = get_session()

        # Build base query
        query = db_session.query(Asset)

        # Apply initial filters
        if outlet_code:
            query = query.filter(Asset.outlet_code == outlet_code)
        elif client:
            query = query.filter(Asset.client == client)
            if sub_client:
                query = query.filter(Asset.sub_client == sub_client)

        # Apply advanced filters
        if outlets:
            query = query.filter(Asset.outlet.in_(outlets))
        if cities:
            query = query.filter(Asset.city.in_(cities))
        if states:
            query = query.filter(Asset.state.in_(states))
        if countries:
            query = query.filter(Asset.country.in_(countries))

        # Apply text search
        if search:
            search_pattern = f"%{search}%"
            query = query.filter(
                (Asset.oem_serial_number.ilike(search_pattern))
                | (Asset.street.ilike(search_pattern))
                | (Asset.city.ilike(search_pattern))
            )

        # Get all assets matching basic filters
        all_filtered_assets = query.all()

        # Apply health-based filters (temperature, battery)
        filtered_assets_final = []
        for asset in all_filtered_assets:
            # Get health data
            health_event = (
                db_session.query(HealthEvent)
                .filter(
                    HealthEvent.asset_serial_number == asset.oem_serial_number,
                    HealthEvent.event_type == "Cabinet Temperature",
                )
                .first()
            )

            asset_dict = asset.to_dict()
            asset_dict["health"] = health_event.to_dict() if health_event else None

            # Temperature filter
            if temperatures:
                if (
                    asset_dict["health"]
                    and asset_dict["health"].get("temperature_c") is not None
                ):
                    temp_c = float(asset_dict["health"].get("temperature_c"))
                    asset_temp = "high" if temp_c >= 40 else "good"
                else:
                    asset_temp = "unknown"

                if asset_temp not in temperatures:
                    continue

            # Battery filter
            if batteries:
                if asset.battery_level is not None:
                    asset_battery = "low" if asset.battery_level < 30 else "good"
                else:
                    asset_battery = "unknown"

                if asset_battery not in batteries:
                    continue

            filtered_assets_final.append((asset, asset_dict))

        # Total count after all filters
        total_assets = len(filtered_assets_final)

        # Apply pagination
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_data = filtered_assets_final[start_idx:end_idx]

        # Get serial numbers for batch movement query
        serial_numbers = [
            asset_obj.oem_serial_number for asset_obj, _ in paginated_data
        ]

        # Batch query for latest movements
        movement_map = {}
        if serial_numbers:
            latest_movement_subquery = (
                db_session.query(func.max(Movement.id).label("max_id"))
                .filter(Movement.asset_serial_number.in_(serial_numbers))
                .group_by(Movement.asset_serial_number)
                .subquery()
            )

            movements = (
                db_session.query(Movement)
                .filter(
                    Movement.id.in_(db_session.query(latest_movement_subquery.c.max_id))
                )
                .all()
            )
            movement_map = {m.asset_serial_number: m.to_dict() for m in movements}

        # Build final response
        assets_response = []
        for asset_obj, asset_dict in paginated_data:
            # Add distance calculation
            distance_km = calculate_asset_distance(asset_obj, user_lat, user_lon)

            # Add movement and distance data
            asset_dict["movement"] = movement_map.get(asset_obj.oem_serial_number)
            asset_dict["distance_km"] = distance_km
            assets_response.append(asset_dict)

        return jsonify(
            {
                "assets": assets_response,
                "page": page,
                "per_page": per_page,
                "total": total_assets,
                "total_pages": (total_assets + per_page - 1) // per_page,
            }
        )

    except Exception as e:
        print(f"[ERROR] Error in search_assets_api: {str(e)}")
        import traceback

        traceback.print_exc()
        return (
            jsonify(
                {"assets": [], "page": 1, "per_page": 10, "total": 0, "total_pages": 0}
            ),
            500,
        )


@assets_bp.route("/api/filter-options", methods=["GET"])
def get_filter_options_api():
    """
    FILTER OPTIONS API - Get unique filter values for advanced search

    Response:
    {
        "outlets": [string],
        "cities": [string],
        "states": [string],
        "countries": [string]
    }
    """
    try:
        db_session = get_session()

        # Get unique values for each filter category
        outlets = [
            row[0] for row in db_session.query(Asset.outlet).distinct().all() if row[0]
        ]
        cities = [
            row[0] for row in db_session.query(Asset.city).distinct().all() if row[0]
        ]
        states = [
            row[0] for row in db_session.query(Asset.state).distinct().all() if row[0]
        ]
        countries = [
            row[0] for row in db_session.query(Asset.country).distinct().all() if row[0]
        ]

        return jsonify(
            {
                "outlets": sorted(outlets),
                "cities": sorted(cities),
                "states": sorted(states),
                "countries": sorted(countries),
            }
        )

    except Exception as e:
        print(f"[ERROR] Error in get_filter_options_api: {str(e)}")
        return (
            jsonify({"outlets": [], "cities": [], "states": [], "countries": []}),
            500,
        )


@assets_bp.route("/<string:asset_serial_number>", methods=["GET"])
def asset_detail(asset_serial_number):
    """
    ASSET DETAIL - Individual asset detail page with comprehensive information from MATERIALIZED VIEW

    URL Parameters:
    - asset_serial_number: Asset OEM serial number

    Features:
    - Authentication required
    - Client security (users only see their client's assets)
    - Consumes mv_asset_current_status MATERIALIZED VIEW for all asset data
    - Health events data with historical charts
    - Movement data with door activity
    - Distance calculation from user location
    """
    try:
        # Authentication check
        user = require_authentication()
        if not isinstance(user, dict):  # It's a redirect response
            return user

        db_session = get_session()
        client_code = user["client"]

        # Query MATERIALIZED VIEW for asset data (includes all view fields)
        # This replaces multiple ORM queries for optimal performance
        from sqlalchemy import text as sql_text

        sql_query = sql_text(
            """
            SELECT * FROM mv_asset_current_status 
            WHERE oem_serial_number = :serial AND client = :client
        """
        )
        result = db_session.execute(
            sql_query, {"serial": asset_serial_number, "client": client_code}
        )

        row = result.first()
        if not row:
            return render_template("/app/asset_not_found.html"), 404

        # Convert row to dictionary
        asset_dict = dict(row._mapping)

        # Ensure location fields exist
        asset_dict["has_location"] = bool(
            asset_dict.get("latitude") and asset_dict.get("longitude")
        )

        # Get latest health events (separate query - necessary for charts)
        latest_health_temperature = (
            db_session.query(HealthEvent)
            .filter(
                HealthEvent.asset_serial_number == asset_serial_number,
                HealthEvent.event_type == "Cabinet Temperature",
            )
            .order_by(HealthEvent.event_time.desc())
            .first()
        )

        # Get latest any health event
        latest_health_any = (
            db_session.query(HealthEvent)
            .filter(HealthEvent.asset_serial_number == asset_serial_number)
            .order_by(HealthEvent.event_time.desc())
            .first()
        )

        # Get latest movement
        latest_movement = (
            db_session.query(Movement)
            .filter(Movement.asset_serial_number == asset_serial_number)
            .order_by(Movement.start_time.desc())
            .first()
        )

        # Get comprehensive health data - already in mv_asset_current_status
        # The view contains: battery, last_health_time, temperature_c,
        # avg_power_consumption_watt, total_compressor_on_time_percent
        asset_dict["battery_level"] = asset_dict.get("battery")
        asset_dict["latest_power_consumption"] = asset_dict.get(
            "avg_power_consumption_watt"
        )
        asset_dict["compressor_time"] = asset_dict.get(
            "total_compressor_on_time_percent"
        )

        # Get historical data for charts (last 30 days) - filtrar temperaturas anômalas
        temperature_history = (
            db_session.query(HealthEvent)
            .filter(
                HealthEvent.asset_serial_number == asset_serial_number,
                HealthEvent.event_type == "Cabinet Temperature",
                HealthEvent.temperature_c.isnot(None),
                HealthEvent.temperature_c >= -30,
                HealthEvent.temperature_c <= 20,
            )
            .order_by(HealthEvent.event_time.desc())
            .limit(30)
            .all()
        )

        # Get door events from door table (last 7 days for chart)
        from datetime import datetime, timedelta

        seven_days_ago = datetime.utcnow() - timedelta(days=7)

        door_events_query = sql_text(
            """
            SELECT 
                open_event_time,
                close_event_time,
                door_open_duration_sec,
                time_of_day,
                door_count,
                door_open_temperature,
                door_close_temperature
            FROM door 
            WHERE asset_serial_number = :serial 
                AND open_event_time >= :seven_days_ago
            ORDER BY open_event_time DESC 
            LIMIT 100
        """
        )
        door_events_result = db_session.execute(
            door_events_query,
            {"serial": asset_serial_number, "seven_days_ago": seven_days_ago},
        )
        door_events = door_events_result.fetchall()

        # Get movement history
        movement_history = (
            db_session.query(Movement)
            .filter(Movement.asset_serial_number == asset_serial_number)
            .order_by(Movement.start_time.desc())
            .limit(20)
            .all()
        )

        # Calculate distance from user
        user_lat, user_lon = get_user_location()
        distance_km = None
        if (
            user_lat
            and user_lon
            and asset_dict.get("latitude")
            and asset_dict.get("longitude")
        ):
            try:
                distance_km = haversine_distance(
                    user_lat,
                    user_lon,
                    float(asset_dict["latitude"]),
                    float(asset_dict["longitude"]),
                )
                if distance_km is None or not math.isfinite(distance_km):
                    distance_km = None
                else:
                    distance_km = round(distance_km, 2)
            except (TypeError, ValueError):
                distance_km = None

        # Prepare comprehensive chart data
        chart_data = {"temperature": [], "movements": [], "door_events": []}

        # Temperature chart data (reverse for chronological order)
        for event in reversed(temperature_history):
            if event.event_time and event.temperature_c:
                chart_data["temperature"].append(
                    {
                        "time": event.event_time.isoformat(),
                        "temperature": float(event.temperature_c),
                        "battery": event.battery if event.battery else None,
                    }
                )

        # Door events chart data (reverse for chronological order)
        for door_event in reversed(door_events):
            door_dict = dict(door_event._mapping)
            if door_dict.get("open_event_time"):
                chart_data["door_events"].append(
                    {
                        "time": door_dict["open_event_time"].isoformat(),
                        "duration": door_dict.get("door_open_duration_sec", 0),
                        "time_of_day": door_dict.get("time_of_day"),
                        "door_count": door_dict.get("door_count", 0),
                        "open_temp": door_dict.get("door_open_temperature"),
                        "close_temp": door_dict.get("door_close_temperature"),
                    }
                )

        # Movement chart data (reverse for chronological order)
        # Only include movements with door_open flag for door activity chart
        for movement in reversed(movement_history):
            if movement.start_time and movement.door_open and movement.duration:
                chart_data["movements"].append(
                    {
                        "time": movement.start_time.isoformat(),
                        "door_open": movement.door_open,
                        "duration": movement.duration,
                    }
                )

        # Debug: Log chart data counts
        print(f"[DEBUG] Asset {asset_serial_number} - Chart data counts:")
        print(f"  - Temperature: {len(chart_data['temperature'])} events")
        print(f"  - Door events: {len(chart_data['door_events'])} events")
        print(f"  - Movements (door related): {len(chart_data['movements'])} events")
        if chart_data["movements"]:
            print(
                f"  - Sample movement: {chart_data['movements'][0] if chart_data['movements'] else 'None'}"
            )

        return render_template(
            "/app/asset_detail.html",
            asset=asset_dict,
            latest_health=(
                latest_health_temperature.to_dict()
                if latest_health_temperature
                else None
            ),
            latest_health_any=(
                latest_health_any.to_dict() if latest_health_any else None
            ),
            latest_movement=latest_movement.to_dict() if latest_movement else None,
            temperature_history=[t.to_dict() for t in temperature_history],
            movement_history=[m.to_dict() for m in movement_history],
            distance_km=distance_km,
            chart_data=chart_data,
            user=user,
        )

    except Exception as e:
        print(f"[ERROR] Error in asset_detail: {str(e)}")
        import traceback

        traceback.print_exc()
        return render_template("asset_not_found.html"), 500


# ===================================
# API ROUTES (JSON RESPONSES)
# ===================================


@assets_bp.route("/api/by-distance", methods=["POST"])
def get_assets_by_distance_api():
    """
    ASSETS BY DISTANCE API - Get assets sorted by distance from coordinates

    Request Body (JSON):
    {
        "latitude": float,
        "longitude": float,
        "outlet_code": string (optional),
        "client": string (optional),
        "sub_client": string (optional),
        "page": int (default: 1),
        "per_page": int (default: 10)
    }

    Response:
    {
        "assets": [...],
        "page": int,
        "per_page": int,
        "total": int,
        "total_pages": int,
        "user_location": {
            "latitude": float,
            "longitude": float
        }
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required"}), 400

        user_lat = data.get("latitude")
        user_lon = data.get("longitude")
        outlet_code = data.get("outlet_code")
        client = data.get("client")
        sub_client = data.get("sub_client")
        page = data.get("page", 1)
        per_page = data.get("per_page", 10)

        if not user_lat or not user_lon:
            return jsonify({"error": "latitude and longitude required"}), 400

        db_session = get_session()

        # Build query with filters
        query = db_session.query(Asset)

        if outlet_code:
            query = query.filter(Asset.outlet_code == outlet_code)
        elif client:
            query = query.filter(Asset.client == client)
            if sub_client:
                query = query.filter(Asset.sub_client == sub_client)

        # Get all matching assets
        all_assets = query.all()

        # Calculate distances and sort
        assets_with_distance = []
        for asset in all_assets:
            distance = calculate_asset_distance(asset, user_lat, user_lon)
            assets_with_distance.append(
                {
                    "asset": asset,
                    "distance": distance if distance is not None else float("inf"),
                }
            )

        # Sort by distance (closest first)
        assets_with_distance.sort(key=lambda x: x["distance"])

        # Apply pagination
        total = len(assets_with_distance)
        total_pages = (total + per_page - 1) // per_page
        start = (page - 1) * per_page
        end = start + per_page
        paginated = assets_with_distance[start:end]

        # Build response assets
        result_assets = []
        for item in paginated:
            asset_dict = item["asset"].to_dict()
            distance_value = item["distance"]
            asset_dict["distance_km"] = (
                distance_value if math.isfinite(distance_value) else None
            )
            result_assets.append(asset_dict)

        return jsonify(
            {
                "assets": result_assets,
                "page": page,
                "per_page": per_page,
                "total": total,
                "total_pages": total_pages,
                "user_location": {"latitude": user_lat, "longitude": user_lon},
            }
        )

    except Exception as e:
        print(f"[ERROR] Error in get_assets_by_distance_api: {str(e)}")
        import traceback

        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@assets_bp.route(
    "/api/temperature-history/<string:asset_oem_serial_number>", methods=["GET"]
)
def get_asset_temperature_history(asset_oem_serial_number):
    """
    Get asset with temperature history and location correlation.
    Fetches the location temperature at the time the asset temperature was recorded.
    """
    try:
        db_session = get_session()

        # Get asset
        asset = (
            db_session.query(Asset)
            .filter(Asset.oem_serial_number == asset_oem_serial_number)
            .first()
        )

        if not asset:
            return jsonify({"error": "Asset not found"}), 404

        asset_dict = asset.to_dict()

        # Get latest health event with temperature
        health_event = (
            db_session.query(HealthEvent)
            .filter(
                HealthEvent.asset_serial_number == asset_oem_serial_number,
                HealthEvent.event_type == "Cabinet Temperature",
                HealthEvent.temperature_c.isnot(None),
                HealthEvent.event_time.isnot(None),
            )
            .order_by(HealthEvent.event_time.desc())
            .first()
        )

        asset_dict["health_history"] = None

        if health_event and asset.latitude and asset.longitude:
            # Correlate with location temperature
            location_data = None

            # If we have outlet location, use that; otherwise try asset location
            outlet = None
            if asset.outlet_code:
                from models.models import Outlet

                outlet = (
                    db_session.query(Outlet)
                    .filter(Outlet.code == asset.outlet_code)
                    .first()
                )

            if outlet and outlet.latitude and outlet.longitude:
                # Use outlet location for comparison
                location_data = correlate_asset_temperature_with_location(
                    float(health_event.temperature_c),
                    float(asset.latitude),
                    float(asset.longitude),
                    health_event.event_time,
                    float(outlet.latitude),
                    float(outlet.longitude),
                )
            else:
                # Try using asset's own location (not ideal but works)
                location_data = correlate_asset_temperature_with_location(
                    float(health_event.temperature_c),
                    float(asset.latitude),
                    float(asset.longitude),
                    health_event.event_time,
                    float(asset.latitude),
                    float(asset.longitude),
                )

            asset_dict["health_history"] = {
                "temperature_c": health_event.temperature_c,
                "event_time": (
                    health_event.event_time.isoformat()
                    if health_event.event_time
                    else None
                ),
                "location_temperature_correlation": location_data,
                "battery": health_event.battery,
                "battery_status": health_event.battery_status,
            }

        # Get all health events for this asset (last 10)
        all_health_events = (
            db_session.query(HealthEvent)
            .filter(
                HealthEvent.asset_serial_number == asset_oem_serial_number,
                HealthEvent.event_type == "Cabinet Temperature",
                HealthEvent.temperature_c.isnot(None),
            )
            .order_by(HealthEvent.event_time.desc())
            .limit(10)
            .all()
        )

        asset_dict["recent_temperatures"] = [
            {
                "temperature_c": event.temperature_c,
                "event_time": (
                    event.event_time.isoformat() if event.event_time else None
                ),
            }
            for event in all_health_events
        ]

        return jsonify(asset_dict)

    except Exception as e:
        print(f"[ERROR] Error in get_asset_temperature_history: {str(e)}")
        import traceback

        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
