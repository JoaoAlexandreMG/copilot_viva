from flask import Blueprint, jsonify, request
from models.models import Outlet, Asset
from db.database import get_session
from utils.location import haversine_distance, get_location_info, filter_outlets_by_location

outlets_bp = Blueprint("outlets", __name__, url_prefix="/outlets")


@outlets_bp.route("/", methods=["GET"])
def get_outlets():
    session = get_session()  # Obtenha a sessão do banco de dados
    client = request.args.get("client")  # Obtenha o parâmetro "client" da query string
    query = session.query(Outlet)

    if client:  # Se o parâmetro "client" for fornecido, aplique o filtro
        query = query.filter(Outlet.client == client)

    outlets = query.all()  # Execute a consulta
    return jsonify([outlet.to_dict() for outlet in outlets])

@outlets_bp.route("/<string:outlet_code>", methods=["GET"])
def get_outlet(outlet_code):
    session = get_session()  # Obtenha a sessão do banco de dados
    outlet = session.query(Outlet).filter(Outlet.code == outlet_code).first()  # Use a sessão para consultar o outlet
    if outlet:
        return jsonify(outlet.to_dict())
    else:
        return jsonify({"error": "Outlet not found"}), 404


@outlets_bp.route("/api/nearby", methods=["POST"])
def get_nearby_outlets():
    """
    Get outlets filtered by user location (same country/city first) and sorted by distance.
    Expects JSON body with 'latitude' and 'longitude'.
    """
    try:
        data = request.get_json()
        user_lat = data.get("latitude")
        user_lon = data.get("longitude")
        client = data.get("client")
        page = data.get("page", 1)
        per_page = data.get("per_page", 10)

        if not user_lat or not user_lon:
            return jsonify({"error": "latitude and longitude required"}), 400

        session = get_session()

        # Get user's location info first
        user_location_info = get_location_info(user_lat, user_lon)

        # Get outlets from database
        query = session.query(Outlet)
        if client:
            query = query.filter(Outlet.client == client)
        else:
            query = query.filter(Outlet.is_active == True)

        outlets = query.all()
        outlets_dict = [outlet.to_dict() for outlet in outlets]


        # Filter by location (same country/city) and sort by distance
        filtered_outlets = filter_outlets_by_location(
            user_lat, user_lon, outlets_dict, user_location_info
        )

        # Add asset count to each outlet
        for outlet in filtered_outlets:
            asset_count = session.query(Asset).filter(
                Asset.outlet_code == outlet.get("code")
            ).count()
            outlet["asset_count"] = asset_count

        # Apply pagination
        total_outlets = len(filtered_outlets)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_outlets = filtered_outlets[start_idx:end_idx]

        # Calculate pagination info
        total_pages = (total_outlets + per_page - 1) // per_page
        
        return jsonify({
            "user_location": {
                "latitude": user_lat,
                "longitude": user_lon,
                "location_info": user_location_info
            },
            "outlets": paginated_outlets,
            "pagination": {
                "current_page": page,
                "per_page": per_page,
                "total_outlets": total_outlets,
                "total_pages": total_pages,
                "has_prev": page > 1,
                "has_next": page < total_pages
            }
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@outlets_bp.route("/api/outlet-with-assets", methods=["POST"])
def get_outlet_with_asset_distances():
    """
    Get a specific outlet with distances from all its assets to user location.
    Expects JSON body with 'latitude', 'longitude', and 'outlet_code'.
    """
    try:
        data = request.get_json()
        user_lat = data.get("latitude")
        user_lon = data.get("longitude")
        outlet_code = data.get("outlet_code")

        if not user_lat or not user_lon or not outlet_code:
            return jsonify({
                "error": "latitude, longitude, and outlet_code required"
            }), 400

        session = get_session()

        # Get outlet
        outlet = session.query(Outlet).filter(Outlet.code == outlet_code).first()
        if not outlet:
            return jsonify({"error": "Outlet not found"}), 404

        outlet_dict = outlet.to_dict()

        # Calculate distance from user to outlet
        outlet_distance = haversine_distance(
            user_lat, user_lon,
            outlet.latitude, outlet.longitude
        )
        outlet_dict["distance_from_user_km"] = round(outlet_distance, 2) if outlet_distance else None

        # Get all assets for this outlet
        assets = session.query(Asset).filter(
            Asset.outlet_code == outlet_code
        ).all()

        # Calculate distances from user to each asset
        assets_with_distance = []
        for asset in assets:
            asset_dict = asset.to_dict()

            if asset.latitude and asset.longitude:
                asset_distance = haversine_distance(
                    user_lat, user_lon,
                    asset.latitude, asset.longitude
                )
                asset_dict["distance_from_user_km"] = round(asset_distance, 2) if asset_distance else None

                # Calculate distance from outlet to asset
                if outlet.latitude and outlet.longitude:
                    outlet_to_asset_distance = haversine_distance(
                        outlet.latitude, outlet.longitude,
                        asset.latitude, asset.longitude
                    )
                    asset_dict["distance_from_outlet_km"] = round(outlet_to_asset_distance, 2) if outlet_to_asset_distance else None
            else:
                asset_dict["distance_from_user_km"] = None
                asset_dict["distance_from_outlet_km"] = None

            assets_with_distance.append(asset_dict)

        # Sort assets by distance from user
        assets_with_distance.sort(
            key=lambda x: (
                x["distance_from_user_km"] is None,
                x["distance_from_user_km"] if x["distance_from_user_km"] else float('inf')
            )
        )

        outlet_dict["assets"] = assets_with_distance
        outlet_dict["asset_count"] = len(assets_with_distance)

        return jsonify(outlet_dict)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500