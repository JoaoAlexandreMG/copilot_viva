from flask import Blueprint, render_template, session, redirect, url_for, jsonify, request
import math
from datetime import datetime
from zoneinfo import ZoneInfo
from routes.portal.decorators import require_authentication
from db.database import get_session
from models.models import AssetsInventory, AssetInventoryVisit
from sqlalchemy import func

inventory_bp = Blueprint("inventory", __name__, url_prefix="/inventory")


def _serialize_inventory_asset(asset: AssetsInventory) -> dict:
    data = asset.to_dict()
    # Normaliza campos de data para strings ISO legíveis no front
    for date_field in ("created_at", ):
        if data.get(date_field):
            data[date_field] = data[date_field].isoformat()
    return data


def _visit_iso_in_brazil(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        # Assume que valores salvos sem timezone estão em UTC
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(ZoneInfo("America/Sao_Paulo")).isoformat()


def DateTimeSortHelper(value):
    try:
        return DateTimeSortHelper._cache.setdefault(value, DateTimeSortHelper._parse(value))
    except Exception:
        return 0


DateTimeSortHelper._cache = {}


def _datetime_sort_helper_parse(value):
    if not value:
        return 0
    try:
        return datetime.fromisoformat(value).timestamp()
    except Exception:
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z").timestamp()
        except Exception:
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").timestamp()
            except Exception:
                return 0


# Atribui função de parsing ao helper para reutilizar o cache acima
DateTimeSortHelper._parse = _datetime_sort_helper_parse


def _haversine_distance_m(lat1, lon1, lat2, lon2):
    """Calcula a distância em metros entre dois pontos lat/lon (Haversine)."""
    try:
        R = 6371000  # raio da Terra em metros
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c
    except Exception:
        return None


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
        assets_serialized = [_serialize_inventory_asset(asset) for asset in assets]
        return render_template(
            'inventory/list.html',
            user=user,
            assets_inventory=assets,
            assets_json=assets_serialized
        )
    except Exception as e:
        print(f"[ERROR] Error rendering inventory dashboard: {e}")
        return redirect(url_for('inventory.render_inventory_index'))


@inventory_bp.route('/dashboard', methods=['GET'])
@require_authentication
def legacy_inventory_dashboard_redirect():
    """Compat redirect from old /dashboard path to /list."""
    return redirect(url_for('inventory.render_inventory_list'))


@inventory_bp.route('/visits', methods=['GET'])
@require_authentication
def render_inventory_visits():
    """Renderiza a página com o histórico de visitas dos assets."""
    try:
        user = session.get('user')
        if not user:
            return redirect(url_for('index'))

        db_session = get_session()
        visits_query = (
            db_session.query(AssetInventoryVisit, AssetsInventory)
            .join(AssetsInventory, AssetInventoryVisit.asset_id == AssetsInventory.id)
            .filter(AssetsInventory.is_deleted.is_(False))
            .order_by(AssetInventoryVisit.visit_at.desc())
        )

        visits = []
        visits_by_asset = {}  # Agrupar visitas por asset para o mapa

        for visit, asset in visits_query:
            visit_data = {
                "id": visit.id,
                "asset_id": asset.id,
                "serial_number": asset.serial_number,
                "asset_type": asset.asset_type,
                "outlet_name": asset.outlet_name,
                "city": asset.city,
                "street": asset.street,
                "visit_at": _visit_iso_in_brazil(visit.visit_at),
                "prev_visit_at": _visit_iso_in_brazil(visit.prev_visit_at),
                "latitude": visit.latitude,
                "longitude": visit.longitude,
                "prev_latitude": visit.prev_latitude,
                "prev_longitude": visit.prev_longitude,
                "distance_from_prev_m": visit.distance_from_prev_m,
                "scanned_by": visit.scanned_by,
                "notes": visit.notes,
            }
            visits.append(visit_data)

            # Prepara dados para o mapa
            if asset.id not in visits_by_asset:
                visits_by_asset[asset.id] = {
                    "serial_number": asset.serial_number,
                    "path": []
                }
            
            if visit.latitude is not None and visit.longitude is not None:
                visits_by_asset[asset.id]["path"].append({
                    "lat": visit.latitude,
                    "lng": visit.longitude,
                    "at": _visit_iso_in_brazil(visit.visit_at)
                })

        # Inverte o caminho para desenhar do mais antigo para o mais novo
        for asset_id in visits_by_asset:
            visits_by_asset[asset_id]["path"].reverse()

        return render_template('inventory/visits.html', user=user, visits=visits, visits_map_data=visits_by_asset)
    except Exception as e:
        print(f"[ERROR] Error rendering inventory visits: {e}")
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

        # Subquery com última visita por asset
        visits_subq = (
            db_session.query(
                AssetInventoryVisit.asset_id,
                func.max(AssetInventoryVisit.visit_at).label('last_visit_at')
            )
            .group_by(AssetInventoryVisit.asset_id)
            .subquery()
        )

        # Carrega assets + última visita para o mapa/cards
        assets_with_visit = (
            db_session.query(AssetsInventory, visits_subq.c.last_visit_at)
            .outerjoin(visits_subq, visits_subq.c.asset_id == AssetsInventory.id)
            .filter(AssetsInventory.is_deleted.is_(False))
            .all()
        )
        operation_assets = []
        for asset, last_visit in assets_with_visit:
            data = _serialize_inventory_asset(asset)
            data['last_visit_at'] = _visit_iso_in_brazil(last_visit)
            operation_assets.append(data)

        # Ranking de visitas (mais antigo sem visita -> mais recente) - usa os mesmos dados
        stale_assets = sorted(operation_assets, key=lambda a: DateTimeSortHelper(a.get('last_visit_at')))[:10]

        return render_template('inventory/operation.html', user=user, operation_assets=operation_assets, stale_assets=stale_assets)
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


@inventory_bp.route('/operation/check/<serial_number>', methods=['GET'])
@require_authentication
def check_inventory_asset(serial_number):
    """Retorna o asset pelo serial, se existir (uso para pré-checagem no front)."""
    try:
        db_session = get_session()
        asset = db_session.query(AssetsInventory).filter(
            AssetsInventory.serial_number == serial_number,
            AssetsInventory.is_deleted.is_(False)
        ).first()
        if not asset:
            return jsonify({"error": "Not found"}), 404
        return jsonify(_serialize_inventory_asset(asset)), 200
    except Exception as e:
        print(f"[ERROR] Error checking inventory asset: {e}")
        return jsonify({"error": "Erro ao buscar asset"}), 500


@inventory_bp.route('/operation/create', methods=['POST'])
@require_authentication
def create_inventory_asset():
    """Cria um novo asset no inventário via JSON ou form."""
    try:
        payload = request.get_json(silent=True) or request.form

        serial_number = (payload.get('serial_number') or '').strip()
        if not serial_number:
            return jsonify({"error": "O campo serial_number é obrigatório."}), 400

        street = (payload.get('street') or '').strip()
        if not street:
            return jsonify({"error": "O campo Rua é obrigatório."}), 400

        city = (payload.get('city') or '').strip()
        if not city:
            return jsonify({"error": "O campo Cidade é obrigatório."}), 400

        asset_type = (payload.get('asset_type') or '').strip() or None
        material = (payload.get('material') or '').strip() or None
        outlet_name = (payload.get('outlet_name') or '').strip() or None
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

        user = session.get('user') or {}
        created_by = user.get('upn') or user.get('email') or 'system'

        if existing:
            # Atualiza coordenadas e calcula distância se possível
            prev_lat = existing.last_latitude
            prev_lng = existing.last_longitude
            prev_time = existing.created_at
            visit_time = datetime.now(ZoneInfo("America/Sao_Paulo"))

            if last_latitude is not None:
                existing.last_latitude = last_latitude
            if last_longitude is not None:
                existing.last_longitude = last_longitude

            if prev_lat is not None and prev_lng is not None and last_latitude is not None and last_longitude is not None:
                existing.last_visit_distance_m = _haversine_distance_m(prev_lat, prev_lng, last_latitude, last_longitude)

            # Opcional: atualiza metadados se enviados
            if asset_type is not None:
                existing.asset_type = asset_type
            if material is not None:
                existing.material = material
            if outlet_name is not None:
                existing.outlet_name = outlet_name
            if street is not None:
                existing.street = street
            if city is not None:
                existing.city = city
            if notes is not None:
                existing.notes = notes

            # Registrar visita/movimento
            visit = AssetInventoryVisit(
                asset_id=existing.id,
                visit_at=visit_time,
                latitude=last_latitude if last_latitude is not None else prev_lat or 0,
                longitude=last_longitude if last_longitude is not None else prev_lng or 0,
                prev_visit_at=prev_time,
                prev_latitude=prev_lat,
                prev_longitude=prev_lng,
                distance_from_prev_m=existing.last_visit_distance_m,
                scanned_by=created_by,
                notes=notes
            )
            db_session.add(visit)

            db_session.commit()
            asset_payload = _serialize_inventory_asset(existing)
            asset_payload["last_visit_at"] = _visit_iso_in_brazil(visit_time)
            return jsonify({"asset": asset_payload, "updated": True}), 200

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
        db_session.flush()
        visit_time = datetime.now(ZoneInfo("America/Sao_Paulo"))

        visit = AssetInventoryVisit(
            asset_id=asset.id,
            visit_at=visit_time,
            latitude=last_latitude or 0,
            longitude=last_longitude or 0,
            prev_visit_at=None,
            prev_latitude=None,
            prev_longitude=None,
            distance_from_prev_m=None,
            scanned_by=created_by,
            notes=notes
        )
        db_session.add(visit)

        db_session.commit()

        asset_payload = _serialize_inventory_asset(asset)
        asset_payload["last_visit_at"] = _visit_iso_in_brazil(visit_time)

        return jsonify({"asset": asset_payload, "created": True}), 201
    except Exception as e:
        print(f"[ERROR] Error creating inventory asset: {e}")
        return jsonify({"error": "Erro ao criar asset"}), 500
