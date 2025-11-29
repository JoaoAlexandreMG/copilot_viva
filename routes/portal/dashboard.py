from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify
from sqlalchemy import text
from pathlib import Path
from models.models import Asset, AlertsDefinition, User
from .decorators import require_authentication
from db.database import get_session
from datetime import datetime, timezone
from sqlalchemy import text
import os
import tempfile
from utils.new_excel_to_db import importar_dados_generico

dashboard_bp = Blueprint("dashboard", __name__, url_prefix="/portal_associacao")

def get_technicians_activity(client):
    """
    Retorna métricas de atividade dos técnicos (perfil Technician) do cliente nos últimos 30 dias.
    
    Métricas:
    - user_coolers_read: Contagem de health_events do usuário
    - ghost_read: Contagem de ghost_assets reportados pelo usuário
    - Sinalização: Técnicos sem atividade em ambas as tabelas
    
    Args:
        client: Nome do cliente para filtrar
        
    Returns:
        Lista de dicionários com dados de cada técnico
    """
    db_session = get_session()
    
    # Query combinada que obtém ambas as métricas em uma única consulta
    technicians_sql = text("""
        WITH technician_users AS (
            SELECT 
                upn, 
                COALESCE(first_name || ' ' || last_name, user_name, upn) as name,
                email
            FROM users
            WHERE client = :client
            AND role = 'Technician'
            AND is_active = true
        ),
        health_counts AS (
            SELECT
                u.upn,
                COUNT(he.data_uploaded_by) AS user_coolers_read
            FROM
                technician_users u
            LEFT JOIN
                health_events he
                ON u.upn = he.data_uploaded_by
                AND he.event_time >= NOW() - INTERVAL '30 days'
            GROUP BY
                u.upn
        ),
        ghost_counts AS (
            SELECT
                u.upn,
                COUNT(ga.reported_by) AS ghost_read
            FROM
                technician_users u
            LEFT JOIN
                ghost_assets ga
                ON u.upn = ga.reported_by
                AND ga.reported_on >= NOW() - INTERVAL '30 days'
            GROUP BY
                u.upn
        )
        SELECT
            tu.upn,
            tu.name,
            tu.email,
            COALESCE(hc.user_coolers_read, 0) AS user_coolers_read,
            COALESCE(gc.ghost_read, 0) AS ghost_read,
            CASE 
                WHEN COALESCE(hc.user_coolers_read, 0) = 0 
                AND COALESCE(gc.ghost_read, 0) = 0 
                THEN true 
                ELSE false 
            END AS no_activity
        FROM
            technician_users tu
        LEFT JOIN
            health_counts hc ON tu.upn = hc.upn
        LEFT JOIN
            ghost_counts gc ON tu.upn = gc.upn
        ORDER BY
            no_activity DESC,
            (COALESCE(hc.user_coolers_read, 0) + COALESCE(gc.ghost_read, 0)) DESC
    """)
    
    results = db_session.execute(technicians_sql, {"client": client}).fetchall()
    
    technicians_data = []
    for row in results:
        technicians_data.append({
            "upn": row.upn,
            "name": row.name,
            "email": row.email,
            "user_coolers_read": int(row.user_coolers_read),
            "ghost_read": int(row.ghost_read),
            "total_activity": int(row.user_coolers_read + row.ghost_read),
            "no_activity": bool(row.no_activity)
        })
    
    db_session.close()
    return technicians_data

def process_hourly_data(raw_data, key, now):
    """
    Processa o array JSONB (com agregação horária) em um array de 24 pontos.
    Preenche as horas que faltam com 0.
    """
    if not raw_data:
        return [0.0] * 24, []
        
    # Mapeia a hora para o valor (e garante que seja float)
    data_by_hour = {item['hour']: float(item[key]) for item in raw_data if item and key in item}
    
    hourly_array = [0.0] * 24
    current_date = now.strftime("%Y-%m-%d")
    hourly_data_with_timestamps = []

    for h in range(24):
        value = data_by_hour.get(h, 0.0)
        
        # Filtro de Sanidade para Temperatura (reproduzindo a lógica SQL)
        if key == 'avg_temp' and not (-30 <= value <= 20):
             value = 0.0 
             
        hourly_array[h] = value

        # Mantém a estrutura de raw_data_with_timestamps para o frontend
        timestamp_utc = f"{current_date}T{h:02d}:00:00Z"
        
        # Ajusta o nome da chave para manter a compatibilidade com o retorno final
        key_name = "temp" if key == 'avg_temp' else "door"
        
        # O objeto de timestamps precisa incluir ambas as chaves (temp e door)
        # Como estamos processando apenas um dado por vez, só incluímos o valor relevante.
        # No frontend, você precisará combinar estas duas listas se precisar de uma única lista com 'temp' e 'door'.
        # Para fins de compatibilidade com a estrutura anterior, retornaremos a lista de timestamps, mas o front-end
        # provavelmente só precisa dos arrays [0..23].
        
        hourly_data_with_timestamps.append({
            "hour": h,
            "timestamp_utc": timestamp_utc,
            key_name: value
        })
            
    return hourly_array, hourly_data_with_timestamps

def stats_for_dashboard(days=30):
    """
    Calcula as principais estatísticas para o dashboard de forma otimizada, 
    consultando apenas a Materialized View (MV) mv_client_overview.
    
    Args:
        days: Número de dias para filtrar (30 ou 7)
    """
    db_session = get_session()
    
    # 1. Filtro e Parâmetros
    client = session.get("user", {}).get("client") 
    if not client:
        return {}
    
    now = datetime.now(timezone.utc)

    # 2. Query 1: Obter TODOS os status e dados horários da MV otimizada
    # Esta query substitui as Queries 2, 3 e 4 anteriores.
    status_and_counts_sql = text("""
        SELECT
            total_assets_count,
            alerts_30d,
            alerts_7d,
            active_assets_24h_count AS assets_health_last_24h_count,
            count_battery_ok AS good_battery_assets_count,
            count_battery_low AS low_battery_assets_count,
            count_battery_critical AS critical_battery_assets_count,
            count_temp_ok AS ok_temperatures_count,
            count_temp_low AS below_temperatures_count,
            count_temp_high AS above_temperatures_count,
            applied_min AS temp_min,
            applied_max AS temp_max,
            avg_compressor_percent_30d,
            avg_compressor_percent_7d,
            avg_consumption_watt_30d,
            avg_consumption_watt_7d,
            hourly_temp_data_30d,
            hourly_temp_data_7d,
            hourly_door_data_30d,
            hourly_door_data_7d
        FROM mv_client_overview
        WHERE client = :client
    """)
    
    status_results = db_session.execute(status_and_counts_sql, {"client": client}).fetchone()
    
    if not status_results:
        # Se o cliente não tem dados na MV, retorna zero
        temp_min = 0
        temp_max = 7
        return {
            "total_assets": 0,
            "assets_health_last_24h_count": 0,
            "alerts_period_count": 0,
            "ok_temperatures_count": 0, "not_ok_temperatures_count": 0, "total_with_temperature": 0,
            "temp_ok_percentage": 0, "temp_not_ok_percentage": 0,
            "good_battery_assets_count": 0, "low_battery_assets_count": 0, "critical_battery_assets_count": 0,
            "avg_compressor_on_time": 0,
            "avg_power_consumption": 0,
            "hourly_temp_chart": {"labels": [f"{h:02d}:00" for h in range(24)], "data": [0] * 24, "raw_data": []},
            "hourly_door_chart": {"labels": [f"{h:02d}:00" for h in range(24)], "data": [0] * 24, "raw_data": []},
            "temperature_status_chart": {"labels": [f'OK ({temp_min}°C-{temp_max}°C)', f'Acima (>{temp_max}°C)', f'Abaixo (<{temp_min}°C)'], "data": [0, 0, 0]},
        }

    # --- 3. Extração e Seleção Dinâmica (7 ou 30 dias) ---
    
    temp_min = status_results.temp_min if status_results.temp_min is not None else 0
    temp_max = status_results.temp_max if status_results.temp_max is not None else 7

    if days == 7:
        raw_temp_data = status_results.hourly_temp_data_7d
        raw_door_data = status_results.hourly_door_data_7d
        alerts_period_count = status_results.alerts_7d
        avg_compressor_on_time = status_results.avg_compressor_percent_7d
        avg_power_consumption = status_results.avg_consumption_watt_7d
    else: # days == 30 (padrão)
        raw_temp_data = status_results.hourly_temp_data_30d
        raw_door_data = status_results.hourly_door_data_30d
        alerts_period_count = status_results.alerts_30d
        avg_compressor_on_time = status_results.avg_compressor_percent_30d
        avg_power_consumption = status_results.avg_consumption_watt_30d
        
    # --- 4. Processamento de Dados de Gráfico ---
    
    # Processa os dados de temperatura (lista JSONB -> array de 24 pontos)
    hourly_temp_data, hourly_temp_raw_timestamps = process_hourly_data(raw_temp_data, 'avg_temp', now)
    
    # Processa os dados de porta (lista JSONB -> array de 24 pontos)
    hourly_door_data, hourly_door_raw_timestamps = process_hourly_data(raw_door_data, 'avg_door_count', now)
    
    # --- 5. Cálculo de Métricas de Temperatura ---
    
    ok_temperatures_count = int(status_results.ok_temperatures_count or 0)
    above_temperatures_count = int(status_results.above_temperatures_count or 0)
    below_temperatures_count = int(status_results.below_temperatures_count or 0)
    
    not_ok_temperatures_count = above_temperatures_count + below_temperatures_count
    
    # total_with_temperature é agora calculado pela soma das categorias (já que o WHERE macs.temperature_c IS NOT NULL foi removido do SELECT principal)
    total_with_temperature = ok_temperatures_count + not_ok_temperatures_count 

    # Calcular percentuais corretos
    temp_ok_percentage = round((ok_temperatures_count / total_with_temperature) * 100) if total_with_temperature > 0 else 0
    temp_not_ok_percentage = round((not_ok_temperatures_count / total_with_temperature) * 100) if total_with_temperature > 0 else 0
    
    if temp_ok_percentage + temp_not_ok_percentage != 100 and total_with_temperature > 0:
        temp_not_ok_percentage = 100 - temp_ok_percentage
    
    temp_status_labels = [
        f'OK ({temp_min}°C-{temp_max}°C)', 
        f'Acima (>{temp_max}°C)', 
        f'Abaixo (<{temp_min}°C)'
    ]
    temp_status_data = [
        ok_temperatures_count, 
        above_temperatures_count, 
        below_temperatures_count
    ]

    # 6. Query 3: Top 10 Ativos por Displacement (últimas 24h) - MANTIDA
    # Esta query não pode ser materializada pois requer dados de movimento (movements) recentes
    top10_sql = text("""
        SELECT
            a.sales_office,
            r.asset_serial_number,
            r.displacement_meter,
            r.start_time,
            ROW_NUMBER() OVER (
                ORDER BY r.displacement_meter DESC
            ) as global_rank
        FROM (
            SELECT
                asset_serial_number,
                displacement_meter,
                start_time,
                ROW_NUMBER() OVER (
                    PARTITION BY asset_serial_number
                    ORDER BY start_time DESC
                ) as rank_recente
            FROM
                movements
            WHERE
                start_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
                AND client = :client
        ) AS r
        JOIN
            assets a ON r.asset_serial_number = a.oem_serial_number
        WHERE
            r.rank_recente = 1
        ORDER BY
            r.displacement_meter DESC
        LIMIT 10;
    """)

    top10_results = db_session.execute(top10_sql, {"client": client}).fetchall()

    # Processamento Top 10 (mantido inalterado)
    top10_by_office = {}
    for row in top10_results:
        office = row.sales_office or "Sem Escritório"
        if office not in top10_by_office:
            top10_by_office[office] = []
        top10_by_office[office].append({
            "rank": int(row.global_rank),
            "asset_serial_number": row.asset_serial_number,
            "displacement_meter": float(row.displacement_meter) if row.displacement_meter else 0,
            "start_time": row.start_time.isoformat() if row.start_time else None
        })

    top10_data = []
    for office in sorted(top10_by_office.keys(), key=lambda o: min(asset["rank"] for asset in top10_by_office[o])):
        top10_data.append({
            "office": office,
            "assets": sorted(top10_by_office[office], key=lambda a: a["rank"])
        })

    # 7. Retorno Final
    hourly_labels = [f"{h:02d}:00" for h in range(24)]
    
    return {
        # Período de referência
        "period_days": days,

        # Métricas de Cartão (Totais e Contagens)
        "total_assets": int(status_results.total_assets_count or 0),
        "assets_health_last_24h_count": int(status_results.assets_health_last_24h_count or 0),
        "alerts_period_count": int(alerts_period_count or 0), # Dinâmico (7/30d)

        # Métricas de Temperatura (Contagens e Percentuais)
        "ok_temperatures_count": ok_temperatures_count,
        "not_ok_temperatures_count": not_ok_temperatures_count,
        "total_with_temperature": total_with_temperature, # Calculado pela soma das categorias
        "temp_ok_percentage": temp_ok_percentage,
        "temp_not_ok_percentage": temp_not_ok_percentage,

        # Métricas de Bateria (Contagens)
        "good_battery_assets_count": int(status_results.good_battery_assets_count or 0),
        "low_battery_assets_count": int(status_results.low_battery_assets_count or 0),
        "critical_battery_assets_count": int(status_results.critical_battery_assets_count or 0),

        # Estatísticas de Consumo (Dinâmico 7/30d)
        "avg_compressor_on_time": round(avg_compressor_on_time, 2) if avg_compressor_on_time else 0,
        "avg_power_consumption": round(avg_power_consumption, 2) if avg_power_consumption else 0,

        # Dados para Gráficos (MV - Dinâmico 7/30d)
        "hourly_temp_chart": {
            "labels": hourly_labels,
            "data": hourly_temp_data,
            "raw_data": hourly_temp_raw_timestamps # Mantido para compatibilidade de timezone no frontend
        },
        "hourly_door_chart": {
            "labels": hourly_labels,
            "data": hourly_door_data,
            "raw_data": hourly_door_raw_timestamps # Mantido para compatibilidade de timezone no frontend
        },
        "temperature_status_chart": {
            "labels": temp_status_labels,
            "data": temp_status_data,
        },

        # Top 10 Ativos por Displacement (últimas 24h)
        "top10_assets": top10_data,
    }

@dashboard_bp.route("/dashboard", methods=["GET"])
@require_authentication
def render_dashboard():
    """
    Render main portal dashboard with advanced statistics
    Shows assets health, temperature, battery, door openings, and alerts
    """
    try:
        # Check if user is authenticated
        user = session.get("user")
        if not user:
            return redirect(url_for("index"))

        # Get dashboard statistics (default to 30 days)
        stats = stats_for_dashboard(days=30)
        
        return render_template("portal/dashboard.html", stats=stats, page_type="list")

    except Exception as e:
        print(f"[ERROR] Error rendering dashboard: {str(e)}")
        return redirect(url_for("index"))

@dashboard_bp.route("/technicians", methods=["GET"])
@require_authentication
def render_technicians_page():
    """
    Render technicians monitoring page
    Shows activity metrics for all technicians in the last 30 days
    """
    try:
        user = session.get("user")
        if not user:
            return redirect(url_for("index"))
        
        client = user.get("client")
        if not client:
            return redirect(url_for("index"))
        
        # Get technicians activity data
        technicians_data = get_technicians_activity(client)
        
        return render_template(
            "portal/technicians.html", 
            technicians=technicians_data,
            page_type="list"
        )
        
    except Exception as e:
        print(f"[ERROR] Error rendering technicians page: {str(e)}")
        import traceback
        traceback.print_exc()
        return redirect(url_for("dashboard.render_dashboard"))
    
@dashboard_bp.route('/api/dashboard-stats', methods=['GET'])
@require_authentication
def get_dashboard_stats():
    """
    API endpoint para retornar estatísticas do dashboard com período variável.
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
        
        stats = stats_for_dashboard(days=period)
        return jsonify({'status': 'ok', 'data': stats}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@dashboard_bp.route('/api/technicians-activity', methods=['GET'])
@require_authentication
def get_technicians_activity_api():
    """
    API endpoint para retornar métricas de atividade dos técnicos nos últimos 30 dias.
    
    Retorna:
    - Lista de técnicos com user_coolers_read e ghost_read
    - Sinalização para técnicos sem atividade
    """
    try:
        user = session.get("user")
        if not user:
            return jsonify({'status': 'error', 'message': 'Unauthorized'}), 401
        
        client = user.get("client")
        if not client:
            return jsonify({'status': 'error', 'message': 'Client not found in session'}), 400
        
        technicians_data = get_technicians_activity(client)
        
        return jsonify({
            'status': 'ok', 
            'data': technicians_data,
            'total_technicians': len(technicians_data),
            'inactive_technicians': len([t for t in technicians_data if t['no_activity']])
        }), 200
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@dashboard_bp.route('/import_file', methods=['POST'])
@require_authentication
def import_file():
    """Endpoint para importar um único arquivo enviado via formulário.
    Usa o novo sistema de importação genérica do utils/new_excel_to_db.py
    Retorna JSON com o resultado (inserted, updated, read)
    """
    try:
        file = request.files.get('file')
        if not file:
            return jsonify({'status': 'error', 'message': 'No file uploaded'}), 400

        filename = file.filename
        if not filename:
            return jsonify({'status': 'error', 'message': 'No filename provided'}), 400

        # Save to a temp file
        
        suffix = Path(filename).suffix
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        file.save(tmp.name)
        tmp.close()


        

        # Detect model type based on filename patterns
        name_normalized = filename.lower().replace(' ', '').replace('-', '').replace('_', '')
        
        # Map filename patterns to model names
        file_patterns = {
            'HealthEvent': ['health'],
            'Movement': ['movements'],
            'SmartDevice': ['smart', 'devices'],
            'User': ['users'],
            'Client': ['client'],
            'Asset': ['assets'],
            'Outlet': ['outlet'],
            'Alert': ['alerts'],
            'AlertsDefinition': ['definition'],
            'DoorEvent': ['door', 'status']
        }

        detected_model = None
        detection_log = []
        
        # Try to detect based on filename patterns
        for model_name, patterns in file_patterns.items():
            if any(pattern in name_normalized for pattern in patterns):
                detected_model = model_name
                detection_log.append(f"filename_pattern_match: {model_name} via {patterns}")
                break

        db_session = get_session()
        try:
            # Use the new generic import function
            result = importar_dados_generico(db_session, detected_model, tmp.name)
            
            if result and (result.get('inserted', 0) > 0 or result.get('updated', 0) > 0):
                return jsonify({
                    'status': 'ok', 
                    'result': {'inserted': result.get('inserted', 0), 'updated': result.get('updated', 0)},
                    'model': detected_model,
                    'detection': detection_log,
                    'filename': filename
                }), 200
            elif result:
                return jsonify({
                    'status': 'warning', 
                    'message': 'Import completed but no data was inserted or updated',
                    'result': {'inserted': result.get('inserted', 0), 'updated': result.get('updated', 0)},
                    'model': detected_model,
                    'detection': detection_log,
                    'filename': filename
                }), 200
            else:
                return jsonify({
                    'status': 'error', 
                    'message': 'Import failed - check file format and content',
                    'model': detected_model,
                    'detection': detection_log,
                    'filename': filename
                }), 400
                
        finally:
            try:
                os.remove(tmp.name)
            except Exception:
                pass
            db_session.close()
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500