from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify
from pathlib import Path
from models.models import Asset, AlertsDefinition
from .decorators import require_authentication
from db.database import get_session
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
import os
import tempfile
from utils.new_excel_to_db import importar_dados_generico

dashboard_bp = Blueprint("dashboard", __name__, url_prefix="/portal_associacao")

def stats_for_dashboard(days=30):
    """
    Calcula as principais estatísticas para o dashboard de forma otimizada.
    
    - Consulta a MV para dados de gráficos horários.
    - Usa uma query dinâmica para os status de Bateria e Temperatura (devido aos limites de Alerta).
    
    Args:
        days: Número de dias para filtrar (30 ou 7)
    """
    db_session = get_session()
    
    # 1. Filtro e Parâmetros
    # O cliente deve ser obtido do contexto da sessão do usuário
    client = session.get("user", {}).get("client") 
    if not client:
        return {}
    
    # Parâmetros de tempo (para as queries dinâmicas que não estão na MV)
    now = datetime.now(timezone.utc)
    one_day_before = now - timedelta(hours=24)
    period_start = now - timedelta(days=days)

    # 2. Obter Definições Dinâmicas de Alerta
    temp_alert_definition = db_session.query(AlertsDefinition).filter(
        AlertsDefinition.client == client, 
        AlertsDefinition.type == "Temperature Alert"
    ).first()
    
    # Definir limites de temperatura (usados no SQL dinâmico)
    temp_min = temp_alert_definition.temperature_below if temp_alert_definition and temp_alert_definition.temperature_below is not None else 0
    temp_max = temp_alert_definition.temperature_above if temp_alert_definition and temp_alert_definition.temperature_above is not None else 7

    # 3. Query 1: Status Dinâmicos e Contagens (Bateria, Temperatura, Movimento 24h, Totais)
    # A lógica complexa é encapsulada em um CTE para extrair o último status de saúde (igual à MV, mas filtrado por cliente)
    status_and_counts_sql = text(f"""
        WITH LatestHealthStatus AS (
            SELECT he.asset_serial_number, he.battery, he.temperature_c,
                    ROW_NUMBER() OVER (
                        PARTITION BY he.asset_serial_number
                        ORDER BY he.event_time DESC
                    ) AS rn
            FROM health_events he
            WHERE he.client = :client
                AND he.event_time >= :period_start
                AND (he.battery IS NOT NULL OR he.temperature_c IS NOT NULL)
        )
        SELECT 
            -- Totais
            (SELECT COUNT(oem_serial_number) FROM assets WHERE client = :client) AS total_assets,
            (SELECT COUNT(id) FROM alerts WHERE client = :client AND alert_at >= :period_start) AS alerts_period_count,
            (SELECT COUNT(DISTINCT(asset_serial_number)) FROM movements WHERE client = :client AND start_time > :one_day_before) AS assets_health_last_24h_count,
        
            -- Status de Bateria (Regras: >50% Boa, 25-50% Baixa, <25% Crítica)
            SUM(CASE WHEN l.battery > 50 THEN 1 ELSE 0 END) AS good_battery_assets_count,
            SUM(CASE WHEN l.battery >= 25 AND l.battery <= 50 THEN 1 ELSE 0 END) AS low_battery_assets_count,
            SUM(CASE WHEN l.battery < 25 AND l.battery IS NOT NULL THEN 1 ELSE 0 END) AS critical_battery_assets_count,

            -- Status de Temperatura (Regras de Alerta Dinâmicas)
            SUM(CASE WHEN l.temperature_c BETWEEN :temp_min AND :temp_max THEN 1 ELSE 0 END) AS ok_temperatures_count,
            SUM(CASE WHEN l.temperature_c > :temp_max THEN 1 ELSE 0 END) AS above_temperatures_count,
            SUM(CASE WHEN l.temperature_c < :temp_min AND l.temperature_c IS NOT NULL THEN 1 ELSE 0 END) AS below_temperatures_count,

            -- Novas estatísticas de consumo e compressor (médias apenas de registros > 0)
            (SELECT AVG(he.total_compressor_on_time_percent) FROM health_events he WHERE he.event_time >= :period_start AND he.client = :client AND he.total_compressor_on_time_percent > 0) AS avg_compressor_on_time,
            (SELECT AVG(he.avg_power_consumption_watt) FROM health_events he WHERE he.event_time >= :period_start AND he.client = :client AND he.avg_power_consumption_watt > 0) AS avg_power_consumption
        FROM
            LatestHealthStatus l
        WHERE
            l.rn = 1;
    """)
    
    status_results = db_session.execute(
        status_and_counts_sql, 
        {
            "client": client, 
            "one_day_before": one_day_before,
            "period_start": period_start, 
            "temp_min": temp_min, 
            "temp_max": temp_max
        }
    ).fetchone()
    
    if not status_results:
        # Retorna um dicionário vazio ou com zeros se o cliente não tiver dados
        return {
            "total_assets": db_session.query(Asset).filter(Asset.client == client).count(),
            "assets_health_last_24h_count": 0,
            "alerts_period_count": 0,
            "ok_temperatures_count": 0, "total_with_temperature": 0,
            "good_battery_assets_count": 0, "low_battery_assets_count": 0, "critical_battery_assets_count": 0,
            "avg_compressor_on_time": 0,
            "avg_power_consumption": 0,
            "hourly_temp_chart": {"labels": [f"{h:02d}:00" for h in range(24)], "data": [0] * 24},
            "hourly_door_chart": {"labels": [f"{h:02d}:00" for h in range(24)], "data": [0] * 24},
            "temperature_status_chart": {"labels": [f'OK ({temp_min}°C-{temp_max}°C)', f'Acima ({temp_max}°C+)', f'Abaixo (<{temp_min}°C)'], "data": [0, 0, 0]},
        }

    # 4. Query 2: Dados Horários (Consultando a MV)
    # Usar colunas de 7 dias se period=7, senão usar 30 dias
    if days == 7:
        hourly_data_sql = text("""
            SELECT
                hour_label,
                hourly_avg_temp_7d AS hourly_avg_temp,
                hourly_avg_door_7d AS hourly_avg_door
            FROM
                mv_dashboard_hourly_metrics
            WHERE
                client = :client
        """)
    else:
        hourly_data_sql = text("""
            SELECT
                hour_label,
                hourly_avg_temp,
                hourly_avg_door
            FROM
                mv_dashboard_hourly_metrics
            WHERE
                client = :client
        """)
    hourly_data = db_session.execute(hourly_data_sql, {"client": client}).fetchall()

    # 5. Processamento Python (Preenchimento e Formatação)
    
    # Mapeamento e Preenchimento de Horas
    temp_by_hour = {row.hour_label: row.hourly_avg_temp for row in hourly_data}
    door_by_hour = {row.hour_label: row.hourly_avg_door for row in hourly_data}
    
    # Criar labels simples para o eixo X (fixos)
    hourly_labels = [f"{h:02d}:00" for h in range(24)]
    
    # Criar dados horários com timestamps UTC para conversão de timezone
    current_date = now.strftime("%Y-%m-%d")
    hourly_data_with_timestamps = []
    for h in range(24):
        timestamp_utc = f"{current_date}T{h:02d}:00:00Z"
        temp_value = temp_by_hour.get(f"{h:02d}:00", 0) or 0
        door_value = door_by_hour.get(f"{h:02d}:00", 0) or 0
        hourly_data_with_timestamps.append({
            "hour": h,
            "timestamp_utc": timestamp_utc,
            "temp": float(temp_value) if temp_value else 0,
            "door": float(door_value) if door_value else 0
        })
    
    # Os dados serão processados no frontend baseado no timezone selecionado
    hourly_temp_data = [0] * 24  # Placeholder inicial
    hourly_door_data = [0] * 24  # Placeholder inicial

    # Processar dados para valores iniciais (UTC)
    for i in range(24):
        label = f"{i:02d}:00"
        # Temperatura:
        avg_temp = temp_by_hour.get(label)
        if avg_temp is not None:
            hourly_temp_data[i] = float(avg_temp)

        # Porta:
        avg_doors = door_by_hour.get(label)
        if avg_doors is not None and avg_doors > 0:
            hourly_door_data[i] = float(avg_doors)

    # Status de Temperatura para Gráfico
    # Coerce possible NULLs (from SQL SUM) to integers
    ok_temperatures_count = int(status_results.ok_temperatures_count or 0)
    above_temperatures_count = int(status_results.above_temperatures_count or 0)
    below_temperatures_count = int(status_results.below_temperatures_count or 0)
    
    temp_status_labels = [
        f'OK ({temp_min}°C-{temp_max}°C)', 
        f'Acima ({temp_max}°C+)', 
        f'Abaixo (<{temp_min}°C)'
    ]
    temp_status_data = [
        ok_temperatures_count, 
        above_temperatures_count, 
        below_temperatures_count
    ]

    # 6. Retorno Final
    return {
        # Período de referência
        "period_days": days,
        
        # Métricas de Cartão (Totais e Contagens)
        "total_assets": int(status_results.total_assets or 0),
        "assets_health_last_24h_count": int(status_results.assets_health_last_24h_count or 0),
        "alerts_period_count": int(status_results.alerts_period_count or 0),
        
        # Métricas de Temperatura (Contagens)
        "ok_temperatures_count": ok_temperatures_count,
        "total_with_temperature": sum(temp_status_data), # OK + Acima + Abaixo
        
        # Métricas de Bateria (Contagens)
        "good_battery_assets_count": int(status_results.good_battery_assets_count or 0),
        "low_battery_assets_count": int(status_results.low_battery_assets_count or 0),
        "critical_battery_assets_count": int(status_results.critical_battery_assets_count or 0),
        
        # Novas Estatísticas
        "avg_compressor_on_time": round(status_results.avg_compressor_on_time, 2) if status_results.avg_compressor_on_time else 0,
        "avg_power_consumption": round(status_results.avg_power_consumption, 2) if status_results.avg_power_consumption else 0,
        
        # Dados para Gráficos (MV)
        "hourly_temp_chart": {
            "labels": hourly_labels,
            "data": hourly_temp_data,
            "raw_data": hourly_data_with_timestamps  # Dados com timestamps para conversão
        },
        "hourly_door_chart": {
            "labels": hourly_labels,
            "data": hourly_door_data,
            "raw_data": hourly_data_with_timestamps  # Dados com timestamps para conversão
        },
        "temperature_status_chart": {
            "labels": temp_status_labels,
            "data": temp_status_data,
        },
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
            'SmartDevice': ['smart'],
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