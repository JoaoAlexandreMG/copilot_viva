"""
API Routes para o Dashboard Admin
"""

from flask import (
    Blueprint,
    jsonify,
    request,
    render_template,
    redirect,
    url_for,
    session,
)
from utils.import_manager import import_manager
from health_check import HealthCheck
import os
import psutil
from datetime import datetime
import subprocess
from functools import wraps

admin_dashboard_bp = Blueprint(
    "admin_dashboard", __name__, url_prefix="/api/admin-dashboard"
)

# Password para proteger o dashboard
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "789852")
SESSION_KEY = "dashboard_authenticated"


def require_dashboard_auth(f):
    """Decorator para validar autenticação do dashboard via sessão"""

    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Verifica se tem sessão válida
        if not session.get(SESSION_KEY):
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "Unauthorized",
                        "message": "Sessão inválida ou expirada",
                    }
                ),
                401,
            )

        return f(*args, **kwargs)

    return decorated_function


@admin_dashboard_bp.route("/login", methods=["POST"])
def login():
    """Endpoint de login - valida senha e cria sessão"""
    data = request.get_json()
    password = data.get("password") if data else None

    if not password or password != DASHBOARD_PASSWORD:
        return jsonify({"success": False, "message": "Senha incorreta"}), 401

    # Cria sessão
    session[SESSION_KEY] = True
    session.permanent = True  # Sessão permanente

    return jsonify({"success": True, "message": "Login realizado com sucesso"}), 200


@admin_dashboard_bp.route("/logout", methods=["POST"])
def logout():
    """Endpoint de logout - remove sessão"""
    session.pop(SESSION_KEY, None)
    return jsonify({"success": True}), 200


@admin_dashboard_bp.route("/", methods=["GET"])
def dashboard_page():
    """Serve the admin dashboard page - com verificação de sessão"""
    # Verifica se tem sessão ativa
    if session.get(SESSION_KEY):
        return render_template("admin_dashboard.html")

    # Se não tem sessão, mostra login
    return render_template("admin_login.html")


@admin_dashboard_bp.route("/overview", methods=["GET"])
@require_dashboard_auth
def get_overview():
    """Retorna visão geral completa do sistema"""
    health = HealthCheck.full_check()
    import_status = import_manager.get_status()

    # Info do servidor (SEM interval para não travar)
    cpu_percent = psutil.cpu_percent(interval=0)  # Non-blocking
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    # Uptime do SERVIÇO copilot_prod (não do sistema)
    try:
        result = subprocess.run(
            [
                "systemctl",
                "show",
                "copilot_prod",
                "--property=ActiveEnterTimestampMonotonic",
            ],
            capture_output=True,
            text=True,
        )
        # Pega o timestamp de quando o serviço iniciou
        active_timestamp = result.stdout.strip().split("=")[1]

        if active_timestamp and active_timestamp != "0":
            # Calcula tempo desde que o serviço iniciou
            with open("/proc/uptime", "r") as f:
                system_uptime = float(f.readline().split()[0])

            # ActiveEnterTimestampMonotonic está em microsegundos
            service_start_time = int(active_timestamp) / 1000000

            # Uptime do serviço = tempo atual do sistema - tempo desde que ligou + tempo que o serviço está rodando
            # Na prática: lê /proc/uptime e subtrai o tempo de boot para achar o uptime do serviço
            result2 = subprocess.run(
                [
                    "systemctl",
                    "show",
                    "copilot_prod",
                    "--property=ActiveEnterTimestamp",
                ],
                capture_output=True,
                text=True,
            )

            # Calcula diferença entre agora e quando o serviço iniciou
            from dateutil import parser as date_parser

            active_since = result2.stdout.strip().split("=")[1]

            if active_since:
                service_start = date_parser.parse(active_since)
                uptime_seconds = int(
                    (datetime.now(service_start.tzinfo) - service_start).total_seconds()
                )
            else:
                uptime_seconds = 0
        else:
            uptime_seconds = 0
    except Exception as e:
        print(f"Erro ao calcular uptime do serviço: {e}")
        # Fallback para uptime do sistema
        with open("/proc/uptime", "r") as f:
            uptime_seconds = int(float(f.readline().split()[0]))

    return (
        jsonify(
            {
                "success": True,
                "timestamp": datetime.now().isoformat(),
                "health": {
                    **health,
                    "uptime_seconds": uptime_seconds,
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "disk_percent": disk.percent,
                },
                "import_status": import_status,
                "scraping_status": get_scraping_status_internal(),
                "system": {
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "memory_available_gb": round(memory.available / (1024**3), 2),
                    "disk_percent": disk.percent,
                    "disk_free_gb": round(disk.free / (1024**3), 2),
                    "uptime_hours": round(uptime_seconds / 3600, 2),
                },
            }
        ),
        200,
    )


def get_scraping_status_internal():
    """Verifica status de scraping (função interna sem autenticação)"""
    try:
        # Verifica se há processo rodando
        result = subprocess.run(
            ["pgrep", "-f", "scraping_parallel.py"], capture_output=True, text=True
        )
        is_running = result.returncode == 0

        # Informações do último scraping
        last_run = None
        last_status = "unknown"
        success_count = None

        log_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "logs", "scraping_daily.log"
        )
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()[-100:]

                # Procura conclusão
                for line in reversed(lines):
                    if (
                        "Scraping concluído com sucesso" in line
                        or "Scraping diário finalizado" in line
                    ):
                        if "sucesso" in line.lower():
                            last_status = "success"
                        # Extrai timestamp
                        if "[" in line and "]" in line:
                            timestamp_str = line[line.find("[") + 1 : line.find("]")]
                            try:
                                from dateutil import parser
                                from datetime import datetime, timezone

                                dt = parser.parse(timestamp_str)
                                # Calcula tempo decorrido
                                now = datetime.now(timezone.utc)
                                if dt.tzinfo is None:
                                    dt = dt.replace(tzinfo=timezone.utc)
                                elapsed = now - dt

                                # Formata tempo decorrido
                                if elapsed.total_seconds() < 60:
                                    last_run = "Agora"
                                elif elapsed.total_seconds() < 3600:
                                    mins = int(elapsed.total_seconds() / 60)
                                    last_run = f"Há {mins} min"
                                elif elapsed.total_seconds() < 86400:
                                    hours = int(elapsed.total_seconds() / 3600)
                                    last_run = f"Há {hours}h"
                                else:
                                    days = int(elapsed.total_seconds() / 86400)
                                    last_run = f"Há {days}d"
                            except:
                                pass
                        break

                # Procura contagem de sucessos
                import re

                for line in reversed(lines):
                    if "Total processado:" in line:
                        match = re.search(r"(\d+)/(\d+) clientes", line)
                        if match:
                            success_count = f"{match.group(1)}/{match.group(2)}"
                        break

            except Exception as e:
                print(f"Erro ao ler log: {e}")

        return {
            "running": is_running,
            "last_run": last_run,
            "last_status": last_status,
            "success_count": success_count,
        }
    except:
        return {"running": False}


@admin_dashboard_bp.route("/import/start", methods=["POST"])
@require_dashboard_auth
def start_import():
    """Inicia importação"""
    if import_manager.is_running():
        return jsonify({"success": False, "message": "Importação já em andamento"}), 409

    refresh_views = request.json.get("refresh_views", False) if request.json else False
    import_manager.start_import(refresh_views=refresh_views)

    return (
        jsonify(
            {
                "success": True,
                "message": "Importação iniciada",
                "status": import_manager.get_status(),
            }
        ),
        202,
    )


@admin_dashboard_bp.route("/import/status", methods=["GET"])
@require_dashboard_auth
def get_import_status():
    """Status da importação"""
    return jsonify(import_manager.get_status()), 200


@admin_dashboard_bp.route("/import/cancel", methods=["POST"])
@require_dashboard_auth
def cancel_import():
    """Cancela importação"""
    if not import_manager.is_running():
        return (
            jsonify({"success": False, "message": "Nenhuma importação em andamento"}),
            400,
        )

    import_manager.status = import_manager.status.__class__("idle")

    return jsonify({"success": True, "message": "Importação cancelada"}), 200


@admin_dashboard_bp.route("/scraping/start", methods=["POST"])
@require_dashboard_auth
def start_scraping():
    """Inicia scraping"""
    scraping_type = (
        request.json.get("scraping_type", "daily") if request.json else "daily"
    )

    try:
        # Caminho absoluto do script e do venv
        base_dir = os.path.dirname(os.path.dirname(__file__))
        script_path = os.path.join(base_dir, "utils", "scraping_parallel.py")
        venv_python = os.path.join(base_dir, "venv", "bin", "python3")
        log_file = os.path.join(base_dir, "logs", "scraping_daily.log")

        # Usa o Python do virtualenv se existir, senão usa python3
        python_cmd = venv_python if os.path.exists(venv_python) else "python3"

        # Abre arquivo de log para escrita (append mode)
        log_handle = open(log_file, "a", buffering=1)

        # Adiciona timestamp no início
        log_handle.write(
            f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ============================================\n"
        )
        log_handle.write(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scraping iniciado via dashboard\n"
        )
        log_handle.write(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ============================================\n"
        )
        log_handle.flush()

        # Executar scraping em background com o Python correto
        process = subprocess.Popen(
            [python_cmd, script_path],
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            cwd=base_dir,
        )

        return (
            jsonify(
                {
                    "success": True,
                    "message": f"Scraping iniciado em background (PID: {process.pid})",
                    "type": scraping_type,
                    "pid": process.pid,
                }
            ),
            202,
        )
    except Exception as e:
        return (
            jsonify(
                {"success": False, "message": f"Erro ao iniciar scraping: {str(e)}"}
            ),
            500,
        )


@admin_dashboard_bp.route("/scraping/status", methods=["GET"])
@require_dashboard_auth
def get_scraping_status():
    """Verifica se há processo de scraping rodando"""
    try:
        # Procura por processos Python executando scraping_parallel.py
        result = subprocess.run(
            ["pgrep", "-f", "scraping_parallel.py"], capture_output=True, text=True
        )

        is_running = result.returncode == 0
        pids = result.stdout.strip().split("\n") if is_running else []

        # Lê últimas linhas do log para pegar o resumo
        log_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "logs", "scraping_daily.log"
        )
        last_log = ""
        if os.path.exists(log_path):
            with open(log_path, "r") as f:
                lines = f.readlines()
                # Pega últimas 5 linhas
                last_log = "".join(lines[-5:]) if lines else ""

        return (
            jsonify(
                {
                    "running": is_running,
                    "pids": [int(p) for p in pids if p],
                    "last_log": last_log,
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"running": False, "error": str(e)}), 200


@admin_dashboard_bp.route("/service/restart", methods=["POST"])
@require_dashboard_auth
def restart_service():
    """Reinicia o serviço (graceful)"""
    try:
        os.system("sudo systemctl restart copilot_prod")
        return jsonify({"success": True, "message": "Serviço reiniciado"}), 200
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@admin_dashboard_bp.route("/service/status", methods=["GET"])
@require_dashboard_auth
def get_service_status():
    """Status do serviço systemd"""
    try:
        result = subprocess.run(
            ["systemctl", "is-active", "copilot_prod"], capture_output=True, text=True
        )
        is_active = result.returncode == 0

        return (
            jsonify(
                {
                    "active": is_active,
                    "status": "Ativo ✅" if is_active else "Inativo ❌",
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@admin_dashboard_bp.route("/logs/recent", methods=["GET"])
@require_dashboard_auth
def get_recent_logs():
    """Últimos 50 logs do serviço"""
    try:
        result = subprocess.run(
            ["journalctl", "-u", "copilot_prod", "-n", "50", "--no-pager"],
            capture_output=True,
            text=True,
        )
        return jsonify({"logs": result.stdout.split("\n")}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
