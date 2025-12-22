"""
Blueprint para gerenciar importações em background via API REST
"""

from flask import Blueprint, jsonify, request
from utils.import_manager import import_manager
import json

admin_bp = Blueprint("admin", __name__, url_prefix="/api/admin")


@admin_bp.route("/import/status", methods=["GET"])
def import_status():
    """Retorna status atual da importação"""
    return jsonify(import_manager.get_status()), 200


@admin_bp.route("/import/start", methods=["POST"])
def import_start():
    """Inicia importação em background"""

    # Verifica se já está rodando
    if import_manager.is_running():
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Importação já está em andamento",
                    "status": import_manager.get_status(),
                }
            ),
            409,
        )

    # Opção de atualizar MVs
    refresh_views = request.json.get("refresh_views", False) if request.json else False

    # Inicia importação
    success = import_manager.start_import(refresh_views=refresh_views)

    if success:
        return (
            jsonify(
                {
                    "success": True,
                    "message": "Importação iniciada em background",
                    "status": import_manager.get_status(),
                }
            ),
            202,
        )
    else:
        return (
            jsonify(
                {"success": False, "message": "Não foi possível iniciar importação"}
            ),
            500,
        )


@admin_bp.route("/import/cancel", methods=["POST"])
def import_cancel():
    """Cancela importação em andamento"""

    if not import_manager.is_running():
        return (
            jsonify({"success": False, "message": "Nenhuma importação em andamento"}),
            400,
        )

    # Marca para cancelamento
    import_manager.status = import_manager.status.__class__("idle")

    return jsonify({"success": True, "message": "Importação cancelada"}), 200


@admin_bp.route("/import/wait", methods=["GET"])
def import_wait():
    """Aguarda conclusão da importação (webhook)"""

    timeout = request.args.get("timeout", 300, type=int)

    # Aguarda até timeout segundos
    import_manager.wait_completion(timeout)

    return jsonify(import_manager.get_status()), 200
