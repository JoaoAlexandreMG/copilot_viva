from flask import Blueprint, jsonify
from models.models import SmartDevice
from db.database import get_session 

smart_devices_bp = Blueprint("smart_devices", __name__, url_prefix="/smartdevices")

@smart_devices_bp.route("/", methods=["GET"])
def get_smart_devices():
    session = get_session()  # Obtenha a sessão do banco de dados
    smart_devices = session.query(SmartDevice).all()  # Use a sessão para consultar os smart devices
    return jsonify([smart_device.to_dict() for smart_device in smart_devices])

@smart_devices_bp.route("/<string:smart_device_mac_address>", methods=["GET"])
def get_smart_device(smart_device_mac_address):
    session = get_session()  # Obtenha a sessão do banco de dados
    smart_device = session.query(SmartDevice).filter(SmartDevice.mac_address == smart_device_mac_address).first()  # Use a sessão para consultar o smart device
    if smart_device:
        return jsonify(smart_device.to_dict())
    else:
        return jsonify({"error": "Smart device not found"}), 404