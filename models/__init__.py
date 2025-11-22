"""
Models package - imports all model definitions
This ensures all models are registered with SQLAlchemy's metadata
"""
from models.models import Base, User, Outlet, Asset, SmartDevice, Movement, HealthEvent, DoorEvent, Alert, Client, VisionAccount, SubClient, AlertsDefinition

__all__ = ['Base', 'User', 'Outlet', 'Asset', 'SmartDevice', 'Movement', 'HealthEvent', 'DoorEvent', 'Alert', 'AlertsDefinition', 'Client', 'VisionAccount', 'SubClient']