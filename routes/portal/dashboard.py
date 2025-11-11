from flask import Blueprint, render_template, session, redirect, url_for
from models.models import Asset, Outlet, SmartDevice, User
from db.database import get_session
from datetime import datetime, timedelta

dashboard_bp = Blueprint("dashboard", __name__, url_prefix="/portal_associacao")


@dashboard_bp.route("/dashboard", methods=["GET"])
def render_dashboard():
    """
    Render main portal dashboard
    Show only statistics related to the user's CLIENT
    """
    try:
        # Check if user is authenticated
        user = session.get("user")
        if not user:
            return redirect(url_for("index"))

        db_session = get_session()
        
        # Get user's client from session
        user_client = user.get("client")
        user_country = user.get("user_country")
        if not user_client:
            print(f"[WARNING] User {user.get('upn')} has no client assigned")
            # If no client, show empty stats
            stats = {
                "total_users": 0,
                "active_outlets": 0,
                "total_assets": 0,
                "online_smartdevices": 0,
            }
            return render_template("portal/dashboard.html", stats=stats, page_type="list")

        # Get statistics FILTERED BY USER'S CLIENT
        total_users = db_session.query(User).filter(User.client == user_client).count()
        active_outlets = db_session.query(Outlet).filter(
            Outlet.client == user_client,
            Outlet.is_active == True
        ).count()
        total_assets = db_session.query(Asset).filter(Asset.client == user_client).count()

        # Consider smart devices online if they have pinged in last 48 hours
        # AND belong to user's client
        recent_threshold = datetime.utcnow() - timedelta(hours=48)
        online_smartdevices = (
            db_session.query(SmartDevice)
            .filter(SmartDevice.client == user_client)
            .filter(SmartDevice.last_ping.isnot(None))
            .filter(SmartDevice.last_ping >= recent_threshold)
            .count()
        )

        stats = {
            "total_users": total_users or 0,
            "active_outlets": active_outlets or 0,
            "total_assets": total_assets or 0,
            "online_smartdevices": online_smartdevices or 0,
        }

        return render_template("portal/dashboard.html", stats=stats, page_type="list")

    except Exception as e:
        print(f"[ERROR] Error rendering dashboard: {str(e)}")
        return redirect(url_for("index"))
