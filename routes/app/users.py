from flask import Blueprint, render_template, request, session, redirect, url_for, flash, jsonify
from models.models import User, SubClient
from db.database import get_session
from sqlalchemy import func
from datetime import datetime

users_bp = Blueprint("users", __name__, url_prefix="/portal_associacao/users")

    


@users_bp.route("/subclients", methods=["GET"])
def get_user_subclients():
    """
    Get subclients for logged-in user's client
    """
    try:
        user = session.get("user")
        if not user:
            return jsonify({"error": "Not authenticated"}), 401
        
        db_session = get_session()
        
        # Get subclients for user's client
        subclients = db_session.query(SubClient).filter(
            SubClient.client == user["client"]
        ).all()
        
        subclient_list = [
            {
                "id": sc.id,
                "name": sc.subclient_name,
                "code": sc.subclient_code
            }
            for sc in subclients
        ]
        
        return jsonify({
            "subclients": subclient_list,
            "client": user["client"]
        })
        
    except Exception as e:
        print(f"[ERROR] Error fetching subclients: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

@users_bp.route("/profile", methods=["GET"])
def get_user_profile():
    """
    Get current user profile
    """
    user = session.get("user")
    if not user:
        return jsonify({"error": "Not authenticated"}), 401
    
    return jsonify(user)


# Get user details by UPN
@users_bp.route("/<int:user_id>", methods=["GET"])
def get_user_details(user_id):
    """
    Get user details by UPN (JSON endpoint)
    Note: user_id in the route is actually the upn value
    """
    try:
        db_session = get_session()
        user = db_session.query(User).filter(User.upn == str(user_id)).first()

        if not user:
            return jsonify({"error": "User not found"}), 404

        return jsonify(user.to_dict())

    except Exception as e:
        print(f"[ERROR] Error fetching user: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

# Update user by UPN
@users_bp.route("/<int:user_id>", methods=["PUT", "POST"])
def update_user(user_id):
    """
    Update user by UPN
    Accepts both PUT and POST (for form compatibility)
    Note: user_id in the route is actually the upn value
    """
    try:
        db_session = get_session()
        user = db_session.query(User).filter(User.upn == str(user_id)).first()

        if not user:
            flash("Usuário não encontrado", "error")
            return redirect(url_for("users.list_and_create_users"))

        # Update all fields
        user.first_name = request.form.get("first_name", user.first_name)
        user.last_name = request.form.get("last_name", user.last_name)
        user.user_name = request.form.get("user_name", user.user_name)
        user.email = request.form.get("email", user.email)
        user.upn = request.form.get("upn", user.upn)
        user.phone = request.form.get("phone", user.phone)
        user.role = request.form.get("role", user.role)
        user.reporting_manager = request.form.get("reporting_manager", user.reporting_manager)
        user.country = request.form.get("country", user.country)
        user.responsible_country = request.form.get("responsible_country", user.responsible_country)
        user.sales_organization = request.form.get("sales_organization", user.sales_organization)
        user.sales_office = request.form.get("sales_office", user.sales_office)
        user.sales_group = request.form.get("sales_group", user.sales_group)
        user.sales_territory = request.form.get("sales_territory", user.sales_territory)
        user.teleselling_territory = request.form.get("teleselling_territory", user.teleselling_territory)
        user.bd_territory_name = request.form.get("bd_territory_name", user.bd_territory_name)
        user.ca_territory_name = request.form.get("ca_territory_name", user.ca_territory_name)
        user.mc_territory_name = request.form.get("mc_territory_name", user.mc_territory_name)
        user.p1_territory_name = request.form.get("p1_territory_name", user.p1_territory_name)
        user.p2_territory_name = request.form.get("p2_territory_name", user.p2_territory_name)
        user.p3_territory_name = request.form.get("p3_territory_name", user.p3_territory_name)
        user.p4_territory_name = request.form.get("p4_territory_name", user.p4_territory_name)
        user.p5_territory_name = request.form.get("p5_territory_name", user.p5_territory_name)
        user.ncb_territory_name = request.form.get("ncb_territory_name", user.ncb_territory_name)
        user.preferred_notification_type = request.form.get("preferred_notification_type", user.preferred_notification_type)
        user.client = request.form.get("client", user.client)
        user.modified_on = datetime.now()
        user.modified_by = session.get("user", {}).get("upn", "system")

        db_session.commit()
        flash(f"Usuário {user.first_name} atualizado com sucesso!", "success")
        return redirect(url_for("users.list_and_create_users"))

    except Exception as e:
        db_session.rollback()
        print(f"[ERROR] Error updating user: {str(e)}")
        flash(f"Erro ao atualizar usuário: {str(e)}", "error")
        return redirect(url_for("users.list_and_create_users"))

# Delete user by UPN
@users_bp.route("/<int:user_id>", methods=["DELETE"])
def delete_user(user_id):
    """
    Delete user by UPN
    Note: user_id in the route is actually the upn value
    """
    try:
        db_session = get_session()
        user = db_session.query(User).filter(User.upn == str(user_id)).first()

        if not user:
            flash("Usuário não encontrado", "error")
            return redirect(url_for("users.list_and_create_users"))

        user_name = f"{user.first_name} {user.last_name}"
        db_session.delete(user)
        db_session.commit()

        flash(f"Usuário {user_name} deletado com sucesso!", "success")
        return redirect(url_for("users.list_and_create_users"))

    except Exception as e:
        db_session.rollback()
        print(f"[ERROR] Error deleting user: {str(e)}")
        flash(f"Erro ao deletar usuário: {str(e)}", "error")
        return redirect(url_for("users.list_and_create_users"))

# Search users
@users_bp.route("/search", methods=["GET"])
def search_users():
    """
    Search users by name or email (JSON endpoint)
    """
    try:
        query = request.args.get("q", "").strip()

        if not query:
            return jsonify([])

        db_session = get_session()

        # Search by first_name, last_name, email, or user_name
        search_pattern = f"%{query}%"
        users = db_session.query(User).filter(
            (User.first_name.ilike(search_pattern)) |
            (User.last_name.ilike(search_pattern)) |
            (User.email.ilike(search_pattern)) |
            (User.user_name.ilike(search_pattern))
        ).limit(20).all()

        return jsonify([user.to_dict() for user in users])

    except Exception as e:
        print(f"[ERROR] Error searching users: {str(e)}")
        return jsonify([]), 500