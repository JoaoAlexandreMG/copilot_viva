from flask import Blueprint, session, redirect, url_for

auth_bp = Blueprint("portal_auth", __name__, url_prefix="/portal_associacao/auth")


@auth_bp.route("/logout", methods=["GET", "POST"])
def logout():
    """
    Logout user from portal - clear session
    """
    session.clear()
    return redirect(url_for("index"))
