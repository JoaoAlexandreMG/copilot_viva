"""Portal route decorators for authentication and authorization"""

from flask import session, redirect, url_for
from functools import wraps


def require_authentication(f):
    """Decorator to check if user is authenticated"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = session.get("user")
        if not user:
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return decorated_function
