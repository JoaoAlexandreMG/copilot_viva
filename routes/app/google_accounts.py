"""
Routes for managing Google accounts associated with clients.
"""
from flask import Blueprint, jsonify, send_file
from db.database import get_session
from models.models import GoogleAccount
from utils.google_accounts import get_google_account_by_client, get_all_google_accounts, export_google_accounts_to_csv
import os

google_accounts_bp = Blueprint('google_accounts', __name__, url_prefix='/google-accounts')

@google_accounts_bp.route('/list', methods=['GET'])
def list_google_accounts():
    """
    Get all Google accounts created for clients.
    """
    try:
        session = get_session()
        accounts = get_all_google_accounts(session)

        return jsonify({
            'success': True,
            'total': len(accounts),
            'accounts': [
                {
                    'id': acc.id,
                    'client_id': acc.client_id,
                    'email': acc.email,
                    'created_on': acc.created_on.strftime("%d/%m/%Y %H:%M:%S") if acc.created_on else None,
                    'created_by': acc.created_by
                }
                for acc in accounts
            ]
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@google_accounts_bp.route('/client/<client_id>', methods=['GET'])
def get_client_google_account(client_id):
    """
    Get Google account for a specific client.
    """
    try:
        session = get_session()
        account = get_google_account_by_client(session, client_id)

        if not account:
            return jsonify({
                'success': False,
                'error': 'Google account not found for this client'
            }), 404

        return jsonify({
            'success': True,
            'account': {
                'id': account.id,
                'client_id': account.client_id,
                'email': account.email,
                'created_on': account.created_on.strftime("%d/%m/%Y %H:%M:%S") if account.created_on else None,
                'created_by': account.created_by
            }
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@google_accounts_bp.route('/export', methods=['GET'])
def export_accounts():
    """
    Export all Google accounts to CSV file and download it.
    """
    try:
        session = get_session()
        output_file = 'exports/google_accounts.csv'
        count = export_google_accounts_to_csv(session, output_file)

        # Return the file for download
        if os.path.exists(output_file):
            return send_file(
                output_file,
                mimetype='text/csv',
                as_attachment=True,
                download_name='google_accounts.csv'
            ), 200
        else:
            return jsonify({
                'success': False,
                'error': 'Arquivo não foi criado corretamente'
            }), 500

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
