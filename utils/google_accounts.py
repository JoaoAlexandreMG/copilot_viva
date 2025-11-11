"""
Utility module for generating and managing Google accounts for clients.
"""
import string
import random
from datetime import datetime
from pytz import timezone
from models.models import GoogleAccount, Client
from sqlalchemy.orm import Session

def generate_google_account_password(length=16):
    """
    Generate a random secure password for Google account.
    Contains uppercase, lowercase, numbers, and special characters.
    """
    characters = string.ascii_letters + string.digits + "!@#$%^&*"
    password = ''.join(random.choice(characters) for _ in range(length))
    return password

def generate_google_email(client_code, client_id):
    """
    Generate a Google email based on client code and ID.
    Format: client_code_clientid@gmail.com
    """
    email = f"{client_code.lower()}_{client_id}@gmail.com"
    return email

def create_google_accounts_for_all_clients(session: Session):
    """
    Create Google accounts for all clients that don't have one yet.
    Returns: (created_count, total_clients)
    """
    try:
        # Get all clients
        all_clients = session.query(Client).all()

        created_count = 0
        for client in all_clients:
            # Check if this client already has a Google account
            existing_account = session.query(GoogleAccount).filter_by(client_id=client.id).first()

            if not existing_account:
                # Generate email and password
                email = generate_google_email(client.client_code, client.id)
                password = generate_google_account_password()

                # Create Google account
                google_account = GoogleAccount(
                    id=f"GA_{client.id}",  # Unique ID for the account
                    client_id=client.id,
                    email=email,
                    password=password,
                    created_on=datetime.now(timezone("America/Sao_Paulo")),
                    created_by="system"
                )
                session.add(google_account)
                created_count += 1

        # Commit all changes
        session.commit()

        return created_count, len(all_clients)

    except Exception as e:
        session.rollback()
        raise Exception(f"Erro ao criar contas do Google: {str(e)}")

def get_google_account_by_client(session: Session, client_id: str):
    """
    Retrieve Google account for a specific client.
    """
    return session.query(GoogleAccount).filter_by(client_id=client_id).first()

def get_all_google_accounts(session: Session):
    """
    Retrieve all Google accounts.
    """
    return session.query(GoogleAccount).all()

def export_google_accounts_to_csv(session: Session, output_file: str):
    """
    Export all Google accounts to a CSV file.
    Useful for sharing with the team or storing credentials.
    """
    import csv

    try:
        accounts = session.query(GoogleAccount).all()

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Account ID', 'Client ID', 'Email', 'Password', 'Created On', 'Created By'])

            for account in accounts:
                writer.writerow([
                    account.id,
                    account.client_id,
                    account.email,
                    account.password,
                    account.created_on.strftime("%d/%m/%Y %H:%M:%S") if account.created_on else "",
                    account.created_by
                ])

        return len(accounts)

    except Exception as e:
        raise Exception(f"Erro ao exportar contas do Google: {str(e)}")
