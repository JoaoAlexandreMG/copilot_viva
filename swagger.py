"""
Swagger/OpenAPI Documentation for Flask Asset Management API
Complete documentation for all endpoints in the application
"""

from flask import Flask, Blueprint
from flask_restx import Api, Resource, fields, Namespace
import os

# Create API Blueprint
swagger_bp = Blueprint('api_docs', __name__, url_prefix='/api-docs')

# Configure API with detailed information
api = Api(
    swagger_bp,
    version='1.0.0',
    title='Asset Management API',
    description='''
    ## API de Gerenciamento de Assets - Viva Services AI
    
    Sistema completo para gerenciamento de assets IoT, outlets, usuários e smart devices.
    
    ### Autenticação
    A maioria dos endpoints requer autenticação via sessão Flask.
    Use o endpoint `/` (POST) para fazer login e criar uma sessão.
    
    ### Estrutura
    - **App Routes**: Endpoints legados para aplicação mobile/web
    - **Portal Routes**: Endpoints para o portal administrativo
    
    ### Recursos Principais
    - Assets (Equipamentos IoT)
    - Outlets (Pontos de venda)
    - Users (Usuários do sistema)
    - Smart Devices (Dispositivos inteligentes)
    - Tracking (Rastreamento de assets)
    - Dashboard (Estatísticas e analytics)
    
    ### Base URL
    - **Produção**: https://vivaservicesai.com
    - **Desenvolvimento**: http://localhost:5000
    ''',
    doc='/docs',
    authorizations={
        'session': {
            'type': 'apiKey',
            'in': 'cookie',
            'name': 'session'
        }
    },
    security='session'
)

# =============================================================================
# NAMESPACES (Organizando os endpoints por categoria)
# =============================================================================

ns_auth = api.namespace('auth', description='Autenticação e Login')
ns_app_assets = api.namespace('app/assets', description='Assets - App Routes')
ns_app_outlets = api.namespace('app/outlets', description='Outlets - App Routes')
ns_app_users = api.namespace('app/users', description='Users - App Routes')
ns_app_smartdevices = api.namespace('app/smartdevices', description='Smart Devices - App Routes')
ns_app_google = api.namespace('app/google-accounts', description='Google Accounts - App Routes')
ns_portal_dashboard = api.namespace('portal/dashboard', description='Dashboard e Analytics')
ns_portal_assets = api.namespace('portal/assets', description='Assets - Portal Admin')
ns_portal_outlets = api.namespace('portal/outlets', description='Outlets - Portal Admin')
ns_portal_users = api.namespace('portal/users', description='Users - Portal Admin')
ns_portal_smartdevices = api.namespace('portal/smartdevices', description='Smart Devices - Portal Admin')
ns_portal_tracking = api.namespace('portal/tracking', description='Tracking e Localização')

# =============================================================================
# MODELS (Schemas para Request/Response)
# =============================================================================

# Auth Models
login_model = api.model('Login', {
    'upn': fields.String(required=True, description='User Principal Name (email)', example='user@example.com'),
    'country': fields.String(description='País do usuário', example='brasil'),
    'destination': fields.String(description='Destino após login (assets ou portal)', example='assets'),
    'check_only': fields.Boolean(description='Apenas verificar se usuário existe', example=False),
    'latitude': fields.Float(description='Latitude GPS (mobile)', example=-23.5505),
    'longitude': fields.Float(description='Longitude GPS (mobile)', example=-46.6333),
    'gps_accuracy': fields.Float(description='Precisão GPS em metros', example=10.5),
    'country_code': fields.String(description='Código do país (ISO)', example='BR'),
    'user_ip': fields.String(description='IP do usuário', example='192.168.1.1'),
})

# User Models
user_model = api.model('User', {
    'id': fields.Integer(description='ID do usuário'),
    'upn': fields.String(required=True, description='User Principal Name'),
    'first_name': fields.String(required=True, description='Nome'),
    'last_name': fields.String(required=True, description='Sobrenome'),
    'email': fields.String(description='Email'),
    'client': fields.String(required=True, description='Código do cliente'),
    'role': fields.String(description='Função do usuário'),
    'country': fields.String(description='País'),
    'last_login_on': fields.DateTime(description='Último login')
})

# Asset Models
asset_model = api.model('Asset', {
    'oem_serial_number': fields.String(required=True, description='Número de série OEM'),
    'bottler_equipment_number': fields.String(description='Número de equipamento'),
    'client': fields.String(required=True, description='Código do cliente'),
    'outlet': fields.String(description='Código do outlet'),
    'asset_type': fields.String(description='Tipo do asset'),
    'sales_organization': fields.String(description='Organização de vendas'),
    'sub_trade_channel': fields.String(description='Canal de comércio'),
    'city': fields.String(description='Cidade'),
    'state': fields.String(description='Estado'),
    'country': fields.String(description='País'),
    'latitude': fields.Float(description='Latitude'),
    'longitude': fields.Float(description='Longitude')
})

asset_location_model = api.model('AssetLocation', {
    'asset_serial_number': fields.String(required=True, description='Serial do asset'),
    'latitude': fields.Float(required=True, description='Latitude'),
    'longitude': fields.Float(required=True, description='Longitude')
})

# Outlet Models
outlet_model = api.model('Outlet', {
    'outlet_code': fields.String(required=True, description='Código do outlet'),
    'outlet': fields.String(required=True, description='Nome do outlet'),
    'client': fields.String(required=True, description='Código do cliente'),
    'street': fields.String(description='Endereço'),
    'city': fields.String(description='Cidade'),
    'state': fields.String(description='Estado'),
    'country': fields.String(description='País'),
    'latitude': fields.Float(description='Latitude'),
    'longitude': fields.Float(description='Longitude'),
    'outlet_type': fields.String(description='Tipo do outlet')
})

outlet_nearby_model = api.model('OutletNearby', {
    'latitude': fields.Float(required=True, description='Latitude'),
    'longitude': fields.Float(required=True, description='Longitude'),
    'radius_km': fields.Float(description='Raio de busca em km', example=5.0)
})

# SmartDevice Models
smartdevice_model = api.model('SmartDevice', {
    'mac_address': fields.String(required=True, description='MAC Address do dispositivo'),
    'client': fields.String(required=True, description='Código do cliente'),
    'asset_serial_number': fields.String(description='Serial do asset vinculado'),
    'device_type': fields.String(description='Tipo do dispositivo'),
    'firmware_version': fields.String(description='Versão do firmware'),
    'last_seen': fields.DateTime(description='Última vez visto')
})

# Dashboard Models
dashboard_stats_model = api.model('DashboardStats', {
    'period_days': fields.Integer(description='Período em dias'),
    'total_assets': fields.Integer(description='Total de assets'),
    'assets_health_last_24h_count': fields.Integer(description='Assets com saúde nas últimas 24h'),
    'alerts_period_count': fields.Integer(description='Alertas no período'),
    'ok_temperatures_count': fields.Integer(description='Temperaturas OK'),
    'good_battery_assets_count': fields.Integer(description='Assets com bateria boa'),
    'avg_compressor_on_time': fields.Float(description='Tempo médio compressor ligado'),
    'avg_power_consumption': fields.Float(description='Consumo médio de energia')
})

# Tracking Models
tracking_filter_model = api.model('TrackingFilter', {
    'bottler_equipment_number': fields.String(description='Número do equipamento'),
    'oem_serial_number': fields.String(description='Serial OEM'),
    'outlet': fields.String(description='Nome do outlet'),
    'sub_trade_channel': fields.String(description='Canal de comércio'),
    'city': fields.String(description='Cidade'),
    'state': fields.String(description='Estado'),
    'country': fields.String(description='País'),
    'is_online': fields.Boolean(description='Status online'),
    'is_missing': fields.Boolean(description='Status perdido'),
    'subclient': fields.String(description='Código do subclient')
})

# =============================================================================
# AUTH ENDPOINTS
# =============================================================================

@ns_auth.route('/')
class Login(Resource):
    @ns_auth.doc('login')
    @ns_auth.expect(login_model)
    @ns_auth.response(200, 'Login bem-sucedido - redireciona para dashboard ou assets')
    @ns_auth.response(400, 'Dados inválidos')
    @ns_auth.response(404, 'Usuário não encontrado')
    def post(self):
        '''Fazer login no sistema'''
        return {'message': 'Documentação apenas - use o formulário HTML em /'}, 200

    @ns_auth.doc('render_login')
    @ns_auth.response(200, 'Renderiza página de login')
    def get(self):
        '''Renderizar página de login'''
        return {'message': 'Renderiza template HTML'}, 200

@ns_auth.route('/logout')
class Logout(Resource):
    @ns_auth.doc('logout', security='session')
    @ns_auth.response(200, 'Logout bem-sucedido')
    def post(self):
        '''Fazer logout do sistema'''
        return {'message': 'Logout bem-sucedido'}, 200

# =============================================================================
# APP ASSETS ENDPOINTS
# =============================================================================

@ns_app_assets.route('/')
class AppAssetsList(Resource):
    @ns_app_assets.doc('list_app_assets', security='session')
    @ns_app_assets.response(200, 'Lista de assets retornada')
    def get(self):
        '''Listar todos os assets (renderiza página HTML)'''
        return {'message': 'Renderiza template HTML com lista de assets'}, 200

@ns_app_assets.route('/api/search')
class AppAssetsSearch(Resource):
    @ns_app_assets.doc('search_app_assets', security='session')
    @ns_app_assets.param('search', 'Termo de busca')
    @ns_app_assets.param('page', 'Número da página (default: 1)')
    @ns_app_assets.param('per_page', 'Items por página (default: 50)')
    @ns_app_assets.response(200, 'Assets encontrados')
    def get(self):
        '''Buscar assets por termo'''
        return {
            'data': [],
            'pagination': {
                'page': 1,
                'pages': 1,
                'total': 0,
                'per_page': 50
            }
        }, 200

@ns_app_assets.route('/api/filter-options')
class AppAssetsFilterOptions(Resource):
    @ns_app_assets.doc('get_app_asset_filters', security='session')
    @ns_app_assets.response(200, 'Opções de filtro retornadas')
    def get(self):
        '''Obter opções disponíveis para filtros'''
        return {
            'outlets': [],
            'cities': [],
            'states': [],
            'asset_types': []
        }, 200

@ns_app_assets.route('/<string:asset_serial_number>')
class AppAssetDetail(Resource):
    @ns_app_assets.doc('get_app_asset', security='session')
    @ns_app_assets.response(200, 'Detalhes do asset retornados')
    @ns_app_assets.response(404, 'Asset não encontrado')
    def get(self, asset_serial_number):
        '''Obter detalhes de um asset específico'''
        return {'oem_serial_number': asset_serial_number}, 200

@ns_app_assets.route('/location')
class AppAssetLocation(Resource):
    @ns_app_assets.doc('update_app_asset_location', security='session')
    @ns_app_assets.expect(asset_location_model)
    @ns_app_assets.response(200, 'Localização atualizada')
    @ns_app_assets.response(404, 'Asset não encontrado')
    def post(self):
        '''Atualizar localização de um asset'''
        return {'message': 'Localização atualizada com sucesso'}, 200

@ns_app_assets.route('/api/by-distance')
class AppAssetsByDistance(Resource):
    @ns_app_assets.doc('get_app_assets_by_distance', security='session')
    @ns_app_assets.expect(api.model('AssetsByDistance', {
        'latitude': fields.Float(required=True),
        'longitude': fields.Float(required=True),
        'max_distance_km': fields.Float(description='Distância máxima em km', example=10.0)
    }))
    @ns_app_assets.response(200, 'Assets ordenados por distância')
    def post(self):
        '''Obter assets ordenados por distância de um ponto'''
        return {'data': []}, 200

@ns_app_assets.route('/api/temperature-history/<string:asset_oem_serial_number>')
class AppAssetTemperatureHistory(Resource):
    @ns_app_assets.doc('get_app_asset_temp_history', security='session')
    @ns_app_assets.param('days', 'Número de dias (default: 7)')
    @ns_app_assets.response(200, 'Histórico de temperatura retornado')
    def get(self, asset_oem_serial_number):
        '''Obter histórico de temperatura de um asset'''
        return {'data': []}, 200

# =============================================================================
# APP OUTLETS ENDPOINTS
# =============================================================================

@ns_app_outlets.route('/')
class AppOutletsList(Resource):
    @ns_app_outlets.doc('list_app_outlets', security='session')
    @ns_app_outlets.response(200, 'Lista de outlets retornada')
    def get(self):
        '''Listar todos os outlets'''
        return {'data': []}, 200

@ns_app_outlets.route('/<string:outlet_code>')
class AppOutletDetail(Resource):
    @ns_app_outlets.doc('get_app_outlet', security='session')
    @ns_app_outlets.response(200, 'Detalhes do outlet retornados')
    @ns_app_outlets.response(404, 'Outlet não encontrado')
    def get(self, outlet_code):
        '''Obter detalhes de um outlet específico'''
        return {'outlet_code': outlet_code}, 200

@ns_app_outlets.route('/api/nearby')
class AppOutletsNearby(Resource):
    @ns_app_outlets.doc('get_app_outlets_nearby', security='session')
    @ns_app_outlets.expect(outlet_nearby_model)
    @ns_app_outlets.response(200, 'Outlets próximos retornados')
    def post(self):
        '''Buscar outlets próximos a uma localização'''
        return {'data': []}, 200

@ns_app_outlets.route('/api/outlet-with-assets')
class AppOutletWithAssets(Resource):
    @ns_app_outlets.doc('get_app_outlet_with_assets', security='session')
    @ns_app_outlets.expect(api.model('OutletWithAssets', {
        'outlet_code': fields.String(required=True)
    }))
    @ns_app_outlets.response(200, 'Outlet com assets retornado')
    def post(self):
        '''Obter outlet com seus assets'''
        return {'outlet': {}, 'assets': []}, 200

# =============================================================================
# APP USERS ENDPOINTS
# =============================================================================

@ns_app_users.route('/subclients')
class AppSubclients(Resource):
    @ns_app_users.doc('list_app_subclients', security='session')
    @ns_app_users.response(200, 'Lista de subclients retornada')
    def get(self):
        '''Listar subclients do usuário logado'''
        return {'data': []}, 200

@ns_app_users.route('/profile')
class AppUserProfile(Resource):
    @ns_app_users.doc('get_app_user_profile', security='session')
    @ns_app_users.response(200, 'Perfil do usuário retornado')
    def get(self):
        '''Obter perfil do usuário logado'''
        return {'user': {}}, 200

@ns_app_users.route('/<int:user_id>')
class AppUserDetail(Resource):
    @ns_app_users.doc('get_app_user', security='session')
    @ns_app_users.response(200, 'Detalhes do usuário retornados')
    @ns_app_users.response(404, 'Usuário não encontrado')
    def get(self, user_id):
        '''Obter detalhes de um usuário'''
        return {'id': user_id}, 200
    
    @ns_app_users.doc('update_app_user', security='session')
    @ns_app_users.expect(user_model)
    @ns_app_users.response(200, 'Usuário atualizado')
    def put(self, user_id):
        '''Atualizar usuário'''
        return {'message': 'Usuário atualizado'}, 200
    
    @ns_app_users.doc('delete_app_user', security='session')
    @ns_app_users.response(200, 'Usuário removido')
    def delete(self, user_id):
        '''Remover usuário'''
        return {'message': 'Usuário removido'}, 200

@ns_app_users.route('/search')
class AppUsersSearch(Resource):
    @ns_app_users.doc('search_app_users', security='session')
    @ns_app_users.param('query', 'Termo de busca')
    @ns_app_users.response(200, 'Usuários encontrados')
    def get(self):
        '''Buscar usuários'''
        return {'data': []}, 200

# =============================================================================
# APP SMART DEVICES ENDPOINTS
# =============================================================================

@ns_app_smartdevices.route('/')
class AppSmartDevicesList(Resource):
    @ns_app_smartdevices.doc('list_app_smartdevices', security='session')
    @ns_app_smartdevices.response(200, 'Lista de smart devices retornada')
    def get(self):
        '''Listar todos os smart devices'''
        return {'data': []}, 200

@ns_app_smartdevices.route('/<string:smart_device_mac_address>')
class AppSmartDeviceDetail(Resource):
    @ns_app_smartdevices.doc('get_app_smartdevice', security='session')
    @ns_app_smartdevices.response(200, 'Detalhes do smart device retornados')
    @ns_app_smartdevices.response(404, 'Smart device não encontrado')
    def get(self, smart_device_mac_address):
        '''Obter detalhes de um smart device'''
        return {'mac_address': smart_device_mac_address}, 200

# =============================================================================
# APP GOOGLE ACCOUNTS ENDPOINTS
# =============================================================================

@ns_app_google.route('/list')
class AppGoogleAccountsList(Resource):
    @ns_app_google.doc('list_google_accounts', security='session')
    @ns_app_google.response(200, 'Lista de contas Google retornada')
    def get(self):
        '''Listar todas as contas Google'''
        return {'data': []}, 200

@ns_app_google.route('/client/<client_id>')
class AppGoogleAccountsByClient(Resource):
    @ns_app_google.doc('get_google_accounts_by_client', security='session')
    @ns_app_google.response(200, 'Contas Google do cliente retornadas')
    def get(self, client_id):
        '''Obter contas Google de um cliente específico'''
        return {'data': []}, 200

@ns_app_google.route('/export')
class AppGoogleAccountsExport(Resource):
    @ns_app_google.doc('export_google_accounts', security='session')
    @ns_app_google.response(200, 'Arquivo Excel gerado')
    def get(self):
        '''Exportar contas Google para Excel'''
        return {'message': 'Download iniciado'}, 200

# =============================================================================
# PORTAL DASHBOARD ENDPOINTS
# =============================================================================

@ns_portal_dashboard.route('/dashboard')
class PortalDashboard(Resource):
    @ns_portal_dashboard.doc('get_portal_dashboard', security='session')
    @ns_portal_dashboard.response(200, 'Dashboard renderizado')
    def get(self):
        '''Renderizar dashboard do portal'''
        return {'message': 'Renderiza template HTML'}, 200

@ns_portal_dashboard.route('/api/dashboard-stats')
class PortalDashboardStats(Resource):
    @ns_portal_dashboard.doc('get_portal_dashboard_stats', security='session')
    @ns_portal_dashboard.param('period', 'Período em dias (7 ou 30)', type='integer')
    @ns_portal_dashboard.marshal_with(dashboard_stats_model)
    @ns_portal_dashboard.response(200, 'Estatísticas retornadas')
    def get(self):
        '''Obter estatísticas do dashboard'''
        return {
            'status': 'ok',
            'data': {
                'period_days': 30,
                'total_assets': 0,
                'assets_health_last_24h_count': 0,
                'alerts_period_count': 0
            }
        }, 200

@ns_portal_dashboard.route('/import_file')
class PortalDashboardImport(Resource):
    @ns_portal_dashboard.doc('import_dashboard_file', security='session')
    @ns_portal_dashboard.response(200, 'Arquivo importado com sucesso')
    @ns_portal_dashboard.response(400, 'Erro ao importar arquivo')
    def post(self):
        '''Importar arquivo Excel para o sistema'''
        return {'status': 'ok', 'result': {'inserted': 0, 'updated': 0}}, 200

# =============================================================================
# PORTAL ASSETS ENDPOINTS
# =============================================================================

@ns_portal_assets.route('')
class PortalAssetsList(Resource):
    @ns_portal_assets.doc('list_portal_assets', security='session')
    @ns_portal_assets.marshal_list_with(asset_model)
    @ns_portal_assets.response(200, 'Lista de assets retornada')
    def get(self):
        '''Listar todos os assets (Portal Admin)'''
        return [], 200
    
    @ns_portal_assets.doc('create_portal_asset', security='session')
    @ns_portal_assets.expect(asset_model)
    @ns_portal_assets.response(201, 'Asset criado com sucesso')
    def post(self):
        '''Criar novo asset'''
        return {'message': 'Asset criado com sucesso'}, 201

@ns_portal_assets.route('/search')
class PortalAssetsSearch(Resource):
    @ns_portal_assets.doc('search_portal_assets', security='session')
    @ns_portal_assets.param('query', 'Termo de busca')
    @ns_portal_assets.response(200, 'Assets encontrados')
    def get(self):
        '''Buscar assets'''
        return {'data': []}, 200

@ns_portal_assets.route('/<string:oem_serial>')
class PortalAssetDetail(Resource):
    @ns_portal_assets.doc('get_portal_asset', security='session')
    @ns_portal_assets.marshal_with(asset_model)
    @ns_portal_assets.response(200, 'Asset retornado')
    @ns_portal_assets.response(404, 'Asset não encontrado')
    def get(self, oem_serial):
        '''Obter detalhes de um asset'''
        return {}, 200
    
    @ns_portal_assets.doc('update_portal_asset', security='session')
    @ns_portal_assets.expect(asset_model)
    @ns_portal_assets.response(200, 'Asset atualizado')
    def put(self, oem_serial):
        '''Atualizar asset'''
        return {'message': 'Asset atualizado'}, 200
    
    @ns_portal_assets.doc('delete_portal_asset', security='session')
    @ns_portal_assets.response(200, 'Asset removido')
    def delete(self, oem_serial):
        '''Remover asset'''
        return {'message': 'Asset removido'}, 200

# =============================================================================
# PORTAL OUTLETS ENDPOINTS
# =============================================================================

@ns_portal_outlets.route('')
class PortalOutletsList(Resource):
    @ns_portal_outlets.doc('list_portal_outlets', security='session')
    @ns_portal_outlets.marshal_list_with(outlet_model)
    @ns_portal_outlets.response(200, 'Lista de outlets retornada')
    def get(self):
        '''Listar todos os outlets (Portal Admin)'''
        return [], 200
    
    @ns_portal_outlets.doc('create_portal_outlet', security='session')
    @ns_portal_outlets.expect(outlet_model)
    @ns_portal_outlets.response(201, 'Outlet criado com sucesso')
    def post(self):
        '''Criar novo outlet'''
        return {'message': 'Outlet criado com sucesso'}, 201

@ns_portal_outlets.route('/search')
class PortalOutletsSearch(Resource):
    @ns_portal_outlets.doc('search_portal_outlets', security='session')
    @ns_portal_outlets.param('query', 'Termo de busca')
    @ns_portal_outlets.response(200, 'Outlets encontrados')
    def get(self):
        '''Buscar outlets'''
        return {'data': []}, 200

@ns_portal_outlets.route('/<string:outlet_code>')
class PortalOutletDetail(Resource):
    @ns_portal_outlets.doc('get_portal_outlet', security='session')
    @ns_portal_outlets.marshal_with(outlet_model)
    @ns_portal_outlets.response(200, 'Outlet retornado')
    @ns_portal_outlets.response(404, 'Outlet não encontrado')
    def get(self, outlet_code):
        '''Obter detalhes de um outlet'''
        return {}, 200
    
    @ns_portal_outlets.doc('update_portal_outlet', security='session')
    @ns_portal_outlets.expect(outlet_model)
    @ns_portal_outlets.response(200, 'Outlet atualizado')
    def put(self, outlet_code):
        '''Atualizar outlet'''
        return {'message': 'Outlet atualizado'}, 200
    
    @ns_portal_outlets.doc('delete_portal_outlet', security='session')
    @ns_portal_outlets.response(200, 'Outlet removido')
    def delete(self, outlet_code):
        '''Remover outlet'''
        return {'message': 'Outlet removido'}, 200

# =============================================================================
# PORTAL USERS ENDPOINTS
# =============================================================================

@ns_portal_users.route('')
class PortalUsersList(Resource):
    @ns_portal_users.doc('list_portal_users', security='session')
    @ns_portal_users.marshal_list_with(user_model)
    @ns_portal_users.response(200, 'Lista de usuários retornada')
    def get(self):
        '''Listar todos os usuários (Portal Admin)'''
        return [], 200
    
    @ns_portal_users.doc('create_portal_user', security='session')
    @ns_portal_users.expect(user_model)
    @ns_portal_users.response(201, 'Usuário criado com sucesso')
    def post(self):
        '''Criar novo usuário'''
        return {'message': 'Usuário criado com sucesso'}, 201

@ns_portal_users.route('/search')
class PortalUsersSearch(Resource):
    @ns_portal_users.doc('search_portal_users', security='session')
    @ns_portal_users.param('query', 'Termo de busca')
    @ns_portal_users.response(200, 'Usuários encontrados')
    def get(self):
        '''Buscar usuários'''
        return {'data': []}, 200

@ns_portal_users.route('/<string:upn>')
class PortalUserDetail(Resource):
    @ns_portal_users.doc('get_portal_user', security='session')
    @ns_portal_users.marshal_with(user_model)
    @ns_portal_users.response(200, 'Usuário retornado')
    @ns_portal_users.response(404, 'Usuário não encontrado')
    def get(self, upn):
        '''Obter detalhes de um usuário'''
        return {}, 200
    
    @ns_portal_users.doc('update_portal_user', security='session')
    @ns_portal_users.expect(user_model)
    @ns_portal_users.response(200, 'Usuário atualizado')
    def put(self, upn):
        '''Atualizar usuário'''
        return {'message': 'Usuário atualizado'}, 200
    
    @ns_portal_users.doc('delete_portal_user', security='session')
    @ns_portal_users.response(200, 'Usuário removido')
    def delete(self, upn):
        '''Remover usuário'''
        return {'message': 'Usuário removido'}, 200

# =============================================================================
# PORTAL SMART DEVICES ENDPOINTS
# =============================================================================

@ns_portal_smartdevices.route('')
class PortalSmartDevicesList(Resource):
    @ns_portal_smartdevices.doc('list_portal_smartdevices', security='session')
    @ns_portal_smartdevices.marshal_list_with(smartdevice_model)
    @ns_portal_smartdevices.response(200, 'Lista de smart devices retornada')
    def get(self):
        '''Listar todos os smart devices (Portal Admin)'''
        return [], 200
    
    @ns_portal_smartdevices.doc('create_portal_smartdevice', security='session')
    @ns_portal_smartdevices.expect(smartdevice_model)
    @ns_portal_smartdevices.response(201, 'Smart device criado com sucesso')
    def post(self):
        '''Criar novo smart device'''
        return {'message': 'Smart device criado com sucesso'}, 201

@ns_portal_smartdevices.route('/search')
class PortalSmartDevicesSearch(Resource):
    @ns_portal_smartdevices.doc('search_portal_smartdevices', security='session')
    @ns_portal_smartdevices.param('query', 'Termo de busca')
    @ns_portal_smartdevices.response(200, 'Smart devices encontrados')
    def get(self):
        '''Buscar smart devices'''
        return {'data': []}, 200

@ns_portal_smartdevices.route('/<string:mac_address>')
class PortalSmartDeviceDetail(Resource):
    @ns_portal_smartdevices.doc('get_portal_smartdevice', security='session')
    @ns_portal_smartdevices.marshal_with(smartdevice_model)
    @ns_portal_smartdevices.response(200, 'Smart device retornado')
    @ns_portal_smartdevices.response(404, 'Smart device não encontrado')
    def get(self, mac_address):
        '''Obter detalhes de um smart device'''
        return {}, 200
    
    @ns_portal_smartdevices.doc('update_portal_smartdevice', security='session')
    @ns_portal_smartdevices.expect(smartdevice_model)
    @ns_portal_smartdevices.response(200, 'Smart device atualizado')
    def put(self, mac_address):
        '''Atualizar smart device'''
        return {'message': 'Smart device atualizado'}, 200
    
    @ns_portal_smartdevices.doc('delete_portal_smartdevice', security='session')
    @ns_portal_smartdevices.response(200, 'Smart device removido')
    def delete(self, mac_address):
        '''Remover smart device'''
        return {'message': 'Smart device removido'}, 200

# =============================================================================
# PORTAL TRACKING ENDPOINTS
# =============================================================================

@ns_portal_tracking.route('/')
class PortalTracking(Resource):
    @ns_portal_tracking.doc('get_portal_tracking', security='session')
    @ns_portal_tracking.param('bottler_equipment_number', 'Filtro: Número do equipamento')
    @ns_portal_tracking.param('oem_serial_number', 'Filtro: Serial OEM')
    @ns_portal_tracking.param('outlet', 'Filtro: Nome do outlet')
    @ns_portal_tracking.param('sub_trade_channel', 'Filtro: Canal de comércio')
    @ns_portal_tracking.param('city', 'Filtro: Cidade')
    @ns_portal_tracking.param('state', 'Filtro: Estado')
    @ns_portal_tracking.param('country', 'Filtro: País')
    @ns_portal_tracking.param('is_online', 'Filtro: Status online (true/false)')
    @ns_portal_tracking.param('is_missing', 'Filtro: Status perdido (true/false)')
    @ns_portal_tracking.param('subclient', 'Filtro: Código do subclient')
    @ns_portal_tracking.response(200, 'Página de tracking renderizada')
    def get(self):
        '''Renderizar página de tracking com filtros via GET'''
        return {'message': 'Renderiza template HTML com mapa e filtros'}, 200

@ns_portal_tracking.route('/devices')
class PortalTrackingDevices(Resource):
    @ns_portal_tracking.doc('get_portal_tracking_devices', security='session')
    @ns_portal_tracking.param('bottler_equipment_number', 'Filtro: Número do equipamento')
    @ns_portal_tracking.param('oem_serial_number', 'Filtro: Serial OEM')
    @ns_portal_tracking.param('outlet', 'Filtro: Nome do outlet')
    @ns_portal_tracking.param('sub_trade_channel', 'Filtro: Canal de comércio')
    @ns_portal_tracking.param('city', 'Filtro: Cidade')
    @ns_portal_tracking.param('state', 'Filtro: Estado')
    @ns_portal_tracking.param('country', 'Filtro: País')
    @ns_portal_tracking.param('is_online', 'Filtro: Status online (true/false)')
    @ns_portal_tracking.param('is_missing', 'Filtro: Status perdido (true/false)')
    @ns_portal_tracking.param('page', 'Número da página', type='integer')
    @ns_portal_tracking.param('load_all', 'Carregar todos para o mapa (true/false)')
    @ns_portal_tracking.response(200, 'Devices retornados com paginação')
    def get(self):
        '''Obter lista de devices para tracking (API otimizada com materialized view)'''
        return {
            'data': [],
            'map_data': [],
            'pagination': {
                'page': 1,
                'pages': 1,
                'total': 0,
                'per_page': 50,
                'has_prev': False,
                'has_next': False
            },
            'available_subclients': [],
            'active_filters': {},
            'optimized': True
        }, 200

@ns_portal_tracking.route('/asset_details/<serial_number>')
class PortalTrackingAssetDetails(Resource):
    @ns_portal_tracking.doc('get_portal_tracking_asset_details', security='session')
    @ns_portal_tracking.response(200, 'Detalhes completos do asset retornados')
    @ns_portal_tracking.response(404, 'Asset não encontrado')
    def get(self, serial_number):
        '''Obter detalhes completos de um asset para tracking'''
        return {
            'oem_serial_number': serial_number,
            'bottler_equipment_number': '',
            'outlet': '',
            'city': '',
            'state': '',
            'country': '',
            'latest_cabinet_temperature_c': 0,
            'is_online': False,
            'is_missing': False,
            'latest_latitude': 0,
            'latest_longitude': 0
        }, 200

@ns_portal_tracking.route('/asset-analytics/<serial_number>')
class PortalTrackingAssetAnalytics(Resource):
    @ns_portal_tracking.doc('get_portal_tracking_asset_analytics', security='session')
    @ns_portal_tracking.response(200, 'Analytics do asset retornados')
    def get(self, serial_number):
        '''Obter dados analíticos detalhados de um asset'''
        return {
            'status': 'success',
            'hourly_data': {
                'labels': [],
                'temperature': [],
                'doors': []
            },
            'statistics': {
                'avg_temperature': 0,
                'max_temperature': 0,
                'min_temperature': 0,
                'avg_battery': 0,
                'avg_power_consumption': 0,
                'avg_compressor_time': 0
            },
            'door_periods': {
                'morning': 0,
                'afternoon': 0,
                'night': 0
            },
            'temperature_status': {
                'ok': 0,
                'above': 0,
                'below': 0,
                'temp_min': 0,
                'temp_max': 7
            }
        }, 200

# =============================================================================
# FUNCTION TO REGISTER SWAGGER
# =============================================================================

def register_swagger(app):
    """
    Register Swagger documentation blueprint to Flask app
    
    Usage:
        from swagger import register_swagger
        register_swagger(app)
    
    Access:
        http://localhost:5000/api-docs/docs
    """
    app.register_blueprint(swagger_bp)
    print("✅ Swagger documentation registered at /api-docs/docs")
