// View fields configuration for Assets
const viewFields = [
    { label: 'Serial OEM', key: 'oem_serial_number' },
    { label: 'Tipo Asset', key: 'asset_type' },
    { label: 'Número Equipamento', key: 'bottler_equipment_number' },
    { label: 'ID Técnico', key: 'technical_id' },
    { label: 'Serial OEM', key: 'oem_serial_number' },
    { label: 'Categoria', key: 'category' },
    { label: 'Competição', key: 'is_competition', type: 'boolean' },
    { label: 'Factory Asset', key: 'is_factory_asset', type: 'boolean' },
    { label: 'Associado em Factory', key: 'associated_in_factory', type: 'boolean' },
    { label: 'Outlet', key: 'outlet' },
    { label: 'Código Outlet', key: 'outlet_code' },
    { label: 'Tipo Outlet', key: 'outlet_type' },
    { label: 'Localização Loja', key: 'store_location' },
    { label: 'Canal', key: 'trade_channel' },
    { label: 'Tier Cliente', key: 'customer_tier' },
    { label: 'Sub Canal', key: 'sub_trade_channel' },
    { label: 'Org. Vendas', key: 'sales_organization' },
    { label: 'Escritório', key: 'sales_office' },
    { label: 'Grupo Vendas', key: 'sales_group' },
    { label: 'Terr. Vendas', key: 'sales_territory' },
    { label: 'Issue', key: 'issue' },
    { label: 'Asset Ping', key: 'asset_ping' },
    { label: 'Smart Device', key: 'smart_device' },
    { label: 'Tipo Smart Device', key: 'smart_device_type' },
    { label: 'Smart Device Ping', key: 'smart_device_ping' },
    { label: 'Gateway', key: 'gateway' },
    { label: 'Tipo Gateway', key: 'gateway_type' },
    { label: 'Gateway Ping', key: 'gateway_ping' },
    { label: 'Último Scan', key: 'last_scan', type: 'date' },
    { label: 'Status Visit Scan', key: 'visit_scan_status' },
    { label: 'Cliente', key: 'client' },
    { label: 'Cidade', key: 'city' },
    { label: 'Rua', key: 'street' },
    { label: 'Rua 2', key: 'street_2' },
    { label: 'Rua 3', key: 'street_3' },
    { label: 'Estado', key: 'state' },
    { label: 'País', key: 'country' },
    { label: 'Posição Prime', key: 'prime_position', type: 'boolean' },
    { label: 'Desaparecido', key: 'is_missing', type: 'boolean' },
    { label: 'Vision', key: 'is_vision', type: 'boolean' },
    { label: 'Smart', key: 'is_smart', type: 'boolean' },
    { label: 'Movimento Autorizado', key: 'is_authorized_movement', type: 'boolean' },
    { label: 'Não Saudável', key: 'is_unhealthy', type: 'boolean' },
    { label: 'Latitude', key: 'latitude' },
    { label: 'Longitude', key: 'longitude' },
    { label: 'Última Lat. Conhecida', key: 'last_known_latitude' },
    { label: 'Última Long. Conhecida', key: 'last_known_longitude' },
    { label: 'Fonte Geolocalização', key: 'geolocation_source' },
    { label: 'Precisão Localização', key: 'location_accuracy' },
    { label: 'Deslocamento (m)', key: 'displacement_meter' },
    { label: 'Ligado', key: 'is_power_on', type: 'boolean' },
    { label: 'Último Evento Saúde', key: 'latest_health_record_event_time', type: 'date' },
    { label: 'Nível Bateria', key: 'battery_level' },
    { label: 'Status Bateria', key: 'battery_status' },
    { label: 'Planograma', key: 'planogram' },
    { label: 'BD Responsável', key: 'responsible_bd_username' },
    { label: 'Nome BD Responsável', key: 'responsible_bd_first_name' },
    { label: 'Telefone BD', key: 'responsible_bd_phone_number' },
    { label: 'Solução IoT', key: 'iot_solution' },
    { label: 'Tem SIM', key: 'has_sim', type: 'boolean' },
    { label: 'Asset Associado em', key: 'asset_associated_on', type: 'date' },
    { label: 'Gateway Associado em', key: 'gateway_associated_on', type: 'date' },
    { label: 'Data Aquisição', key: 'acquisition_date', type: 'date' },
    { label: 'Associado por (username)', key: 'associated_by_bd_user_name' },
    { label: 'Associado por (nome)', key: 'associated_by_bd_name' },
    { label: 'Gateway Assoc. por (username)', key: 'gateway_associated_by_bd_user_name' },
    { label: 'Gateway Assoc. por (nome)', key: 'gateway_associated_by_bd_name' },
    { label: 'Fuso Horário', key: 'time_zone' },
    { label: 'Tipo Capacidade', key: 'capacity_type' },
    { label: 'Sub Cliente', key: 'sub_client' },
    { label: 'Último Evento Movimento', key: 'latest_movement_record_event_time', type: 'date' },
    { label: 'Último Evento Power', key: 'latest_power_record_event_time', type: 'date' },
    { label: 'Status Localização', key: 'location_status' },
    { label: 'Último Status Local em', key: 'last_location_status_on', type: 'date' },
    { label: 'Último Status Local (recente)', key: 'latest_location_status_on', type: 'date' },
    { label: 'Latitude Estática', key: 'static_latitude' },
    { label: 'Longitude Estática', key: 'static_longitude' },
    { label: 'Status Movimento Estático', key: 'static_movement_status' },
    { label: 'Criado em', key: 'created_on', type: 'date' },
    { label: 'Criado por', key: 'created_by' },
    { label: 'Modificado em', key: 'modified_on', type: 'date' },
    { label: 'Modificado por', key: 'modified_by' }
];

// Form fields configuration
const formFields = {
    text: ['asset_type', 'bottler_equipment_number', 'technical_id', 'oem_serial_number', 'category',
           'outlet', 'outlet_code', 'outlet_type', 'store_location', 'client', 'trade_channel',
           'country', 'sales_organization', 'sales_office', 'city', 'state', 'time_zone', 'sub_client'],
    number: ['latitude', 'longitude'],
    boolean: ['is_competition', 'is_factory_asset', 'is_smart', 'is_vision', 'prime_position']
};

// Initialize manager with assets-specific settings
const manager = new CRUDManager({
    baseUrl: '/portal_associacao/assets',
    entityName: 'Asset',
    entityNamePlural: 'assets',
    fields: formFields
});
window.manager = manager;

// Build a table row element for an asset result
function buildAssetRow(asset) {
    const row = document.createElement('tr');
    const encodedSerial = encodeURIComponent(asset.oem_serial_number || '');
    const isSmart = Boolean(asset.is_smart);
    const smartBadgeClass = isSmart ? 'badge-success' : 'badge-secondary';
    const smartLabel = isSmart ? 'Smart' : 'Normal';

    row.innerHTML = `
        <td><span class="badge badge-secondary">${asset.oem_serial_number || '-'}</span></td>
        <td style="font-weight: 500;">${asset.bottler_equipment_number || '-'}</td>
        <td>${asset.asset_type || '-'}</td>
        <td>${asset.outlet || '-'}</td>
        <td><span class="badge ${smartBadgeClass}">${smartLabel}</span></td>
        <td>
            <div style="display: flex; gap: 0.5rem; justify-content: center;">
                <button class="btn btn-info btn-icon-sm" data-serial="${encodedSerial}" onclick="manager.showViewDetails(decodeURIComponent(this.dataset.serial), viewFields)" title="Visualizar">
                    <i class="fas fa-eye"></i>
                </button>
                <button class="btn btn-warning btn-icon-sm" data-serial="${encodedSerial}" onclick="manager.showEditForm(decodeURIComponent(this.dataset.serial))" title="Editar">
                    <i class="fas fa-edit"></i>
                </button>
                <button class="btn btn-danger btn-icon-sm" data-serial="${encodedSerial}" onclick="manager.deleteEntity(decodeURIComponent(this.dataset.serial))" title="Deletar">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        </td>
    `;

    return row;
}

let assetSearchTimeout;

function reloadAssets() {
    // Reload all assets without page refresh
    fetch(`${manager.baseUrl}`)
        .then(response => response.text())
        .then(html => {
            const parser = new DOMParser();
            const doc = parser.parseFromString(html, 'text/html');
            const newTbody = doc.querySelector('table tbody');
            const currentTbody = document.querySelector('table tbody');
            
            if (newTbody && currentTbody) {
                currentTbody.innerHTML = newTbody.innerHTML;
            }
        })
        .catch(error => {
            console.error('Erro ao recarregar assets:', error);
            location.reload(); // Fallback to full reload if fetch fails
        });
}

function filterAssets(event) {
    clearTimeout(assetSearchTimeout);
    
    const searchInput = document.getElementById('searchBar');
    if (!searchInput) return;
    
    const searchValue = searchInput.value.trim().toLowerCase();

    if (!searchValue) {
        // Reload data without reloading page to maintain focus
        reloadAssets();
        return;
    }

    // Set a small delay to debounce
    assetSearchTimeout = setTimeout(() => {
        fetch(`${manager.baseUrl}/search?q=${encodeURIComponent(searchValue)}`)
            .then(response => {
                if (response.status === 204) {
                    return [];
                }
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                const tbody = document.querySelector('table tbody');
                if (!tbody) {
                    return;
                }

                tbody.innerHTML = '';

                if (!Array.isArray(data) || data.length === 0) {
                    const colspan = document.querySelectorAll('table thead th').length;
                    tbody.innerHTML = `<tr><td colspan="${colspan}" style="text-align: center; padding: 2rem; color: var(--color-gray-500);">Nenhum asset encontrado.</td></tr>`;
                    return;
                }

                data.forEach(asset => {
                    tbody.appendChild(buildAssetRow(asset));
                });
            })
            .catch(error => {
                console.error('Erro na busca:', error);
                alert('Erro ao realizar busca.');
            });
    }, 500);
}
