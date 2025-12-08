// View fields configuration for Smart Devices
const viewFields = [
    { label: 'MAC Address', key: 'mac_address' },
    { label: 'Tipo Dispositivo', key: 'device_type' },
    { label: 'Fabricante', key: 'manufacturer' },
    { label: 'MAC Address', key: 'mac_address' },
    { label: 'Serial', key: 'serial_number' },
    { label: 'Serial Ordem', key: 'order_serial_number' },
    { label: 'País Envio', key: 'shipped_country' },
    { label: 'Número Porta', key: 'door_no' },
    { label: 'Número Equipamento', key: 'bottler_equipment_number' },
    { label: 'ID Técnico', key: 'technical_identification_number' },
    { label: 'Gateway', key: 'gateway' },
    { label: 'Serial Fabricante', key: 'manufacturer_serial_number' },
    { label: 'IMEI', key: 'imei' },
    { label: 'SIM', key: 'sim_number' },
    { label: 'Provedor SIM', key: 'sim_provider' },
    { label: 'Plugin FFXY', key: 'plugin_connected_ffxy' },
    { label: 'Último Ping', key: 'last_ping', type: 'date' },
    { label: 'Firmware', key: 'firmware_version' },
    { label: 'iBeacon UUID', key: 'ibeacon_uuid' },
    { label: 'iBeacon Major', key: 'ibeacon_major' },
    { label: 'iBeacon Minor', key: 'ibeacon_minor' },
    { label: 'Eddystone UID Namespace', key: 'eddystone_uid_namespace' },
    { label: 'Eddystone UID Instance', key: 'eddystone_uid_instance' },
    { label: 'Local Inventário', key: 'inventory_location' },
    { label: 'Número Rastreamento', key: 'tracking_number' },
    { label: 'Cliente', key: 'client' },
    { label: 'Tipo Asset', key: 'asset_type' },
    { label: 'Asset Vinculado', key: 'linked_with_asset' },
    { label: 'Factory Asset', key: 'is_factory_asset', type: 'boolean' },
    { label: 'Associado em Factory', key: 'associated_in_factory', type: 'boolean' },
    { label: 'Data Aquisição', key: 'acquisition_date', type: 'date' },
    { label: 'Asset Associado em', key: 'asset_associated_on', type: 'date' },
    { label: 'Associação', key: 'association' },
    { label: 'Associado por (username)', key: 'associated_by_bd_user_name' },
    { label: 'Associado por (nome)', key: 'associated_by_bd_name' },
    { label: 'Versão App Assoc.', key: 'associated_by_app_version' },
    { label: 'Nome App Assoc.', key: 'associated_by_app_name' },
    { label: 'Desaparecido', key: 'is_missing', type: 'boolean' },
    { label: 'Outlet', key: 'outlet' },
    { label: 'Código Outlet', key: 'outlet_code' },
    { label: 'Tipo Outlet', key: 'outlet_type' },
    { label: 'Canal', key: 'trade_channel' },
    { label: 'Tier Cliente', key: 'customer_tier' },
    { label: 'Sub Canal', key: 'sub_trade_channel' },
    { label: 'Org. Vendas', key: 'sales_organization' },
    { label: 'Escritório', key: 'sales_office' },
    { label: 'Grupo Vendas', key: 'sales_group' },
    { label: 'Terr. Vendas', key: 'sales_territory' },
    { label: 'Rua', key: 'street' },
    { label: 'Cidade', key: 'city' },
    { label: 'Estado', key: 'state' },
    { label: 'País', key: 'country' },
    { label: 'Fuso Horário', key: 'time_zone' },
    { label: 'Último Evento Saúde', key: 'latest_health_record_event_time', type: 'date' },
    { label: 'Nível Bateria', key: 'battery_level' },
    { label: 'URL Anúncio', key: 'advertisement_url' },
    { label: 'Registrado IoT Hub', key: 'is_device_registered_in_iot_hub', type: 'boolean' },
    { label: 'É Gateway', key: 'is_sd_gateway', type: 'boolean' },
    { label: 'Sub Cliente', key: 'sub_client' },
    { label: 'Modelo Dispositivo', key: 'device_model_number' },
    { label: 'Tipo Módulo', key: 'module_type' },
    { label: 'Status SIM', key: 'sim_status' },
    { label: 'Última Atualização SIM', key: 'last_sim_status_updated_on', type: 'date' },
    { label: 'Criado em', key: 'created_on', type: 'date' },
    { label: 'Criado por', key: 'created_by' },
    { label: 'Modificado em', key: 'modified_on', type: 'date' },
    { label: 'Modificado por', key: 'modified_by' }
];

// Form fields configuration
const formFields = {
    text: ['device_type', 'manufacturer', 'mac_address', 'serial_number', 'order_serial_number',
           'shipped_country', 'bottler_equipment_number', 'outlet', 'outlet_code', 'gateway',
           'imei', 'sim_number', 'sim_provider', 'linked_with_asset', 'client', 'country',
           'manufacturer_serial_number', 'firmware_version', 'trade_channel', 'city', 'state',
           'time_zone', 'sub_client'],
    number: ['battery_level'],
    boolean: ['is_factory_asset', 'associated_in_factory', 'is_missing',
              'is_device_registered_in_iot_hub', 'is_sd_gateway']
};

// Initialize manager with smartdevices-specific settings
const manager = new CRUDManager({
    baseUrl: '/portal_associacao/smartdevices',
    entityName: 'Smart Device',
    entityNamePlural: 'smartdevices',
    fields: formFields
});
window.manager = manager;

function buildSmartDeviceRow(device) {
    const row = document.createElement('tr');
    const encodedMac = encodeURIComponent(device.mac_address || '');

    row.innerHTML = `
        <td><span class="badge badge-secondary">${device.mac_address || '-'}</span></td>
        <td style="font-weight: 500;">${device.order_serial_number || device.serial_number || '-'}</td>
        <td>${device.linked_with_asset || '-'}</td>
        <td>${device.outlet || '-'}</td>
        <td>${device.city || '-'}</td>
        <td>${device.last_ping || '-'}</td>
        <td>
            <div style="display: flex; gap: 0.5rem; justify-content: center;">
                <button class="btn btn-info btn-icon-sm" data-mac="${encodedMac}" onclick="manager.showViewDetails(decodeURIComponent(this.dataset.mac), viewFields)" title="Visualizar">
                    <i class="fas fa-eye"></i>
                </button>
                <button class="btn btn-warning btn-icon-sm" data-mac="${encodedMac}" onclick="manager.showEditForm(decodeURIComponent(this.dataset.mac))" title="Editar">
                    <i class="fas fa-edit"></i>
                </button>
                <button class="btn btn-danger btn-icon-sm" data-mac="${encodedMac}" onclick="manager.deleteEntity(decodeURIComponent(this.dataset.mac))" title="Deletar">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        </td>
    `;

    return row;
}

let smartDeviceSearchTimeout;

function reloadSmartDevices() {
    // Reload all smart devices without page refresh
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
            console.error('Erro ao recarregar dispositivos:', error);
            location.reload(); // Fallback to full reload if fetch fails
        });
}

function filterSmartDevices(event) {
    clearTimeout(smartDeviceSearchTimeout);
    
    const searchInput = document.getElementById('searchBar');
    if (!searchInput) return;
    
    const searchValue = searchInput.value.trim().toLowerCase();

    if (!searchValue) {
        // Reload data without reloading page to maintain focus
        reloadSmartDevices();
        return;
    }

    // Set a small delay to debounce
    smartDeviceSearchTimeout = setTimeout(() => {
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
                    tbody.innerHTML = `<tr><td colspan="${colspan}" style="text-align: center; padding: 2rem; color: var(--color-gray-500);">Nenhum dispositivo encontrado.</td></tr>`;
                    return;
                }

                data.forEach(device => {
                    tbody.appendChild(buildSmartDeviceRow(device));
                });
            })
            .catch(error => {
                console.error('Erro na busca:', error);
                alert('Erro ao buscar dispositivos.');
            });
    }, 500);
}

// Debounce search
document.addEventListener('DOMContentLoaded', () => {
    const searchInput = document.getElementById('searchBar');
    if (!searchInput) {
        return;
    }

    searchInput.addEventListener('input', (event) => {
        clearTimeout(smartDeviceSearchTimeout);

        if (!event.target.value.trim()) {
            reloadSmartDevices();
            return;
        }

        smartDeviceSearchTimeout = setTimeout(() => filterSmartDevices(event), 500);
    });
});

// MAC uniqueness/format inline validation on blur to avoid form submission and modal closure
document.addEventListener('DOMContentLoaded', () => {
    const macField = document.getElementById('mac_address');
    if (!macField) return;

    const macPattern = /^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$/;

    macField.addEventListener('blur', async (e) => {
        const value = e.target.value.trim().toUpperCase();
        // Clear previous errors if any
        manager.clearFormError();
        if (!value) return;

        if (!macPattern.test(value)) {
            manager.showFormError("O 'MAC Address' deve estar no formato XX:XX:XX:XX:XX:XX ou XX-XX-XX-XX-XX-XX.");
            macField.classList.add('is-invalid');
            return;
        }

        // Check uniqueness
        try {
            const form = document.getElementById('entityForm');
            const action = form ? (form.action || '') : '';
            const cleanAction = action.endsWith('/') ? action.slice(0, -1) : action;
            const entityId = cleanAction.split('/').pop();

            // If editing and mac equals entityId (existing), treat as fine
            if (action.includes(`/${manager.entityNamePlural}/`) && entityId && decodeURIComponent(entityId).toUpperCase() === value) {
                macField.classList.remove('is-invalid');
                manager.clearFormError();
                return;
            }

            const resp = await fetch(`${manager.baseUrl}/${encodeURIComponent(value)}`);
            if (resp.ok) {
                manager.showFormError("Já existe um dispositivo com este 'MAC Address'.");
                macField.classList.add('is-invalid');
            } else {
                macField.classList.remove('is-invalid');
                manager.clearFormError();
            }
        } catch (err) {
            console.error('Erro ao verificar unicidade do MAC', err);
        }
    });

    macField.addEventListener('input', () => {
        manager.clearFormError();
    });
});

// Helper to format MAC Address input
function formatMAC(input) {
    let value = input.value.replace(/[^0-9A-Fa-f]/g, ''); // Remove non-hex chars
    
    // Limit to 12 hex characters
    if (value.length > 12) {
        value = value.substring(0, 12);
    }
    
    // Add colons
    let formatted = '';
    for (let i = 0; i < value.length; i++) {
        if (i > 0 && i % 2 === 0) {
            formatted += ':';
        }
        formatted += value[i];
    }
    
    input.value = formatted.toUpperCase();
}
