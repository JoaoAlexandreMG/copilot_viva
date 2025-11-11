// View fields configuration for Outlets
const viewFields = [
    { label: 'Código', key: 'code' },
    { label: 'Nome', key: 'name' },
    { label: 'Tipo', key: 'outlet_type' },
    { label: 'Key Outlet', key: 'is_key_outlet', type: 'boolean' },
    { label: 'Smart', key: 'is_smart', type: 'boolean' },
    { label: 'Ativo', key: 'is_active', type: 'boolean' },
    { label: 'País', key: 'country' },
    { label: 'Estado', key: 'state' },
    { label: 'Cidade', key: 'city' },
    { label: 'Rua', key: 'street' },
    { label: 'Endereço 2', key: 'address_2' },
    { label: 'Endereço 3', key: 'address_3' },
    { label: 'Endereço 4', key: 'address_4' },
    { label: 'CEP', key: 'postal_code' },
    { label: 'Latitude', key: 'latitude' },
    { label: 'Longitude', key: 'longitude' },
    { label: 'Varejista', key: 'retailer' },
    { label: 'Telefone', key: 'primary_phone' },
    { label: 'Email', key: 'email' },
    { label: 'Celular', key: 'mobile_phone' },
    { label: 'Rep. Vendas', key: 'primary_sales_rep' },
    { label: 'Nome Rep. Vendas', key: 'sales_rep_name' },
    { label: 'Técnico', key: 'technician' },
    { label: 'Mercado', key: 'market' },
    { label: 'Meta Vendas', key: 'sales_target' },
    { label: 'Cliente', key: 'client' },
    { label: 'Canal Comercial', key: 'trade_channel' },
    { label: 'Grupo Comercial', key: 'trade_group' },
    { label: 'Código Grupo', key: 'trade_group_code' },
    { label: 'Tier Cliente', key: 'customer_tier' },
    { label: 'Sub Canal', key: 'sub_trade_channel' },
    { label: 'Org. Vendas', key: 'sales_organization' },
    { label: 'Escritório', key: 'sales_office' },
    { label: 'Grupo Vendas', key: 'sales_group' },
    { label: 'Terr. Vendas', key: 'sales_territory' },
    { label: 'Terr. Teleselling', key: 'teleselling_territory_name' },
    { label: 'BD', key: 'bd_territory_name' },
    { label: 'CA', key: 'ca_territory_name' },
    { label: 'MC', key: 'mc_territory_name' },
    { label: 'P1', key: 'p1_territory_name' },
    { label: 'P2', key: 'p2_territory_name' },
    { label: 'P3', key: 'p3_territory_name' },
    { label: 'P4', key: 'p4_territory_name' },
    { label: 'P5', key: 'p5_territory_name' },
    { label: 'Rota Reserva', key: 'reserve_route_name' },
    { label: 'Cliente RD', key: 'rd_customer_name' },
    { label: 'Fuso Horário', key: 'time_zone' },
    { label: 'Sub Cliente', key: 'sub_client' },
    { label: 'Cluster', key: 'cluster' },
    { label: 'Segmento Mercado', key: 'market_segment' },
    { label: 'Segmento', key: 'segment' },
    { label: 'Ambiente', key: 'environment' },
    { label: 'Sortido 1', key: 'assortment_1' },
    { label: 'Sortido 2', key: 'assortment_2' },
    { label: 'Sortido 3', key: 'assortment_3' },
    { label: 'Sortido 4', key: 'assortment_4' },
    { label: 'Sortido 5', key: 'assortment_5' },
    { label: 'Código Barras', key: 'barcode' },
    { label: 'Cluster Local', key: 'local_cluster' },
    { label: 'Canal Local', key: 'local_trade_channel' },
    { label: 'Cadeia', key: 'chain' },
    { label: 'Região', key: 'region_name' },
    { label: 'CPL', key: 'cpl_name' },
    { label: 'BDAA', key: 'bdaa' },
    { label: 'ITVLF', key: 'itvlf' },
    { label: 'Cap. Ativo', key: 'combined_asset_capacity' },
    { label: 'ASM', key: 'asm_name' },
    { label: 'Email ASM', key: 'asm_email' },
    { label: 'TSM', key: 'tsm_name' },
    { label: 'Email TSM', key: 'tsm_email' },
    { label: 'Campo Extra', key: 'extra_field' },
    { label: 'Criado em', key: 'created_on' },
    { label: 'Criado por', key: 'created_by' },
    { label: 'Modificado em', key: 'modified_on' },
    { label: 'Modificado por', key: 'modified_by' }
];

// Form fields configuration
const formFields = {
    text: ['name', 'code', 'outlet_type', 'country', 'state', 'city', 'street', 'postal_code',
           'retailer', 'primary_phone', 'client', 'trade_channel', 'sales_organization',
           'sales_office'],
    number: ['latitude', 'longitude'],
    boolean: ['is_key_outlet', 'is_smart', 'is_active']
};

// Initialize manager with outlets-specific settings
const manager = new CRUDManager({
    baseUrl: '/portal_associacao/outlets',
    entityName: 'Outlet',
    entityNamePlural: 'outlets',
    fields: formFields
});

window.manager = manager;

function buildOutletRow(outlet) {
    const row = document.createElement('tr');
    const encodedCode = encodeURIComponent(outlet.code || '');

    row.innerHTML = `
        <td><span class="badge badge-secondary">${outlet.code || '-'}</span></td>
        <td style="font-weight: 500;">${outlet.name || '-'}</td>
        <td>${outlet.outlet_type || '-'}</td>
        <td>${outlet.country || '-'}</td>
        <td>${outlet.client || '-'}</td>
        <td>
            <div style="display: flex; gap: 0.5rem; justify-content: center;">
                <button class="btn btn-info btn-icon-sm" data-code="${encodedCode}" onclick="manager.showViewDetails(decodeURIComponent(this.dataset.code), viewFields)" title="Visualizar">
                    <i class="fas fa-eye"></i>
                </button>
                <button class="btn btn-warning btn-icon-sm" data-code="${encodedCode}" onclick="manager.showEditForm(decodeURIComponent(this.dataset.code))" title="Editar">
                    <i class="fas fa-edit"></i>
                </button>
                <button class="btn btn-danger btn-icon-sm" data-code="${encodedCode}" onclick="manager.deleteEntity(decodeURIComponent(this.dataset.code))" title="Deletar">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        </td>
    `;

    return row;
}

let outletSearchTimeout;

function filterOutlets(event) {
    clearTimeout(outletSearchTimeout);
    
    const searchInput = document.getElementById('searchBar');
    if (!searchInput) return;
    
    const searchValue = searchInput.value.trim().toLowerCase();

    if (!searchValue) {
        location.reload();
        return;
    }

    // Set a small delay to debounce
    outletSearchTimeout = setTimeout(() => {
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
                    tbody.innerHTML = `<tr><td colspan="${colspan}" style="text-align: center; padding: 2rem; color: var(--color-gray-500);">Nenhum outlet encontrado.</td></tr>`;
                    return;
                }

                data.forEach(outlet => {
                    tbody.appendChild(buildOutletRow(outlet));
                });
            })
            .catch(error => {
                console.error('Erro na busca:', error);
                alert('Erro ao buscar outlets.');
            });
    }, 500);
}
