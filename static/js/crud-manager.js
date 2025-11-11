/**
 * CRUD Manager - Generic JavaScript for CRUD operations
 * Portal Associação Viva 2025
 */

class CRUDManager {
    constructor(config) {
        this.entityName = config.entityName; // e.g., 'outlet', 'asset', 'smartdevice'
        this.entityNamePlural = config.entityNamePlural; // e.g., 'outlets', 'assets'
        this.fields = config.fields || {}; // Field configuration
        this.baseUrl = config.baseUrl || `/portal_associacao/${this.entityNamePlural}`;
    }

    // Modal Management
    showViewModal() {
        document.getElementById('viewModal').style.display = 'flex';
        document.getElementById('viewModalBackdrop').style.display = 'block';
        document.body.style.overflow = 'hidden';
    }

    closeViewModal() {
        document.getElementById('viewModal').style.display = 'none';
        document.getElementById('viewModalBackdrop').style.display = 'none';
        document.body.style.overflow = 'auto';
    }

    showFormModal() {
        document.getElementById('formModal').style.display = 'flex';
        document.getElementById('formModalBackdrop').style.display = 'block';
        document.body.style.overflow = 'hidden';
    }

    closeFormModal() {
        document.getElementById('formModal').style.display = 'none';
        document.getElementById('formModalBackdrop').style.display = 'none';
        document.body.style.overflow = 'auto';
    }

    // Create Form
    showCreateForm() {
        document.getElementById('formTitle').textContent = `Criar Novo ${this.entityName}`;
        const form = document.getElementById('entityForm');
        form.reset();
        form.action = this.baseUrl;
        form.method = 'POST';

        // Reset advanced fields if they exist
        const advancedFields = document.getElementById('advancedFields');
        if (advancedFields) {
            advancedFields.style.display = 'none';
            const icon = document.getElementById('advancedToggleIcon');
            if (icon) icon.className = 'fas fa-chevron-down';
        }

        this.showFormModal();
    }

    // Edit Form
    showEditForm(entityId) {
        fetch(`${this.baseUrl}/${entityId}`)
            .then(response => response.json())
            .then(data => {
                document.getElementById('formTitle').textContent = `Editar ${this.entityName}`;

                const form = document.getElementById('entityForm');

                // Fill all form fields automatically
                Object.keys(data).forEach(key => {
                    const element = document.getElementById(key);
                    if (element) {
                        if (element.type === 'checkbox') {
                            element.checked = data[key] || false;
                        } else if (element.type === 'number') {
                            element.value = data[key] !== null && data[key] !== undefined ? data[key] : '';
                        } else if (element.type === 'datetime-local') {
                            // Convert ISO datetime to datetime-local format (YYYY-MM-DDTHH:MM)
                            if (data[key]) {
                                const date = new Date(data[key]);
                                const year = date.getFullYear();
                                const month = String(date.getMonth() + 1).padStart(2, '0');
                                const day = String(date.getDate()).padStart(2, '0');
                                const hours = String(date.getHours()).padStart(2, '0');
                                const minutes = String(date.getMinutes()).padStart(2, '0');
                                element.value = `${year}-${month}-${day}T${hours}:${minutes}`;
                            } else {
                                element.value = '';
                            }
                        } else {
                            element.value = data[key] || '';
                        }
                    }
                });

                form.action = `${this.baseUrl}/${entityId}`;
                form.method = 'POST';
                this.showFormModal();
            })
            .catch(error => {
                alert(`Erro ao carregar dados do ${this.entityName}.`);
                console.error(error);
            });
    }

    // View Details
    showViewDetails(entityId, fieldsConfig) {
        fetch(`${this.baseUrl}/${entityId}`)
            .then(response => response.json())
            .then(data => {
                let html = '<div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 1.5rem;">';

                fieldsConfig.forEach(field => {
                    let value = data[field.key];

                    // Format value based on type
                    if (field.type === 'boolean') {
                        value = value ? 'Sim' : 'Não';
                    } else if (field.type === 'date' && value) {
                        value = new Date(value).toLocaleString('pt-BR');
                    } else if (field.type === 'percent' && value) {
                        value = `${value}%`;
                    }

                    html += `
                        <div>
                            <div style="font-size: 0.75rem; font-weight: 600; color: var(--color-gray-500); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.25rem;">${field.label}</div>
                            <div style="font-size: 0.9375rem; color: var(--color-gray-900);">${value || '-'}</div>
                        </div>
                    `;
                });

                html += '</div>';
                document.getElementById('viewContent').innerHTML = html;
                this.showViewModal();
            })
            .catch(error => {
                alert(`Erro ao carregar detalhes do ${this.entityName}.`);
                console.error(error);
            });
    }

    // Submit Form
    submitForm() {
        const form = document.getElementById('entityForm');
        const action = form.action;
        const entityId = action.split('/').pop();

        if (action.includes(`/${this.entityNamePlural}/`) && entityId !== this.entityNamePlural) {
            this.updateEntity(entityId);
        } else {
            this.createEntity();
        }
    }

    createEntity() {
        const form = document.getElementById('entityForm');
        form.action = this.baseUrl;
        form.method = 'POST';
        form.submit();
    }

    updateEntity(entityId) {
        const form = document.getElementById('entityForm');

        let methodInput = form.querySelector('input[name="_method"]');
        if (!methodInput) {
            methodInput = document.createElement('input');
            methodInput.type = 'hidden';
            methodInput.name = '_method';
            form.appendChild(methodInput);
        }
        methodInput.value = 'PUT';

        form.action = `${this.baseUrl}/${entityId}`;
        form.method = 'POST';
        form.submit();
    }

    // Delete Entity
    deleteEntity(entityId) {
        if (confirm(`Tem certeza que deseja deletar este ${this.entityName}?`)) {
            const form = document.createElement('form');
            form.method = 'POST';
            form.action = `${this.baseUrl}/${entityId}`;

            const methodInput = document.createElement('input');
            methodInput.type = 'hidden';
            methodInput.name = '_method';
            methodInput.value = 'DELETE';
            form.appendChild(methodInput);

            document.body.appendChild(form);
            form.submit();
        }
    }

    // Search/Filter
    filterEntities(searchInput, tableRowBuilder) {
        if (!searchInput.trim()) {
            location.reload();
            return;
        }

        const normalizedTerm = searchInput.trim();
        this._activeSearchTerm = normalizedTerm;

        const table = document.querySelector('table');
        const tbody = table?.querySelector('tbody');
        const totalColumns = table ? table.querySelectorAll('thead th').length : 0;

        if (tbody) {
            const colspan = totalColumns || 1;
            tbody.innerHTML = `
                <tr>
                    <td colspan="${colspan}" style="text-align: center; padding: 1.5rem; color: var(--color-gray-500);">
                        Buscando...
                    </td>
                </tr>
            `;
        }

        fetch(`${this.baseUrl}/search?q=${encodeURIComponent(normalizedTerm)}`)
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
                if (this._activeSearchTerm !== normalizedTerm) {
                    return;
                }

                if (!tbody) {
                    return;
                }

                tbody.innerHTML = '';

                if (data.length === 0) {
                    const colspan = totalColumns || document.querySelectorAll('table thead th').length;
                    tbody.innerHTML = `<tr><td colspan="${colspan}" style="text-align: center; padding: 2rem; color: var(--color-gray-500);">Nenhum registro encontrado.</td></tr>`;
                    return;
                }

                data.forEach(item => {
                    const row = tableRowBuilder(item);
                    tbody.appendChild(row);
                });
            })
            .catch(error => {
                console.error('Erro na busca:', error);
                alert('Erro ao realizar busca.');
            });
    }

    // Toggle Advanced Fields
    toggleAdvancedFields() {
        const fields = document.getElementById('advancedFields');
        const icon = document.getElementById('advancedToggleIcon');
        if (fields && icon) {
            if (fields.style.display === 'none') {
                fields.style.display = 'block';
                icon.className = 'fas fa-chevron-up';
            } else {
                fields.style.display = 'none';
                icon.className = 'fas fa-chevron-down';
            }
        }
    }
}

// Export for global use
window.CRUDManager = CRUDManager;
