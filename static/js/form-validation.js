/**
 * Form Validation Library
 * Provides validation for forms with required fields and email validation
 * Preserves user input data on validation errors
 */

const FormValidator = {
    /**
     * Configuração de campos obrigatórios por formulário
     */
    requiredFields: {
        users: ['first_name', 'last_name', 'user_name', 'upn', 'role'],
        outlets: ['code', 'name'],
        assets: ['oem_serial_number'],
        smartdevices: ['mac_address']
    },

    emailFields: {
        users: ['email'],
        outlets: ['email'],
        assets: [],
        smartdevices: []
    },

    /**
     * Valida um formulário antes do envio
     * @param {string} formType - Tipo do formulário (users, outlets, assets, smartdevices)
     * @param {HTMLFormElement} form - Elemento do formulário
     * @returns {boolean} - true se válido, false caso contrário
     */
    validate: function(formType, form) {
        // Limpar erros anteriores
        this.clearAllErrors(form);
        
        let isValid = true;
        const errors = [];
        const requiredFields = this.requiredFields[formType] || [];
        const emailFields = this.emailFields[formType] || [];

        // Validar campos obrigatórios
        for (const fieldName of requiredFields) {
            const field = form.querySelector(`[name="${fieldName}"]`);
            if (field && !this.isFieldValid(field)) {
                isValid = false;
                this.showFieldError(field, 'Este campo é obrigatório');
                errors.push(`${this.getFieldLabel(fieldName)}`);
            }
        }

        // Validar emails
        for (const fieldName of emailFields) {
            const field = form.querySelector(`[name="${fieldName}"]`);
            if (field && field.value.trim()) {
                // Se tem valor, validar formato
                if (!this.isValidEmail(field.value)) {
                    isValid = false;
                    this.showFieldError(field, 'Email inválido (use: user@example.com)');
                    errors.push(`${this.getFieldLabel(fieldName)} - formato inválido`);
                }
            }
        }

        // Se houver erros, mostrar mensagem
        if (!isValid) {
            this.showFormError(form, errors);
        }

        return isValid;
    },

    /**
     * Verifica se um campo está preenchido
     */
    isFieldValid: function(field) {
        const value = field.value.trim();
        return value.length > 0;
    },

    /**
     * Valida formato de email
     */
    isValidEmail: function(email) {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        return emailRegex.test(email);
    },

    /**
     * Mostra erro em um campo específico
     */
    showFieldError: function(field, message) {
        // Adicionar classe de erro
        field.classList.add('form-control-error');
        
        // Criar elemento de erro se não existir
        let errorElement = field.nextElementSibling;
        if (!errorElement || !errorElement.classList.contains('form-error-message')) {
            errorElement = document.createElement('div');
            errorElement.classList.add('form-error-message');
            field.parentNode.insertBefore(errorElement, field.nextSibling);
        }
        
        errorElement.textContent = message;
        errorElement.style.display = 'block';
    },

    /**
     * Remove erro de um campo
     */
    clearFieldError: function(field) {
        field.classList.remove('form-control-error');
        const errorElement = field.nextElementSibling;
        if (errorElement && errorElement.classList.contains('form-error-message')) {
            errorElement.style.display = 'none';
        }
    },

    /**
     * Remove todos os erros do formulário
     */
    clearAllErrors: function(form) {
        // Remover classes de erro
        form.querySelectorAll('.form-control-error').forEach(field => {
            field.classList.remove('form-control-error');
        });
        
        // Ocultar mensagens de erro
        form.querySelectorAll('.form-error-message').forEach(el => {
            el.style.display = 'none';
        });
        
        // Ocultar erro geral do formulário
        const formError = form.querySelector('.form-general-error');
        if (formError) {
            formError.style.display = 'none';
        }
    },

    /**
     * Mostra erro geral do formulário
     */
    showFormError: function(form, errors) {
        let errorContainer = form.querySelector('.form-general-error');
        
        if (!errorContainer) {
            errorContainer = document.createElement('div');
            errorContainer.classList.add('form-general-error');
            form.insertBefore(errorContainer, form.firstChild);
        }
        
        const errorList = errors.map(e => `• ${e}`).join('<br>');
        errorContainer.innerHTML = `
            <div class="alert alert-danger">
                <strong>Atenção!</strong> Verifique os campos obrigatórios:<br>
                ${errorList}
            </div>
        `;
        errorContainer.style.display = 'block';
        
        // Scroll para o topo do formulário
        form.scrollTop = 0;
    },

    /**
     * Obtém label amigável de um campo
     */
    getFieldLabel: function(fieldName) {
        const labels = {
            'first_name': 'Nome',
            'last_name': 'Sobrenome',
            'user_name': 'Nome de Usuário',
            'upn': 'UPN',
            'email': 'Email',
            'role': 'Função',
            'code': 'Código',
            'name': 'Nome',
            'oem_serial_number': 'Número de Série OEM',
            'mac_address': 'Endereço MAC'
        };
        return labels[fieldName] || fieldName;
    },

    /**
     * Configura listeners para limpar erros enquanto o usuário digita
     */
    setupLiveValidation: function(form, formType) {
        const requiredFields = this.requiredFields[formType] || [];
        const emailFields = this.emailFields[formType] || [];
        const allFields = [...new Set([...requiredFields, ...emailFields])];

        allFields.forEach(fieldName => {
            const field = form.querySelector(`[name="${fieldName}"]`);
            if (field) {
                field.addEventListener('blur', (e) => {
                    // Validar ao sair do campo
                    if (requiredFields.includes(fieldName)) {
                        if (!this.isFieldValid(e.target)) {
                            this.showFieldError(e.target, 'Este campo é obrigatório');
                        } else {
                            this.clearFieldError(e.target);
                        }
                    }

                    if (emailFields.includes(fieldName) && e.target.value.trim()) {
                        if (!this.isValidEmail(e.target.value)) {
                            this.showFieldError(e.target, 'Email inválido (use: user@example.com)');
                        } else {
                            this.clearFieldError(e.target);
                        }
                    }
                });

                // Limpar erro ao começar a digitar
                field.addEventListener('focus', (e) => {
                    this.clearFieldError(e.target);
                });
            }
        });
    }
};

// Exportar para uso global
window.FormValidator = FormValidator;
