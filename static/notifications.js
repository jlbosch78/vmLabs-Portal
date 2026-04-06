// Sistema de notificaciones en tiempo real
class NotificationSystem {
    constructor() {
        this.container = null;
        this.socket = null;
        this.init();
    }

    init() {
        // Crear contenedor para toasts
        if (!document.getElementById('toast-container')) {
            this.container = document.createElement('div');
            this.container.id = 'toast-container';
            this.container.className = 'toast-container position-fixed bottom-0 end-0 p-3';
            this.container.style.zIndex = '1090';
            document.body.appendChild(this.container);
        } else {
            this.container = document.getElementById('toast-container');
        }
    }

    show(message, type = 'info', title = '', duration = 5000) {
        const toastId = 'toast-' + Date.now() + '-' + Math.random().toString(36).substr(2, 9);
        
        // Determinar ícono según tipo
        let icon = '';
        let bgClass = '';
        switch(type) {
            case 'success':
                icon = '<i class="bi bi-check-circle-fill text-success me-2"></i>';
                bgClass = 'border-success';
                break;
            case 'danger':
                icon = '<i class="bi bi-exclamation-triangle-fill text-danger me-2"></i>';
                bgClass = 'border-danger';
                break;
            case 'warning':
                icon = '<i class="bi bi-exclamation-triangle-fill text-warning me-2"></i>';
                bgClass = 'border-warning';
                break;
            case 'info':
            default:
                icon = '<i class="bi bi-info-circle-fill text-info me-2"></i>';
                bgClass = 'border-info';
                break;
        }

        // Crear HTML del toast
        const toastHtml = `
            <div id="${toastId}" class="toast bg-white shadow-lg border-0 border-start border-4 ${bgClass}" role="alert" aria-live="assertive" aria-atomic="true" data-bs-autohide="true" data-bs-delay="${duration}">
                <div class="toast-header bg-white border-0">
                    ${icon}
                    <strong class="me-auto">${title || this.getTitleByType(type)}</strong>
                    <small class="text-muted">ahora</small>
                    <button type="button" class="btn-close" data-bs-dismiss="toast" aria-label="Close"></button>
                </div>
                <div class="toast-body">
                    ${message}
                </div>
            </div>
        `;

        // Insertar en el DOM
        this.container.insertAdjacentHTML('beforeend', toastHtml);
        
        // Inicializar y mostrar toast
        const toastElement = document.getElementById(toastId);
        const toast = new bootstrap.Toast(toastElement, {
            autohide: true,
            delay: duration
        });
        toast.show();

        // Eliminar del DOM cuando se oculte
        toastElement.addEventListener('hidden.bs.toast', () => {
            toastElement.remove();
        });
    }

    getTitleByType(type) {
        const titles = {
            'success': 'Éxito',
            'danger': 'Error',
            'warning': 'Advertencia',
            'info': 'Información'
        };
        return titles[type] || 'Notificación';
    }

    success(message, title = 'Éxito', duration = 5000) {
        this.show(message, 'success', title, duration);
    }

    error(message, title = 'Error', duration = 8000) {
        this.show(message, 'danger', title, duration);
    }

    warning(message, title = 'Advertencia', duration = 6000) {
        this.show(message, 'warning', title, duration);
    }

    info(message, title = 'Información', duration = 4000) {
        this.show(message, 'info', title, duration);
    }
}

// Inicializar sistema de notificaciones global
window.notify = new NotificationSystem();

// Función para mostrar notificaciones desde cualquier lugar
window.showToast = function(message, type = 'info', title = '') {
    window.notify.show(message, type, title);
};
