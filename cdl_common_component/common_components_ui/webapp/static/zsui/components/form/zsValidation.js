var zs = (function(zs) {
	'use strict';
	zs.validation = {

    	isValid: true,
		_messages: null,  // Array of objects like {type: 'error', text: 'my error', element: myElement}
		_container: null,
		_messageContainer: null,		
		getMessages: function() {
			return this._messages;
		},
		showMessage: function (message) {
			if (!this._container) { this.addContainer();}
			this._messages.push(message);
			var messageElement = document.createElement('p');
			messageElement.setAttribute('class', 'zs-message zs-' + message.type);
			messageElement.setAttribute('title', message.text || '');
			messageElement.innerHTML= message.text || '';
			this._messageContainer.appendChild(messageElement);
			if (message.element) {
				message.element.classList.add('zs-' + message.type);
			} else {
				this.classList.add('zs-' + message.type);
			}
			if (message.type == 'error' || message.type == 'warning') {
				this.isValid = false;
			}
			
		},

		hideMessage: function() {
			if (!this._container) { this.addContainer(); }

			// Remove all messages.
			var element;
			while (element = this._messageContainer.querySelector('.zs-message')) {
				element.parentNode.removeChild(element);
			}

			// Remove classes from the field
			this._container.classList.remove('zs-error');
			this._container.classList.remove('zs-warning');
			this._container.classList.remove('zs-success');
			this._container.classList.remove('zs-info');
			var elements = this._container.querySelectorAll('.zs-error,.zs_warning,.zs-info,.zs-success');
			if (!elements) {return}
			for (var i = 0; i < elements.length; i++) {
				elements[i].classList.remove('zs-error');
				elements[i].classList.remove('zs-warning');
				elements[i].classList.remove('zs-success');
				elements[i].classList.remove('zs-info');
			}
		},
		clearMessages: function() {
			this._messages = [];
			this.hideMessage();
		},
		validate: function() {
			// to be overriden in the component
		},
		addContainer: function() {
			this._container = this._container || this;
			if (!this._container) { return; }
			this._messageContainer = this._messageContainer || this._container.querySelector('[message]') || this;
		},
		events: {
			create: function() {
				this._messages = [];
			}
		}
	};
	return zs;
})(window.zs || {});
