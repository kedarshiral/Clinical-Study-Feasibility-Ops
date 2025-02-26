// require zs.customElements, zs.validation, zs.clear
var zs = (function (zs) {
	'use strict';	
	
	zs.field = {
		_name: null,
		_value: null,
		fieldElement: null,
		labelElement: null,
		ignoreChange: false,
		fieldContainer: null,
		isRendered: false,
		debounceDelay: 0, //  undefined => no debounce; >=0 => via zs.debounce
		renderTimer: null,
		blockAttrRender: false, // Blocks render on attribute change one time
		blockAttrRenderAlways: false, // Blocks render on attribute change permanently
		blockAttachRender: false, // Blocks render on attach one time
		blockAttachRenderAlways: false, // Blocks render on attach permanently
	    setValue: function (str) {
	    	var newValue;
	    	if (str == null) { // Null or undefined should be converted to null.
	    		newValue = null; 
	    	} else {
	    		newValue = str + '';
	    	}

	    	// Sync with attribure
	    	if (newValue != this.getAttribute('value')) {
	    		this.setAttribute('value', newValue);
	    	}

	    	this._value = newValue;
	    },
	    getValue: function () {
	    	if (this._value == null) { return '';}
	    	return this._value;
	    },
		getName: function () {
			return this._name || '';
		},
		setName: function (str) {
			var newValue
			if (str == null) { // Null or undefined should be converted to null.
				newValue = null;
			} else {
				newValue = str;
			}

			// Sync with attribure
			if (newValue != this.getAttribute('name')) {
				this.setAttribute('name', newValue);
			}

			this._name = newValue;
		},		
		clearField: function() {
			while (this.firstChild) {
				this.removeChild(this.firstChild);
			}
		},
		whenRendered: function () {
			// Use this method to detect and trigger browser to finish rendering of our component
			var comp = this;
			if (!this.fieldElement) { return new Promise(function (resolve, reject) { resolve(); });}
			return new Promise(function (resolve, reject) {
				resolve(comp.fieldElement.offsetHeight + 0);
			});
			comp.isRendered = true;
		},

		renderLabel: function () {
			this.labelElement = this.querySelector('label');
			if (!this.labelElement && this.getAttribute('label')) {
				this.labelElement = document.createElement('label');
				this.labelElement.innerText = this.getAttribute('label');
				this.appendChild(this.labelElement);
			} else if (this.getAttribute('label')) {
				this.labelElement.innerText = this.getAttribute('label');
			}
		},
		wrapField: function (fieldElement) {
			this.fieldContainer = this.querySelector('span');
			
			if (this.fieldContainer) {
				this.fieldContainer.appendChild(fieldElement);
				return this.fieldContainer;
			} else {
				this.fieldContainer = document.createElement('span');				
				this.fieldContainer.appendChild(fieldElement);
				return this.fieldContainer;
			}
		},
		renderField: function () {	
			if (!this.fieldElement && this.getAttribute('type')) { 
				this.fieldElement = document.createElement('input');
				this.fieldElement.setAttribute('type', this.getAttribute('type'));

				// Events
				//zs.pipeEvent('change', this.fieldElement, this); // Change event bubbles. We don't have to propogate is manually.
				zs.pipeEvent('blur', this.fieldElement, this); // Blur doesn't bubble
				zs.pipeEvent('focus', this.fieldElement, this); // Focus doesn't bubble

				this.fieldElement.setAttribute('value', this.getValue());
				this.appendChild(this.wrapField(this.fieldElement));
				
			} else if (this.fieldElement && this.getAttribute('type')) {
				this.fieldElement.setAttribute('value', this.getValue());
			}
		},
		decorate: function() {
			this.classList.add('zs-field');
			if (this.fieldContainer) {
				this.fieldContainer.style.position = 'relative';
			}

			// Transfer placeholder
			if (this.fieldElement && this.getAttribute('placeholder')) {
				this.fieldElement.setAttribute('placeholder', this.getAttribute('placeholder'));
			}
		},
		waitAndRender: function (debounceDelay) {			
			var comp = this;
			debounceDelay = debounceDelay || this.debounceDelay;
			// Debounce
			if (!this.renderWaitId && debounceDelay == null) { // No debounce
				this.render();
			} else if (!this.renderWaitId) {  // Debouncing

				// Set the function
				this.renderDebounced = zs.debounce(function () {
					comp.render();
					comp.renderWaitId = null;
				}, debounceDelay);

				// Remember the ID in case we want to cancel it
				this.renderWaitId = this.renderDebounced();
			}
		},
		render: function () {
			//console.error('render field');
			if (this.blockRender) {this.blockRender =  false; return;}
			var comp = this;
			if (this.renderWaitId) {
				clearTimeout(this.renderWaitId);
				this.renderWaitId = null;
			}
	
			// Init
			var viewMode = comp.getAttribute('view');

			// Label
			comp.renderLabel();

			// Input
			comp.renderField();

			// Decorate field
			comp.decorate();

			comp.whenRendered().then(function () {
				comp.dispatchEvent(new CustomEvent('render'));
			});

			comp.isRendered = true;
		},
		update: function() {

		},
		observedAttributes: ['name', 'value', 'view', 'type', 'label'],
		events: {
			render: function() {
				this.isRendered = true;	
			},
			attach: function() {
				if (this.blockAttachRenderAlways) {return;}
				if (this.blockAttachRender) { this.blockAttachRender = false; return;}
				if(this.innerHTML != ''){
					this.innerHTML = '';
				}
				
				this.render();
			},
			create: function () {			
				if (this.getAttribute('value') != this.value) {
					this.value = this.getAttribute('value');
				}
				if (this.getAttribute('name') != this.name) {
					this.name = this.getAttribute('name');
				}				
			},
			change: function (e) {
				var comp = this;
				if (e.detail && typeof e.detail.newValue != 'undefined') {
					this.value = e.detail.newValue; 
				} else {
				    this.value = this.fieldElement!=null ? this.fieldElement.value : "";
				}
			},
			attributeChange: function(e) {
				var attributeName = e.detail.attributeName;			
				if (attributeName == 'name' && this.getAttribute('name') != this.name) {
					this.name = this.getAttribute('name');
				}
				if (attributeName == 'value' && this.getAttribute('value') != this.value) {
					this.value = this.getAttribute('value');
				}
				if (attributeName == 'view' || attributeName == 'type' || attributeName == 'value' || attributeName == 'label') {
					if (!this.isRendered) {return;}
					if (this.blockAttrRender) {this.blockAttrRender = false; return;}
					if (this.blockAttrRenderAlways) {return;}
					this.waitAndRender();
				}
			}
		},
		properties: {
			value: {
				set: function (newValue) {
					this.setValue(newValue);					
				},
				get: function () {
					return this.getValue();
				}
			},
			name: {
				set: function (newValue) {
					this.setName(newValue);
				},
				get: function () {
					return this.getName();
				}
			}
		}
	}
	
	zs.fieldElement =  zs.customElement(HTMLParagraphElement, 'zs-field', 'p', [zs.validation, zs.clear, zs.field]);

	return zs;
})(window.zs || {});