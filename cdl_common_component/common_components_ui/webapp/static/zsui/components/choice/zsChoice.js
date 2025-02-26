/**
 * @module Choice component
 * @required zs.customElements, zs.field
 * @member ZSUI library
 */
var zs = (function (zs) {
	'use strict';

	/**
	 * Choice behavior 
	 * @namespace
	 */
	zs.choice = {

		/**
		 * Delimeter for labels and values
		 * @type {string}
		 */
		delimeter: ',',

		/**
		 * Labels for options
		 * @type {array}
		 * @readonly
		 */
		_labels: null,


		/**
		 * Values for options
		 * @type {array}
		 * @readonly
		 */
		_values: null,

		/**
		 * Display radio button option
		 * @param value {string} value for the option
		 * @param label {string} lable of the option
		 * @param selected {boolean} selected
		 */
		renderRadio: function (value, label, selected) {
			var comp = this;
			var option = this.querySelector('span[name="' + value + '"]');
			if (!option) {
				option = document.createElement('span');
				option.setAttribute('class', 'zs-checkbox' + (selected && ' zs-active' || ''));
				option.setAttribute('name', value);
				option.innerHTML = '<input type="radio" name="' + comp.getAttribute('name') + '" value="' + value + '" ' + (selected && ' checked' || '') + '/><span>' + label + '</span>';
				this.appendChild(option);
				option.firstChild.addEventListener('click', function (e) {
					e.preventDefault();
					e.stopPropagation();
					var e = new CustomEvent('click');
					this.parentNode.dispatchEvent(e);
				});
				option.addEventListener('click', function (e) {
					var value = this.firstChild.getAttribute('value');
					var newValue = value;
					// In order to comply with zsField behavior we need to dispatch event instead of just changing a value comp.value
					var event = new CustomEvent('change', { detail: { newValue: newValue } });
					comp.dispatchEvent(event);
				});
			} else {
				option.firstChild.setAttribute('name', comp.getAttribute('name'));
				option.firstChild.setAttribute('value', value);
				option.firstChild.checked = selected;
				if (!selected) {
					option.setAttribute('class', 'zs-checkbox');
				} else {
					option.setAttribute('class', 'zs-checkbox zs-active');
				}
				option.querySelector('span').innerHTML = label;
			}
		},

		/**
		 * Display checkbox option
		 * @param value {string} value for the option
		 * @param label {string} lable of the option
		 * @param selected {boolean} selected
		 */
		renderCheckbox: function (value, label, selected) {
			console.log('render checkbox', value, label, selected);
			var comp = this;
			var option = this.querySelector('span[name="' + value + '"]');
			if (!option) {
				option = document.createElement('span');
				option.setAttribute('class', 'zs-checkbox' + (selected && ' zs-active' || ''));
				option.setAttribute('name', value);
				option.innerHTML = '<input type="checkbox" name="' + value + '"' + (selected && ' checked' || '') + ' /><span>' + label + '</span>';
				this.appendChild(option);
				option.addEventListener('click', function (e) {
					if (e.target == this.firstChild) { return; } // if clicked on the checkbox
					var checked = this.firstChild.checked;
					this.firstChild.checked = !this.firstChild.checked;
					var event = new CustomEvent('change');
					this.dispatchEvent(event);
				});
				option.addEventListener('change', function (e) {
					var value = this.firstChild.getAttribute('name');
					var checked = this.firstChild.checked;
					var valueArray = comp.value && comp.value.split(comp.delimeter) || [];
					console.log('option change', this, value, checked);
					if (!checked) { // uncheck the value
						var index = valueArray.indexOf(value);
						if (index >= 0) {
							valueArray.splice(index, 1)
						}
					} else {
						valueArray.push(value);
					}
					var newValue = valueArray.join(comp.delimeter);
					e.stopPropagation();

					// In order to comply with zsField behavior we need to dispatch event instead of just changing a value comp.value
					var event = new CustomEvent('change', { detail: { newValue: newValue } });
					comp.dispatchEvent(event);
				});
			} else {
				option.firstChild.setAttribute('name', value);
				option.firstChild.checked = selected;
				if (!selected) {
					option.setAttribute('class', 'zs-checkbox');
				} else {
					option.setAttribute('class', 'zs-checkbox zs-active');
				}
				option.querySelector('span').innerHTML = label;
			}
		},
		
		/**
		 * Display field element
		 * @override 
		 */
		renderField: function () {
			console.log('render field', this.getAttribute('value'));
			if (this.values && this.labels && this.values.length != this.labels.length) {
				console.warn('Lables should match values');
			}
			var valueArray = this.value.split(this.delimeter);
			var type = this.getAttribute('type');
			if (type != 'radio' && type != 'checkbox') { type = 'checkbox'; }
			for (var i = 0; i < this.values.length; i++) {
				var selected = valueArray.indexOf(this.values[i]) >= 0;
				if (type == 'checkbox') {
					this.renderCheckbox(this.values[i], this.labels[i], selected);
				} else {
					this.renderRadio(this.values[i], this.labels[i], selected);
				}
			}
		},


		events: {
			attributeChange: function (e) {
				var attributeName = e.detail.attributeName;
				if (attributeName == 'labels') {
					this.labels = this.getAttribute('labels');
				}
				if (attributeName == 'values') {
					this.values = this.getAttribute('values');
				}
				if (attributeName == 'limit') {
					this.limit = this.getAttribute('limit');
				}
			}
		},
		observedAttributes: ['labels', 'values', 'limit'],
		properties: {
			labels: {
				set: function (str) {
					var newValue;
					if (str == null) {
						newValue = str;
					} else {
						newValue = str + '';
					}

					// Sync with attribure
					if (newValue != this.getAttribute('labels')) {
						this.setAttribute('labels', newValue);
					}

					this._labels = newValue.split(this.delimeter);
				},
				get: function () {
					if (this._labels == null) { return ''; }
					return this._labels;
				}
			},
			values: {
				set: function (str) {
					var newValue;
					if (str == null) {
						newValue = str;
					} else {
						newValue = str + '';
					}

					// Sync with attribure
					if (newValue != this.getAttribute('values')) {
						this.setAttribute('values', newValue);
					}
					this._values = newValue.split(this.delimeter);
				},
				get: function () {
					if (this._values == null) { return ''; }
					return this._values;
				}
			}
		}
	}

	/**
	 * @description Choice component
	 */
	zs.choiceElement = zs.customElement(HTMLElement, 'zs-choice', null, [zs.loading, zs.field, zs.choice]);

	return zs;
})(window.zs || {});