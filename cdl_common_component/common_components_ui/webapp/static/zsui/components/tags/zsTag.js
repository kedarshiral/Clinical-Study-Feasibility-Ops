var zs = (function (zs) {
	'use strict';

	zs.tag = {
		debounceDelay: 0,
		setValue: function (str) {
			if (str != this.getAttribute('value')) {
				this.setAttribute('value', str);
			}

			this._value = str;
		},
		getValue: function () {
			return this._value == null ? '' : this._value;
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

			if (this.dismissable && !this.dismissEle) {
				this.dismissEle = document.createElement('a');
				this.dismissEle.classList.add('zs-icon');
				this.dismissEle.classList.add('zs-icon-remove');
				this.appendChild(this.dismissEle);

				this.dismissEle.addEventListener('click', this.dismiss.bind(this));
			} else if (!this.dismissable && this.dismissEle) {
				this.removeChild(this.dismissEle);
				this.dismissEle = null;
			}

			this.classList.add('zs-tag');
		},
		dismiss: function () {
			// Provide a hook for the consumer to perform any action before the tag element is removed.
			if (typeof this.handleDismiss == 'function') {
				this.handleDismiss.call(this);
			}
			this.parentElement.removeChild(this);
		},
		observedAttributes: ['value', 'dismissable', 'nonclickable'],
		events: {
			attributeChange: function (e) {
				var attributeName = e.detail.attributeName;
				if (attributeName == 'value') {
					this.value = this.getAttribute('value');
				}
				if (attributeName == 'dismissable') {
					this.dismissable = (this.getAttribute('dismissable') !== null);
				}
				if (attributeName == 'nonclickable') {
					this.nonclickable = (this.getAttribute('nonclickable') !== null);
				}
				this.waitAndRender();
			},
			attach: function () {
				this.waitAndRender();
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
			dismissable: {
				set: function (newValue) {
					if (newValue) {
						this.setAttribute('dismissable', '');
					} else {
						this.removeAttribute('dismissable');
					}
				},
				get: function () {
					return this.hasAttribute('dismissable');
				}
			},
			nonclickable: {
				set: function (newValue) {
					if (newValue) {
						this.setAttribute('nonclickable', '');
					} else {
						this.removeAttribute('nonclickable');
					}
				},
				get: function () {
					return this.hasAttribute('nonclickable');
				}
			}
		}
	}

	zs.tagElement = zs.customElement(HTMLSpanElement, 'zs-tag', 'span', [zs.tag]);

	return zs;
})(window.zs || {});