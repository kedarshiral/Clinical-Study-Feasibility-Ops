var zs = (function (zs) {
	'use strict';
	zs.clear = {
		clearIcon: null,
		clearInput: null,
		clearIconSelector: '.zs-icon.zs-icon-remove',
		clearIconDisplayStyle: null,
		clearIconDefaultDisplay: 'inline-block',
		triggerClearIconBound: null,
		showClearIcon: function () {
			this.clearIcon.style.display = this.clearIconDisplayStyle;
		},
		hideClearIcon: function () {
			this.clearIcon.style.display = 'none';
		},
		toogleClearIcon: function()  {
			if (this.clearIcon.style.display == 'none') {
				this.showClearIcon();
			} else {
				this.hideClearIcon();
			}
		},
		triggerClearIcon: function() {
			if (this.clearInput.value) {
				this.showClearIcon();
			} else {
				this.hideClearIcon();
			}
		},
		clearIconClick: function (event) {
			this.clearInput.value = '';			
			this.hideClearIcon();
			var event = new CustomEvent('clear', { bubbles: false });
			this.dispatchEvent(event);
		},
		renderClearIcon: function () {
			var comp = this;
			var container = this.fieldContainer || this;
			this.clearIcon = document.createElement('a');
			this.clearIcon.classList.add('zs-icon');
			this.clearIcon.classList.add('zs-icon-remove');
			this.clearIcon.setAttribute('clear', '');
			this.clearIcon.setAttribute('href','javascript:void(0)');
			container.classList.add('zs-input-icon');
			Object.assign(this.clearIcon.style, {
				position: 'absolute',			
				zIndex: 2, // TODO: not reliable
			});
			this.clearIcon.addEventListener('click',function (event) {
				comp.clearIconClick();
			});
			
			container.appendChild(this.clearIcon);

		},
		removeClear: function () {
			// Can't remove the class because it might be needed for other behaviors like date field.
			//var container = this.fieldContainer || this;
			//container.classList.remove('zs-input-icon');


			if (this.triggerClearIconBound) {
				this.clearInput.removeEventListener('propertychange', this.triggerClearIconBound);
				this.clearInput.removeEventListener('change', this.triggerClearIconBound);
				this.clearInput.removeEventListener('click', this.triggerClearIconBound);
				this.clearInput.removeEventListener('keyup', this.triggerClearIconBound);
				this.clearInput.removeEventListener('input', this.triggerClearIconBound);
				this.clearInput.removeEventListener('paste', this.triggerClearIconBound);
			}
			if (this.clearIcon && this.clearIcon.parentNode) {
				this.clearIcon.parentNode.removeChild(this.clearIcon);
			}
			this.clearIcon = null;
			this.clearInput = null;
		},
		useClear: function () {
			var comp = this;
			var addClear = this.getAttribute('clear') != null;
			if (!addClear) { this.removeClear(); return;}

			this.clearInput = this.querySelector('input');
			if (!this.clearInput) { return; } // can't work without an input field

			// Render icon
			if (!this.clearIcon) { this.clearIcon = this.querySelector(this.clearIconSelector); }			
			if (!this.clearIcon) {
				this.renderClearIcon();
				// Remember initial display type
				this.clearIconDisplayStyle = this.clearIcon.style.display || this.clearIconDefaultDisplay;
			}

			// Listen to input events
			this.triggerClearIconBound = this.triggerClearIcon.bind(this);
			this.clearInput.addEventListener('propertychange', this.triggerClearIconBound);
			this.clearInput.addEventListener('change', this.triggerClearIconBound);
			this.clearInput.addEventListener('click', this.triggerClearIconBound);
			this.clearInput.addEventListener('keyup', this.triggerClearIconBound);
			this.clearInput.addEventListener('input', this.triggerClearIconBound);
			this.clearInput.addEventListener('paste', this.triggerClearIconBound);

			// Show icon
			this.triggerClearIcon();
		},
		observedAttributes: ['clear'],
		events: {
			change: function () {
				var comp = this;

				// Yeld to other change handlers first.
				setTimeout(function () {
					if (comp.getAttribute('clear') != null) {
						//console.log('clear', 'change', comp.value + '');
						comp.triggerClearIcon();
					}
				});
			},
			clear: function() {
				var event = new CustomEvent('change', { bubbles: true});
				this.fieldElement.dispatchEvent(event);
			},
			render: function () {
				this.useClear();
			},
			attributeChange: function (event) {
				var attributeName = event.detail.attributeName;
				if (attributeName == 'clear') {
					this.useClear();
				}			
			}
		}
	};
	return zs;
})(window.zs || {});