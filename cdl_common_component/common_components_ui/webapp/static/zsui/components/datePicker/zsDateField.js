// require zsField, jquery, jquery.zsDatePicker

var zs = (function (zs) {
	'use strict';	
	
	zs.dateField = {
		fieldContainer: null,
		daysOfWeek: null,
		minDate: null,
		maxDate: null,
		months: null,
		mode: null,
		stringToDate: function (str) {
			return new Date(str);
		},
		dateToString: function (date) {
			if (!date) { return ''; }
			if (isNaN(date)) {return date;}
			return date.toLocaleDateString('en-US').replace(/[^ -~]/g, ''); 
		},		
		renderField: function () {
			var comp = this;
			var isEdit = this.getAttribute('view') == null;

			// Create field container
			if (!this.fieldContainer) { this.fieldContainer = document.createElement('span'); }

			// Create field element
			if (!this.fieldElement && isEdit) {

				this.fieldElement = document.createElement('input');					
				this.fieldElement.classList.add('zs-input');

				// Date picker component will not trigger change event on input.
				zs.pipeEvent('blur', this.fieldElement, this); // Blur doesn't bubble
				zs.pipeEvent('focus', this.fieldElement, this); // Focus doesn't bubble				

				//
				if (this.getAttribute('mode') == 'inline') {
					this.fieldElement.addEventListener('change', function (event) {
						event.stopPropagation();
						var stringValue = this.value;
						comp.changeDate(stringValue);
						
					})
				}
			}

			// Updating value
			if (isEdit) {
				this.fieldContainer.classList.add('zs-input-icon');
				this.fieldContainer.classList.add('zs-icon-calendar');
				
			} else {
				this.fieldContainer.classList.remove('zs-input-icon');
				this.fieldContainer.classList.remove('zs-icon-calendar');
				if (this.fieldElement) {
					$(this.fieldElement).zsDatePicker('destroy');
					if (this.fieldElement.parentNode) {
						this.fieldElement.parentNode.removeChild(this.fieldElement);
					}
					this.fieldElement = null;
				}
			}

			// Add to the field
			if (this.fieldElement) { this.fieldContainer.appendChild(this.fieldElement); }
			this.appendChild(this.fieldContainer);
			this.updateElements();
			this.configurePlugin();
		},
		changeDate: function(newDate) {
			var event = new CustomEvent('change', { bubbles: false, detail: { newValue: newDate } });
			this.dispatchEvent(event); // Make sure to dispatch event on the component level and not input field level to avoid endless loop.
		},
		updateElements: function () { // With new value
			var isEdit = this.getAttribute('view') == null;
			if (isEdit) {
				this.fieldElement.setAttribute('value', this.value);
				this.fieldElement.value = this.value;
				this.fieldElement.dateValue = this._dateValue;
			} else {
				this.fieldContainer.innerHTML =  this.value;
			}			
		},
		getValue: function () {
			if (this._value == null) { return '';}
			return this._value;
		},
		setValue: function (newValue) { // we use _value to store strings and _dateValue to store datetime object
			if (typeof newValue == 'string') {
				this._dateValue = newValue ? this.stringToDate(newValue) : null;
				this._value = newValue;
			} else if (newValue == null) { // Null or undefined
				this._dateValue = newValue;
				this._value = newValue;
			} else {
				this._dateValue = new Date(newValue);
				this._value = this.dateToString(this._dateValue);				
			}
			
			if (this._value != this.getAttribute('value')) {
				this.setAttribute('value', this._value);
			}
			
		},		
		removePlugin: function() {
			if ($ && $(this.fieldElement).data('zsDatePicker')) {
				$(this.fieldElement).zsDatePicker('remove');
			};
		},
		configurePlugin: function () {
			var comp = this;

			// View mode. Don't have to use plugin
			if (this.getAttribute('view') != null) {
				this.removePlugin();
				return;
			};

			// Inline edit mode;
			if (this.getAttribute('mode') == 'inline') {
				this.removePlugin();
				return;		
			}

			// Initialize zsDatePicker jQuery plugin. 
			$(function () {

				// Prepare plugin configuration
				var cfg = {
					onChange: function () {
						comp.changeDate(this.value);
					}
				}
				if (comp.daysOfWeek) { cfg.daysOfWeek = comp.daysOfWeek }
				if (comp.minDate) { cfg.minDate = comp.minDate }
				if (comp.maxDate) { cfg.maxDate = comp.maxDate }
				if (comp.months) { cfg.months = comp.months }
				if (comp.stringToDate) { cfg.stringToDate = comp.stringToDate }
				if (comp.dateToString) { cfg.dateToString = comp.dateToString }

				$(comp.fieldElement).zsDatePicker(cfg);

                //Prevent change event from bubbling from month and year dropdowns. 
				comp.querySelector('select[name="month"]').addEventListener("change", function (e) { e.stopPropagation(); });
				comp.querySelector('select[name="year"]').addEventListener("change", function (e) { e.stopPropagation(); });

				var event = new CustomEvent('configure');
				comp.dispatchEvent(event);
			});
		},
		observedAttributes: ['view', 'mode', 'value'],
		events: {						
			change: function (e) {
				if (e.srcElement != this) { return;} // filter events changes bubbling from selects
				var comp = this;
				this.value = e.detail.newValue;
				this.updateElements();
			},
			clear: function () {
				var comp = this;
				setTimeout(function () {
					comp.changeDate(null);
				},0);
			},		
			detach: function () {
				this.removePlugin();
			},
			attributeChange: function (event) {
				var attributeName = event.detail.attributeName;				
				if ((attributeName == 'view' || attributeName == 'mode' || attributeName == 'value')) {
					this.waitAndRender();
				}
			}
		}
	}
	
	zs.dateFieldElement =  zs.customElement(HTMLParagraphElement, 'zs-date-field', 'p', [zs.field, zs.dateField]);

	return zs;
})(window.zs || {});

// ZSUI Calendar fixes
$(function () {
	if ($.fn.zsDatePicker) {// if we have it
		$.fn.zsDatePicker({
			getDate: function () {
				if (this.$input.dateValue) { // Avoiding stringToDate if we can work with dates directly
					this.value = this.$input.dateValue;
				} else {
					var str = this.$input.val();
					if (str) {
						this.value = this.stringToDate(str);
					} else {
						this.value = this.options.value || new Date();
					}
				}

				// Set selected year and month
				this.$container.find('select[name="month"]').val(this.value.getMonth());
				this.$container.find('select[name="year"]').val(this.value.getFullYear());

				if (this.displayDate != this.value) {
					this.buildCalendar(this.value);
				}
			}
		});
	}
})