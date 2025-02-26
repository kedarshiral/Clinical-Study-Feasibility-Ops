// require zs.customElements, zs.validation, zs.clear
var zs = (function (zs) {
	'use strict';	
	
	/**
	 * ZS Select component
	 * 
	 * @description Provides an interface for select element. Meets ZSUI standarts
	 * 
	 * @property {HTMLELement} overlay
	 * @property {HTMLELement} menu
	 * 
	 * @private {String} _value
	 * 
	 * @method setValue
	 * @method getValue
	 * 
	 * @event create
	 * @event change
	 * @event open
	 * @event close
	 * @event destroy
	 */
	zs.select =  {
		
		/**
		 * Ref to overlay element
		 * 
		 * @type {HTMLELement}
		 */
		overlay: null,
		
		/**
		 * Ref to menu element
		 * 
		 * @type {HTMLELement}
		 */
		menu: null,
		
		/**
		 * Value of select
		 * 
		 * @type {String}
		 */
		_value : '',

		/**
		 * Previous value
		 * 
		 * @type {String}
		 */
		prevValue : '',

		/**
		 * Set provided value.
		 * This method allows you to add additional validation rules to value
		 * 
		 * @chainable
		 * 
		 * @param {String} str
		 * 
		 * @returns {zsSelect}
		 */
		setValue: function (str) {
			this.prevValue = this._value; // save prev value

			this._value = str || '';

			this.dispatchEvent(new CustomEvent('change'));
			
			return this;
		},

		/**
		 * Return current value
		 * 
		 * @returns {String}
		 */
		getValue: function () {
			return this._value ? this._value +'' : '';
		},
	
		/**
		 * Render component
		 * 
		 * @chainable
		 * 
		 * @returns {zsSelect}
		 */
		render: function() {
			this
				.renderOverlay()
				.renderMenu();
			
			return this;
		},

		/**
		 * Render manu elm
		 * 
		 * @chainable
		 * 
		 * @returns {zsSelect}
		 */
		renderMenu: function(){			
			var options = this.querySelectorAll('option'),			
				nav 	= document.createElement('nav');

			for(var i = 0; i < options.length; i++){
				nav.appendChild(this.renderOption(options[i], i));
				options[i].remove(); // we don't need this items anymore
			}
									

			this.menu = document.createElement('menu');
			this.menu.appendChild(nav);
			
			this.appendChild(this.menu);

			this.close(null, true);

			return this;
		},

		renderOption: function(option, i){
			var a = document.createElement('a');

			a.innerText = option.innerText;
			a.setAttribute('href', 'javascript:;');
			a.setAttribute('value', option.value);

			a.addEventListener('click', this.optionClick.bind(this));

			return a;
		},

		/**
		 * Render overlay elm
		 * 
		 * @chainable
		 * 
		 * @returns {zsSelect}
		 */
		renderOverlay: function(){
			this.overlay = document.createElement('overlay');
			this.overlay.innerText = 'Select value';

			this.overlay.addEventListener('click', this.toggle.bind(this));

			this.appendChild(this.overlay);

			return this;
		},

		/**
		 * When option clicked - save new value
		 * 
		 * @chainable
		 * 
		 * @returns {zsSelect}
		 */
		optionClick: function(e){
			this.deselectAll();

			var option = e.currentTarget;

			option.setAttribute('active', '');

			this.setValue(option.getAttribute('value'));
			
			this.close();

			return this;
		},

		/**
		 * Deselect all selected items
		 * 
		 * @chainable
		 * 
		 * @returns {zsSelect}
		 */
		deselectAll: function(){
			var selected = this.querySelectorAll('nav [active]');
			for(var i = 0; i < selected.length; i++){
				selected[i].removeAttribute('active');
			}
			
			return this;
		},

		/**
		 * Toggle menu 
		 * 
		 * @chainable
		 * 
		 * @param {Event} e 		- Fired event
		 * @param {Boolean} silent 	- Should we trigger ecustom event or not
		 * 
		 * @returns {zsSelect}
		 */
		toggle: function(e, silent){
			if(!this.hasAttribute('opened')){
				return this.open(e, silent);
			}

			return this.close(e, silent);
		},

		/**
		 * Open menu 
		 * 
		 * @chainable
		 * 
		 * @param {Event} e 		- Fired event
		 * @param {Boolean} silent 	- Should we trigger ecustom event or not
		 * 
		 * @returns {zsSelect}
		 */
		open: function(e, silent){
			this.setAttribute('opened', '');
			
			if(silent){
				return this;
			}

			this.dispatchEvent(new CustomEvent('open'));
			
			return this;
		},

		/**
		 * Close menu 
		 * 
		 * @chainable
		 * 
		 * @param {Event} e 		- Fired event
		 * @param {Boolean} silent 	- Should we trigger ecustom event or not
		 * 
		 * @returns {zsSelect}
		 */
		close: function(e, silent){
			this.removeAttribute('opened');

			if(silent){
				return this;
			}

			this.dispatchEvent(new CustomEvent('close'));

			return this;
		},

		/**
		 * Supported events list
		 */
		events: {
			create: function (e) {
				this.render();
			},

			attach: function(e){
				console.log('on attach');
			},

			change: function (e) {
				console.log('changed');
			},

			attributeChange: function(e) {
				
			},

			render: function(e){

			},

			open: function(e){
				console.log('on open');
			},

			close: function(e){
				console.log('on close', this.getValue());
			},

			detach: function(e){
				console.log('on detach');
			},
		}
	};

	zs.selectAlwaysOpen = {};
	zs.selectMultiple = {};
	zs.selectSearchable = {};
	zs.selectWithCheckbox = {};


	zs.selectElement =  zs.customElement(HTMLOptionElement, 'zs-select', null, [zs.validation, zs.clear, zs.select]);

	return zs;
})(window.zs || {});