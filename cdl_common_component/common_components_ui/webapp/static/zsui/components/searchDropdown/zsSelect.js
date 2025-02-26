// require zs.customElements, zs.validation, zs.clear
var zs = (function (zs) {
	'use strict';	
	
	/**
	 * ZS Select component
	 */
	zs.select =  {

		/**
		 * Supported events
		 */
		events: {
			create: function (e) {},

			attach: function(e){
				
				if(!this.hasAttribute('searchableDropdown')){
					return;
				}
				var self = this;
				$(this).find('select').zsSearchDropdown({
					multiple: $(this).find('select').prop('multiple'),
					events:{
						/**
						 * @todo For perfomance purpose in future this should be moved to onRender or smth
						 */
						beforeOpen: function(){
							if(!self.hasAttribute('zs-clear')){
								return;
							}
							self.fieldContainer = self.querySelector('.zs-input-icon');
							self.fieldElement = self.querySelector('input');
							self.setAttribute('clear', '');
						}
					}
				});
			},

			attributeChange: function(e) {},

			detach: function(e){},
		}
	};


	zs.selectElement =  zs.customElement(HTMLElement, 'zs-select', 'div', [zs.validation, zs.clear, zs.select]);

	return zs;
})(window.zs || {});