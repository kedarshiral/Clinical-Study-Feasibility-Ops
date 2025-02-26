var zs = (function(zs) {
	'use strict';

	// zs form behavior
	zs.form = {
		getFields: function() {
			return this.querySelectorAll('input,button,select,textarea');
		},

		events: {	// Through events object we can attach hooks to lifecycle methods of an element and other custom or native events
			submit: function(e) {
				e.preventDefault();
			}
		}	
	}	
	
	// 				 zs.customElement(parentClass,		isWhat,		tag,	behaviors)
	zs.formElement = zs.customElement(HTMLFormElement, 'zs-form', 'form', [zs.form, zs.loading, zs.validation]);
	
	return zs;
})(window.zs || {});