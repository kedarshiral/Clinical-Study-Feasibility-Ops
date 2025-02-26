var zs = (function (zs) {
	'use strict';

	zs.myComp = {
		debounceDelay: 0,
		blockAttachRender: false,
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
			console.log('render called');
		},
		observedAttributes: ['myattr1', 'myattr2', 'myattr3'],
		events: {
			create: function () {
				console.log('created');
			},
			attach: function () {
				console.log('attached');
				if (this.blockAttachRender) { this.blockAttachRender = false; return; }
				this.waitAndRender();
			},
			attributeChange: function (event) {
				console.log('attributeChanged');
				this.waitAndRender();
			}
		}
	}

	zs.myCompEle = zs.customElement(HTMLElement, 'my-comp', null, [zs.myComp]);

	return zs;
})(window.zs || {});
