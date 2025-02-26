var zs = (function(zs) {
	'use strict';
	zs.loading = {
		observedAttributes: ['loading'],
		loadingTimer1: null,
		loadingTimer2: null,
		loadingWait1: 200,
		loadingWait2: null,
		loadingOverlay: null,
		loadingShow: function (wait1, wait2) {
			var self = this;
			wait1 = wait1 || this.loadingWait1;
			wait2 = wait2 || this.loadingWait2;

			if (wait1 != null) {
				this.loadingTimer1 = setTimeout(this.loadingShowSpinner.bind(this), wait1);
			}

			if (wait2 != null) {
				this.loadingTimer2 = setTimeout(this.loadingShowGlobalSpinner.bind(this), wait2);
			}

		},
		loadingShowSpinner: function() {
			if (this.loadingTarget) {
				this.loadingTarget.classList.add('zs-loading');
			} else {
				this.classList.add('zs-loading');
			}
		},
		loadingShowGlobalSpinner: function () {
			
			if (!this.loadingOverlay) {
				var overlays = document.querySelectorAll('body>.zs-overlay.zs-loading');	
				if (!overlays.length) {
					this.loadingOverlay = document.createElement('div');
					this.loadingOverlay.setAttribute('class', 'zs-overlay zs-loading');
					document.body.appendChild(this.loadingOverlay);
				} else {
					this.loadingOverlay = overlays[0];
				}
			}
			this.loadingHideSpinner();
			this.loadingOverlay.style.display="block";		
			
		},

		loadingHideSpinner: function () {
			this.classList.remove('zs-loading');
			var elements = this.querySelectorAll('.zs-loading');	
			for (var i =0 ; i<elements.length; i++) {
				elements[i].classList.remove('zs-loading');
			}
			if (this.loadingOverlay) {
				this.loadingOverlay.style.display="none";
			}				
		},



		loadingClearTimers: function() {
			if (this.loadingTimer1) {
				clearTimeout(this.loadingTimer1);
				this.loadingTimer1 = null;
			}
			if (this.loadingTimer2) {
				clearTimeout(this.loadingTimer2);
				this.loadingTimer2 = null;
			}		
		},

		loadingSet: function(value, target) {
			this.loadingTarget = target;
			this.isLoading = value;
		},
		loadingChange: function(value) {
			if (value) {
				this.loadingClearTimers();
				this.loadingHideSpinner();								
				this.loadingShow();
			} else {
				this.loadingClearTimers();
				this.loadingHideSpinner();
			}
		},
		events: {  
			attach: function(){
				var value = (this.getAttribute('loading') == 'on');
				this.loadingTarget = this;
				if (value) {
					this.loadingChange(value);
				}
			},
			attributeChange: function(e){				
				if (e.detail.attributeName == 'loading') {
					var value = (e.target.getAttribute('loading') == 'on');
					this.loadingChange(value);
				}

			}
		}
	};
	return zs;
})(window.zs || {});