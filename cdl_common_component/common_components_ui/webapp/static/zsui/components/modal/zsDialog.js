var zs = (function (zs) {
	'use strict';
	zs.dialog = {
		isConfigured: false,
		removePlugin: function() {
			$(this).zsModalDialog('remove');
			this.isConfigured = false;
		},
		configurePlugin: function (afterConfigure) {
			var comp = this;

			var configuration = {
				autoOpen: this.getAttribute('open') != null,
				submitSelector: '>footer button[submit]',
				events: {
					open: function () {
						comp.syncAttribute(true);
					},
					close: function () {
						comp.syncAttribute(false);
					},
					submit: function(e) {
						if (typeof(comp.handleSubmitAction) === 'function') {
							comp.handleSubmitAction.call(comp);
							e.preventDefault();
						} else {
							comp.close();
						}
					}
				}
			};

			$(function () {
				$(comp).zsModalDialog(configuration);
				comp.isConfigured = true;
				if (typeof afterConfigure == 'function') {
					afterConfigure.apply(comp);
				}
				var event = new CustomEvent('configure');
				comp.dispatchEvent(event);
			});
		},
		syncAttribute: function (open) {
			// Sync attribute value
			this.ignoreChange = true;
			if (open) {
				this.setAttribute('open', '');
			} else {
				this.removeAttribute('open');
			}
		},
		open: function () {
			if (!this.isConfigured) {
				this.configurePlugin(function() {
					$(this).zsModalDialog('open');
				});
			} else {
				$(this).zsModalDialog('open');
			}
		},
		close: function () {
			if (!this.isConfigured) {
				this.configurePlugin(function () {
					$(this).zsModalDialog('close');
				});
			} else {
				$(this).zsModalDialog('close');
			}			
		},
		events: {
			attach: function () {
				this.configurePlugin();
			},
			detach: function() {
				this.removePlugin();
			},
			create: function () {
				console.log('zs-dialog', 'created');
			},
			attributeChange: function (event) {
				if (this.ignoreChange) { this.ignoreChange = false;return;}
				var attributeName = event.detail.attributeName;
				if (attributeName == 'open') {
					this.open();
				}
			}
		}
	};

	zs.dialogElement = zs.customElement(HTMLElement, 'zs-dialog', null, [zs.dialog]);

	return zs;
})(window.zs || {});