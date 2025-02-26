var zs = (function (zs) {
	'use strict';

	zs.tab = {

		anchorEle: null,

		render: function () {
			if (!this.anchorEle) {
				this.anchorEle = this.querySelector('a');

				if (!this.anchorEle) {
					this.anchorEle = document.createElement('a');
					this.appendChild(this.anchorEle);
				}
			}
		},

		observedAttributes: ['tab-id', 'active'],

		events: {
			attributeChange: function (e) {
				var attributeName = e.detail.attributeName;
				if (attributeName == 'tab-id') {
					this.tabId = this.getAttribute('tab-id');
				}
				if (attributeName == 'active') {
					this.isActive = (this.getAttribute('active') !== null);
				}
			},
			attach: function (e) {
				this.render();
			}
		},

		properties: {
			tabId: {
				set: function (newValue) {
					if (newValue != this.getAttribute('tab-id')) {
						this.setAttribute('tab-id', newValue);
					}

					this._tabId = newValue;
				},
				get: function () {
					return this._tabId == null ? '' : this._tabId;
				}
			},
			isActive: {
				set: function (newValue) {
					if (newValue) {
						this.setAttribute('active', '');
					} else {
						this.removeAttribute('active');
					}
				},
				get: function () {
					return this.hasAttribute('active');
				}
			}
		}

	}

	zs.tabElement = zs.customElement(HTMLLIElement, 'zs-tab', 'li', [zs.tab]);



	zs.tabsContainer = {

		tabsContainer: null,

		renderTabsContainer: function () {
			if (!this.tabsContainer) {
				this.tabsContainer = this.querySelector('ul');

				if (!this.tabsContainer) {
					this.tabsContainer = document.createElement('ul');
					this.appendChild(this.tabsContainer);
				}
			}
		},

		render: function () {
			this.renderTabsContainer();
			this.classList.add('zs-tabs');
		},

		navigateTo: function (tabElement) {

			if (tabElement instanceof zs.tabElement) {
				// Show active tab
				var activeTab = this.tabsContainer.querySelector('[active]');
				if (activeTab) {
					activeTab.isActive = false;
				}
				tabElement.isActive = true;

				// Show corresponding panel
				var panels = this.querySelectorAll('[source-id]');
				for (var i = 0; i < panels.length; i++) {
					var panel = panels[i];

					// The panel should be immediate child of the component. It could return multiple panels in case of nested tabs.
					if (panel.parentElement == this) {
						if (panel.getAttribute('source-id') == tabElement.tabId) {
							panel.style.display = 'block';
						} else {
							panel.style.display = 'none';
						}
					}
				}

			}
		},

		events: {
			attach: function (e) {
				this.render();
			},

			click: function (e) {
				this.navigateTo(e.target.parentElement);
			}
		}

	}

	zs.tabsContainerElement = zs.customElement(HTMLElement, 'zs-tabs-container', null, [zs.tabsContainer]);

	return zs;
})(window.zs || {});
