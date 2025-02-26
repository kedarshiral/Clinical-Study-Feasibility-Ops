var zs = (function (zs) {
	'use strict';

	zs.wizard = {

		addNavigationListeners: function () {

			// Add listener to previous button
			this.prevButton = this.querySelector('[prevButton]');
			if (this.prevButton) {
				this.prevButton.addEventListener('click', this.handlePrevAction.bind(this));
			}

			// Add listener to next button
			this.nextButton = this.querySelector('[nextButton]');
			if (this.nextButton) {
				this.nextButton.addEventListener('click', this.handleNextAction.bind(this));
			}

		},

		handlePrevAction: function () {
			var activeChevron = this.querySelector('.chevron[active]');
			var prevChevron = activeChevron.previousElementSibling;
			if (prevChevron) {
				this.navigateTo(prevChevron);
			}

		},

		handleNextAction: function () {
			var activeChevron = this.querySelector('.chevron[active]');
			var nextChevron = activeChevron.nextElementSibling;
			if (nextChevron) {
				this.navigateTo(nextChevron);
			}
		},

		/**
		 * Overridden method from zs.tabsContainerElement
		 */
		render: function () {
			this.renderTabsContainer();
			this.addNavigationListeners();
			this.classList.add('zs-wizard');
		},

		observedAttributes: ['clickable'],

		events: {
			attributeChange: function (e) {
				var attributeName = e.detail.attributeName;
				if (attributeName == 'clickable') {
					this.clickable = (this.getAttribute('clickable') !== null);
				}
			},

			click: function (e) {
				if (this.isClickable) {
					var element = e.target;
					this.navigateTo(this.closestParent('.chevron', element));
				}
			}
		},

		properties: {
			isClickable: {
				set: function (newValue) {
					if (newValue) {
						this.setAttribute('clickable', '');
					} else {
						this.removeAttribute('clickable');
					}
				},
				get: function () {
					return this.hasAttribute('clickable');
				}
			}
		}

	};


	zs.wizardElement = zs.customElement(zs.tabsContainerElement, 'zs-wizard', null, [zs.domHelper, zs.wizard]);

	return zs;
})(window.zs || {});