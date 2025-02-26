// TODO: ZSUI-547: Create zs-nav components and unify behaviors accross Tabs, Navs, Menus
var zs = (function (zs) {
	'use strict';
	
	/**
	 * Menu behaviors
	 */
	zs.menu = {
		
		/**
		 * Build a popup menu
		 */
		createMenu: function () {
			var elms = this.querySelectorAll('.zs-icon-collapse');
			for(var i=0;i<elms.length; i++) {
				this.subMenu(elms[i]);
			}
		},

		/**
		 * Create a sub menu based on provided icon 
		 */
		subMenu: function(icon) {
			var $icon = $(icon);
			var $li = $icon.parent().parent();
			var $nav = $li.find('>nav');
			var inProcess = false;
			var $a = $li.find('>a').first();
			var isDisabled = false;
	
			function clickAnywhere(event) {
				if (!inProcess && $nav[0].isExpanded &&
						$nav[0] != event.target &&
						!$.contains($li[0], event.target) &&
						!$.contains($nav[0], event.target)) {
					collapse();
				}
			}
	
	
			function disable() {
				//console.log('disable');
				if (isDisabled) { return; }
				isDisabled = true;
				setTimeout(function () {
					//console.log('enable');
					isDisabled = false;
				}, 500);
			}
	
			function collapse() {
				//console.log('collapse', $nav[0].isDelayingCollapse, $nav[0].isDelayingExpand);
				$nav.hide();
				$li.removeClass('zs-selected');
				$(document).off('click touchstart', clickAnywhere);
				$nav[0].isExpanded = false;
				$nav[0].isDelayingCollapse = false;
	
			}
			function expand() {
				//console.log('expand', $nav[0].isDelayingCollapse, $nav[0].isDelayingExpand);
	
				// Detect the offset			
				var rect = $li[0].getBoundingClientRect(), alignClass = '';
				if (window.innerWidth / 2 > (rect.left + rect.width/2)) {
					alignClass = 'zs-to-left';
				}
							
				inProcess = true;
				$li.addClass('zs-selected');
				if (alignClass) {
					$li.addClass('zs-to-left');
				} else {
					$li.removeClass('zs-to-left');
				}
				$(document).on('click touchstart', clickAnywhere);
				$nav.show();
				$nav[0].isExpanded = true;
				$nav[0].isDelayingExpand = false;
	
				setTimeout(function () {
					inProcess = false;
				});
	
			}
	
			function delayedExpand() {
				//console.log('delayedExpand', event.target, $nav[0].isExpanded, $nav[0].isDelayingCollapse, $nav[0].isDelayingExpand);				
				if ($nav[0].isDelayingCollapse) { $nav[0].isDelayingCollapse = false; }
				if ($nav[0].isExpanded) { return; }
				if ($nav[0].isDelayingExpand) { return; }
				$nav[0].isDelayingExpand = true;
				disable();
				setTimeout(function () {
					if (!$nav[0].isDelayingExpand) { return; } // Block expansion when is terminated by mouse out.
					expand();
				}, 300);
			}
	
			function delayedCollapse(event) {
				//console.log('delayedCollapse', event.target, $nav[0].isExpanded, $nav[0].isDelayingCollapse, $nav[0].isDelayingExpand);				
				if ($nav[0].isDelayingExpand) { $nav[0].isDelayingExpand = false; }
				if (!$nav[0].isExpanded) { return; }
				$nav[0].isDelayingCollapse = true;
				disable();
				setTimeout(function () {
					if (!$nav[0].isDelayingCollapse) { return; } // Block expansion when is terminated by mouse out.
					collapse();
					$nav[0].isDelayingCollapse = false;
				}, 300);
			}
	
			if ($nav.length) {
				// State of the submenu
				$nav[0].isDelayingExpand = false;
				$nav[0].isDelayingCollapse = false;
				$nav[0].isExpanded = false;
	
				$li.on('click',function (event) {
					event.stopPropagation();
	
					if (isDisabled) { return; }
					if ($nav[0].isExpanded) {
						
						collapse($nav);
					} else {
						expand($nav);
					}
					
				});
	
	
				$a.click(function (event) {
					event.preventDefault();
					//return false;
				});
	
				$li.on('mouseenter', delayedExpand);
				$li.on('mouseleave', delayedCollapse);
			}
		}
	}
	
	/**
	 * Top navigation component
	 */
	zs.topNavigation = {

		/**
		 * Holds a select element for compact navigation
		 * @type object
		 */
		select: null,

		isActive: function (menuItem) {
			if (menuItem.url.indexOf('#') != -1) { // index.html#mypage
				if (this.url.href.indexOf(menuItem.url) != -1 ) {
					return true;
				}
				return false;
			} else { //  host/path/index.html
				if (this.url.href.indexOf('/' + menuItem.url) != -1) {
					return true;
				}
				return false;
			}
		},

		renderMenu: function (menu) {
			var item = document.createElement('li');
			var a = document.createElement('a');
			a.setAttribute('href', menu.url);
			if (this.isActive(menu)) {
				item.setAttribute('class', 'zs-active');
			}
			a.innerHTML = menu.label;
			item.appendChild(a);
			
			// If we click on the <li> we want to navigate to the URL in the <a> 
			item.addEventListener('click', function(event) {
				if (event.target == this) {
					this.firstChild.click();
				}
			});

			if (menu.navigation) { // Sub menu
				var nav = document.createElement('nav');
				nav.setAttribute('class', 'zs-menu');
				nav.setAttribute('style', 'display:none;position:absolute');
				var icon = document.createElement('span');
				icon.setAttribute('class', 'zs-icon zs-icon-collapse');
				a.appendChild(icon);
				item.classList.add('zs-submenu');
				for (var i = 0;i<menu.navigation.length; i++) {
					nav.appendChild(this.renderMenu(menu.navigation[i]));
				}
				item.appendChild(nav);
			}
			return item;
		},

		renderCompactMenu: function (menu, container, prefix) {
			var option = document.createElement('option');
			if (this.isActive(menu)) {
				option.setAttribute('selected', 'selected');
			}
			option.innerHTML = (prefix && (prefix + ' ') || '') + menu.label;
			option.setAttribute('value', menu.url);
			container.appendChild(option);
			if (menu.navigation) {
				for (var i = 0; i<menu.navigation.length; i++) {
					this.renderCompactMenu(menu.navigation[i], container, (prefix || '') + '--');
				}
			}

		},


		renderCompact: function () {
			// Remove full navigation first
			if (this.ul) {
				this.removeChild(this.ul);
				this.ul = null;
			}
			if (!this.select) {
				this.select = document.createElement('select');
				this.appendChild(this.select);
				var self = this;				
			}

			// Render menu items
			this.empty(this.select);
			for (var i = 0; i<this.config.navigation.length; i++) {
				this.renderCompactMenu(this.config.navigation[i], this.select);
			}
		},

		render: function () {			
			if (this.getAttribute('compact') !== null) {

				this.renderCompact();
				return;
			} else { // remove compact navigation
				if (this.select) {
					this.removeChild(this.select);
					this.select = null;
				}
			}

			if (!this.ul) {
				this.ul = this.querySelector('ul');
				if (!this.ul) {
					this.ul = document.createElement('ul');
					this.appendChild(this.ul);
				}
			}

			// Clear menu			
			while (this.ul.firstChild) { this.ul.firstChild.parentNode.removeChild(this.ul.firstChild); }

			if (!this.config || !this.config.navigation) {return;}

			for (var i = 0; i<this.config.navigation.length; i++) {				
				var menuItem = this.renderMenu(this.config.navigation[i]);
				this.ul.appendChild(menuItem);
			}

			// Configure sub menu
			this.createMenu();
		},

		observedAttributes: ['compact'],
		events: {	
	
			attach: function() {
				this.updateUrl();
				this.render();
			},
			attributeChange: function(event) {
				if (event.detail && event.detail.attributeName == 'compact') {
					this.render();
				}
			},
			configure: function() {
				this.render();
			}
		}
	}

	zs.topNavigationElement = zs.customElement(HTMLElement, 'zs-top-navigation', null, [zs.domHelper, zs.configuration, zs.state, zs.menu, zs.topNavigation]);

	return zs;
})(window.zs || {});
