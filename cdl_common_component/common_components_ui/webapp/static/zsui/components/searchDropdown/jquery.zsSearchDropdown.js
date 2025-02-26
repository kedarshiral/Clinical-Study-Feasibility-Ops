(function ($) {
	'use strict';

	/**
     * Search Dropdown
     *
     * @extends $ collection
     *
     * @property {String} emptyText
     * @property {Boolean} addCheckboxes
     * @property {Number} maxOptionsVisible
     * @property {Number} minOptionsForSearch
     *
     * @method optionMouseOverBind
     * @method readOptions
     * @method renderOptions
     * @method selectOption
     * @method choseOption
     * @method activateOption
     * @method open
     * @method close
     * @method optionClick
     * @method documentClick
     * @method highlight
     * @method update
     * @method locateOption
     * @method search
     * @method up
     * @method down
     * @method onKeyUp
     * @method render
     * @method select
     * @method format
     * @method destroy
     * @method configure
     * @method mapSelection
     * @method clearAllSelection
     * @method calculateFlipPosition
     * @event beforeOpen
     * @event open
     * @event beforeClose
     * @event close
     * @event change
     */
	var defaults = {
		/**
		 * Text that will be shown if nothing selected
		 * @type string
		 */
		emptyText: 'Select value',

		/**
		 * Do we need a checkboxes prior elm text?
		 * @type boolean
		 */
		addCheckboxes: false,

		/**
		 * How many options will be shown in one list.
		 * If we have more items they will be shown 
		 * portionly when user scrolls to the bottom
		 * of the list (aka 'Infinitive scroll')
		 */
		maxOptionsVisible: 100,

		minOptionsForSearch: 6,

		addSelectAll: false,

		selectAllText: 'Select all',

		deselectAllText: 'Deselect all',

		events: {
		},
		
		match: function(option, keyword, returnIndex) {
			if (!option || !keyword || !option.text) {
				return false;
			}
			if (returnIndex) {
				return option.text.indexOf(keyword);
			} else {
				return option.text.toLowerCase().indexOf(keyword.toLowerCase()) != -1;
			}	
		},
		flippable: false,   //Renders dropdown on top of field in case of insufficient space at the bottom of field.
		hideOnScroll: false, //Hides dropdown on scrolling the page.
		defaultInputFocus: true //Focuses on input text field by default.
	};
	
	function $isThing($elem) {
		if ($elem && $elem.length) {
			return true;
		}

		

		return false;
	}
		
	function zsSearchDropdown(options, $select) {
		this.$select = $select;
		this.configure(options);
		
		this._allSelected = false;

		if(this.options.addSelectAll && !this.options.multiple){ // selectAll makes sense only for multiple 
			throw new Error('addSelectAll is available in multiple mode only');
		}

		if(this.options.addSelectAll && this.$select[0].length > 999){
			console.warn('We do not recommend to use addSelectAll with a long list of options due to possible lack of performance');
		}

		if(!this.options.noRender){
			this.render();
		}
				
	};
	
	zsSearchDropdown.prototype.optionMouseOverBind = function(event) {
		if(this.options.alwaysOpen){
			this.selectOption(null, false);
			return;
		}

		var $a = $(event.target);
		this.selectOption($a, false);
	}
	
	zsSearchDropdown.prototype.readOptions = function() {
		var i, option, $a;
		if (!$isThing(this.$select) || !this.$select[0].options) { // options is an array-like object
			return;
		}
		this.data = [];
		this.$nav.html('');
		
		var count = 0;
		for(i=0;i<this.$select[0].children.length;i++){
			var child = this.$select[0].children[i];

			if(child.children.length > 0){ // in case select contains optgroup
				this.data.push({
					// id: count++,
					text: child.getAttribute('label'),
					value: child.getAttribute('value'),
					group: true
				});	

				for (var k = 0; k < child.children.length; k++) {
					this.data.push({
						id: count++,
						text: '&#160;&#160;&#160;' + child.children[k].text,
						value: child.children[k].value
					});	
				}
			}else{
				this.data.push({
					id: count++,
					text: child.text,
					value: child.value
				});	
			}
		}
		// for (i=0;i<this.$select[0].options.length;i++) {
		// 	option = this.$select[0].options[i];
				
		// 	this.data.push({
		// 		id: i,
		// 		text: option.text,
		// 		value: option.value
		// 	});		
		// }

		this.renderOptions({
			data: this.data
		});

		// Adjust the height
		if (this.$select[0].options.length >= this.options.minOptionsForSearch) {
			this.$container.css('minHeight', this.$container.outerHeight());
		}

		// Logic to show or hide search dropdown
		if (this.$select[0].options.length >= this.options.minOptionsForSearch) {
			this.$input.parent().show();
			this.$container.css('minHeight', this.$container.css('height'));
		} else {
			this.$input.parent().hide();
		}		
	}
	
	zsSearchDropdown.prototype.renderOptions = function(params){
		this.$nav.off('scroll');
		this.$nav.children('a').off('focus');

		var $a,
			data 	  	= params.data || [],
			start 	  	= params.start || 0,
			highlight 	= params.highlight,
			selectFirst = params.selectFirst,
			max 		= start + (data.length < this.options.maxOptionsVisible ? data.length : this.options.maxOptionsVisible);

		if(this.options.addSelectAll && start === 0 && !highlight){ // we need to render selectAll button only once
			if(this.options.addCheckboxes){
				$a = $('<a href="javascript:void(0)"><span class="zs-checkbox"><input type="checkbox"><span zs-dropdown-option>' + this.options.selectAllText + '</span></span></a>');	
			}else{
				$a = $('<a href="javascript:void(0)"><span zs-dropdown-option>' + this.options.selectAllText + '</span></a>');	
			}

			$a.addClass('select-all');
			$a.on('click touch', this.toggleAll.bind(this));
			$a.appendTo(this.$nav);
		}

		for (var i = start; i < max; i++) {
			if(!data[i]){ // when we have items count that cannot be devided to this.options.maxOptionsVisible
				break; 
			}

			var index = '';
			if(typeof data[i].id !== 'undefined'){
				index = 'index="' + data[i].id + '"';
			}
			if(this.options.addCheckboxes){
				$a = $('<a href="javascript:void(0)" ' + index + '><span class="zs-checkbox"><input type="checkbox"><span zs-dropdown-option>' + data[i].text + '</span></span></a>');	
			}else{
				$a = $('<a href="javascript:void(0)" ' + index + '><span zs-dropdown-option>' + data[i].text + '</span></a>');	
			}

			$a.appendTo(this.$nav);
			if(index){
				$a.on('click touch', this.optionClick.bind(this));
				$a.on('mouseover', this.optionMouseOverBind.bind(this));	
			}
			

			if(highlight){
				this.highlight(true, $a, highlight);
			}
			
			// we need to update because of cleaning after search
			if(this.options.multiple){
				if(this.selectedMap && this.selectedMap[data[i].id]){
					$a.attr('active','');
					$a.find('input').prop('checked', true);
				}
			}else{
				if(this.$select[0].selectedIndex === data[i].id){
					$a.attr('active','');
					$a.find('input').prop('checked', true);
				}
			}
		}

		if(start < 1){
			this.$nav.scrollTop(0);
		}

		if (selectFirst) {
			this.selectOption(this.$nav.children('a').first(), true);
		}
		
		if(max >= data.length){
			return;
		}

		var renderOptions = this.renderOptions.bind(this, {
			data: data,
			start: max,
			highlight: highlight,
			// selectFirst: selectFirst
		});
		this.$nav.on('scroll', function() {
			if($(this).scrollTop() + $(this).innerHeight() < $(this)[0].scrollHeight - 10) { // give a little room to begin loading erlier
				return;
			}
			renderOptions();
		});

		this.$nav.children('a:last').on('focus', function(){
			renderOptions();
		});
	}

	zsSearchDropdown.prototype.selectOption = function($a, scrollTo) {
		if ($isThing(this.$selected)) {
			this.$selected.removeAttr('hover');	
			this.$selected = null;
		}	
		
		if ($isThing($a)) {
			this.$selected = $a;
			this.$selected.attr('hover','');
		
			// Scroll to the element
			if(window.navigator && window.navigator.userAgent && window.navigator.userAgent.match(/iPad/i)){ // avoid keyboard changing on ipad
				return;
			}

			if (scrollTo) {
				$a.focus();
				this.$input.focus().val(this.$input.val());
			}
			return;
		}
		
	}
	
	zsSearchDropdown.prototype.choseOption = function($a) {
		var index, checkbox;
		if (!this.options.multiple) {
			this.$nav.find('>[active]').removeAttr('active');
		}
		
		if ($isThing($a)) {
			checkbox = $a.find('input');
			$a.attr('active','');
			index = $a.attr('index');
			if(this.options.multiple){
				// Add list of selected options
				this.mapSelection($a.attr('index'));

				// Activate or not an option
				if (typeof this.selectedMap[index] != 'undefined') {
					$a.attr('active','');
					checkbox.prop('checked', true);
				} else {
					$a.removeAttr('active');
					checkbox.prop('checked', false);
				}
			}else{
				this.selectOption($a, false);
				this.$select[0].selectedIndex = index;
			}
			

			if(!this.options.alwaysOpen) {
				this.$select.trigger('change');		
			}

			
			
			if (typeof this.options.events.change == 'function') {
				this.options.events.change.call(this);
			}

			if(this.options.multiple){
				return;
			}

			/**
			 * I commented this because we probably do not need to reset search after selecting element
			 */
			// if (this.enteredText) {
			// 	this.enteredText = '';
			// 	this.$input.val('').trigger('change');
			// 	this.search();
			// 	$a.blur();
			// 	$a.focus();
			// }
		}		
	}
	
	zsSearchDropdown.prototype.activateOption = function (option, scrollTo) {				
		var $a = this.locateOption(option);
		if (!this.options.multiple) {
			this.$nav.find('>[active]').removeAttr('active');
		} else {
			this.mapSelection($a.attr('index'), true);
		}

		if ($isThing($a)) {
			$a.attr('active','');
			
			if (scrollTo) {
				$a.focus();
			}
		}
	}

	zsSearchDropdown.prototype.isInViewPort = function (bottom) {
	    var viewportBottom = $(window).scrollTop() + $(window).height();
	    return bottom < viewportBottom;
	};

	zsSearchDropdown.prototype.calculateFlipPosition = function () {
	    var self = this;
	    var containerWidth = self.$container.outerWidth();
	    var containerHeight = self.$container.outerHeight();
	    var fieldPos = self.$overlay.offset();
	    var bodyScrollPos = $(document).scrollTop();

	    var containerTop;
	    var containerLeft = fieldPos.left - $("body").scrollLeft();

	    self.$parentModal = self.$container.parents('.zs-modal')

	    var inViewPort = this.isInViewPort(fieldPos.top + containerHeight);

	    if (!inViewPort) {
	        containerTop = fieldPos.top + self.$overlay.outerHeight() - containerHeight - bodyScrollPos;
	    } else {
	        containerTop = fieldPos.top - bodyScrollPos;
	    }

	    /* 
            Remove transform applied to modal temporarily if calendar is rendered inside modal.
            For fixed positioned element, when an ancestor has the transform property set then 
            this ancestor is used as container instead of the viewport. 
            Therfore, offset calculation fails with transform applied.
            https://developer.mozilla.org/en/docs/Web/CSS/position
        */

	    if (self.$parentModal.length && self.$parentModal.css("transform") != "none") {
            var modalOffset = self.$parentModal.offset();
            var width = self.$parentModal[0].style.width;
            if (width) {
                self.removeModalWidthOnHide = false;
            } else {
                self.removeModalWidthOnHide = true;
                width=self.$parentModal.css('width');
            }
	        self.$parentModal.css({
	            transform: "none",
	            top: (modalOffset.top - bodyScrollPos) + "px",
	            left: modalOffset.left + "px",
                width: width
	        });
            
	        self.removeModalStylesOnHide = true;
	    }

	    self.$container.css({
	        position: "fixed",
	        top: containerTop + "px",
	        left: containerLeft + "px"
	    });
	}

	zsSearchDropdown.prototype.open = function () {
		if (!$isThing(this.$select)) {
			return;
		}
		
		// Reset search
		if (this.enteredText) {
			this.enteredText = '';	
			this.$input.val('');
			this.search();			
		}

		if (typeof this.options.events.beforeOpen == 'function') {
			this.options.events.beforeOpen.call(this);
		}
				
		
		
		if (this.$select[0].selectedIndex != -1) {
			// also scrolls to the selected item
			var elm = this.data.filter(function(one){
				return one.id === this.$select[0].selectedIndex;
			}, this);
			elm = elm[0];
			if(elm){
				this.activateOption(elm, true)
			}
			
			// this.activateOption(this.data[this.$select[0].selectedIndex], true); // also scrolls to the selected item
		}
		
		// Adjust width to fix a strange issue 
		this.$container.css({
			width: this.$select.outerWidth() + 4,
			top: -5
		});

		if (this.options.flippable) {
			$(document).click();    //Close all open dropdowns before calculating flip positions
			this.$container.show();
			this.calculateFlipPosition();
		} else {
			this.$container.show();
		}

		if (this.options.defaultInputFocus) {
			this.$input.focus();
		}

		this.isOpening = true;	

		if (typeof this.options.events.open == 'function') {
			this.options.events.open.call(this);
		}	
	}
	
	zsSearchDropdown.prototype.close = function() {
		if (typeof this.options.events.beforeClose == 'function') {
			this.options.events.beforeClose.call(this);
		}

		this.$container.hide();

		var self = this;
		setTimeout(function(){ // we need this because otherwise submit on keydown is blocked
			self.$input.blur();
		}, 0);
		

		if (typeof this.options.events.close == 'function') {
			this.options.events.close.call(this);
		}
	}
	
	zsSearchDropdown.prototype.optionClick=function(event) {
		var $a = $(event.currentTarget);
		if ($a.is('mark')) { $a = $a.parent(); }
		this.choseOption($a);

		if(this.options.alwaysOpen) {
			return;
		}

		if(this.options.multiple) {
			return;
		}

		this.close();
	}
	
	zsSearchDropdown.prototype.documentClick = function (event) {
		if (this.isOpening) {
			// The case when we just clicked on the overlay to open the component and event is still propagating further to the document
			this.isOpening = false;
			return;
		}
		if ($(event.target) == this.$overlay) {
			// my overlay click
			return;
		}
		if (!this.$container.is(':visible')) {
			// I am not active yet.
			return;
		}
		if (this.$container.is($(event.target).closest('[container]'))) {

		} else {
			this.close();

			if (this.removeModalStylesOnHide) {
			    var cssStyle = {
			        transform: "",
			        top: "",
			        left: ""
			    };
                if (self.removeModalWidthOnHide) {
                    cssStyle.width = "";
                }
                this.$parentModal.css(cssStyle);
			} 
		}
	}
	
	zsSearchDropdown.prototype.highlight = function (on, $el, keyword) {	
		// Remove highlight from all elements
		if (!on && !$el) {
			this.$nav.find('mark').each(function() {
				$(this).parent().html($(this).parent().text());
			});			
			return;
		}
		
		if (!$isThing($el)) {
			return;
		}
	 
		
		if (on) {
			var keywordLower = keyword.toLowerCase();
			var stringToSearch = $el.text().toLowerCase();
			var original = $el.find("[zs-dropdown-option]").text();
			var index = stringToSearch.indexOf(keywordLower);
			var offset = keywordLower.length;
			var text = '';
			
			while(index > -1) {
				text += original.substr(0, index) + '<mark>' + original.substr(index, offset) + '</mark>';
				stringToSearch = stringToSearch.substr(index + offset, stringToSearch.length - 1);
				original = original.substr(index + offset, original.length - 1);
				index = stringToSearch.indexOf(keywordLower);
			}
			text += original;

		    $el.find("[zs-dropdown-option]").html(text);
		} else {
			$el.find("[zs-dropdown-option]").html($el.text());
		}
	}
	
	zsSearchDropdown.prototype.update = function () {
		this.readOptions();
		if (this.$select[0].selectedIndex != -1) {
			this.activateOption(this.data[this.$select[0].selectedIndex]);
		}
		
	}
	
	zsSearchDropdown.prototype.locateOption = function(option) {
		/**
		 * @todo this approach is actually ~2x faster because search stops when first match found
		//  * return $(this.$nav[0].querySelector('a[index="' + option.id +'"]'));
		 */
		// return $(this.$nav.children()[option.id]);
		return $(this.$nav[0].querySelector('a[index="' + option.id +'"]'));
	}
	
	zsSearchDropdown.prototype.search = function(keyword) {
		var self = this;
		keyword = keyword || this.$input.val();
		
		this.$nav.html('');
		if (!keyword) {
			this.renderOptions({
				data: this.data
			});

			return;
		}

		var data = [], option, match = defaults.match;
		if (typeof self.options.match == 'function') {
			match= self.options.match;
		}

		for(var i = 0; i < this.data.length; i++){
			option = this.data[i];
			if (match.call(self, option, keyword)) {
				data.push(option);
			}
		}

		
		this.renderOptions({
			data: data,
			highlight: keyword,
			selectFirst: true
		});
	}
	
	zsSearchDropdown.prototype.up = function () {
		var $prev, $selected
		if (!this.data || !this.data.length) {
			return;
		}
		
		// Get the next option
		$selected = this.$selected;
		if (!$isThing($selected)) {
			$prev = this.$nav.children(':visible:last');
		} else {
			$prev = $selected.prevAll(':visible:first');			
		}
		if ($isThing($prev)) {
			this.selectOption($prev, true);	
		}
	}
	
	zsSearchDropdown.prototype.down = function () {
		var $next, $selected;
		if (!this.data || !this.data.length) {
			return;
		}
		
		// Get the next option
		$selected = this.$selected;
		if (!$isThing($selected)) {
			$next = this.$nav.children(':visible:first');
		} else  {
			$next = $selected.nextAll(':visible:first');			
		}
		if ($isThing($next)) {
			this.selectOption($next, true);	
		}		
	}

	zsSearchDropdown.prototype.onKeyUp = function (event) {
		switch (event.keyCode) {
			case 8: // Backspace
				// ?	
				break;
			case 27: // Escape
				// ?
			case 9: // Tab
				//
				event.preventDefault();
				event.stopPropagation();
				break; 
			case 37:
			case 39:
			case 16:
			case 91:
			case 17:
				break;
			
			case 38: // Up
			    event.preventDefault();
				event.stopPropagation();
          		this.up();
          		break;
	        case 40:
    	    	event.preventDefault();
				event.stopPropagation();
          		this.down();
				break;			

		}
		
		// Do we need to search
		if (this.enteredText != this.$input.val()) {
			if (this.enteredText) { // Clear selected option only if we searched before
				this.selectOption(false); 
			}
			this.enteredText = this.$input.val();			
			this.search();			
		}

	}	

	zsSearchDropdown.prototype.onKeyDown = function(event){
		// Select item when user preses enter
		// We do that on keydown not keyup otherwise we face issue with submitting data:
		// submit fires on keydown, if we set new value on keyup (which fires a bit latly than keydown) new value would be set AFTER submiting
		if(event.keyCode === 13){ 
			if(this.$selected && this.$selected[0].hasAttribute('index')){
				if(this.$input.is(":visible")){ this.choseOption(this.$selected);}
				if (!this.options.alwaysOpen && !this.options.multiple) { this.close(); }
			}
		}

		return true;
	}
	
	zsSearchDropdown.prototype.render = function () {
		var self = this;
		this.enteredText = '';
		if(this.options.alwaysOpen) {
			this.$container = $('<div class="zs-search-dropdown zs-menu" container><p class="zs-input-icon zs-icon-search" style="display:none;"><input input type="text" class="zs-input"/></p><nav options></nav></div>');

			// Check for a parent container
			if (this.$select.parent().is('.zs-select')) {
				this.$parent = this.$select.parent();
			} else {
				this.$parent = this.$select;
			}

			// Create an overlay to handle clicks
			this.$parent.css({
				position: 'relative'
			});

			this.$overlay = $('<div overlay></div>').css({
				display: 'none',
				top: 0,
				left: 0,
				right: 0,
				bottom: 0
			});

			if (this.$select.is(this.$parent)) {
				this.$container.css({
					top: 0,
					left: 0,
					width: '100%'
				});
				this.$select
					.after(this.$overlay)
					.after(this.$container);
				this.$select.parent().addClass('zs-open-dropdown');
			} else {
				this.$container.css({
					top: 0, // cover the border
					left: 0,
					width: this.$select.outerWidth() + 4
				});
				this.$parent
					.append(this.$overlay)
					.append(this.$container)
					.addClass('zs-open-dropdown');
			}


			this.$input = this.$container.find('[input]');
			this.$input.on('keyup', this.onKeyUp.bind(this));
			this.$input.on('blur', this.onKeyUp.bind(this));
			this.$input.on('paste', this.onKeyUp.bind(this));
			this.$input.on('change', this.onKeyUp.bind(this));
			this.$nav = this.$container.find('[options]');
			this.$nav.on('keyup', this.onKeyUp.bind(this));
			
			this.$select.hide();
			var destroy = this.destroy.bind(this);
			if(this.$select[0].addEventListener){
				this.$select[0].addEventListener('DOMNodeRemoved', destroy);
			}else if(this.$select[0].attachEvent){
				this.$select[0].attachEvent('DOMNodeRemoved', destroy);
			}
			this.readOptions();
		} else {
			var offset;
			
			this.$container = $('<div class="zs-search-dropdown zs-menu" container style="display:none;position:absolute"><p class="zs-input-icon zs-icon-search"><input input type="text" class="zs-input"/></p><nav options></nav></div>');
			
			// Click anywhere 		
			this.documentClickBind = this.documentClick.bind(this);
			$(document).on('click touchstart', this.documentClickBind);

		    // Hide on scroll
			if (this.options.hideOnScroll) {
			    if (window.navigator && window.navigator.userAgent.match(/iPad/i)) {
			        $(document).on("mousewheel DOMMouseScroll touchmove", this.documentClickBind);    
			    }else {
			        $(document).on("mousewheel DOMMouseScroll scroll", this.documentClickBind);    
			    }
			}
			
			// Detect a parent container
			if (this.$select.parent().is('.zs-select')) {
				this.$parent = this.$select.parent();
			} else {
				this.$parent = this.$select;
			}	
					
			// Create an overlay to handle clicks
			this.$parent.css({
				position:'relative'
			});
			this.$overlay = $('<div overlay></div>').css({
				// position: 'absolute',
				top:0,
				left: 0,
				right: 0,
				bottom: 0,
			}).appendTo(this.$parent).click(function () {
				self.open();
			}).text(this.options.emptyText);		

			offset = this.$select.offset();
			this.$container.css({
				top: offset.top - 2, // cover the border
				left: offset.left - 2,
				width: this.$select.outerWidth() + 4 
			});
			
			this.$container.css({
				top: 0, // cover the border
				left: 0,
				width: this.$select.outerWidth() + 4 
			});
			this.$container.appendTo(this.$parent);
			
			this.$input = this.$container.find('[input]');
			this.$input.on('keyup', this.onKeyUp.bind(this));
			this.$input.on('blur', this.onKeyUp.bind(this));
			this.$input.on('paste', this.onKeyUp.bind(this));
			this.$input.on('change', this.onKeyUp.bind(this));
			this.$nav = this.$container.find('[options]');
			this.$nav.on('keyup', this.onKeyUp.bind(this));

			var destroy = this.destroy.bind(this);
			if(this.$select[0].addEventListener){
				this.$select[0].addEventListener('DOMNodeRemoved', destroy);
			}else if(this.$select[0].attachEvent){
				this.$select[0].attachEvent('DOMNodeRemoved', destroy);
			}
			
			this.readOptions();		

			var self = this;
			this.$input.on('focus', function(){
			    if (window.navigator && window.navigator.userAgent.match(/iPad/i)) { // viewport is smaller because of keyboard
			        var needSpace = self.$container.outerHeight() + self.$container.offset().top;
				    var hasSpace = $(window).outerHeight() + $(window).scrollTop();
                    
			        var orientation = Math.abs(window.orientation) == 90 ? 'landscape' : 'portrait';
			        if (orientation === 'portrait') {
			            hasSpace = 2 * $(window).outerHeight() / 3 + $(window).scrollTop();
			        } else {
			            hasSpace = $(window).outerHeight() / 2 + $(window).scrollTop();
			        }
			        if (needSpace < hasSpace) {
			            return;
					}
					
					$('html, body').animate({
						scrollTop: $(this).offset().top - 20
					}, 1000);
				}
			});
		}

		this.$select.hide();
		this.$select.on('change', this.select.bind(this));	
		this.$input.on('keydown', this.onKeyDown.bind(this));
		this.$nav.on('keydown', this.onKeyDown.bind(this));

		if (this.$select[0].selectedIndex != -1) {
			this.$select.find('[selected]').each(function(){
				self.$nav.find('[index = ' + this.index + ']').trigger('click');
				// $(self.$nav.children()[$(this).index()]).trigger('click');
			});
		}

		this.select(); // updates text in overlay 
	}
	
	/**
	 * Updates overlay with a values's text
	 * 
	 * @returns {String}
	 */
	zsSearchDropdown.prototype.select = function(e) {
		var val;		
		if(this.options.multiple){
			val = [];
			for(var i in this.selectedMap){
				var elm = this.data.filter(function(one){
					return one.id == i;
				}, this);
				
				elm = elm[0];
				if(elm){
					val.push(elm.text);
				}

				// val.push(this.data[i].text);
			}
			
			val = val.join(', ');
		}else if(this.data[this.$select[0].selectedIndex] && this.data[this.$select[0].selectedIndex].text){
			var elm = this.data.filter(function(one){
				return one.id === this.$select[0].selectedIndex;
			}, this);
			
			elm = elm[0];
			if(elm){
				val = elm.text;
			}
		}

		if(!val){
			val = this.options.emptyText;
		}

		val = val.replace(new RegExp('&#160;', 'g'), '');
		this.$overlay.text(val);

		return val;
	}
	
	zsSearchDropdown.prototype.format = function(formatName, index) {
		if (typeof this.options.format[formatName] == 'function') {
			return this.options.format[formatName].call(this, index);	
		} else {
			return defaults.format[formatName].call(this, index);
		}		
	}
	
	zsSearchDropdown.prototype.destroy = function () {
		
		this.$nav.off();
		this.$nav.find('>a').off();
		this.$input.off();			
		this.$overlay.off();
		$(document).off('click touchstart', this.documentClickBind);
		this.$overlay.remove();
		this.$container.remove();
		if ($isThing(this.$select)) {
			this.$select.removeData('zsSearchDropdown');
		}		
	
	}
	
	zsSearchDropdown.prototype.configure = function (options) {
		if (typeof options.minOptionsForSearch == 'undefined') {
			options.minOptionsForSearch = defaults.minOptionsForSearch;
		}

		/**
		 * @todo
		 * hold this feature for future
		 */
		// if(this.$select.prop('multiple')){
		// 	options.multiple = true;
		// }

		/**
		 * @todo
		 * remove this when plugin will be changed to web component
		 */
		if(options.multiple){
			this.$select.prop('multiple', true);
		}

		if (options) {
			this.options = options;
		}	
	}

	zsSearchDropdown.prototype.mapSelection = function(index, forceSelectUnselect) {
		if (!this.selectedMap) {
			this.selectedMap = {};
		}
		if(!index){
			return;
		}
		if (typeof forceSelectUnselect != 'undefined') {
			this.$select[0].options[index].selected = forceSelectUnselect;
			if (!forceSelectUnselect) {
				delete this.selectedMap[index];
			}
			return;
		}

		if (typeof this.selectedMap[index] != 'undefined') {
			this._allSelected = false;
			this.toggleSelectAllButton();
			this.$select[0].options[index].selected = false;
			delete this.selectedMap[index];
		} else {
			this.$select[0].options[index].selected = true;
			this.selectedMap[index] = index;

			if(this.$select[0].length == Object.keys(this.selectedMap).length){
				this._allSelected = true;
				this.toggleSelectAllButton();
			}
		}
	}

	zsSearchDropdown.prototype.clearAllSelection = function () {
        var self = this;

        if (!self.options.multiple) {
            return;
        }

        var selectedOptions = this.$nav.find("[active]");

        $.each(selectedOptions, function (index, elem) {
            self.choseOption($(elem));
        });
	}
	
	zsSearchDropdown.prototype.toggleSelectAllButton = function(){
		if(!this.options.addSelectAll){
			return;
		}
		
		var elm = this.$nav.find('a').first();
		if(elm.attr('index')){
			return;
		}

		if(this.options.addCheckboxes){
			elm.find('input').prop('checked', this._allSelected ? true : false);
		}else{
			elm.text(this._allSelected ?  this.options.deselectAllText : this.options.selectAllText);
		}
	}
	zsSearchDropdown.prototype.toggleAll = function(){
		var _this = this;
		this.$nav.find('a').each(function(){
			if(!$(this).attr('index')){				
				return;
			}	

			if(_this._allSelected){
				$(this).removeAttr('active');
				$(this).find('input').prop('checked', false);
			}else{
				$(this).attr('active', '');
				$(this).find('input').prop('checked', true);
			}
		});

		var options = this.$select[0].options;

		this.selectedMap = {};
		
		for(var i = 0; i < options.length; i++){
			if(this._allSelected){
				options[i].selected = false;
			}else{
				options[i].selected = true;
				this.selectedMap[i] = i;
			}
		}

		this._allSelected = !this._allSelected;
		this.toggleSelectAllButton();

		if(!this.options.alwaysOpen) {
			this.$select.trigger('change');		
		}
		
		if (typeof this.options.events.change == 'function') {
			this.options.events.change.call(this);
		}
	}
	
	$.fn.zsSearchDropdown = function (opt) {
		// Override mode
		if (this == $.fn) {
			$.extend(zsSearchDropdown.prototype, opt);
			return;
		}
		
		var options = ($.isPlainObject(opt) || !opt) ? $.extend(true, {}, defaults, opt) : $.extend(true, {}, defaults);

		return this.each(function () {
			var plugin = $(this).data('zsSearchDropdown');
			if (plugin) {
				if ($.type(opt) == 'string') {
					switch (opt) {						
						case 'destroy':
							plugin.destroy($(this));
							break;
						case 'update':
							plugin.update();
							break;
							
					}
				} else {
					plugin.configure($.extend(true, plugin.options, opt));
					plugin.update();
				}
				return;
			}
			if ($.type(opt) != 'string') {
				$(this).data('zsSearchDropdown', new zsSearchDropdown(options, $(this)));
				return;
			}
		});
	}
} (jQuery));
