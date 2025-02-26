(function ($) {
	'use strict';
	var defaults = {
		caption: {
          // captions for the ON/OFF buttons
          on: 'ON',
          off: 'OFF'
        },
        on: true, // is toggle ON at initialisation
		// Provide the onClick hook. This can be used to get the current state of the toggle component.
		onClick: function () {
		}
	};
	
	function zsToggle(options, $container) {
		this.$container = $container;
		this.configure(options);
		this.render();		
	};
	
	zsToggle.prototype.defaults = defaults;

	zsToggle.prototype.render = function () {
		var self = this;
		var caption = self.options.caption;
	    var toggleContainer = $('<div class="toggle-container">');
	    var toggleDiv = $('<div class="zs-toggle">');
	    var toggleInnerDiv = $('<div class="toggle-inner">');
		var toggleOn = $('<div class="zs-toggle-on">').html(caption.on); 
		var toggleButton = $('<div class="zs-toggle-button">');
		var toggleOff = $('<div class="zs-toggle-off">').html(caption.off);
		
		toggleInnerDiv.append(toggleOn, toggleButton, toggleOff);
		toggleDiv.html(toggleInnerDiv);
      	toggleContainer.html(toggleDiv);
		self.$container.html(toggleContainer);
		
		if (typeof self.options.on == 'function') {
			self.isActive = self.options.on.apply(self);
		} else {
			self.isActive = self.options.on;
		}

	    toggleInnerDiv.toggleClass('active', self.isActive);
		
		self.$target = $(toggleContainer);
		self.$target.click(function (e) {
			self.toggle();
			
			if (typeof self.options.onClick == 'function') {
				return self.options.onClick.call(self,e);	
			}
		});

	}


	zsToggle.prototype.destroy = function () {
		this.$container.find('.toggle-container').remove();
	};


	zsToggle.prototype.setState = function (active) {


	}
	
	zsToggle.prototype.toggle = function () {
		var toggleDiv = this.$container.find('.zs-toggle');
		var toggleInnerDiv = toggleDiv.find('.toggle-inner');

		// Toggle the active state
		this.isActive = !this.isActive;
		toggleInnerDiv.toggleClass('active', this.isActive);
	}

	zsToggle.prototype.configure = function (options) {
		if (options) {
			this.options = options;
		}	
	}
	
	$.fn.zsToggle = function (opt) {			
		// Override mode
		if (this == $.fn) {
			$.extend(zsToggle.prototype, opt);
			return;
		}		
		var options = ($.isPlainObject(opt) || !opt) ? $.extend(true, {}, zsToggle.prototype.defaults, opt) : $.extend(true, {}, zsToggle.prototype.defaults);
	
		return this.each(function () {
			var plugin = $(this).data('zsToggle');
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
					plugin.render();
				}
				return;
			}
			if ($.type(opt) != 'string') {
				$(this).data('zsToggle', new zsToggle(options, $(this)));
				return;
			}
		});
	}
	
} (jQuery));