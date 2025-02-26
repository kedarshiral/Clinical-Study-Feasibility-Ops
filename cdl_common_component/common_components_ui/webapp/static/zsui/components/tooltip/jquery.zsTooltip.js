(function ($) {
	'use strict';
	
	var _tooltipString = '<div class="tooltip top" style="display: none;"><div class="tooltip-arrow"></div><div class="tooltip-content"></div></div>';
	var _defaults = {
		alignment: 'center',				// Alignment of the tooltip respective to the parent element
		animation: null,
		arrowAlignment: 'center',			// Alignment of the arrow respective to the tooltip bubble
		events: {},
		position: 'top',					// Position of the tooltip around the parent element
		text: '',
		triggers: 'hover'
	};
	
	function zsTooltip (options, $targetElement) {
		this.$targetElement = $targetElement;
		this.configure(options);
	}
		
	zsTooltip.prototype.configure = function (options) {
		this.options = options;
		this.$targetElement.css('position', 'relative');
		this.setEventHandlers();
	};
	
	zsTooltip.prototype.setAlignment = function ($tooltip) {
		// TODO: handle alignment of bubble based on position of tooltip
		if(this.options.alignment == 'left' && !$tooltip.parent().hasClass('zs-icon')) {
			$tooltip.attr('align', 'left');
		} else if(this.options.alignment == 'right' && !$tooltip.parent().hasClass('zs-icon')) {
			$tooltip.attr('align', 'right');
		} else {
			$tooltip.attr('align', 'center');
			var marginLeft = $tooltip.outerWidth() * -0.5 + this.$targetElement[0].offsetWidth * 0.5;
			$tooltip.css('marginLeft', marginLeft);
		}
	};
	
	zsTooltip.prototype.setAnimation = function($tooltip) {
		// TODO: develop this in next release after 2.1		
	}
	
	zsTooltip.prototype.setArrowAlignment = function ($tooltip) {
		// TODO: handle arrow alignment based on position of tooltip
		var $arrow = $tooltip.find('.tooltip-arrow');
		if(this.options.arrowAlignment == 'left' && !$tooltip.parent().hasClass('zs-icon')) {
			$arrow.attr('align', 'left');
		} else if(this.options.arrowAlignment == 'right' && !$tooltip.parent().hasClass('zs-icon')) {
			$arrow.attr('align', 'right');
		} else {
			$arrow.attr('align', 'center');
		}
	};
	
	zsTooltip.prototype.setEventHandlers = function () {
		this.documentClickBind = this.documentClick.bind(this);
		$(document).on('touchstart', this.documentClickBind);
		var triggers = this.options.triggers.split(' ');

		if(triggers.indexOf('hover') >= 0) {
			this.$targetElement.on('mouseenter', this.show.bind(this));
			this.$targetElement.on('mouseleave', this.hide.bind(this));
		}
		if(triggers.indexOf('click') >= 0) {
			$(document).on('click', this.documentClickBind);
		}
	};
	
	zsTooltip.prototype.setPosition = function (tooltip) {
		// TODO: develop this in next release after 2.1
	};
	
	zsTooltip.prototype.setText = function (tooltip) {
		var $content = tooltip.find('.tooltip-content');
		$content.text(this.options.text);
	};
	
	zsTooltip.prototype.render = function () {
		var $tooltip = $(_tooltipString);
		this.setText($tooltip);
		this.$targetElement.append($tooltip);
		
		this.setAlignment($tooltip);
		this.setArrowAlignment($tooltip);
		return $tooltip;
	};
	
	zsTooltip.prototype.show = function (event) {
		var $tooltip = this.$targetElement.find('.tooltip');
		if(!$tooltip.length) {
			$tooltip = this.render();
		}
		$tooltip.show();
	};
	
	zsTooltip.prototype.hide = function (event) {
		this.$targetElement.children('.tooltip').hide();
	};
	
	zsTooltip.prototype.documentClick = function (event) {
		if(event.target != this.$targetElement[0]) {
			this.hide(event);	
		} else {
			this.show(event);
		}
	};
	
	zsTooltip.prototype.destroy = function () {
		$(document).off('click', this.documentClickBind);
		$(document).off('touchstart', this.documentClickBind);
		this.$targetElement.off();
		this.$targetElement.remove('.tooltip');
		this.$targetElement.removeData('zsTooltip');		
	};
	
	$.fn.zsTooltip = function (opt) {		
		// Override mode
		if (this == $.fn) {
			$.extend(zsTooltip.prototype, opt);
			return;
		}
		
		var options = ($.isPlainObject(opt) || !opt)
			? $.extend(true, {}, _defaults, opt)
			: $.extend(true, {}, _defaults);
			
			
			
			
		return this.each(function () {
			var plugin = $(this).data('zsTooltip');
			if(plugin) {
				if($.type(opt) == 'string') {
					switch (opt) {
						case 'destroy':
							plugin.destroy($(this));
							break;
					}
				}
				return;
			}
			if($.type(opt) != 'string') {
				$(this).data('zsTooltip', new zsTooltip(options, $(this)));
				return;
			}
		});
	};	
} (jQuery));