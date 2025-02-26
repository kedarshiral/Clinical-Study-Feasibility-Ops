describe('zsTooltip', function () {
	var $paragraph,
		$icon,
		_tooltipText = 'Some hover text';
	
	beforeEach(function () {
		$paragraph = $('<p>Something to add a tooltip to</p>');
		$icon = $('<span class="zs-icon zs-icon-info tooltip-me"></span>');
		$('body').append($paragraph).append($icon);
	});
	
	afterEach(function () {
		$paragraph.zsTooltip('destroy');
		$paragraph.remove();
		$icon.zsTooltip('destroy');
		$icon.remove();
	});
	
	it('is a jQuery plugin', function () {
		utils.isPlugin(expect, 'zsTooltip');
	});
	
	it('creates a tooltip component based on provided options', function () {
		$paragraph.zsTooltip({ text: _tooltipText });
		var plugin = $paragraph.data('zsTooltip');

		expect(plugin).toBeDefined();
		expect(plugin.$targetElement).toBeDefined();
		expect($paragraph.first().nodeName).toEqual(plugin.$targetElement.first().nodeName);
		expect($paragraph.find('.tooltip')).toBeDefined();
		expect(plugin.options).toBeTruthy();
		expect(plugin.options.text).toEqual(_tooltipText);
	});
	
	it('creates a tooltip element which contains an arrow and content inside', function () {
		$paragraph.zsTooltip();
		$paragraph.trigger('touchstart');
		var $tooltip = $paragraph.find('.tooltip');
		expect($tooltip.is(':visible')).toBeTruthy();
		expect($tooltip.children('.tooltip-content').length).toEqual(1);
		expect($tooltip.children('.tooltip-arrow').length).toEqual(1);
	});
	
	it('does not render the tooltip before the first interaction', function () {
		$paragraph.zsTooltip({ text: _tooltipText });

		var $tooltip = $paragraph.find('.tooltip');
		expect($tooltip.length).toEqual(0);

		$paragraph.trigger('mouseenter');
		$tooltip = $paragraph.find('.tooltip');
		expect($paragraph.find('.tooltip').length).toEqual(1);
		expect($tooltip.is(':visible')).toBeTruthy();		
	});
	
	it('opens on mouseenter and closes on mouseleave', function () {
		$paragraph.zsTooltip({ text: _tooltipText });

		$paragraph.trigger('mouseenter');
		var $tooltip = $paragraph.find('.tooltip');
		expect($tooltip.is(':visible')).toBeTruthy();
		
		$paragraph.trigger('mouseleave');
		expect($tooltip.is(':visible')).toBeFalsy();
	});

	it('opens on item click and closes on click elsewhere', function () {
		$paragraph.zsTooltip({
			text: _tooltipText,
			triggers: 'click'
		});

		$paragraph.trigger('click');
		var $tooltip = $paragraph.find('.tooltip');
		expect($tooltip.is(':visible')).toBeTruthy();
		
		$('body').trigger('click');
		expect($tooltip.is(':visible')).toBeFalsy();
	});
	
	it('opens on item touch and closes on touch elsewhere', function () {
		$paragraph.zsTooltip({ text: _tooltipText });

		$paragraph.trigger('touchstart');
		var $tooltip = $paragraph.find('.tooltip');
		expect($tooltip.is(':visible')).toBeTruthy();
		
		$('body').trigger('touchstart');
		expect($tooltip.is(':visible')).toBeFalsy();		
	});
	
	it('allows custom alignment of the tooltip bubble', function () {
		$paragraph.zsTooltip({
			alignment: 'left',
		});
		$paragraph.trigger('mouseenter');
		var plugin = $paragraph.data('zsTooltip');
		var $tooltip = $paragraph.find('.tooltip');		
		expect($tooltip.attr('align')).toEqual('left');
	});
	
	it('allows custom alignment of the tooltip arrow', function () {
		$paragraph.zsTooltip({
			arrowAlignment: 'left',
		});
		$paragraph.trigger('mouseenter');
		var plugin = $paragraph.data('zsTooltip');
		var $arrow = $paragraph.find('.tooltip-arrow');		
		expect($arrow.attr('align')).toEqual('left');
	});

	it('stays center aligned for icons', function () {
		$icon.zsTooltip({
			alignment: 'left',
			arrowAlignment: 'left',
		});
		$icon.trigger('mouseenter');
		var plugin = $icon.data('zsTooltip');
		var $tooltip = $icon.find('.tooltip');

		expect($tooltip.attr('align')).toEqual('center');		
		expect($tooltip.children('.tooltip-arrow').attr('align')).toEqual('center');
	});
	
	it('uses default settings if no options are specified', function () {
		$paragraph.zsTooltip();
		var plugin = $paragraph.data('zsTooltip');
		
		expect(plugin.options.alignment).toEqual('center');
		expect(plugin.options.animation).toBeNull();
		expect(plugin.options.arrowAlignment).toEqual('center');
		expect(plugin.options.triggers).toEqual('hover');
		expect(plugin.options.position).toEqual('top');
	});
	
	it('opens on item hover and closes on mouseleave', function () {
		$paragraph.zsTooltip({
			text: _tooltipText,
			triggers: 'hover'
		});

		$paragraph.trigger('mouseenter');
		var $tooltip = $paragraph.find('.tooltip');
		expect($tooltip.is(':visible')).toBeTruthy();

		$paragraph.trigger('mouseleave');
		expect($tooltip.is(':visible')).toBeFalsy();
	});
});