describe("zsClear", function () {
	var $input, $container;

	beforeEach(function () {
		$container = $('<div class="zs-input-icon"><input type="text"></div>');
		$container.appendTo('body');
		$input = $container.find('>select');
	});

	it('Is a jQuery pluging', function () {
		utils.isPlugin(expect, 'zsClear');
	})

	it('Creates a clear icon inside an input element', function () {
		$container.zsClear({
			text: 1
		});
		
		var plugin = $container.data('zsClear');
		expect(plugin).toBeTruthy();
		expect(plugin.$input.length).toBeTruthy();
		expect($container.data('zsClear')).toBeTruthy();
		expect($container.find('[clear]').length).toBeTruthy();				
		expect(plugin.options).toBeTruthy();
		expect(plugin.options.text).toBe(1);
				
	});
	
	it('Can be reconfigured', function () {
		$container.zsClear({
			a: 1,
			b: 2
		});
		
		var plugin = $container.data('zsClear');
		
		expect(plugin.options.a).toBe(1);
		expect(plugin.options.b).toBe(2);
		
		$container.zsClear({
			b: 3
		});
	
		expect(plugin.options.a).toBe(1);
		expect(plugin.options.b).toBe(3);		
		
	});
	
	it('Can be removed', function() {
		$container.zsClear();
		var plugin = $container.data('zsClear');
		$container.zsClear('destroy');
		plugin = null;
		expect($container.find('[clear]').length).toBeFalsy();				
		
		// Remove itself when select is destroyed
		$container.zsClear();
		$container.remove();		
		$container = null;
		expect($('body').find('[clear]').length).toBeFalsy();		
		
	});
	
	it('Shows the icon only when text is entered', function() {
		$container.zsClear();	
		
		// Hiden by default
		expect($container.find('[clear]').is(':visible')).toBeFalsy();
		
		// Mimic text entering		
		var e = $.Event('keypress');
		e.which = 65; // Character 'A'
		$input.trigger(e);
		expect($container.find('[clear]').is(':visible')).toBeFalsy();
		
	});
	
	it('Clears the input box when the clear icon is clicked', function() {
		$input.val('test');
		$container.zsClear();	
		var plugin = $container.data('zsClear');
		
		plugin.$icon.trigger('click');
		expect($input.val()).toBeFalsy();		
	});	
		
	it('Supports a custom icon', function() {
		$container.zsClear({
			render: {
				icon: function() {
					return '<a clear>test</a>';
				}
			}
		});		
		expect($container.find('[clear]').text()).toBe('test');		
	});
	it('Can be called using \'show\' option that shows up the clear icon',function(){
		
		$container.zsClear();
		$input=$container.find('input');

		spyOn($container.data('zsClear'),'show').and.callThrough();
		
		$input.val('test');

		expect($container.data('zsClear').$icon.css('display')).toBe('none');
		
		$container.zsClear('show');
		
		expect($container.data('zsClear').show).toHaveBeenCalled();
		expect($container.data('zsClear').$icon.is(":visible")).toBeTruthy();
	});
	it('Can be called using \'hide\' option that hides the clear icon',function(){
		
		$container.zsClear();
		$input=$container.find('input');

		spyOn($container.data('zsClear'),'hide').and.callThrough();
		
		$input.val('test');

		$container.zsClear('show');

		expect($container.data('zsClear').$icon.is(":visible")).toBeTruthy();

		$container.zsClear('hide');

		expect($container.data('zsClear').$icon.css('display')).toBe('none');
		
		expect($container.data('zsClear').hide).toHaveBeenCalled();				
	});
	it('Can be called using \'toggle\' option that toggles clear icon inside input field',function(){
		
		$container.zsClear();
		$input=$container.find('input');

		spyOn($container.data('zsClear'),'toggle').and.callThrough();
		
		$input.val('test');

		expect($container.data('zsClear').$icon.css('display')).toBe('none');

		$container.zsClear('toggle');

		expect($container.data('zsClear').$icon.is(":visible")).toBeTruthy();

		$container.zsClear('toggle');

		expect($container.data('zsClear').$icon.css('display')).toBe('none');
		
		expect($container.data('zsClear').toggle).toHaveBeenCalled();
	});
	afterEach(function () {
		if ($container && $container.length && $container.data('zsClear')) {
			$container.zsClear('destroy');
		}
		if ($container && $container.length) {
			$container.remove();
		}		
	});
});
