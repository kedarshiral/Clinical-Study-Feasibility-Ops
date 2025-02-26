describe("zsToggle", function () {
	var $ele;

	beforeEach(function () {
		$('<div class="toggle-component"></div>').appendTo('body');
		$ele = $('.toggle-component');
	});

	it('Is a jQuery plugin', function () {
		utils.isPlugin(expect, 'zsDatePicker');
	});

	it('should create a toggle component', function () {
		$ele.zsToggle();
		expect($ele.data('zsToggle')).toBeTruthy();
	});

	it('should create the component with default values', function () {
		$ele.zsToggle();
		var toggleComponent = $ele.data('zsToggle');
		expect(toggleComponent.isActive).toBeTruthy();
		expect(toggleComponent.options.caption.on).toBe('ON');
		expect(toggleComponent.options.caption.off).toBe('OFF');
	});

	it('should be possible to customize the component by passing optional parameters', function () {
		$ele.zsToggle({
			caption: {
				on: 'YES',
				off: 'NO'
			},
			on: false
		});
		var toggleComponent = $ele.data('zsToggle');
		expect(toggleComponent.isActive).toBeFalsy();
		expect(toggleComponent.options.caption.on).toBe('YES');
		expect(toggleComponent.options.caption.off).toBe('NO');
	});
	
	it('should toggle the state when clicked', function () {
		$ele.zsToggle();
		var toggleComponent = $ele.data('zsToggle');
		// By default, the toggle is initially ON
		expect(toggleComponent.isActive).toBeTruthy();
		
		$ele.find('.toggle-container').trigger('click');
		// Clicking on toggle should switch it to OFF
		expect(toggleComponent.isActive).toBeFalsy();
	});

	it('should support config for initial OFF value', function () {
		$ele.zsToggle({
			on: false
		});
		var toggleComponent = $ele.data('zsToggle');
		expect(toggleComponent.isActive).toBeFalsy();
		
		$ele.find('.toggle-container').trigger('click');
		expect(toggleComponent.isActive).toBeTruthy();
	});

	it('should get settings from ON attribute - true case', function () {
		$ele.attr('on', 'true');
		$ele.zsToggle({
			on: function () {
				if (this.$container.is('[on="true"]')) {
					return true;
				} else {
					return false;
				}
			}
		});
		var toggleComponent = $ele.data('zsToggle');
		expect(toggleComponent.isActive).toBeTruthy();
		
		$ele.find('.toggle-container').trigger('click');
		expect(toggleComponent.isActive).toBeFalsy();
	});

	it('should get settings from ON attribute - false case', function () {
		$ele.attr('on', 'false');
		$ele.zsToggle({
			on: function () {
				if (this.$container.is('[on="true"]')) {
					return true;
				} else {
					return false;
				}
			}
		});
		var toggleComponent = $ele.data('zsToggle');
		expect(toggleComponent.isActive).toBeFalsy();
		
		$ele.find('.toggle-container').trigger('click');
		expect(toggleComponent.isActive).toBeTruthy();
	});
	
	it('should reflect the correct state when it is initially hidden', function () {
		$('<div id="hiddenToggle" class="toggle-component" style="display:none"></div>').appendTo('body');
		var $element = $('#hiddenToggle');
		$element.zsToggle({
			on: false
		});
		var toggleComponent = $element.data('zsToggle');
		
		//verify isActive
		expect(toggleComponent.isActive).toBeFalsy();
		//verify appropriate class is applied
		expect($element.find('.toggle-inner').hasClass('active')).toBeFalsy();
		
		// It should reflect same styles when the component is made visible
		$element.css('display', 'block')
		expect(toggleComponent.isActive).toBeFalsy();
		expect($element.find('.toggle-inner').hasClass('active')).toBeFalsy();
		
		// Now clicking on the component should toggle these values
		$element.find('.toggle-container').trigger('click');
		expect(toggleComponent.isActive).toBeTruthy();
		expect($element.find('.toggle-inner').hasClass('active')).toBeTruthy();
		
		$element.remove();
	});
	
	afterEach(function () {
		$ele.remove();
	});
});
