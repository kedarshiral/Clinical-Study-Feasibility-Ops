describe("zsProgress", function () {
	var $ele;

	beforeEach(function () {
		$('<div class="zs-progress" data-percent="40" style="height:1.5em"></div>').appendTo('body');
		$ele = $('.zs-progress');
	});

	it('Is a jQuery plugin', function () {
		utils.isPlugin(expect, 'zsProgress');
	});

	it('should create a progress bar component', function () {
		$ele.zsProgress();
		expect($ele.data('zsProgress')).toBeTruthy();
	});

	it('should create the component with default values', function () {
		$ele.zsProgress();
		var progressComponent = $ele.data('zsProgress');
		expect(progressComponent.options.showLabel).toBe(true);
		expect(progressComponent.options.duration).toBe(300);
		
	});

	it('should be possible to customize the component by passing optional parameters', function () {
		$ele.zsProgress({
			duration:500,
			showLabel:false
		});
		var progressComponent = $ele.data('zsProgress');
		expect(progressComponent.options.duration).toBe(500);
		expect(progressComponent.options.showLabel).toBe(false);
	});
	
	it('should set the progress value through function call', function () {
		$ele.zsProgress();
		var progressComponent = $ele.data('zsProgress');
		spyOn(progressComponent, 'setValue').and.callThrough();
		progressComponent.setValue(50);
		
		expect(progressComponent.value).toBe(50);
		expect(progressComponent.setValue).toHaveBeenCalled();
	});
	it('should get the progress value through function call', function () {
		$ele.zsProgress();
		var progressComponent = $ele.data('zsProgress');
		spyOn(progressComponent, 'getValue').and.callThrough();
		progressComponent.setValue(48);
		
		expect(progressComponent.getValue()).toBe(48);
		expect(progressComponent.getValue).toHaveBeenCalled();
	});

	it('should callback a function when value is set', function () {
		$ele.zsProgress();

		var progressComponent = $ele.data('zsProgress');
		spyOn(progressComponent.options, 'onChange').and.callThrough();
		progressComponent.setValue(30);
		
		expect(progressComponent.options.onChange).toHaveBeenCalled();
	});
	it('should hide the percent label if it\'s text does not fit in progress bar', function () {
		jQuery.fx.off = true

		$ele.zsProgress({
			showLabel:true
		});
		
		var progressComponent = $ele.data('zsProgress');
	
		progressComponent.setValue(50);
		expect($ele.find('.progress-value').css('display')).toBe('block');
		progressComponent.setValue(0);
		expect($ele.find('.progress-value').css('display')).toBe('none');
		progressComponent.setValue(100);
		expect($ele.find('.progress-value').css('display')).toBe('block');
	});
	afterEach(function () {
		$ele.remove();
	});
});
