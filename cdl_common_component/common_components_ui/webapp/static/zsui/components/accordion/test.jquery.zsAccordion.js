describe("zsAccordion", function () {
	var $ele;

	beforeEach(function () {
		$('<div class="zs-accordion"><div class="zs-accordion-bar"><span><a>Accordion Panel</a></span></div><div class="zs-accordion-panel"><p>Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. </p></div></div>').appendTo('body');
		$ele = $('.zs-accordion');
	});

	it('Is a jQuery plugin', function () {
		utils.isPlugin(expect, 'zsAccordion');
	});

	it('should create a accordion component', function () {
		$ele.zsAccordion();
		expect($ele.data('zsAccordion')).toBeTruthy();
	});

	it('should create the component with default values', function () {
		$ele.zsAccordion();
	
		var accordionComponent = $ele.data('zsAccordion');
		expect(accordionComponent.options.isOpen).toBe(false);
			
	});

	it('should be possible to customize the component by passing optional parameters', function () {
		$ele.zsAccordion({
			isOpen:true			
		});
		var accordionComponent = $ele.data('zsAccordion');
		expect(accordionComponent.options.isOpen).toBe(true);
		expect(accordionComponent.$panel.css("display")).toBe("block");		
	});
	
	it('should update isOpen variable as per open state of accordion', function () {
		$ele.zsAccordion({
			isOpen:false			
		});
		var accordionComponent = $ele.data('zsAccordion');
		expect(accordionComponent.options.isOpen).toBe(false);	
		expect(accordionComponent.$panel.css("display")).not.toBe("block");		
	
	});
	
	it('should open accordion panel through function call', function () {
		$ele.zsAccordion({
			isOpen:false			
		});
		var accordionComponent = $ele.data('zsAccordion');
		
		spyOn(accordionComponent, 'openPanel').and.callThrough();

		accordionComponent.openPanel();
		
		expect(accordionComponent.options.isOpen).toBe(true);

		expect(accordionComponent.$panel.is(":visible")).toBe(true);
	});
	
	it('should close accordion panel through function call', function () {
		$ele.zsAccordion({
			isOpen:true			
		});
		var accordionComponent = $ele.data('zsAccordion');
		
		spyOn(accordionComponent, 'closePanel').and.callThrough();

		expect(accordionComponent.$panel.is(":visible")).toBe(true);
		
		accordionComponent.closePanel();
		
		expect(accordionComponent.options.isOpen).toBe(false);

		expect(accordionComponent.$panel.is(":visible")).toBe(false);
	});

	afterEach(function () {
		$ele.remove();
	});
});
