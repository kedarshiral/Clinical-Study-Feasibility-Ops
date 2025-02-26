describe("dom helpers", function () {
	var el;
	beforeEach(function() {
		el = document.createElement('div');
		Object.assign(el, zs.domHelper);
		document.body.appendChild(el);
	});

	it('is a behavior', function() {
		utils.isBehavior(expect, zs.domHelper);
	});

	xit('closestParent', function() { // TODO

	});

	it("debounce", function (done) {
		var calls = 0, timeout;
		expect(typeof el.debounce).toBe('function');
		var test = function() {
			calls++;
		};

		// Immediate call
		var deTest = el.debounce(test);
		deTest();
		expect(calls).toBe(1);

		// Debounce 10 calls
		deTest = el.debounce(test, 100);
		for (var i=0;i<10;i++) {
			timeout = deTest();
		}
		expect(calls).toBe(1);
		setTimeout(function() {
			clearTimeout(timeout);
			expect(calls).toBe(2);
			done();
		},100);
	});
	
	it("fire, listen", function (done) {
		expect(typeof el.fire).toBe('function');
		expect(typeof el.listen).toBe('function');
		el.listen('test', function(event) {
			expect(event.detail).toBe(1);
			done();
		});
		el.fire('test', 1);				
	});

	it('attr', function() {
		expect(typeof el.attr).toBe('function');
		el.attr('test', 1);
		expect(el.getAttribute('test')).toBe('1');
		expect(el.attr('test')).toBe('1');
	});

	it('html', function() {
		expect(typeof el.html).toBe('function');
		el.html('<b>123</b>');
		expect(el.innerHTML).toBe('<b>123</b>');
		expect(el.html()).toBe('<b>123</b>');
	});

	it('empty', function() {
		expect(typeof el.empty).toBe('function');
		el.html('<b>123</b>567');
		expect(el.firstChild).toBeTruthy();
		el.empty();
		expect(el.firstChild).toBeFalsy();
		expect(el.innerHTML).toBeFalsy();
	});

	it('find', function() {
		var el1 = document.createElement('span');
		var el2 = document.createElement('span');
		el.appendChild(el1);
		el.appendChild(el2);
		expect(typeof el.find).toBe('function');
		expect(el.find('span')).toBe(el1);
		expect(el.find('span', true).length).toBe(2);
	});

	it('parse', function() {
		expect(typeof el.parse).toBe('function');
		var el1 = el.parse('<b>test</b>');
		expect(el1.innerHTML).toBe('<b>test</b>');
		var el2 = el.parse('<b>test</b>', 'span');
		expect(el2.tagName).toBe('SPAN');
	});

	afterEach(function() {
		el.parentNode.removeChild(el);
	});
	
});