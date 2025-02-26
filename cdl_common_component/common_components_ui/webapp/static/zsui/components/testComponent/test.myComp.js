describe('Custom elements multiple re-render issue', function () {
	var el;

	beforeEach(function () {
		el = new zs.myCompEle();
	});

	it('is a custom element', function () {
		utils.isCustomElement(expect, null, 'my-comp', zs.myCompEle, HTMLElement);
		utils.isBehavior(expect, zs.myComp);
	});

	it('should not render twice for attach and attribute change events', function (done) {
		utils.isRenderedOnce(el, {'myattr1': ''}, expect, done, spyOn);
	});

	it('should not render multiple times for every attribute change', function (done) {
		utils.isRenderedOnce(el, {'myattr1': '', 'myattr2': '', 'myattr3': ''}, expect, done, spyOn);
	});

	it('should be possible to render synchronously - using debounceDelay', function (done) {
		spyOn(el, 'render').and.callThrough();
		el.debounceDelay = null;
		el.setAttribute('myattr1', '');
		document.body.appendChild(el);

		setTimeout(function () {
			// For synchronous calls, it would be rendered twice
			expect(el.render).toHaveBeenCalledTimes(2);
			done();
		}, 10);
	});

	it('should be possible to render synchronously - using blockAttachRender', function (done) {
		spyOn(el, 'render').and.callThrough();
		el.blockAttachRender = true;
		el.setAttribute('myattr1', '');
		document.body.appendChild(el);

		setTimeout(function () {
			// For synchronous calls with blockAttachRender, it would not render on attach
			expect(el.render).toHaveBeenCalledTimes(1);
			done();
		}, 10);
	});


	afterEach(function () {
		if (el && el.parentNode) { document.body.removeChild(el); }
	});

});
