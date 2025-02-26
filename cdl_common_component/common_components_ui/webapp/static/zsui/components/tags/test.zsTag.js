describe('zsTag', function () {
	var el;

	beforeEach(function () {
		el = new zs.tagElement();
	});

	it('is a custom element', function () {
		utils.isCustomElement(expect, 'zs-tag', 'span', zs.tagElement, HTMLSpanElement);
		utils.isBehavior(expect, zs.tag);
	});

	it('should create a dismissible tag', function (done) {
		el.dismissable = true;
		document.body.appendChild(el);

		setTimeout(function () {
			expect(el.dismissEle).toBeDefined();
			done();
		}, 10);
	});

	it('should create a non dismissible tag', function (done) {
		document.body.appendChild(el);

		setTimeout(function () {
			expect(el.dismissEle).not.toBeDefined();
			done();
		}, 10);
	});

	it('should observe attribute changes', function (done) {
		document.body.appendChild(el);

		el.setAttribute('value', 'tag1');
		el.setAttribute('dismissable', '');
		el.setAttribute('nonclickable', '');

		setTimeout(function () {
			expect(el.value).toBe('tag1');
			expect(el.dismissable).toBeTruthy();
			expect(el.nonclickable).toBeTruthy();
			done();
		}, 10);
	});

	it('should call render only once for multiple attribute changes', function (done) {
		document.body.appendChild(el);
		spyOn(el, 'render');

		el.setAttribute('value', 'tag1');
		el.setAttribute('dismissable', '');
		el.setAttribute('nonclickable', '');

		setTimeout(function () {
			expect(el.render).toHaveBeenCalledTimes(1);
			done();
		}, 10);
	});

	it('should handle dismiss action', function (done) {
		el.dismissable = true;
		el.handleDismiss = function () { };
		spyOn(el, 'handleDismiss');
		document.body.appendChild(el);

		setTimeout(function () {
			el.dismissEle.click();

			setTimeout(function () {
				expect(el.handleDismiss).toHaveBeenCalled();
				done();
			}, 0);
		}, 10);
	});

	afterEach(function () {
		if (el && el.parentNode) { document.body.removeChild(el); }
	});

});
