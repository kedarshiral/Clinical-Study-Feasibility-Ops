
describe("Loading", function () {
	var el;
	var MyElement = zs.customElement(HTMLElement, 'my-loading-element', null, zs.loading);

	beforeEach(function() {		
		el = new MyElement();
		document.body.appendChild(el);
	});

	it('is a behavior', function () {
		utils.isBehavior(expect, zs.loading);		
	});

	it('Shows and hides spinner', function () {
		expect(typeof el.loadingShowSpinner).toBe('function');
		el.loadingShowSpinner();
		expect(el.getAttribute('class')).toBe('zs-loading');
		expect(typeof el.loadingHideSpinner).toBe('function');
		el.loadingHideSpinner();
		expect(el.getAttribute('class')).toBeFalsy();
	});

	it('Can show global spinner with an overlay', function() {
		expect(typeof el.loadingShowGlobalSpinner).toBe('function');
		el.loadingShowGlobalSpinner();
		expect(document.querySelector('.zs-overlay')).toBeTruthy();
		el.loadingShowGlobalSpinner(); // reuse overlay
		expect(document.querySelectorAll('.zs-overlay').length).toBe(1);
		el.loadingHideSpinner();
		expect(document.querySelector('.zs-overlay').style.display).toBe('none');
	});

	it('is smart about when to show spinners', function(done) {
		el.loadingWait1 = 10;
		el.loadingWait2 = 20;
		el.loadingChange(true);
		expect(el.getAttribute('class')).toBeFalsy();
		expect(document.querySelector('.zs-overlay')).toBeFalsy();
		setTimeout(function() {
			expect(el.getAttribute('class')).toBeTruthy();
		},11);

		setTimeout(function() {
			expect(document.querySelector('.zs-overlay')).toBeTruthy();
			done();
		},21);
		
	});

	it('can be triggered via attribute "loading"', function(done) {
		el.loadingWait1 = 0;
		el.setAttribute('loading','on');		
		setTimeout(function() {
			expect(el.getAttribute('class')).toBeTruthy();
			el.removeAttribute('loading');
			setTimeout(function() {
				expect(el.getAttribute('class')).toBeFalsy();
				done();
			},10);			
		},10);		
	});

	afterEach(function () {
		el.parentNode.removeChild(el);
		var overlay = document.querySelector('.zs-overlay');
		if (overlay) {
			overlay.parentNode.removeChild(overlay);
		}
	});
});