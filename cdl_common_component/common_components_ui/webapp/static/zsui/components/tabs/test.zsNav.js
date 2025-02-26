describe('zsNav', function () {
	var el, config = [{
			label: 'item1',
			url: '#item1'
		},
		{
			label: 'item2',
			url: '#item2'
		},
		{
			label: 'item3',
			url: '#item3',
			navigation: [
				{
					label: 'item31',
					url: '#item31'
				},
				{
					label: 'item32',
					url: '#item32'
				},
				{
					label: 'item33',
					url: '#item33'
				}
			]
		}];

	beforeEach(function(done) {
		el = new zs.topNavigationElement();
		document.body.appendChild(el);
		setTimeout(function(){
			done(); // IE11 wait to get URL updated.
		},10);
	});

	it('is a custom element', function () {
		utils.isCustomElement(expect, null, 'zs-top-navigation', zs.topNavigationElement, HTMLElement);
		utils.isBehavior(expect, zs.topNavigation);
	});

	it('builds navigation items from a configuration', function(done) {
		el.configure({
			navigation: config
		});

		// First level
		expect(el.querySelectorAll('li').length).toBe(6);
		expect(el.querySelectorAll('li')[1].firstChild.innerHTML).toBe('item2');
		expect(el.querySelectorAll('li')[1].firstChild.getAttribute('href')).toBe('#item2');
		

		// Submenu
		var li3 = el.querySelectorAll('li')[2];
		var nav = li3.querySelector('nav');
		expect(nav).toBeTruthy();
		expect(nav.style.display).toBe('none');
		done();		
	});

	it('supports compact mode', function(done) {
		el.configure({
			navigation: config
		});
		el.setAttribute('compact', true);

		// First level
		setTimeout(function() {
			var select = el.querySelector('select');

			expect(select).toBeTruthy();
			expect(select.options.length).toBe(6);
			expect(select.children[3].innerHTML).toBe('-- item31');
			// TODO: can't programmatically trigger change without actually creating manually a change event.

			done();
		},10);

	});

	afterEach(function() {
		document.body.removeChild(el);
	});

});
