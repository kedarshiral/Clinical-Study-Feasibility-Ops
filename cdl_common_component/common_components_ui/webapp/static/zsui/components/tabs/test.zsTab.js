describe('zsTab', function () {
	var el, tabEle;

	beforeEach(function () {
		el = new zs.tabsContainerElement();
		tabEle = new zs.tabElement();
		el.appendChild(tabEle);
	});

	it('is a custom element', function () {
		utils.isCustomElement(expect, null, 'zs-tabs-container', zs.tabsContainerElement, HTMLElement);
		utils.isBehavior(expect, zs.tabsContainer);

		utils.isCustomElement(expect, 'zs-tab', 'li', zs.tabElement, HTMLLIElement);
		utils.isBehavior(expect, zs.tab);
	});

	it('should render the ul tag as tabsContainer', function (done) {
		expect(el.tabsContainer).toBe(null);
		document.body.appendChild(el);

		setTimeout(function () { // IE11
			expect(el.tabsContainer.tagName).toBe('UL');
			done();
		});
	});

	it('should render a tag as anchorEle', function (done) {
		expect(tabEle.anchorEle).toBe(null);
		document.body.appendChild(el);

		setTimeout(function () { // IE11
			expect(tabEle.anchorEle.tagName).toBe('A');
			done();
		});
	});

	it('should observe attribute changes', function (done) {
		expect(tabEle.tabId).toBe('');
		expect(tabEle.isActive).toBe(false);
		document.body.appendChild(el);

		tabEle.setAttribute('tab-id', 'tab1');
		tabEle.setAttribute('active', '');

		setTimeout(function () { // IE11
			expect(tabEle.tabId).toBe('tab1');
			expect(tabEle.isActive).toBe(true);
			done();
		});
	});

	it('should navigate to respective tab when clicked on it', function (done) {
		var tab2 = new zs.tabElement();
		tab2.tabId = 'tab2';
		el.appendChild(tab2);
		var panel = document.createElement('div');
		panel.setAttribute('source-id', 'tab2');
		panel.style.display = 'none';
		el.appendChild(panel);
		document.body.appendChild(el);
		tabEle.isActive = true;

		spyOn(el, 'navigateTo').and.callThrough();

		setTimeout(function () { // IE11
			el.tabsContainer.appendChild(tabEle);
			tab2.anchorEle.click();

			setTimeout(function () { // yield to click
				expect(el.navigateTo).toHaveBeenCalledWith(tab2);
				expect(tab2.isActive).toBeTruthy();
				expect(tabEle.isActive).toBeFalsy();
				expect(panel.style.display).toBe('block');
				done();
			}, 0);
		});
	});

	afterEach(function () {
		if (el && el.parentNode) { document.body.removeChild(el); }
	});

});
