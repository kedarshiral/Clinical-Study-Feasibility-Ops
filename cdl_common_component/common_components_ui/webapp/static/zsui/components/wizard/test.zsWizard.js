describe('zsWizard', function () {
	var el, chevron1, chevron2, panel1, panel2;

	beforeEach(function () {
		el = new zs.wizardElement();

		// Create chevrons
		chevron1 = new zs.tabElement();
		chevron1.tabId = '1';
		chevron1.classList.add('chevron');
		el.appendChild(chevron1);
		chevron2 = new zs.tabElement();
		chevron2.tabId = '2';
		chevron2.classList.add('chevron');
		el.appendChild(chevron2);

		// Create panels
		panel1 = document.createElement('div');
		panel1.setAttribute('source-id', '1');
		panel1.style.display = 'block';
		el.appendChild(panel1);
		panel2 = document.createElement('div');
		panel2.setAttribute('source-id', '2');
		panel2.style.display = 'none';
		el.appendChild(panel2);
	});

	it('is a custom element', function () {
		utils.isCustomElement(expect, null, 'zs-wizard', zs.wizardElement, zs.tabsContainerElement);
		utils.isBehavior(expect, zs.wizard);
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
		expect(chevron1.anchorEle).toBe(null);
		document.body.appendChild(el);

		setTimeout(function () { // IE11
			expect(chevron1.anchorEle.tagName).toBe('A');
			done();
		});
	});

	it('should observe attribute changes', function (done) {
		expect(el.isClickable).toBe(false);
		document.body.appendChild(el);
		el.setAttribute('clickable', '');

		setTimeout(function () { // IE11
			expect(el.isClickable).toBe(true);
			done();
		});
	});

	it('should navigate to respective chevron in clickable mode', function (done) {
		el.isClickable = true;

		// Mock closestParent function
		el.closestParent = function () {
			return chevron2;
		}
		document.body.appendChild(el);
		spyOn(el, 'navigateTo').and.callThrough();

		setTimeout(function () { // IE11
			chevron2.click();

			setTimeout(function () { // yield to click
				expect(el.navigateTo).toHaveBeenCalledWith(chevron2);
				expect(chevron2.isActive).toBeTruthy();
				expect(panel2.style.display).toBe('block');
				done();
			}, 0);
		});
	});

	it('should not allow navigation in non-clickable mode', function (done) {
		document.body.appendChild(el);
		spyOn(el, 'navigateTo').and.callThrough();

		setTimeout(function () { // IE11
			chevron2.click();

			setTimeout(function () { // yield to click
				expect(el.navigateTo).not.toHaveBeenCalledWith(chevron2);
				expect(chevron2.isActive).toBeFalsy();
				expect(panel2.style.display).toBe('none');
				done();
			}, 0);
		});
	});

	it('should be able to handle previous and next button click actions', function (done) {
		chevron1.isActive = true;
		document.body.appendChild(el);
		spyOn(el, 'navigateTo').and.callThrough();

		setTimeout(function () { // IE11
			el.tabsContainer.appendChild(chevron1);
			el.tabsContainer.appendChild(chevron2);
			el.handleNextAction();
			expect(el.navigateTo).toHaveBeenCalledWith(chevron2);
			expect(chevron2.isActive).toBeTruthy();
			expect(panel2.style.display).toBe('block');

			el.handlePrevAction();
			expect(el.navigateTo).toHaveBeenCalledWith(chevron1);
			expect(chevron1.isActive).toBeTruthy();
			expect(panel1.style.display).toBe('block');
			done();
		}, 0);
	});

	afterEach(function () {
		if (el && el.parentNode) { document.body.removeChild(el); }
	});

});
