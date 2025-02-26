describe("date field component", function () {
	it('is a custom element', function () {
		utils.isBehavior(expect, zs.dateField);		
		utils.isCustomElement(expect, 'zs-date-field', 'p', zs.dateFieldElement, HTMLParagraphElement);
	});

	it('Uses jquery plugin to render a calendar control', function (done) {
		var elem, plugin, flag;
		elem = new zs.dateFieldElement();
		elem.addEventListener('change', function () {
			flag = 'changed';
		});
		elem.addEventListener('configure', function () {
			plugin = $(elem.fieldElement).data('zsDatePicker');
			expect(plugin).toBeTruthy();
			plugin.setDate(new Date('2017-11-13'));			
		})
		
		document.body.appendChild(elem);						

		setTimeout(function () {
			expect(flag).toBe('changed');
			expect(elem.value).toBe(elem.dateToString(new Date('2017-11-13')));			
			document.body.removeChild(elem);
			done();
		},20);
	});


});