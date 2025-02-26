describe('Highlight', function () {
	var el;
	it('is a behavior', function() {
		utils.isBehavior(expect, zs.highlight);
	});

	beforeEach(function () {
		el = document.createElement('div');
		el2 = document.createElement('div');
		Object.assign(el, zs.highlight); // we can do that just because it is a flat behavior.
		el.innerHTML = 'My keyword. Test test test. Check Spl<i>itted</i>';
		el2.innerHTML = 'My keyword. Test test test. Check Spl<i>itted</i>';
		document.body.appendChild(el);
		document.body.appendChild(el2);
	});

	it('Can highlight found keyword in the text fragment', function () {
		var keyword = 'My keyword';
		var keyword2 = 'Check';
		expect(typeof el.highlight).toBe('function');
		el.highlight(keyword);
		var mark = el.querySelector('mark');
		expect(mark).toBeTruthy();
		expect(mark.innerHTML).toBe(keyword);

		el.highlight.call(el2, keyword2);
		mark = el2.querySelector('mark');
		expect(mark).toBeTruthy();
		expect(mark.innerText).toBe(keyword2);
	});

	it('Can lowlight', function () {
		var keyword = 'My keyword';
		el.highlight(keyword);
		el.lowlight();
		var mark = el.querySelector('mark');
		expect(mark).toBeFalsy();
	});

	it('Highlights all found matches', function () {
		var keyword = 'test';
		el.highlight(keyword);
		var marks = el.querySelectorAll('mark');
		expect(marks.length).toBe(3);
	});

	it('Highlights splitted with tags words', function () {
		var keyword = 'Splitted';
		el.highlight(keyword);
		var mark = el.querySelector('mark');
		expect(mark).toBeTruthy();
	});	

	it('Can by case sensitive', function () {
		var keyword = 'check';
		el.highlight(keyword, true);		
		var mark = el.querySelector('mark');
		expect(mark).toBeFalsy();
		el.highlight(keyword, false);		
		var mark = el.querySelector('mark');
		expect(mark).toBeTruthy();

	});

	afterEach(function () {
		el.parentNode.removeChild(el);
		el2.parentNode.removeChild(el2);
	});
});