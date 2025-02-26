describe("zsHighlight", function () {
	var $container, $target;

	beforeEach(function () {
		$container = $('<div test>My keyword. Test test test. Check Spl<i>itted</i>.</div>');
		$container.appendTo('body');		
	});

	it('Is a jQuery utility method', function () {
		expect($.zs).toBeTruthy();
		expect(typeof $.zs.highlight).toBe('function');
	})

	it('Can highlight found keyword in the text fragment', function () {
		var keyword = 'My keyword';
		$.zs.highlight(keyword,'[test]');
		expect($container.find('mark').length).toBe(1);
	});

	it('Can lowlight', function () {
		var keyword = 'My keyword';
		$.zs.highlight(keyword,'[test]');
		$.zs.lowlight('[test]');
		expect($container.find('mark').length).toBe(0);
	});

	it('Highlights all found matches', function () {
		var keyword = 'test';
		$.zs.highlight(keyword,'[test]');
		expect($container.find('mark').length).toBe(3);
	});

	it('Highlights splitted with tags words', function () {
		var keyword = 'Splitted';
		$.zs.highlight(keyword,'[test]');
		expect($container.find('mark').length).toBe(1);
	});	

	afterEach(function () {
		if ($container.length) {
			$container.remove();	
		}	
	});
});
