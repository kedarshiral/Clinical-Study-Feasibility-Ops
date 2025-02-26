describe("zsSearchDropdown", function () {
	var $select, $parent;

	function $isThing($elem) {
		if ($elem && $elem.length) {
			return true;
		}
		return false;
	}

	beforeEach(function () {
		$parent = $('<div class="zs-select"><select zs></select></select></div>');
		$parent.appendTo('body');
		$select = $parent.find('>select');
	});

	describe('In general', function() {
		it('Is a jQuery plugin', function () {
			//utils.isPlugin(expect, 'zsSearchDropdown'); // TODO: have issues for this plugin
			expect(true).toBeTruthy();
		});

		it('Creates a searchable dropdown element based on the provided options', function () {

			$select.zsSearchDropdown({
				text: 1
			});
			
			var plugin = $select.data('zsSearchDropdown');
			expect(plugin).toBeTruthy();
			expect($isThing(plugin.$select)).toBeTruthy();
			expect($select.data('zsSearchDropdown')).toBeTruthy();
			expect($isThing($('body').find('[overlay]'))).toBeTruthy();				
			expect($isThing($('body').find('[container]'))).toBeTruthy();
			expect($isThing($('body').find('[options]'))).toBeTruthy();
			expect($isThing($('body').find('[input]'))).toBeTruthy();
			expect(plugin.options).toBeTruthy();
			expect(plugin.options.text).toBe(1);
					
		});
		
		it('Can be reconfigured', function () {
			$select.zsSearchDropdown({
				a: 1,
				b: 2
			});
			
			var plugin = $select.data('zsSearchDropdown');
			
			expect(plugin.options.a).toBe(1);
			expect(plugin.options.b).toBe(2);
			
			$select.zsSearchDropdown({
				b: 3
			});
		
			expect(plugin.options.a).toBe(1);
			expect(plugin.options.b).toBe(3);		
			
		});
		
		it('Can be removed', function() {
			$select.zsSearchDropdown();
			var plugin = $select.data('zsSearchDropdown');
			$select.zsSearchDropdown('destroy');
			plugin = null;
			expect($isThing($('body').find('[overlay]'))).toBeFalsy();				
			expect($isThing($('body').find('[container]'))).toBeFalsy();
			expect($select.data('zsSearchDropdown')).toBeFalsy();
			
			// Remove itself when select is destroyed
			if (typeof module === 'undefined' || !module.exports) { // because jsdom doesnt support special DOM events
				$select.zsSearchDropdown();			
				$select.remove();		
				$select = null;
				expect($isThing($('body').find('[container]'))).toBeFalsy();		
			}
		});
		
		it('Opens on a select click and closes on a click anywhere', function () {
			$select.zsSearchDropdown();
			var plugin = $select.data('zsSearchDropdown');
			plugin.$overlay.click();
			expect(plugin.$container.is(':visible')).toBeTruthy();
			$(document).click();
			expect(plugin.$container.is(':visible')).toBeFalsy();
		});
		
		it('Reads the options renders them and can update them', function() {
			$select.html('<option value="1">1</option><option value="2">2</option>');
			$select.zsSearchDropdown();
			var plugin = $select.data('zsSearchDropdown');
			expect($.isArray(plugin.data)).toBeTruthy();
			expect(plugin.data.length).toBe(2);
			expect(plugin.data[0].text).toBe('1');
			expect(plugin.data[0].value).toBe('1');
			expect(plugin.data[0].id).toBe(0);
			expect($('[options]').children().length).toBe(2);
			
			expect(plugin.data[1].text).toBe('2');
			expect(plugin.data[1].value).toBe('2');
			expect(plugin.data[1].id).toBe(1);
			
			$select.append('<option value="3">3</option>');
			$select.zsSearchDropdown('update');
			expect(plugin.data.length).toBe(3);
			expect(plugin.data[2].text).toBe('3');
			expect(plugin.data[2].value).toBe('3');
			expect(plugin.data[2].id).toBe(2);
			expect($('[options]').children().length).toBe(3);				
		});	
		
		it('Activates a selected option', function() {
			$select.html('<option value="1">1</option><option value="2">2</option>');
			$select[0].selectedIndex = 1;
			$select.zsSearchDropdown();
			
			var plugin = $select.data('zsSearchDropdown');
			plugin.open();
			var $active = $('body').find('[active]');
			expect($isThing($active)).toBeTruthy();
			expect($active.attr('index')).toBe('1');
			
			$select[0].selectedIndex = 0;
			$select.zsSearchDropdown('update');	
			$active = $('body').find('[active]');
			expect($isThing($active)).toBeTruthy();
			expect($active.attr('index')).toBe('0');			
		});	
		
		it('Supports selecting an option with a cursor keys and enter', function () {
			$select.html('<option value="1">1</option><option value="3">3</option><option value="4">4</option><option value="5">5</option><option value="6">6</option><option value="7">7</option><option value="8">8</option>');
			$select.zsSearchDropdown();				
			var plugin = $select.data('zsSearchDropdown');
			plugin.open();
			
			// Down once
			var e = $.Event('keyup');
			e.keyCode = 40; // down 
			plugin.$input.trigger(e);
			var $selected = plugin.$container.find('[hover]');
			expect($isThing($selected)).toBeTruthy();
			expect($selected.attr('index')).toBe('0');
			
			// Down twice
			e = $.Event('keyup');		
			e.keyCode = 40; // down
			plugin.$input.trigger(e);
			$selected = plugin.$container.find('[hover]');
			expect($isThing($selected)).toBeTruthy();
			expect($selected.attr('index')).toBe('1');
			
			// Stop moving Down
			e = $.Event('keyup');		
			e.keyCode = 40; // down
			plugin.$input.trigger(e);
			$selected = plugin.$container.find('[hover]');
			expect($isThing($selected)).toBeTruthy();
			expect($selected.attr('index')).toBe('2');

			
			// Up once
			e = $.Event('keyup');		
			e.keyCode = 38;
			plugin.$input.trigger(e);
			$selected = plugin.$container.find('[hover]');
			expect($isThing($selected)).toBeTruthy();
			expect($selected.attr('index')).toBe('1');

			// Stop moving up		
			e = $.Event('keyup');		
			e.keyCode = 38;
			plugin.$input.trigger(e);
			$selected = plugin.$container.find('[hover]');
			expect($isThing($selected)).toBeTruthy();
			expect($selected.attr('index')).toBe('0');		
			
			// Down once to select second one and then choose it
			expect($select[0].selectedIndex).toBe(0);
			e = $.Event('keyup');
			e.keyCode = 40; // down 
			plugin.$input.trigger(e);
			e = $.Event('keydown');
			e.keyCode = 13; // Enter
			plugin.$input.trigger(e);
			var $active = plugin.$container.find('[active]');
			expect($isThing($active)).toBeTruthy();
			expect($active.attr('index')).toBe('1');
			expect($select[0].selectedIndex).toBe(1);
		});
		
		it('Supports mouse events to select an option', function () {
			$select.html('<option value="1">1</option><option value="2">2</option>');
			$select.zsSearchDropdown();				
			var plugin = $select.data('zsSearchDropdown');
			plugin.open();
			plugin.$nav.children().last().trigger('mouseover');
			$selected = plugin.$container.find('[hover]');
			expect($isThing($selected)).toBeTruthy();
			expect($selected.attr('index')).toBe('1');
			expect($select[0].selectedIndex).toBe(0);
			$selected.trigger('click');
			expect($selected.attr('index')).toBe('1');
			expect($select[0].selectedIndex).toBe(1);
		});

		it('Can search for options and highlight entered keyword', function () {
			$select.html('<option value="1">1</option><option value="2">2</option>');
			$select.zsSearchDropdown();		
			var plugin = $select.data('zsSearchDropdown');
			var $input = plugin.$container.find('[input]');
			var $options = plugin.$container.find('[options]');
			plugin.open();
			$input.val('2');
			$input.keyup();
			expect($options.children(':visible').length).toBe(1);
			var $highlight = $('[highlight],mark');
			expect($isThing($highlight)).toBeTruthy();
			expect($highlight.html()).toBe('2');
		});

		it('should react on "change" event on input', function () {
			// we have to stop initial render because plugin binds event handlers in render 
			// we need to give jasmine posibility to replace initial onKeyUp handler before it will be bound
			// additional info here http://stackoverflow.com/questions/18321993/how-to-test-that-a-function-has-been-called-after-an-event-was-fired
			// @todo consider remove actual logic from constructor
			$select.zsSearchDropdown({
				noRender: true
			});

			var plugin = $select.data('zsSearchDropdown');
			spyOn(plugin, 'onKeyUp');
			
			plugin.render();
			plugin.$input.trigger('change');
			expect(plugin.onKeyUp).toHaveBeenCalled();
		});

		it('should update overlay text after changing', function(){
			$select.html('<option value="1">Text 1</option><option value="2" selected>Text 2</option><option value="3">Text 3</option>');
			$select.zsSearchDropdown();		
			var plugin = $select.data('zsSearchDropdown');

			expect(plugin.$overlay.text()).toBe('Text 2');

			plugin.$nav.find('a').first().trigger('click');
			expect(plugin.$overlay.text()).toBe('Text 1');
		});

		it('should scroll the window if container doesn\'t fit visible area on iPad only', function(done){
			$select.html('<option value="1">Text 1</option><option value="2" selected>Text 2</option><option value="3">Text 3</option><option value="4">Text 4</option><option value="5">Text 5</option><option value="6">Text 6</option><option value="7">Text 7</option>');
			$select.zsSearchDropdown();		
			var plugin = $select.data('zsSearchDropdown');
			$(window).height(1000);
			plugin.$container.css({ height: $(window).outerHeight()});
			plugin.open();
			plugin.$input.focus();
			
			var offset = parseInt(plugin.$input.offset().top - 20);
			if(isNaN(plugin.$input.offset().top)){ // for node jasmine
				offset = 0;
			}

			setTimeout(function(){
				if (window.navigator && window.navigator.userAgent.match(/iPad/i)){
					expect(parseInt($(document).scrollTop())).toEqual(offset);
				}else{
					expect(parseInt($(document).scrollTop())).toEqual(0);
				}
				
				$(window).height('auto');
				done();
			},2000); 
		});

		it('should support empty values', function(){
			$select.html('<option value="">Empty value</option><option value="">Empty value 1</option><option value="3">Text 3</option>');
			$select.zsSearchDropdown();		
			var plugin = $select.data('zsSearchDropdown');

			expect(plugin.$nav.find('a').length).toEqual(3);
		});

		it('should select first item by default if nothing other is selected (as native element does)', function(){
			$select.html('<option value="1">Text 1</option><option value="2">Text 2</option><option value="3">Text 3</option>');
			$select.zsSearchDropdown();		
			var plugin = $select.data('zsSearchDropdown');

			expect(plugin.$select.val()).toEqual('1');
		});

		it('should highlight preselected default value', function(){
			$select.html('<option value="1">Text 1</option><option value="2">Text 2</option><option value="3">Text 3</option>');
			$select.zsSearchDropdown();		
			var plugin = $select.data('zsSearchDropdown');

			expect(plugin.$overlay.text()).toEqual('Text 1');
		});
	});

	describe('Always open mode', function() {
		it('Stays open', function() {
			$select.html('<option value="1">1</option><option value="2">2</option>');
			$select.zsSearchDropdown({
				alwaysOpen: true
			});		
			var plugin = $select.data('zsSearchDropdown');
			expect(plugin.options.minOptionsForSearch).toBe(6); // Check an additional option
			expect(plugin.$container.is(':visible')).toBeTruthy();
			$selected = plugin.$container.find('[index="1"]');
			$selected.trigger('click');
			expect(plugin.$container.is(':visible')).toBeTruthy(); // Stays open after
		});
	});

	describe('Multiple mode', function() {
		it('should stay open when option is choosen', function() {
			$select.html('<option value="1">1</option><option value="2">2</option>');
			$select.zsSearchDropdown({
				multiple: true
			});		
			var plugin = $select.data('zsSearchDropdown');
			plugin.open();
			expect(plugin.options.minOptionsForSearch).toBe(6); // Check an additional option
			expect(plugin.$container.is(':visible')).toBeTruthy();
			$selected = plugin.$container.find('[index="1"]');
			$selected.trigger('click');
			expect(plugin.$container.is(':visible')).toBeTruthy(); // Stays open after
		});

		it('should fill in overlay with coma-separated items when they are choosen', function() {
			$select.html('<option value="1">Text 1</option><option value="2">Text 2</option><option value="3">Text 3</option>');			
			$select.zsSearchDropdown({
				multiple: true
			});		
			
			var plugin = $select.data('zsSearchDropdown');
			plugin.$container.find('[index="0"]').trigger('click');
			plugin.$container.find('[index="1"]').trigger('click');
			plugin.$container.find('[index="2"]').trigger('click');

			expect(plugin.$overlay.text()).toEqual('Text 1, Text 2, Text 3');
		});

		it('should not clean search input when item is selected', function() {
			$select.html('<option value="1">1</option><option value="2">2</option><option value="3">3</option>');
			$select.zsSearchDropdown({
				multiple: true
			});		
			
			var plugin = $select.data('zsSearchDropdown');
			plugin.$input.val('search text');
			plugin.$container.find('[index="0"]').trigger('click');
			plugin.$container.find('[index="1"]').trigger('click');
			plugin.$container.find('[index="2"]').trigger('click');

			expect(plugin.$input.val()).toEqual('search text');

		});

		it('should highlight selected items if there are preselected options', function() {
			// when 2 preselected items
			$select.html('<option value="1">1</option><option value="2" selected>2</option><option value="3" selected>3</option>');
			$select.zsSearchDropdown({
				multiple: true
			});		

			expect($select.data('zsSearchDropdown').$nav.find('[active]').length).toEqual(2);
			$select.data('zsSearchDropdown').destroy();

			// when 1 preselected items
			$select.html('<option value="1">1</option><option value="2">2</option><option value="3" selected>3</option>');
			$select.zsSearchDropdown({
				multiple: true
			});		
			
			expect($select.data('zsSearchDropdown').$nav.find('[active]').length).toEqual(1);
			$select.data('zsSearchDropdown').destroy();

			// when 0 preselected items
			$select.html('<option value="1">1</option><option value="2">2</option><option value="3">3</option>');
			$select.zsSearchDropdown({
				multiple: true
			});		
			
			expect($select.data('zsSearchDropdown').$nav.find('[active]').length).toEqual(0);
		});

		describe('Empty text', function() {
			it('should add empty text', function(){
				$select.zsSearchDropdown({
					emptyText: 'some text',
					multiple: true
				});	
				var plugin = $select.data('zsSearchDropdown');
				expect(plugin.$overlay.text()).toEqual('some text');
			});

			it('should add default empty text if no specific text provided', function(){
				$select.zsSearchDropdown({
					multiple: true
				});	
				var plugin = $select.data('zsSearchDropdown');
				expect(plugin.$overlay.text()).toEqual('Select value');
			});
		});
	});

	describe('Checkboxes', function() {
		it('should add checkboxes by request', function(){
			$select.html('<option value="1">1</option><option value="2">2</option><option value="3">3</option>');
			$select.zsSearchDropdown({
				addCheckboxes: true
			});	

			var plugin = $select.data('zsSearchDropdown');
			expect(plugin.$container.find('[index="0"]').find('[type=checkbox]').length).toEqual(1);
		});

		it('should NOT add checkboxes by default', function(){
			$select.html('<option value="1">1</option><option value="2">2</option><option value="3">3</option>');
			$select.zsSearchDropdown();	

			var plugin = $select.data('zsSearchDropdown');
			expect(plugin.$container.find('[index="0"]').find('[type=checkbox]').length).toEqual(0);
		})
	});
		
	describe('Infinite scroll', function() {
		var plugin;
		beforeEach(function () {
			for(var i = 0; i < 150; i++){
				$select.append('<option value="' + i +'">' + i +'</option>');
			}
			$select.zsSearchDropdown();
			plugin = $select.data('zsSearchDropdown');

			plugin.$nav.css({
				width: 100,
				height: 100,
				overflow: 'auto'
			})
			plugin.$container.show();
			plugin.$nav.children('a').css({
				display:'block'
			});
		});

		it('should initially render only maxOptionsVisible items', function () {
			expect(plugin.$nav.children().length).toEqual(100);
		});

		it('should load more when scrolled down', function (done) {
			plugin.$nav.scrollTop(plugin.$nav.children('a:last').offset().top + plugin.$nav.children('a:last').height());
			plugin.$nav.trigger('scroll'); // for jsDOM emulation
			setTimeout(function () {
				expect(plugin.$nav.children().length).toEqual(150);
				done();
			},10); 
					
		});

		it('should load more when focused on last item', function (done) {
			plugin.$nav.children('a:last').focus();
			
			setTimeout(function () {
				expect(plugin.$nav.children().length).toEqual(150);
				done();
			},10); 
		});

		it('should reflect on maxOptionsVisible', function (done) {
			$select.zsSearchDropdown('destroy');
			$select.html('');
			for(var i = 0; i < 15; i++){
				$select.append('<option value="' + i +'">' + i +'</option>');
			}
			$select.zsSearchDropdown({
				maxOptionsVisible: 10
			});
			plugin = $select.data('zsSearchDropdown');

			plugin.$nav.css({
				width: 100,
				height: 100,
				overflow: 'auto'
			})
			plugin.$container.show();
			plugin.$nav.children('a').css({
				display:'block'
			});

			plugin.$nav.children('a:last').focus();
			
			setTimeout(function () {
				expect(plugin.$nav.children().length).toEqual(15);
				done();
			},10); 
		});
	});

	describe('Flippable mode', function() {
		
		it('fixes position of dropdown in flippable mode', function () {
			$select.zsSearchDropdown({
				flippable:true
			});
			
			var plugin = $select.data('zsSearchDropdown');
			plugin.open();
			expect($select.data('zsSearchDropdown')).toBeTruthy();
			expect($select.data('zsSearchDropdown').options.flippable).toBeTruthy();	
			expect($select.data('zsSearchDropdown').$container.css("position")).toBe("fixed");					
		});

		it('opens below field if space is available', function () {

			$select.zsSearchDropdown({
				flippable:true
			});
			
			var plugin = $select.data('zsSearchDropdown');
			
			spyOn(plugin, 'isInViewPort').and.callThrough();

			plugin.open();
			
			expect(plugin.isInViewPort).toHaveBeenCalled();

			expect(plugin).toBeTruthy();
			expect(plugin.options.flippable).toBeTruthy();		
			expect(plugin.$container.css("position")).toBe("fixed");
			expect(parseInt(plugin.$overlay.offset().top)).toEqual(parseInt(plugin.$container.offset().top));
			expect(parseInt(plugin.$overlay.offset().left)).toEqual(parseInt(plugin.$container.offset().left));
							
		});
		it('flips it\'s position to open above field if space isn\'t available below', function () {
			$select.html("<option></option><option></option><option></option><option></option><option></option><option></option>");
			
			$select.zsSearchDropdown({
				flippable:true
			});
							
			$parent.css({position:"fixed",bottom:"0"});

			var plugin = $select.data('zsSearchDropdown');
			spyOn(plugin, 'isInViewPort').and.callThrough();

			plugin.open();
			
			expect(plugin.isInViewPort).toHaveBeenCalled();

			expect(plugin).toBeTruthy();
			expect(plugin.options.flippable).toBeTruthy();		
			expect(plugin.$container.css("position")).toBe("fixed");
			expect(parseInt(plugin.$container.offset().top)).toBeLessThan(parseInt(plugin.$overlay.offset().top));
			expect(parseInt(plugin.$container.offset().left)).toEqual(parseInt(plugin.$overlay.offset().left));							
		});
	});

	describe('Select All feature', function() {
		it('should select all items when click on "select all" button', function(){
			$select.html('<option value="1">1</option><option value="2">2</option><option value="3">3</option>');
			$select.zsSearchDropdown({
				addSelectAll: true,
				multiple: true
			});	

			var plugin = $select.data('zsSearchDropdown');
			plugin.$nav.find('a').first().trigger('click');
			expect(Object.keys(plugin.selectedMap).length).toEqual(3);
		});

		it('should deselect all items when click on "select all" button second time', function(){
			$select.html('<option value="1">1</option><option value="2">2</option><option value="3">3</option>');
			$select.zsSearchDropdown({
				addSelectAll: true,
				multiple: true
			});	

			var plugin = $select.data('zsSearchDropdown');
			plugin.$nav.find('a').first().trigger('click');
			expect(Object.keys(plugin.selectedMap).length).toEqual(3);

			plugin.$nav.find('a').first().trigger('click');
			expect(Object.keys(plugin.selectedMap).length).toEqual(0);
		});

		it('should switch "select all" state to "deselect all" when all items selected and vise versa', function(){
			$select.html('<option value="1">1</option><option value="2">2</option><option value="3">3</option>');
			$select.zsSearchDropdown({
				addSelectAll: true,
				multiple: true
			});	

			var plugin = $select.data('zsSearchDropdown');

			expect(plugin._allSelected).toBeFalsy();

			plugin.$nav.find('a').first().trigger('click');
			expect(plugin._allSelected).toBeTruthy();

			plugin.$nav.find('a').first().trigger('click');
			expect(plugin._allSelected).toBeFalsy();
		});

		it('should show "select all" text on button when appropriate state and vise versa', function(){
			$select.html('<option value="1">1</option><option value="2">2</option><option value="3">3</option>');
			$select.zsSearchDropdown({
				addSelectAll: true,
				multiple: true
			});	

			
			var plugin = $select.data('zsSearchDropdown');
			expect(plugin.$nav.find('a').first().text()).toEqual(plugin.options.selectAllText);

			plugin.$nav.find('a').first().trigger('click');
			expect(plugin.$nav.find('a').first().text()).toEqual(plugin.options.deselectAllText);

			plugin.$nav.find('a').first().trigger('click');
			expect(plugin.$nav.find('a').first().text()).toEqual(plugin.options.selectAllText);
		});

		it('should turn on/off checkbox in "select all" button when all items selected/deselected', function(){
			$select.html('<option value="1">1</option><option value="2">2</option><option value="3">3</option>');
			$select.zsSearchDropdown({
				addCheckboxes: true,
				addSelectAll: true,
				multiple: true
			});	

			
			var plugin = $select.data('zsSearchDropdown');
			expect(plugin.$nav.find('a').first().find('input').prop('checked')).toBeFalsy();

			plugin.$nav.find('a').first().trigger('click');
			expect(plugin.$nav.find('a').first().find('input').prop('checked')).toBeTruthy();

			plugin.$nav.find('a').first().trigger('click');
			expect(plugin.$nav.find('a').first().find('input').prop('checked')).toBeFalsy();
		});

		it('should not render "select all" button on search' , function(){
			$select.html('<option value="1">one</option><option value="2">two</option><option value="3">three</option>');
			$select.zsSearchDropdown({
				addCheckboxes: true,
				addSelectAll: true,
				multiple: true
			});	

			
			var plugin = $select.data('zsSearchDropdown');
			plugin.$input.val('one');
			plugin.$input.keyup();

			expect(plugin.$nav.find('a').first().text()).toEqual('one');
		});

		it('should not render "select all" second time on infinite scroll' , function(done){
			var count = 150;
			for(var i = 0; i < count; i++){
				$select.append('<option value="' + i +'">' + i +'</option>');
			}
			$select.zsSearchDropdown({
				addCheckboxes: true,
				addSelectAll: true,
				multiple: true
			});
			var plugin = $select.data('zsSearchDropdown');

			plugin.$nav.css({
				width: 100,
				height: 100,
				overflow: 'auto'
			})
			plugin.$container.show();
			plugin.$nav.children('a').css({
				display:'block'
			});

			
			plugin.$nav.scrollTop(plugin.$nav.children('a:last').offset().top + plugin.$nav.children('a:last').height());
			plugin.$nav.trigger('scroll'); // for jsDOM emulation
			setTimeout(function () {
				var found = 0;
				plugin.$nav.children('a').each(function(){
					if($(this).text() === plugin.options.selectAllText){
						found++;
					}
				});

				expect(found).toEqual(1);
				done();
			},10); 
		});
	});

	afterEach(function () {
		if ($isThing($select) && $select.data('zsSearchDropdown')) {
			$select.zsSearchDropdown('destroy');
		}
		$parent.remove();		
	});
});
