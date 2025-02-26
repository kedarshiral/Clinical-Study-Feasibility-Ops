describe("datePicker", function () {
	var $input;

	beforeEach(function () {
	//	$('<div class="zs-input-icon zs-icon-calendar"><input type="text" class="zs-input"/></div>').appendTo('body');
		$('<div class="zs-input-icon"><input type="text" class="zs-input"/><a href="javascript:void(0)" class="zs-icon zs-icon-calendar"></a></div>').appendTo('body');
		$input = $('body > .zs-input-icon > input');
	});

	it('Is a jQuery pluging', function () {
		utils.isPlugin(expect, 'zsDatePicker');
	})

	it('Creates a calendar with 42 days based on the current date with a 7 rows of dates', function () {
		$input.zsDatePicker({});
		expect($input.data('zsDatePicker')).toBeTruthy();
		var $calendar = $input.parent().find('.zs-calendar');
		expect($calendar.length).toBeTruthy();
		expect($calendar.find('tr').length).toBe(8);
		expect($calendar.find('td').length).toBe(42);
		
		/*
		var now = new Date();
		testDate = new Date(now.getFullYear(), now.getMonth(), 1);
		expect($calendar.find('td').first().attr('date')).toBe(testDate.toString());		
		*/
	});

	it('Supports a min date and a max date options', function () {
		var now = new Date();
		var minDate = new Date();
		var maxDate = new Date();

		minDate.setDate(minDate.getDate() - 1);
		maxDate.setDate(maxDate.getDate() + 1);

		var testDate = new Date();

		$input.zsDatePicker({
			minDate: minDate,
			maxDate: maxDate
		});

		var $calendar = $input.parent().find('.zs-calendar');

		var datePicker = $input.data('zsDatePicker');
	  
		// Date below minimum should be disabled
		testDate = new Date(now.getFullYear(), now.getMonth(), minDate.getDate() - 1);
		var $td = $calendar.find('td[date="' + testDate.toString() + '"]');
		expect($td.is('[disabled]')).toBeTruthy();
	  
		// Dates within range should be enabled
		testDate = new Date(now.getFullYear(), now.getMonth(), now.getDate());
		var $td = $calendar.find('td[date="' + testDate.toString() + '"]');
		expect($td.is('[disabled]')).toBeFalsy();
		
		// Dates above maximum should be disabled
		testDate = new Date(maxDate.getFullYear(), maxDate.getMonth(), maxDate.getDate() + 1);
		var $td = $calendar.find('td[date="' + testDate.toString() + '"]');
		expect($td.is('[disabled]')).toBeTruthy();		
		
		// Year select should have only one option with current year
		var select = $calendar.find('select[name="year"]')[0];
		expect(select.options.length).toBe(1); 
		expect(select.options[0].value).toBe(now.getFullYear() + '');		
	});
	
	it('Can select a date', function () {
		var now = new Date();
		$input.zsDatePicker({
			value: now
		});
		
		var datePicker = $input.data('zsDatePicker');
		var $calendar = $input.parent().find('.zs-calendar');
		var testDate = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
		var $td = $calendar.find('td[date="' + testDate.toString() + '"]');
		
		expect($td.length).toBe(1);
		
		$td.trigger('click');			
		
		expect($input.val()).toBe(datePicker.dateToString(testDate));
		
	})
	
	it('Highlights today\'s date and indicates the dates outside the current month', function () {
		var now = new Date();
		$input.zsDatePicker();
		var datePicker = $input.data('zsDatePicker');
		var $calendar = $input.parent().find('.zs-calendar');
		var testDate = new Date(now.getFullYear(), now.getMonth(), now.getDate());
		expect(datePicker.value == testDate);
		
		var $td = $calendar.find('td[date="' + testDate.toString() + '"]');
		expect($td.is('[current]')).toBeTruthy();
		expect($td.is('[today]')).toBeTruthy();					
	});
	
	it('Can select a year and a month', function () {
		var now = new Date();
		$input.zsDatePicker();
		var $calendar = $input.parent().find('.zs-calendar');
		var datePicker = $input.data('zsDatePicker');
		$month = $calendar.find('select[name="month"]');
		$year = $calendar.find('select[name="year"]');
		
		expect($month.val()).toBe(now.getMonth() + '');
		expect($year.val()).toBe(now.getFullYear() + '');
		
		// Goto next month
		if ($month[0].selectedIndex == 11 ) {
			$month[0].selectedIndex = 0;
		} else {
			$month[0].selectedIndex = $month[0].selectedIndex + 1;
		}
		$month.trigger('change');		
		expect(datePicker.displayDate.getMonth()+'').toBe($month.val());
		
		// Goto next year
		$year[0].selectedIndex = $year[0].selectedIndex + 1; // In our case next year is always available.
		$year.trigger('change');		
		expect(datePicker.displayDate.getFullYear()+'').toBe($year.val());		 
	});

	it('Can go to the next and the previous month', function () {
		var now = new Date();
		$input.zsDatePicker();
		var $calendar = $input.parent().find('.zs-calendar');
		var datePicker = $input.data('zsDatePicker');
		
		$prev = $calendar.find('header>[role="prev"]');
		$next = $calendar.find('header>[role="next"]');
		
		expect($prev.length).toBeTruthy();
		expect($next.length).toBeTruthy();
		$month = $calendar.find('select[name="month"]');
		$year = $calendar.find('select[name="year"]');		
		
		var month = now.getMonth();
		$prev.trigger('click');
		if (month == 0) {
			month = 11;
		} else {
			month--;
		}
		expect($month.val()).toBe(month+'');
		expect(datePicker.displayDate.getMonth()).toBe(month);
		
		$next.trigger('click');
		if (month == 11) {
			month = 0;
		} else {
			month++;
		}
		expect($month.val()).toBe(month+'');
		expect(datePicker.displayDate.getMonth()).toBe(month);		
	});
	
	it('Can be shown and hidden', function() {
		$input.zsDatePicker();
		var $calendar = $input.parent().find('.zs-calendar');
		$input.zsDatePicker('show');
		expect($calendar.is(':visible')).toBeTruthy();
		$input.zsDatePicker('hide');
		expect($calendar.is(':visible')).toBeFalsy();
	})
	
	it('Can be removed', function() {
		$input.zsDatePicker();
		$input.zsDatePicker('destroy');
		expect($input.data('zsDatePicker')).toBeFalsy();
		var $calendar = $input.parent().find('.zs-calendar');
		expect($calendar.length).toBeFalsy();	
	});
	
	it('should set the given date', function() {
		$input.zsDatePicker();
		var datePicker = $input.data('zsDatePicker');
		var today = new Date();
		datePicker.setDate(today);
		expect(datePicker.value.getDate()).toBe(today.getDate());
	});

	it('should build the header properly', function() {
		var minDate = new Date();
		$input.zsDatePicker({
			minDate: minDate
		});
		var $calendar = $input.parent().find('.zs-calendar');
		
		// Should have the prev link in the header
		$prev = $calendar.find('header>[role="prev"]');
		expect($prev[0].tagName).toBe("A");
		
		// Should have the next link in the header
		$next = $calendar.find('header>[role="next"]');
		expect($next[0].tagName).toBe("A");
		
		// Should create 101 year options in dropdown as we have min date as today
		$year = $calendar.find('header select[name="year"]');
		expect($year.find('option').length).toBe(101);
		
		// Should create 12 month options in dropdown
		$month = $calendar.find('header select[name="month"]')
		expect($month.find('option').length).toBe(12);
	});
	
	it('should convert date to string', function() {
		$input.zsDatePicker();
		var datePicker = $input.data('zsDatePicker');
		var today = '6/9/2016';
		datePicker.setDate(new Date(today));
		expect(datePicker.dateToString(datePicker.value)).toBe(today);
	});

	it('should render the calendar control inside provided container element', function() {
		$('<div id="calendar-container"></div>').appendTo('body');
		$container = $('#calendar-container');

		$input.zsDatePicker({
			$calendarContainer: $('#calendar-container')
		});

		// Check if the id of the parent container is same as that of we provided
		expect($('.zs-calendar').parent()[0].id).toBe("calendar-container");
	});

	xit('Supports parameters update', function() {
		
	});
	
	xit('Can disable next and prev arrows', function() {
		
	});

	
	it('should set input in readonly mode by default', function() {
		$input.zsDatePicker({});
		expect($input.prop('readonly')).toBeTruthy();
	});
		
	it('should NOT set input in readonly mode if readOnly param is turned off', function() {
		$input.zsDatePicker({
			readOnly: false
		});
		expect($input.prop('readonly')).toBeFalsy();
	});

	it('should call plugin\'s onChange function when date value is typed in input field', function() {
		$input.zsDatePicker({
			readOnly: false			
		});
	
		spyOn($input.data('zsDatePicker').options, 'onChange');
		$input.val("").change();
		expect($input.data('zsDatePicker').options.onChange).toHaveBeenCalled();
	});

	it('should open calendar when calendar icon is clicked', function () {
		$input.zsDatePicker({});
		
		expect($input.data('zsDatePicker').$container.is(":visible")).toBeFalsy();
		
		$input.data('zsDatePicker').$anchor.trigger("click");

		expect($input.data('zsDatePicker').$container.is(":visible")).toBeTruthy();

		$input.data('zsDatePicker').$anchor.trigger("click");

		expect($input.data('zsDatePicker').$container.is(":visible")).toBeFalsy();
	});
	it('fixes position of dropdown in flippable mode', function () {
		$input.zsDatePicker({
			flippable:true
		});
		
		var plugin = $input.data('zsDatePicker');
		$input.click();
		expect(plugin).toBeTruthy();
		expect(plugin.options.flippable).toBeTruthy();	
		expect(plugin.$container.css("position")).toBe("fixed");					
	});

	it('opens below field if space is available', function () {
		$input.zsDatePicker({
			flippable:true
		});
		
		var plugin = $input.data('zsDatePicker');
		
		spyOn(plugin, 'isInViewPort').and.callThrough();

		$input.click();
		
		expect(plugin.isInViewPort).toHaveBeenCalled();

		expect(plugin).toBeTruthy();
		expect(plugin.options.flippable).toBeTruthy();		
		expect(plugin.$container.css("position")).toBe("fixed");
		expect(plugin.$input.offset().top).toBeLessThan(plugin.$container.offset().top);
		expect(plugin.$input.offset().left).toEqual(plugin.$container.offset().left);
							
	});
	it('flips it\'s position to open above field if space isn\'t available below', function () {
		$input.zsDatePicker({
			flippable:true
		});
		
		$input.css({position:"fixed",bottom:"0"});
		
		var plugin = $input.data('zsDatePicker');
		
		spyOn(plugin, 'isInViewPort').and.callThrough();

		$input.click();
		
		expect(plugin.isInViewPort).toHaveBeenCalled();

		expect(plugin).toBeTruthy();
		expect(plugin.options.flippable).toBeTruthy();		
		expect(plugin.$container.css("position")).toBe("fixed");
		expect(plugin.$input.offset().top).toBeGreaterThan(plugin.$container.offset().top);
		expect(plugin.$input.offset().left).toEqual(plugin.$container.offset().left);							
	});
	afterEach(function () {
		$input.parent().remove();
	});
});
