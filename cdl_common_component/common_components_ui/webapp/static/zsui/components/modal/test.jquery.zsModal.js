describe("zsModalDialog", function () {
	var $dialog, $parent;

	beforeEach(function () {
		$parent = $('<div><div class="zs-modal"><header>title</header><section></section><footer></footer></div></div>');
		$parent.appendTo('body');
		$dialog = $parent.find('.zs-modal');
	});

	it('Is a jQuery plugin', function () {
		utils.isPlugin(expect, 'zsModalDialog'); 
		utils.isConfigurable(expect, 'zsModalDialog');
	})

	it('Can open and close a modal dialog', function () {
		$dialog.zsModalDialog({
			text: 1
		});		
		var plugin = $dialog.data('zsModalDialog');
		$dialog.zsModalDialog('open');
		expect($('.zs-overlay').length).toBeTruthy();
		expect($dialog.is(':visible')).toBeTruthy();	
		expect($('.zs-overlay').is(':visible')).toBeTruthy();
		$dialog.zsModalDialog('close');
		expect($dialog.is(':visible')).toBeFalsy();	
		expect($('.zs-overlay').is(':visible')).toBeFalsy();			

	});

	it('Can be removed', function() {
		$dialog.zsModalDialog({});
		expect($('.zs-overlay').length).toBeTruthy();
		utils.isRemovable(expect, 'zsModalDialog');
		$dialog.zsModalDialog('destroy');
		expect($('.zs-overlay').length).toBeFalsy();		
	});
	

	it('Adds a close icon', function () {
		$dialog.zsModalDialog({
			render: {
				closeIcon: '<span class="zs-icon test" close></span>'
			}
		});
		$dialog.zsModalDialog('open');
		expect($dialog.is(':visible')).toBeTruthy();
		expect($dialog.find('[close]').length).toBe(1);
		expect($dialog.find('[close]').is('.test')).toBeTruthy();
		$dialog.find('[close]').trigger('click');	
		expect($dialog.is(':visible')).toBeFalsy();
	});

	describe('Supports event handling', function () {
		var test,events = {
			close: function () {
				test = 'close';
			},
			open: function() {
				test = 'open';
			},
			submit: function() {
				test = 'submit';
			}
		}, plugin;

		beforeEach( function () {
			test = '';
			$dialog.find('>footer').append('<button>Submit</button>');
			$dialog.zsModalDialog({
				events: events
			});
			plugin = $dialog.data('zsModalDialog');
		});
		
		
		it('open', function() {
			plugin.open();
			
			expect(test).toBe('open');
		});
		
		it('close', function() {
			plugin.open();
			plugin.close();
			expect(test).toBe('close');
		});

		it('submit', function() {
			plugin.open();
			$dialog.find('>footer button').trigger('click');
			expect(test).toBe('submit');
		});
		
		afterEach(function () {
			$dialog.zsModalDialog('destroy');
		});		
	});


	it('Closes on Cancel click and can control what should trigger close', function () {
		$dialog.find('>footer').append('<a close-test="1">Cancel</a>');
		$dialog.zsModalDialog({closeSelector: '[close-test]'});
		$dialog.zsModalDialog('open');
		$dialog.find('>footer [close-test]').trigger('click');
		expect($dialog.is(':visible')).toBeFalsy();
	});

	it('Can open a dialog after creating', function(){
		$dialog.zsModalDialog({autoOpen:true});
		expect($dialog.is(':visible')).toBeTruthy();
	});

	it('Closes on escape click', function(){
		$dialog.zsModalDialog({closeOnEscape:true, autoOpen:true});
		var e = $.Event('keyup');
    	e.keyCode = 27; // esc 
		$(document).trigger(e);
		expect($dialog.is(':visible')).toBeFalsy();
		$dialog.zsModalDialog({closeOnEscape:false});
		expect($dialog.is(':visible')).toBeTruthy();
		$(document).trigger(e);
		expect($dialog.is(':visible')).toBeTruthy();
	});

	it('Can update the title', function () {
		$dialog.zsModalDialog({});
		plugin = $dialog.data('zsModalDialog');
		expect(plugin.$title.length).toBe(1);
		expect(plugin.$title.text()).toBe('title');
		$dialog.zsModalDialog({title: 'test'});
		expect(plugin.$title.text()).toBe('test');
	});

	it('Can auto focus elements and submit on enter', function () {
		var flag = false
		$dialog.find('>section').append('<input type="text" submit-on-enter1 focus1/>');
		$dialog.zsModalDialog({
			autoOpen:true, 
			autoFocus: '[focus1]', 
			submitOnEnter: '[submit-on-enter1]',
			events: {
				submit: function() {
					flag = true;
				}
			}
		}); // check that we can also control selectors
		expect($dialog.find('>section>input').is(':focus')).toBeTruthy();
		var e = $.Event('keyup');
    	e.keyCode = 13; // enter 
		$dialog.find('>section>input').trigger(e);
		expect(flag).toBeTruthy();
	});

	
	it('should be in center of window initially', function() {
		$dialog.zsModalDialog({
			autoOpen: true
		});

		var offsetTop = $dialog.offset().top,
			offsetLeft = $dialog.offset().left,
			offsetRight = $(document).width() - offsetLeft - $dialog.width(),
			offsetBottom = $(document).height() - offsetTop - $dialog.height();
			
		expect(parseInt(offsetTop)).toEqual(parseInt(offsetBottom));
		expect(parseInt(offsetLeft)).toEqual(parseInt(offsetRight));
	});

	it('should be in top of window if it goes out of the window frame', function() {
		$('.zs-modal').css({
			width: $(document).outerWidth() + 200,
			height: $(document).outerHeight() + 200
		});
		
		$dialog.zsModalDialog({
			autoOpen: true
		});
		var pos = $dialog[0].getBoundingClientRect();
		expect(pos.top).toEqual(0);
		expect(pos.left).toEqual(0);
	});
		

	afterEach(function () {
		if ($dialog && $dialog.length && $dialog.data('zsModalDialog')) {
			$dialog.zsModalDialog('destroy');
		}
		$parent.remove();		
	});
});
