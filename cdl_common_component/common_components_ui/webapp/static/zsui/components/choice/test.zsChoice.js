describe('zsChoice', function() {
	var el, el1;
	beforeEach(function() {
		el = new zs.choiceElement();
		el.setAttribute('values', 'red,green,blue');
		el.setAttribute('labels', 'Red,Green,Blue');
		el.setAttribute('label', 'Choice');
		el.setAttribute('value', 'green');
		document.body.appendChild(el);
		el1 = new zs.choiceElement();
		el1.setAttribute('values', 'red,green,blue');
		el1.setAttribute('labels', 'Red,Green,Blue');
		el1.setAttribute('label', 'Choice');
		el1.setAttribute('value', 'green');
		el1.setAttribute('type', 'radio');
		document.body.appendChild(el);
		document.body.appendChild(el1);
	});

	it('Is custom element', function() {
		utils.isCustomElement(expect, 'zs-choice', null, zs.choiceElement, HTMLElement);
		utils.isBehavior(expect, zs.choice);
	});	

	it('Can render options based on provided attributes value, values, lables', function(done) {
		setTimeout(function() { // IE11
			var options = el.querySelectorAll('.zs-checkbox');
			expect(options.length).toBe(3); // Display 3 options

			// Selecte based on the provided value
			expect(options[1].firstChild.checked).toBeTruthy(); // Green is selected
			expect(options[0].firstChild.checked).toBeFalsy(); 
			expect(options[2].firstChild.checked).toBeFalsy();

			// Labels
			expect(el.querySelector('label').innerHTML).toBe('Choice');
			expect(options[0].children[1].innerHTML).toBe('Red');
			expect(options[1].children[1].innerHTML).toBe('Green');
			expect(options[2].children[1].innerHTML).toBe('Blue');

			// Check properties values, labels
			expect(el.values.length).toBe(3);
			expect(el.labels.length).toBe(3);
			expect(el.values.join(el.delimeter)).toBe('red,green,blue');
			expect(el.labels.join(el.delimeter)).toBe('Red,Green,Blue');
			
			done();
		});

	});

	it('Supports radio buttons', function(done) {


		setTimeout(function() { // IE11
			var options = el1.querySelectorAll('.zs-checkbox');
			expect(options.length).toBe(3); // Display 3 options

			// Selecte based on the provided value
			expect(options[1].firstChild.checked).toBeTruthy(); // Green is selected
			expect(options[0].firstChild.checked).toBeFalsy(); 
			expect(options[2].firstChild.checked).toBeFalsy();

			// Labels
			expect(el1.querySelector('label').innerHTML).toBe('Choice');
			expect(options[0].children[1].innerHTML).toBe('Red');
			expect(options[1].children[1].innerHTML).toBe('Green');
			expect(options[2].children[1].innerHTML).toBe('Blue');
			
			done();
		});

	});

	it('Selects options on click', function(done) {
		var event1 = new MouseEvent("click", {
			bubbles: true,
			cancelable: true,
			view: window
		});
		var event =  new MouseEvent("click", {
			bubbles: true,
			cancelable: true,
			view: window
		});
		var event2 =  new MouseEvent("click", {
			bubbles: true,
			cancelable: true,
			view: window
		});		

		setTimeout(function() { // IE11
			el1.querySelector('input').dispatchEvent(event1);			
			el.querySelector('input').dispatchEvent(event);		
			el.querySelector('.zs-checkbox[name="blue"]').dispatchEvent(event2);
			setTimeout(function() { // yeld to click
				expect(el1.value).toBe('red');
				expect(el.value.split(',').indexOf('red')>=0).toBeTruthy();
				expect(el.value.split(',').indexOf('blue')>=0).toBeTruthy();
				done();
			},0);
		});
	});

	it('Keeps attributes and properties in sync', function(done) {
		el.value = "blue";
		el.setAttribute('values', 'red,green,blue,purple');
		el.setAttribute('labels', 'Red,Green,Blue,Purple');
		el1.setAttribute('value', 'red');
		el1.values = ['red', 'purple'];
		el1.labels = ['Red', 'Purple'];
		setTimeout(function() { // IE11
			expect(el.getAttribute('value')).toBe('blue');
			expect(el.values.length).toBe(4);
			expect(el.labels.length).toBe(4);
			expect(el.values[3]).toBe('purple');
			expect(el.labels[3]).toBe('Purple');
			expect(el1.value).toBe('red');
			expect(el1.getAttribute('values')).toBe(el1.values.join(el1.delimeter));
			expect(el1.getAttribute('labels')).toBe(el1.labels.join(el1.delimeter));
			done();
		});
	});

	it('Fires a change event', function(done) {
		var n=0;
		var event = new MouseEvent("click", {
			bubbles: true,
			cancelable: true,
			view: window
		});

		el.addEventListener('change',function() {
			n++;
		});

		setTimeout(function() {
			//el.value = "red"; // TODO
			//el.querySelector('input[name="blue"]').checked = true; // TODO
			el.querySelector('input').dispatchEvent(event);	
			setTimeout(function() {
				expect(n).toBe(1);
				done();
			});

		},0);

	});

	afterEach(function() {
		if (el) {document.body.removeChild(el);}
		if (el1) {document.body.removeChild(el1);}
	});

});