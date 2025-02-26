
describe("field element, validation, clear", function () {

	


	it('is a custom element', function () {
		utils.isBehavior(expect, zs.field);		
		utils.isCustomElement(expect, 'zs-field', 'p', zs.fieldElement, HTMLParagraphElement);
	});

	it('Renders a field', function (done) {
		var elem = new zs.fieldElement();
		elem.setAttribute('type', 'text');
		elem.setAttribute('label', 'label');
		document.body.appendChild(elem);
		setTimeout(function() {
			expect(elem.querySelector('input')).toBeTruthy();
			expect(elem.querySelector('label')).toBeTruthy();
			expect(elem.querySelector('label').innerHTML).toBe('label');
			elem.parentNode.removeChild(elem);
			done();
		},10);	
		
	});

	describe('syncronize properties and attributes values', function () {
		var elem, elem2,container;
		elem = new zs.fieldElement();
		it('value', function (done) {
			utils.isPropAttrSync(expect, done, elem, 'value');
		});

		it('name', function (done) {
			utils.isPropAttrSync(expect, done, elem, 'name');
		});

	});

	describe('events', function () {
		var elem, calls = 0, flag = '', elem2, calls2 = 0, flag2;
		
		describe('render', function() {
			beforeEach(function (done) {
				elem = new zs.fieldElement();

				elem2 = new zs.fieldElement();
				elem2.addEventListener('render', function () {
					calls2++;				
				});
				elem2.setAttribute('value', 1);
				elem2.render();
				elem2.render();


				elem.addEventListener('render', function () {
					calls++;
					flag = this.getAttribute('type');
					flag += this.getAttribute('value');
					flag += this.getAttribute('view');
				});
				
				// All of this changes should cause render
				elem.setAttribute('type', 'text');
				elem.setAttribute('value', '1');
				elem.setAttribute('view', 'test');
				document.body.appendChild(elem);

				setTimeout(function () {
					done();
				},150); // IE11 might require longe wait

			});


			it('render once when creating and setting attributes', function () {
				expect(calls).toBe(1); // once when created and or when attribute changed
				expect(flag).toBe('text1test');
				expect(calls2>=2).toBeTruthy(); // 2 for Chrome and 3 for IE.
				console.log(flag2);
			});


			afterEach(function () {
				elem.parentNode.removeChild(elem);
				calls = 0;
				flag = '';
			});
		});

		it('change', function(done) {
			var el = new zs.fieldElement();
			el.setAttribute('type', 'text');
			el.addEventListener('change', function(event) {
				expect(this.value).toBe('3');
				document.body.removeChild(el);
				done();
			});

			document.body.appendChild(el);
			setTimeout(function() {
				var input = el.querySelector('input');
				input.value = '3';
				input.dispatchEvent(new CustomEvent('change', {bubbles:true}));
			},10);

		})
	});

	it('can block render on attach and attributeChange', function(done) {
		var el = new zs.fieldElement();
		var el1 = new zs.fieldElement();
		var calls = 0, calls1 = 0;
		el.addEventListener('render', function() {
			calls++;
		});
		el1.addEventListener('render', function() {
			calls1++;
		});

		el.setAttribute('type', 'text');
		el1.setAttribute('type', 'text'); 
		expect(el.blockAttrRender).toBe(false);
		expect(el.blockAttrRenderAlways).toBe(false);
		expect(el.blockAttachRender).toBe(false);
		expect(el.blockAttachRenderAlways).toBe(false);
		el.blockAttachRender = true;
		el1.blockAttachRenderAlways = true;
		document.body.appendChild(el);		

		setTimeout(function() { // Attached
			expect(el.querySelector('input')).toBeFalsy();
	
			// Change attribute	
			el1.blockAttrRenderAlways = true;
			
			document.body.appendChild(el); // 2nd attach


			el1.setAttribute('value', '1');
			el1.setAttribute('value', '2'); // 2nd 
			document.body.appendChild(el1); // 2nd attach
			
			// After 2nd attach
			setTimeout(function() {
				expect(calls).toBe(1);
				expect(calls1).toBe(0);


				// Attribute change
				el.blockAttrRender = true;		
				el.setAttribute('value', '1');
				el.setAttribute('value', '2');

				el1.blockAttrRenderAlways = false;
				el1.render();
				el1.setAttribute('value', '1');
				el1.setAttribute('value', '2'); 

				setTimeout(function() {
					expect(calls).toBe(2);
					expect(calls1).toBe(2); // Changed attribute twice but rendered once
					document.body.removeChild(el);
					document.body.removeChild(el1);
					done();
				},20);				
				
			},20);
			
		},20);

	});

	describe('validation behavior', function () {
		var elem, form;

		beforeEach(function (done) {
			elem = new zs.fieldElement();
			elem.setAttribute('type', 'text');
			elem.setAttribute('value', 'test');
			elem.value = 'test';
			document.body.appendChild(elem);
			
			form = new zs.formElement();
			form.innerHTML = '<p><input type="text" id="form-input-1"/></p>';
			document.body.appendChild(form);

			done();
		});

		it('is a behavior', function () {
			utils.isBehavior(expect, zs.validation, 'validation');
		});


		it('Can exectute validation rules', function () {
			expect(typeof elem.validate).toBe('function');
			elem.validate = function () {
				if (this.value == 'test') {
					return true;
				} else {
					return false;
				}
			}
		});

		it('Can show error messages', function () {
			expect(typeof elem.showMessage).toBe('function')
			var myMessage = { type: 'error', text: 'my error' };
			elem.showMessage(myMessage);
			elem.showMessage({ type: 'warning', text: 'my warning' });
			var message = elem.querySelector('.zs-message.zs-error');
			var message2 = elem.querySelector('.zs-message.zs-warning');

			expect(message).toBeTruthy();
			expect(message.innerHTML).toBe('my error');
			expect(message2).toBeTruthy();
			expect(message2.innerHTML).toBe('my warning');

			var messages = elem.getMessages();
			expect(messages.length).toBe(2);
			expect(messages[0]).toBe(myMessage);
			expect(messages[1].text).toBe('my warning');


		});

		it('Can hide error messages', function () {
			expect(typeof elem.clearMessages).toBe('function');
			elem.showMessage({ type: 'warning', text: 'my warning' });
			expect(elem.getMessages().length).toBe(1);
			elem.clearMessages();
			expect(elem.getMessages().length).toBe(0);
			expect(elem.querySelector('.zs-message')).toBeFalsy();

		});

		it('Can work with forms', function () {
			form.showMessage({ type: 'error', text: 'my error' });
			form.showMessage({ type: 'warning', text: 'my warning', element: form.querySelector('p')});
			expect(form.querySelector('.zs-message.zs-error')).toBeTruthy();
			expect(form.querySelector('p.zs-warning')).toBeTruthy();
		});


		afterEach(function () {
			elem.parentNode.removeChild(elem);
			form.parentNode.removeChild(form);
		});
	});

	describe('clear behavior', function () {
		var elem, elem2, elem3, icon2, input2, input, icon, icon3;			

		beforeEach(function (done) {
			elem = new zs.fieldElement();
			elem.setAttribute('type', 'text');
			elem.setAttribute('clear', '');
			elem.setAttribute('value', 'test');

			
			elem2 = new zs.fieldElement();
			elem2.setAttribute('type', 'text');
			elem2.setAttribute('clear', '');
			elem2.setAttribute('value', 'test');


			elem3 = new zs.fieldElement();
			elem3.setAttribute('type', 'text');
			elem3.setAttribute('clear', '');
			elem3.setAttribute('value', '');
			
			document.body.appendChild(elem);
			document.body.appendChild(elem2);
			document.body.appendChild(elem3);
			setTimeout(function () {
				icon2 = elem2.querySelector('[clear]');
				icon3 = elem3.querySelector('[clear]');
				icon = elem.querySelector('[clear]');
				input2 = elem2.querySelector('input');
				input = elem.querySelector('input');
				if (icon2) {
					var event = new CustomEvent('click', { bubbles: true });
					icon2.dispatchEvent(event);
				}
				setTimeout(function () {
					done();
				}, 150);				
			}, 50); // IE11 requires >0 delay
		});

		it('is a behavior', function () {
			utils.isBehavior(expect, zs.clear);
		});

		it('Shows a clear icon when the field has a value', function () {			
			expect(elem.querySelector('[clear]')).toBeTruthy();
			expect(input.value).toBe('test');			
			expect(icon.style.display == 'none').toBeFalsy(); // icon is visible // TODO: this fails
		});

		it('Hides icon when no value', function () {
			expect(icon3.style.display == 'none').toBeTruthy(); 
		});

		it('clears value when clicked', function () {
			expect(input2.value).toBeFalsy();
			expect(icon2.style.display).toBe('none');
		});

	

		afterEach(function () {
			elem.parentNode.removeChild(elem);
			elem2.parentNode.removeChild(elem2);
			elem3.parentNode.removeChild(elem3);
		});
	});

});