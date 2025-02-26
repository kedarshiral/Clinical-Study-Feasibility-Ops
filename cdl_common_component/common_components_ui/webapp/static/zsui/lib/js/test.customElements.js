describe("customElements", function () {
	
	it("a helper function to create DOM elements", function () {
		expect(typeof zs).toBe('object');
		expect(typeof zs.customElement).toBe('function');
		var MyElement = zs.customElement(HTMLElement, 'my-element1');
		expect(typeof MyElement).toBe('function');
		var elem = new MyElement();
		expect(elem instanceof HTMLElement).toBeTruthy();

		// Basic example 
		var MyElement11 = zs.customElement(HTMLElement, 'my-element11', null, {
			properties: { // Use this object to declare properties and use setters and getters see more https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/defineProperty
				value: {
					get: function () {
						return this._value;
					},
					set: function (newValue) {
						this._value = newValue + ' set';
					}
				}
			}

		});
		var elem = new MyElement11();
		elem.value = 'test';
		console.log(elem.value); // 'test set' is expected
	
		// How to syncronize attributes
		var MyElement12 = zs.customElement(HTMLElement, 'my-element12', null, {
			setValue: function (str) {
				var newValue
				if (str) {
					newValue = str + '';
				} else {
					newValue = '';
				}

				// Sync with attribure
				if (newValue != this.getAttribute('value')) {
					this.setAttribute('value', newValue);
				}

				this._value = newValue;
			},
			getValue: function () {
				return this._value || '';
			},
			properties: {
				value: {
					get: function () {
						console.log('getter');
						return this.getValue();
					},
					set: function (newValue) {
						console.log('setter', newValue);
						this.setValue(newValue);
					}
				}
			},
			events: {
				attributeChange: function (e) {
					var attributeName = e.detail.attributeName;
					if (attributeName == 'value' && this.getAttribute('value') != this.value) {
						this.value = this.getAttribute('value');
					}
				}
			}
		});
		var elem = new MyElement12();
		elem.value = '1';
		console.log(elem.getAttribute('value')); // '1' is expected
		elem.setAttribute('value', '2');
		console.log(elem.value); // '2' is expected


		
	});
	

	it('can delegate events', function () {
		var listeners = {}, element1, element2, event;

		listeners.click = function () {
		};

		spyOn(listeners,'click');

		element1 = document.createElement('button');
		element2 = document.createElement('button');
		document.body.appendChild(element1);
		document.body.appendChild(element2);

		element2.addEventListener('click', listeners.click);
		event = new CustomEvent('click');
		

		//expect(listeners.click).toHaveBeenCalled();
		expect(typeof zs.pipeEvent).toBe('function');
		zs.pipeEvent('click', element1, element2);
		element1.click();
		expect(listeners.click).toHaveBeenCalled();

	});

	it('supports custom tags', function() {
		// ES5 
		var flag = '';
		var elementClass = zs.customElement(HTMLElement, 'test-element', null , { 
			events: {
				create: function() {
					flag = 'created';
				}
			}
		}); 
		

		
		// ES6
		/*
		class elementClass extends HTMLElement {
			constructor(self) {
				self = super(self);
				self.innerHTML = 'my custom tag';
				return self;
			}
		}
		*/
		// Babel
		/*
		function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

		function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

		function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

		var elementClass = function (_HTMLElement) {
			_inherits(elementClass, _HTMLElement);

			function elementClass(self) {
				var _this, _ret;

				_classCallCheck(this, elementClass);

				self = (_this = _possibleConstructorReturn(this, (elementClass.__proto__ || Object.getPrototypeOf(elementClass)).call(this, self)), _this);
				self.innerHTML = 'my custom tag';
				return _ret = self, _possibleConstructorReturn(_this, _ret);
			}

			return elementClass;
		}(HTMLElement); 
		*/

		//customElements.define('test-element', elementClass);
		var elem = document.createElement('test-element');
		//var elem = new elementClass();
		document.body.appendChild(elem);		
		expect(flag).toBe('created');
		document.body.removeChild(elem);
	});

	it('can extend native elements', function() {
		var elementClass = zs.customElement(HTMLButtonElement, 'my-button', 'button' , { 
			events: {
				create: function() {
					this.innerHTML = 'my custom button';
				}
			}
		}); 
		var elem = new elementClass();
		
		document.body.appendChild(elem);
		expect(elem.innerHTML).toBe('my custom button');
		elem.setAttribute('value', 'test');
		expect(elem.value).toBe('test');
		expect(elem.getAttribute('is') == 'my-button').toBeTruthy();

		/*
		class MyParagraph extends HTMLParagraphElement {};
		customElements.define('my-paragraph',MyParagraph,{extends: 'p'});
		var elem = new MyParagraph();
		document.body.appendChild(elem);
		console.log(elem.getAttribute('is'));
		*/

		document.body.removeChild(elem);
		elem = document.createElement('button', 'my-button');
		document.body.appendChild(elem);
		expect(elem.innerHTML).toBe('my custom button');
		elem.setAttribute('value', 'test');
		expect(elem.value).toBe('test');
		document.body.removeChild(elem);
	});

	it('can add behaviors', function () {
		expect(typeof zs.addBehavior).toBe('function');
		expect(typeof zs.addCustomEvent).toBe('function');
		
		var obj = {
			prop: 1,
			prop1: 2,
			method: function () {

			}
		};
	
		var behavior = {
			prop1: 3,
			prop2: 4,
			method2: function() {

			},
			events: {
				test: function () {

				}
			},
			properties: {
				prop3: {
					set: function (value) {
						this._prop3 = value + '_setter';
					},
					get: function () {
						return this._prop3;
					}
				}
			}
		};

		var behavior2 = {
			events: {
				test: function () {

				}
			}
		}

		zs.addBehavior(obj, behavior);
		zs.addBehavior(obj, behavior2);
		

		// Support event handlers
		expect(typeof obj.eventHandlers).toBe('object');
		expect(Array.isArray(obj.eventHandlers['test'])).toBeTruthy();
		expect(obj.eventHandlers['test'][0]).toBe(behavior.events.test);
		expect(obj.eventHandlers['test'][1]).toBe(behavior2.events.test);

		// Extend with properties and method
		expect(obj.prop).toBe(1);
		expect(obj.prop1).toBe(3);
		expect(obj.prop2).toBe(4);
		expect(typeof obj.method).toBe('function');
		expect(typeof obj.method2).toBe('function');


		// support property definitions (setters and getters)
		obj.prop3 = '3';
		expect(obj.prop3).toBe('3_setter');

	});

	describe('attributeChange delay', function () {
		var elem, delay, attrTime, attrTime2, renderTime, nChanges = 0, nExpected = 10, nChanges2 = 0, elem2;

		// Through custom elements helper
		var MyElement = zs.customElement(HTMLElement, 'my-element3', null, {
			observedAttributes: ['value'],
			events: {
				attributeChange: function () {
					nChanges++;
					console.log('ZS attribute change', 'delay', performance.now() - attrTime, 'changes', nChanges, 'value', this.getAttribute('value'));
					attrTime = performance.now();
				}
			}
		});

		// Direct polyfill usage
		var MyElement2 = document.registerElement(
			'my-element4',
			{
			prototype: Object.create(
				HTMLElement.prototype, {
				attributeChangedCallback: {
					value: function () {
						nChanges2++;
						console.log('Polyfill attribute change', 'delay', performance.now() - attrTime2, 'changes', nChanges2, 'value', this.getAttribute('value'));
						attrTime2 = performance.now();
					}
				}
				})
			}
		);

		beforeEach(function (done) {			
			elem = new MyElement();
			elem2 = new MyElement2();	

			document.body.appendChild(elem2);
			document.body.appendChild(elem);

			attrTime = performance.now();
			attrTime2 = performance.now();
			for (var i = 0; i < nExpected; i++) {
				elem2.setAttribute('value', i);
				elem.setAttribute('value', i);
			}

			setTimeout(function () {
				done();
			}, 100);

		});
		it('As expected', function () {

			expect(nChanges).toBe(nExpected);
			expect(nChanges2).toBe(nExpected);

		});

		afterEach(function () {
			elem.parentNode.removeChild(elem);
			elem2.parentNode.removeChild(elem2);
		})
	});

	describe('events', function () {
		// In IE11 attach, detach, attributeChange callbacks will be asynchronous due to polyfill nature.
		var attributeName, MyElement, element, element2, element3;
		var behavior = {
			flag: '',
			observedAttributes: ['test'],
			events: {
				clear: function(e) {
					this.flag += 'clear' + e.detail;
				},
				create: function () {
					this.flag += 'create';
				},
				attach: function () {
					this.flag += 'attach';
				},
				detach: function () {
					this.flag += 'detach';
				},
				attributeChange: function (e) {
					attributeName = e.detail.attributeName;
					this.flag += 'attributeChange';
				}
			}
		};


	
		MyElement = zs.customElement(HTMLElement, 'my-element2', null, behavior);
	
		beforeEach(function (done) {
			element = new MyElement();
			element2 = new MyElement();
			element3 = new MyElement();
			document.body.appendChild(element);
			document.body.appendChild(element3);
			element.setAttribute('test', '1');
			document.body.removeChild(element);			
			setTimeout(function () { done(); }, 0);
		});
	
		it('lifecycle events create, attach, detach and attribureChange', function () {
			expect(element.flag).toBe('createattachattributeChangedetach');
			expect(element2.flag).toBe('create'); 
			expect(attributeName).toBe('test');
		});

		it('custom events', function() {
			var event = new CustomEvent('clear', {detail: 'test'});
			element2.dispatchEvent(event);
			expect(element2.flag).toBe('createcleartest');
			expect(element3.flag).toBe('createattach');
		});

		afterEach(function () {
			document.body.removeChild(element3);
		});
	});

	it('resistent to prototype mutation', function() {
		var test6;
		Array.prototype.test4Array = 4;

		// When we mutate prototypes it shouldn't cause any errors
		var obj = {test:1};
		var cls = zs.customElement(HTMLElement,'my-test4', null, {
			test5: 5,
			observedAttributes: ['test6'],
			properties: {
				value: {
					get: function() {
						return '1';
					}
				}
			},
			events: {
				test6: function() {
					test6 = 6;
				}
			}
		});
		var el = new cls();
		expect(el.test5).toBe(5);
		expect(el.value).toBe('1');
		console.log(el.eventHandlers, el.observedAttributesList);
		delete Array.prototype.test4Array;
	});


	it('supports inheritance from existing custom elements', function(done) {
		var cls1 = zs.customElement(HTMLElement, 'inherit-parent', null, {
			log: '',
			prop:1,
			method: function() {
				return 2;
			},
			observedAttributes: ['attr'],
			properties: {
				value: {
					get: function() {
						return '1';
					}
				}
			},
			events: {
				
				create: function() {
					this.log += 'Created';
				}
			}
		});

		var cls2 = zs.customElement(cls1, 'inherit-child', null, {
			prop2: 2,
			observedAttributes: ['test'],
			properties: {
				name: {
					get: function() {
						return 'name';
					}
				}
			},
			events: {
				attributeChange: function(event) {
					this.log += event.detail.attributeName;
				},
				attach: function() {
					this.log += 'Attached';
				}
			}
		});

		var el = new cls2();
		console.log(el.eventHandlers);
		console.log(el.__proto__,el.observedAttributesList, el.observedAttributes);
		expect(el.prop).toBe(1);
		expect(el.method()).toBe(2);
		expect(el.value).toBe('1');
		expect(el.name).toBe('name');
		expect(el.prop2).toBe(2);
		document.body.appendChild(el);
		el.setAttribute('test', '1');
		el.setAttribute('attr', '2');
		setTimeout(function() {
			expect(el.log).toBe('CreatedAttachedtestattr');
			done();
		},20);
	});
});

