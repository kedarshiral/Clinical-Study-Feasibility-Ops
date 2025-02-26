var zs = (function (zs) {
	'use strict';

	zs.debounce = function (func, wait, immediate) {
		// taken from http://underscorejs.org/#debounce, but modified to support canceling
		var timeout;
		return function () {
			var context = this, args = arguments;
			var later = function () {
				timeout = null;
				if (!immediate) func.apply(context, args);
			};
			var callNow = immediate && !timeout;
			clearTimeout(timeout);
			timeout = setTimeout(later, wait);
			if (callNow) { func.apply(context, args); return null; }
			return timeout;
		};
	};

	zs.addBehavior = function (obj, properties) {
		var events, propertyDescriptors, i;
		if (!properties) { return; }

		// Clone events
		if (properties.events) {
			events = Object.assign({}, properties.events);
		}

		Object.assign(obj, properties); // IE11 need a polyfill

		// Handle events
		if (events) {
			for (i in events) {
				zs.addCustomEvent(obj, i, events[i]);
			}
			delete obj.events;
		}

		// Handle observed attributes
		if (properties.observedAttributes) {
			if (!obj.observedAttributesList) {			
				obj.observedAttributesList = [];
			}
			Array.prototype.push.apply(obj.observedAttributesList,properties.observedAttributes);			
		}

		// Handle properties getters and setters
		if (properties.properties) {
			propertyDescriptors = Object.assign({}, properties.properties);
			Object.defineProperties(obj, propertyDescriptors);
		}
	};

	zs.addCustomEvent = function (obj, eventType, eventHandler) {
		if (!obj.eventHandlers) { obj.eventHandlers = {}; }
		if (!obj.eventHandlers[eventType]) {
			obj.eventHandlers[eventType] = [];
		}
		if (eventHandler) {
			obj.eventHandlers[eventType].push(eventHandler);
		}
		return obj.eventHandlers[eventType];
	};

	zs.pipeEvent = function (eventType, source, target) {
		source.addEventListener(eventType, function (e) {
			var newEvent = new CustomEvent(e.type);
			if (e.detail) {
				Object.assign(newEvent.detail, e.detail);
			}
			target.dispatchEvent(newEvent);
		});
	};


	// Babel helpers
	function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }
	function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }
	function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

	zs.customElement = function (parentClass, isWhat, tag, behaviors) { // v1 spec implementation using https://github.com/WebReflection/document-register-element 	
		var params, attributesToObserve = [];
		
		var elementClass = function (_parentClass) {
			_inherits(elementClass, _parentClass);	
			

			function elementClass(self) {
				var _this;

				_classCallCheck(this, elementClass);

				// Mimic super
				var self = (_this = _possibleConstructorReturn(this, (elementClass.__proto__ || Object.getPrototypeOf(elementClass)).call(this, self)), _this);

				// Add event listeners only once 
				if (!self._isCreated) {
					for (var i in self.eventHandlers) {					
						var event = new CustomEvent(i);					
						for (var j=0; j<self.eventHandlers[i].length; j++) {
							self.addEventListener(event.type, self.eventHandlers[i][j]);
						}
					}

					// Trigger create
					var event = new CustomEvent('create');
					self.dispatchEvent(event);

					self._isCreated = true;
				}

				return self;
			}		

			return elementClass;
		}(parentClass);


	
		// Add behaviors
		if (behaviors) {
			if (!Array.isArray(behaviors)) {
				behaviors = [behaviors];
			}
			for (var i=0; i<behaviors.length; i++) {
				zs.addBehavior(elementClass.prototype, behaviors[i]);
			}
		}

		// Lifecycle callbacks		
		elementClass.prototype.connectedCallback = function () {
			var event = new CustomEvent('attach');
			this.dispatchEvent(event);
		};
		elementClass.prototype.adoptedCallback = function () {
			var event = new CustomEvent('adapt');
			this.dispatchEvent(event);
		};
		elementClass.prototype.disconnectedCallback = function () {
			var event = new CustomEvent('detach');
			this.dispatchEvent(event);
		};
		elementClass.prototype.attributeChangedCallback = function (attributeName, oldValue, newValue, namespace) {
			var event = new CustomEvent('attributeChange', { detail: { attributeName: attributeName, oldValue: oldValue, newValue: newValue, namespace: namespace } });
			this.dispatchEvent(event);
		};

		// Handle observed attributes		
		if (elementClass.prototype.observedAttributesList) {			
			elementClass.observedAttributes = elementClass.prototype.observedAttributesList.slice(0);
		}

		if (tag) {
			params = { extends: tag };
		}

		customElements.define(isWhat, elementClass, params);
		return elementClass;

	}


	return zs;
})(window.zs || {});


// Object.assign polyfill for IE11 and iOS 9
if (window && window.ObjectAssign) { window.ObjectAssign.polyfill(); }