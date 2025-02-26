describe("Application helpers", function () {
	describe("configuration", function () {
		var comp = zs.customElement(HTMLElement, 'my-element', 'div', zs.configuration);
		var configuration = {
			test: 2
		}
		it('Creates a shared configuration for the element', function () {
			utils.isBehavior(expect, zs.configuration);
			var el = new comp();
			var el2 = new comp();
			el.configure(configuration);
			expect(typeof el.config).toBe('object');
			expect(el.config).toBe(el2.config);
			el.configure({
				value: 3
			});
			expect(el.config.value).toBe(3);
			expect(el2.config.test).toBe(2);

		});
	});
	describe("service", function () {
		var el, el2,
			comp = zs.customElement(HTMLElement, 'my-element-with-service', 'div', zs.service),
			results = {};

		var myService = function (params, resolve, reject) {
			if (params.resolve) {
				resolve({ status: 'ok' });
			} else {
				reject({ status: 'fail' });
			}
		};
		beforeEach(function (done) {
			el = new comp();
			el2 = new comp();
			el.registerService('data', myService);
			el.service('data', { resolve: true }).then(function (data) {
				results.resolve = data;

			});

			el.service('data', { resolve: false }).catch(function (data) {
				results.reject = data;
				done();
			})


		});
		it('Registers shared services', function () {
			utils.isBehavior(expect, zs.service);
			expect(typeof el.registerService).toBe('function');

			el.registerService('data', myService);
			expect(typeof el.services).toBe('object');
			expect(el2.services).toBe(el.services);
			expect(el.services['data']).toBe(myService);
		});

		it('Calls a service', function () {
			expect(typeof el.service).toBe('function');
			expect(results.resolve.status).toBe('ok');
			expect(results.reject.status).toBe('fail');
		});

		it('Can cache data', function (done) {
			var key = 'testKey' + Math.round(Math.random * 1000);
			expect(typeof el.cache).toBe('function');

			// Only if localStorage is enabled
			if (!localStorage) { // In IE11 localStorage doesn't work from localhost by default
				expect(el.cache(key, { test: 1 })).toBeFalsy();
				expect(el.cache(key)).toBeFalsy(); // returns false always
				done();
				return;
			}

			var result = el.cache(key, { test: 1 });
			expect(result).toBeFalsy();
			result = el.cache(key);
			expect(result.test).toBe(1);

			// Expiration
			var result = el.cache(key, { test: 2 }, 10); // store for 10ms
			expect(result).toBeFalsy();
			setTimeout(function () {
				result = el.cache(key);
				expect(result).toBeFalsy(); // Expired after 20ms
				localStorage.removeItem(key);
				done();
			}, 20);

			// Not expired yet
			result = el.cache(key);
			expect(result.test).toBe(2);
		});
	});
	describe("state", function () {
		var el, el2,
			comp = zs.customElement(HTMLElement, 'my-element-with-state', 'div', zs.state),
			results = {};

		beforeEach(function (done) {
			el = new comp();
			el2 = new comp();
			el2.addEventListener('statechange', function (event) {
				results = event.detail;
			})
			el2.updateState({ test: 1, query: 2 });
			el2.updateState({ test: 2, query: 2, some: 3});
			done();

		});
		it('can parse url', function () {
			utils.isBehavior(expect, zs.state);
			el.updateUrl();
			expect(typeof el.url).toBe('object');
			expect(el.url.pathname).toBeTruthy();
			el.parseUrl('http://localhost/test.html?search=1#hash');
			expect(typeof el.url).toBe('object');
			expect(el.url.pathname).toBe('test.html');
			expect(el.url.search).toBeTruthy('search=1');
			expect(el.url.hash).toBeTruthy('#hash');
		});
		it('can parse query string', function () {
			el.parseQuery('?q=2&page=3');
			expect(typeof el.query).toBe('object');
			expect(el.query.q).toBe('2');
			expect(el.query.page).toBe('3');

			el.updateUrl('test.html?q=1&page=2');
			expect(el.query.page).toBe('2');
		});
		it('can update state', function () {
			expect(el.state).toBe(null);
			el.updateState({ test: 1, field: 2 });
			expect(el.state.test).toBe(1);
			el.updateState({ field: 3, some: 4 });
			expect(el.state.test).toBe(1);
			expect(el.state.field).toBe(3);
			expect(el.state.some).toBe(4);
		});
		it('can reflect URL parameters in state', function () {
			el.updateUrl('http://localhost/test.html?search=1#hash'); // we invoke reflectState method
			expect(el.state.pathname).toBe('test.html');
			expect(el.state.search).toBe('1');
			expect(el.state.hash).toBe('#hash');
		});
		it('can save and load state', function () {
			el.updateState({ test: 1, exclude: 2 });
			if (!localStorage) { // In IE11 localStorage doesn't work from localhost by default
				expect(true).toBeTruthy();
				return;
			}
			el.saveState('mystate', undefined, ['exclude']);
			el.state = null;
			el.loadState('mystate');
			expect(el.state.test).toBe(1);
			expect(el.state.exclude).toBeFalsy();
		});
		it('can serialize state object to query like string', function () {
			var obj = { one: 1, two: 'String', three: false, four: null };
			expect(typeof el.serialize).toBe('function');
			var str = el.serialize(obj);
			expect(str).toBe('one=1&two=String&three=false&four=null');

		});
		it('can join URL from object', function () {
			var url = 'http://localhost/test.html?search=1#hash';
			var obj = el.parseUrl(url);
			expect(typeof el.joinUrl).toBe('function');
			var str = el.joinUrl(obj);
			expect(str).toBe(url);

			// Partial url
			obj = { search: 'test=1' };
			str = el.joinUrl(obj);
			var obj2 = el.parseUrl(str);
			expect(obj2.search).toBe(obj.search);
		});

		it('statechange event', function () {
			expect(results.newState.query).toBe(2);
			expect(results.newState.test).toBe(2);
			expect(results.changed.test).toBe(2);
			expect(results.changed.query).toBeFalsy();
			expect(results.changed.some).toBe(3);
		});
	});
});