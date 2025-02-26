// TODO: zs.state: add deleteState method to support removing items from storage.
var zs = (function(zs) {
	'use strict';

	// Manage state of the application using location and localstorage API
	zs.state = {
		url: null,
		query: null,
		state: null,
		parseQuery: function (query) { // A simple method to parse query string of URL which doesn't cover all edge cases. You can use Url native API instead or similar. Override when necessary.			
			var i;
			if (!query) { return null; }
			if (query.slice(0, 1) == '?') {
				query = query.slice(1);
			}
			// Empty query
			this.query = this.query || {};
			for (i in this.query) {
				delete this.query[i];
			}

			var vars = query.split('&');
			for (i = 0; i < vars.length; i++) {
				var pair = vars[i].split('=');
				this.query[decodeURIComponent(pair[0])] = decodeURIComponent(pair[1]);
			}
			return this.query;
		},
		reflectState: function () { // called after URL is updated and defines what is reflected in the state			
			var params = {
				hash: this.url.hash,
				pathname: this.url.pathname
			};
			Object.assign(params, this.query);
			this.updateState(params);
		},		
		updateState: function (newObject) {
			var whatChanged;
			var combinded = Object.create(this.state);
			Object.assign(combinded, newObject);
			if (this.state) {
				for (var i in combinded) {
					if (this.state[i] != combinded[i]) {
						whatChanged = whatChanged || {};
						whatChanged[i] = combinded[i];
					}
				}
			}
			this.state = this.state || {};
			Object.assign(this.state, combinded);
			var event = new CustomEvent('statechange', { detail: { newState: this.state, changed: whatChanged } });
			this.dispatchEvent(event);
		},
		parseUrl: function(url) {
			var a = document.createElement('a');
			a.href = url;
			this.url = {};

			Object.assign(this.url, {
				href: a.href,
				protocol: a.protocol,
				host: a.host,
				hostname: a.hostname,
				port: a.port,
				pathname: a.pathname,
				search: a.search,
				hash: a.hash,
				username: a.username,
				password: a.password,
				origin: a.origin
			});

			// Normalize URL (IE11 vs others)
			if (this.url.pathname && this.url.pathname.slice(0,1) == '/') {
				  this.url.pathname = this.url.pathname.slice(1);
			}
			if (this.url.search && this.url.search.slice(0,1) == '?') {
				  this.url.search = this.url.search.slice(1);
			}
			return this.url;
		},
		watchLocation: function() {
			var comp = this;
			window.addEventListener("hashchange", function () {
				comp.updateUrl();
			});
		},
		saveState: function (name, state, exclude) {
			var toSave = {};
			Object.assign(toSave, state || this.state);
			
			// Remove unwanted properties
			exclude = exclude || ['hash', 'pathname']; // By default we want to exclude URL based parameters from saving to localStorag.
			for (var i in exclude) {
				delete toSave[exclude[i]];
			}

			localStorage.setItem(name , JSON.stringify(toSave));
		},
		loadState: function(name) {
			var str = localStorage.getItem(name);
			if (str) {
				this.updateState(JSON.parse(str));
			}
		},
		/**
		 * Convert object to URL query string like "param1=value1&param2=value2..."
		 * @param {object} obj - Object to convert
		 * @return {string} Query string
		 */
		serialize: function (obj) {
			var str = [];
			if (!obj) { return; }
			for (var p in obj) {
				if (!obj.hasOwnProperty || obj.hasOwnProperty(p)) {
					str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));
				}
			}
			return str.join("&");
		},

		/**
		 * Convert URL object to URL string
		 * @param {object} url - Object to convert
		 * @return {string} URL
		 */
		joinUrl: function (url) {
			url = url || this.url;
			if (url.href) {return url.href};
			var newUrl = document.createElement('a');
			newUrl.href = location.href;
			Object.assign(newUrl, url);			
			return newUrl.href;
		},

		updateUrl: function(newUrl) {
			this.parseUrl(newUrl || location.href);
			this.parseQuery(this.url.search);
			this.reflectState();
		},

		events: {
			create: function() {
				this.watchLocation();
			}
		}		
	};

	// Service
	zs.service = {
		services: {}, // services are shared accross all instances
		registerService: function(name, fn) {
			this.services[name] =  fn;
		},
		service: function(name, params) {
			if (!this.services[name]) {return;}
			var comp = this;
			return new Promise(function(resolve, reject) {
				comp.services[name](params, resolve, reject);
				//resolve({test:1});
			});
		},

		/**
		 * Save or load data from cache
		 * @param {string} key - Key of the data to load
		 * @param {object} data - Data to save
		 * @param {number=} expire - How many milliseconds to store the data for.		 * 
		 * @return {object|false} -  Cached data or false if it is expired.
		 */
		cache: function(key, data, expire) {
			if (!localStorage) {return false;} // In IE11 localstorage is disabled for localhost or via file:///. Add to trusted zone.

			if (!data) {
				// Load data
				var str = localStorage.getItem(key);
				if (!str) {return false;}
				var obj = JSON.parse(str);
				if (!obj.data) {return false}; // No data
				if (obj.expire) { // Check expiration
					var now = (new Date).valueOf();
					if (now>obj.expire) {
						return false;
					} 
				}
				return obj.data
			}

			// Save data
			var toSave = {
				data: data
			};	

			// Add expiration marker
			if (expire) {
				var expireOn = new Date();
				expireOn = (new Date).valueOf() + expire;	
				toSave.expire = expireOn;
			}

			localStorage.setItem(key , JSON.stringify(toSave));
		}	
	};

	// Configuration
	zs.configuration = {
		config: {},
		configure: function() {
			if (!arguments.length) {return;}
			if (!this.config) {this.config = {};}
			for (var i=0;i<arguments.length;i++) {
				if (typeof arguments[i] != 'object') {continue;}
				Object.assign(this.config, arguments[i]);
			}
			var event = new CustomEvent('configure');					
			this.dispatchEvent(event);
		}
	};
	
	
	// App
	zs.app = {
		ready: function() {
			var event = new CustomEvent('ready');					
			this.dispatchEvent(event);
		}
	};
	
	zs.appElement =  zs.customElement(HTMLElement, 'zs-app', 'div', [zs.configuration, zs.store, zs.state, zs.service, zs.app]);
	return zs;
})(window.zs || {});