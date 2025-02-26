/**
* 	ZS Table layout polyfill
*/
var elementMain;
(function (win, doc) {
	var // Variables
		elementAside, elementLayout, elementBody,
		// Constants
		RESIZE_LISTENER,
		RESIZE_TIMEOUT = 50,
		LAYOUT_SELECTOR = '.zs-layout-table';
	
	function hasScrollbar() {
		// The Modern solution
		if (typeof window.innerWidth === 'number')
			return window.innerWidth > document.documentElement.clientWidth
		
		// rootElem for quirksmode
		var rootElem = document.documentElement || document.body
		
		// Check overflow style property on body for fauxscrollbars
		var overflowStyle
		
		if (typeof rootElem.currentStyle !== 'undefined')
			overflowStyle = rootElem.currentStyle.overflow
		
		overflowStyle = overflowStyle || window.getComputedStyle(rootElem, '').overflow
		
			// Also need to check the Y axis overflow
		var overflowYStyle
		
		if (typeof rootElem.currentStyle !== 'undefined')
			overflowYStyle = rootElem.currentStyle.overflowY
		
		overflowYStyle = overflowYStyle || window.getComputedStyle(rootElem, '').overflowY
		
		var contentOverflows = rootElem.scrollHeight > rootElem.clientHeight
		var overflowShown    = /^(visible|auto)$/.test(overflowStyle) || /^(visible|auto)$/.test(overflowYStyle)
		var alwaysShowScroll = overflowStyle === 'scroll' || overflowYStyle === 'scroll'
		
		return (contentOverflows && overflowShown) || (alwaysShowScroll)
	}
	

	function ie() {
		var undef, rv = -1; // Return value assumes failure.
		var ua = window.navigator.userAgent;
		var msie = ua.indexOf('MSIE ');
		var trident = ua.indexOf('Trident/');

		if (msie > 0) {
			// IE 10 or older => return version number
			rv = parseInt(ua.substring(msie + 5, ua.indexOf('.', msie)), 10);
		} else if (trident > 0) {
			// IE 11 (or newer) => return version number
			var rvNum = ua.indexOf('rv:');
			rv = parseInt(ua.substring(rvNum + 3, ua.indexOf('.', rvNum)), 10);
		}

		return ((rv > -1) ? rv : undef);
	}


	function addEvent(type, func) {
		type = "on" + type;
		var oldevent = win[type];

		if (typeof win[type] !== "function") {
			win[type] = func;
		} else {
			win[type] = function () {
				if (oldevent) {
					oldevent();
				}
				func();
			};
		}
	}

	function getElementsByClassName(node, classname) {
		var a = [];
		var re = new RegExp('(^| )' + classname + '( |$)');
		var els = node.getElementsByTagName("*");
		for (var i = 0, j = els.length; i < j; i++)
			if (re.test(els[i].className)) a.push(els[i]);
		return a;
	}

	function getElementByClassName(node, className) {
		var elements;
		elements = getElementsByClassName(node, className);
		return elements && elements[0] || null;
	}

	function setLayout() {
		//return;
		var bodyHeight, mainHeight, asideHeight;
		if (!doc.querySelector) { return; } // FIX a strange IE bug when querySelectoris undefined
	
		// Get layout container
		if (!elementLayout) {
			elementLayout = doc.querySelector(LAYOUT_SELECTOR);
			if (!elementLayout) {
				return; // Layout container not found
			}

		}
		
		// Get body 
		if (!elementBody) {
			elementBody = doc.querySelector(LAYOUT_SELECTOR + ' >section, ' + LAYOUT_SELECTOR + ' .zs-layout-body');
			if (elementBody) {
				elementBody.style.zoom = 1;
			}
		}		
		
		// Get main 
		if (!elementMain && elementBody) {
			elementMain = doc.querySelector(LAYOUT_SELECTOR + ' main, ' + LAYOUT_SELECTOR + ' .zs-layout-main');
		}
		
		// Get aside		
		if (!elementAside && elementMain) {
			elementAside = doc.querySelector(LAYOUT_SELECTOR + ' main>aside, ' + LAYOUT_SELECTOR + ' .zs-layout-aside');
		}


		if (elementBody && elementMain) {				
			bodyHeight = elementBody.clientHeight;			
			mainHeight = elementMain.clientHeight;			
			//asideHeight = elementAside.clientHeight;
			
			if (hasScrollbar()) { // We have a vertical scroll
				var ieVer = ie();
				if (ieVer <= 10) { // Do we need this? Yes we do because we run it in all browser for IE10.
					elementMain.style.height = 0;					
					bodyHeight = elementBody.clientHeight;
					if (bodyHeight == 0) { // IE9 issue
						bodyHeight = elementBody.offsetHeight;
					}
					elementMain.style.height = bodyHeight + 'px';
				}						
			} else if (bodyHeight !== mainHeight) {				
				if (bodyHeight == 0) { // IE9 issue
					bodyHeight = elementBody.offsetHeight;
				}
				elementMain.style.height = bodyHeight + 'px';				
			}
		}
	}


	function attachResizeListener(construct, params) {
		if (!RESIZE_LISTENER) {
			var storedWidth, storedHeight,
				currentWidth, currentHeight,
				docBody = doc.body,
				docEl = doc.documentElement,
				resizeTimer,
				innerWidth = "innerWidth", innerHeight = "innerHeight",
				clientWidth = "clientWidth", clientHeight = "clientHeight";

			addEvent("resize", function () {
				if (resizeTimer) {
					window.clearTimeout(resizeTimer);
				}

				resizeTimer = window.setTimeout(function () {
					currentWidth = win[innerWidth] || docEl[innerWidth] || docEl[clientWidth] || docBody[clientWidth];
					currentHeight = win[innerHeight] || docEl[innerHeight] || docEl[clientHeight] || docBody[clientHeight];

					if (storedWidth !== currentWidth || storedHeight !== currentHeight) {
						setLayout(currentWidth, currentHeight);

						storedWidth = currentWidth;
						storedHeight = currentHeight;
					}
				}, RESIZE_TIMEOUT);
			});
			RESIZE_LISTENER = true;
		}
	}

	function init() {
		//return;
		attachResizeListener();
		setLayout();
	}
	addEvent("load", init);

} (typeof window !== "undefined" ? window : this, document));