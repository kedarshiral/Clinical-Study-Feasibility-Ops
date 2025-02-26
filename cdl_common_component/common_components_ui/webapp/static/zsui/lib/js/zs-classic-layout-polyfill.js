/**
* 	ZS Classic layout polyfill
*/
var elementMain;
;(function(win, doc) {
	var // Variables
		elementHeader, elementAside, elementFooter,  elementLayout, elementBody,
		// Constants
		RESIZE_LISTENER,
		RESIZE_TIMEOUT = 50,
		LAYOUT_SELECTOR  = '.zs-layout-classic-ie8';
		
		
		
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
		var re = new RegExp('(^| )'+classname+'( |$)');
		var els = node.getElementsByTagName("*");
		for(var i=0,j=els.length; i<j; i++)
			if(re.test(els[i].className))a.push(els[i]);
		return a;
	}
	
	function getElementByClassName(node, classname) {
		var elements;
		elements = getElementsByClassName(node, className);
		return elements && elements[0] || null;
	}
	
	function setLayout() {
		var elements,  bodyHeight, asideWidth, bodyWidth, mainPadding;
		if (!doc.querySelectorAll) {return;} // FIX a strange IE bug when querySelectorAll is undefined
	
		// Get layout container
		if (!elementLayout) {
			elements =	doc.querySelectorAll(LAYOUT_SELECTOR);
			elementLayout = elements && elements[0];
			if (!elementLayout) {
				return; // Layout container not found
			}
			
		}
		
		// Get body 
		if (!elementBody) {
			elements = elementLayout.querySelectorAll(LAYOUT_SELECTOR + ' > section');
			elementBody = elements && elements[0];
		}
		
		// Get aside		
		if (!elementAside && elementBody) {
			elements = elementBody.querySelectorAll(LAYOUT_SELECTOR + ' > section > aside');
			elementAside = elements && elements[0];
		}
		
		// Get main 
		if (!elementMain && elementBody) {
			elements = elementBody.querySelectorAll(LAYOUT_SELECTOR + ' > section > main');
			elementMain = elements && elements[0];
		}		
	
		if (elementBody && elementAside) {
			
			elementAside.style.height = 0; // Need this to get real height of the body.
			elementMain.style.width = 0;
			
			bodyHeight = elementBody.clientHeight;
			bodyWidth = elementBody.clientWidth;
			asideWidth = elementAside.clientWidth;
			
			elementAside.style.height = bodyHeight + 'px';
			
			// Paddings are 1.42em from both sides. In case font-size is 14px we would get 39.76px 
			mainPadding = 41;			
			elementMain.style.width = bodyWidth - asideWidth - mainPadding + 'px'; 
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
		attachResizeListener();
		setLayout();	
	}
	addEvent("load", init);
		
}(typeof window !== "undefined" ? window : this, document));