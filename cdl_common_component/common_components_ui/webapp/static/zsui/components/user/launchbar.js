

// TODO: copied from component/mobile/zsMobile.js need to make re-usable
var zs = (function (ns) {
	ns = ns || {};

	ns.subMenu = function () {
		var $icon = $(this);
		var $li = $icon.parent().parent();
		var $nav = $li.find('>nav');
		var inProcess = false;
		var $a = $li.find('>a').first();
		var isDisabled = false;

		function clickAnywhere(event) {
			if (!inProcess && $nav[0].isExpanded &&
					$nav[0] != event.target &&
					!$.contains($li[0], event.target) &&
					!$.contains($nav[0], event.target)) {
				collapse();
			}
		}


		function disable() {
			//console.log('disable');
			if (isDisabled) { return; }
			isDisabled = true;
			setTimeout(function () {
				//console.log('enable');
				isDisabled = false;
			}, 500);
		}

		function collapse() {
			//console.log('collapse', $nav[0].isDelayingCollapse, $nav[0].isDelayingExpand);
			$nav.hide();
			$li.removeClass('zs-selected');
			$(document).off('click touchstart', clickAnywhere);
			$nav[0].isExpanded = false;
			$nav[0].isDelayingCollapse = false;

		}
		function expand() {
			//console.log('expand', $nav[0].isDelayingCollapse, $nav[0].isDelayingExpand);

			// Detect the offset
			if (typeof $li[0].getBoundingClientRect == 'function') {
				var rect = $li[0].getBoundingClientRect(), alignClass = '';
				if (window.innerWidth / 2 > (rect.left + rect.width / 2)) {
					alignClass = 'zs-to-left';
				}
			}

			inProcess = true;
			$li.addClass('zs-selected');
			if (alignClass) {
				$li.addClass('zs-to-left');
			} else {
				$li.removeClass('zs-to-left');
			}
			$(document).on('click touchstart', clickAnywhere);
			$nav.show();
			$nav[0].isExpanded = true;
			$nav[0].isDelayingExpand = false;

			setTimeout(function () {
				inProcess = false;
			});

		}

		function delayedExpand() {
			//console.log('delayedExpand', event.target, $nav[0].isExpanded, $nav[0].isDelayingCollapse, $nav[0].isDelayingExpand);				
			if ($nav[0].isDelayingCollapse) { $nav[0].isDelayingCollapse = false; }
			if ($nav[0].isExpanded) { return; }
			if ($nav[0].isDelayingExpand) { return; }
			$nav[0].isDelayingExpand = true;
			disable();
			setTimeout(function () {
				if (!$nav[0].isDelayingExpand) { return; } // Block expansion when is terminated by mouse out.
				expand();
			}, 300);
		}

		function delayedCollapse(event) {
			//console.log('delayedCollapse', event.target, $nav[0].isExpanded, $nav[0].isDelayingCollapse, $nav[0].isDelayingExpand);				
			if ($nav[0].isDelayingExpand) { $nav[0].isDelayingExpand = false; }
			if (!$nav[0].isExpanded) { return; }
			$nav[0].isDelayingCollapse = true;
			disable();
			setTimeout(function () {
				if (!$nav[0].isDelayingCollapse) { return; } // Block expansion when is terminated by mouse out.
				collapse();
				$nav[0].isDelayingCollapse = false;
			}, 300);
		}

		if ($nav.length) {
			// State of the submenu
			$nav[0].isDelayingExpand = false;
			$nav[0].isDelayingCollapse = false;
			$nav[0].isExpanded = false;

			$li.on('click', function (event) {
				event.stopPropagation();
				if (isDisabled) { return; }
				if ($nav[0].isExpanded) {

					collapse($nav);
				} else {
					expand($nav);
				}

			});


			$a.click(function (event) {
				event.preventDefault();
				//return false;
			});

			$li.on('mouseenter', delayedExpand);
			$li.on('mouseleave', delayedCollapse);
		}
	}
	return ns;
})(zs);



var ZSLB = function () {
    var MOUSEOUT_DELAY = 200;
    var CLASS_MENU_ON = "lbar-menu-on";
    var CLASS_MENU_OFF = "lbar-menu-off";
    var TIMEOUT_NULL = -1;

    var _isTouch = ("ontouchstart" in window);
    var _hoverTimeoutId = TIMEOUT_NULL;
    var _menuElement = null;
    var _touchClickFlag = false;

    var $container,         // jQuery objects
        $appMessageBar,
        $appMessageClose,
        $currentMenu;
    

	

	// private functions
    function showAppLevelMessage () {
        var $appMessage = $appMessageBar.find('.non-prod-msg');
        if ($appMessage.length) {
            $appMessage.show();
            $appMessageBar.show();
        }
    }

    function registerEvents() {

    	$container.find('>div>a>.zs-icon:first-child').each(zs.subMenu);		

        if ($appMessageBar.length) {
            $appMessageClose = $appMessageBar.find('a.zs-icon-rejected-approval');
            var $msg = $appMessageBar;
            $appMessageClose.click(function (event) {
                $msg.hide();
            });
        }	
    }

    function registerZsEventHandlers () {
        // At this point, we know jQuery exists, so we can use it
        $container = $('.zs-user-bar');
        $appMessageBar = $('.zs-app-message');
        registerEvents();
        showAppLevelMessage();
    }

    function displayZsLaunchbar(lbr) {
    	var container = lbr.parentNode;

    	if (container) { // Display message    		
    		var appMessage = container.querySelector('.non-prod-msg');
    		if (appMessage) {
    			appMessage.parentElement.removeChild(appMessage);
    			var appMessageBar = document.querySelector('.zs-app-message');
    			if (appMessageBar) {
    			    appMessageBar.removeChild(appMessageBar.children[0]);
    			    appMessageBar.insertBefore(appMessage, appMessageBar.children[0]);
    			}
    		}
    	}

    	
    	var userBar = document.querySelector('.zs-user-bar');
    	if (!userBar) {
    		userBar = document.createElement('div');
    		userBar.className = 'zs-user-bar';
    		var header = document.querySelector('.zs-header');
    		if (header) {
    			var brandBar = header.querySelector('.zs-brand-bar');
    			if (!brandBar) {
    				brandBar = document.createElement('div');
    				brandBar.className = 'zs-brand-bar';
    				header.appendChild(brandBar);
    			}
    			brandBar.appendChild(userBar);
    		}
    	}
			
		// Disable application when empty (TODO: refactor this)
    	var profileContainer =lbr.children[0];
    	var appContainer = lbr.children[1];
    	if (!appContainer.getElementsByTagName('nav')[0].children.length) {
            appContainer.setAttribute('disabled', 'disabled');
    	}	

		// Add launchbar elements
    	userBar.insertBefore(appContainer, userBar.firstChild);
    	userBar.insertBefore(profileContainer, appContainer);   

        registerZsEventHandlers();
    }

    function displayLegacyLaunchbar(legacyLbr) {
    	var container = document.querySelector('#lbr');
    	if (container) {
    		container.innerHTML = legacyLbr.innerHTML;
    		container.style.display = "block";
    	} else {
    		container = document.querySelector('.zs-header');
    		if (!container) {
    			container = document.body;
    		}
    		if (container.firstChild) {
    			container.insertBefore(legacyLbr, container.firstChild)
    		} else {
    			container.appendChild(legacyLbr);
    		}
    		legacyLbr.style.display = "block";
    	}    	
    	onLoad();    	
    }

    // public functions
    var enterMenu = function(menuElement) {
        if (!menuElement) return;
        var sameMenu = menuElement === _menuElement;
        var hadMenu = _menuElement;

        if (hadMenu && (!sameMenu || _isTouch)) {
            _menuElement.className = CLASS_MENU_OFF;
            _menuElement = null;
        }

        if (!_isTouch || !hadMenu) {
            _menuElement = menuElement;
            _menuElement.className = CLASS_MENU_ON;
        }

        if (_hoverTimeoutId !== TIMEOUT_NULL) {
            clearTimeout(_hoverTimeoutId);
            _hoverTimeoutId = TIMEOUT_NULL;
        }
    };

    var exitMenu = function(menuElement) {
        _hoverTimeoutId = setTimeout(function() {
            if (_menuElement && menuElement !== _menuElement) {
                _menuElement.className = CLASS_MENU_OFF;
                _menuElement = null;
            }
        }, MOUSEOUT_DELAY);
    };

    var menuTouchStart = function() { _touchClickFlag = true; };
    var menuTouchMove = function() { _touchClickFlag = false; };
    var menuTouchCancel = function() { _touchClickFlag = false; };

    var processEventTarget = function (e) {
        var evt = window.event || e;
        if (!evt.target) evt.target = evt.srcElement;
        var target = evt.target;
        while (target.className !== CLASS_MENU_OFF && target.className !== CLASS_MENU_ON && target.parentNode) {
            target = target.parentNode;
        }
        return target;
    };

    var menuTouchEnd = function(e) {
        if (_touchClickFlag && _menuElement) {
            var target = processEventTarget(e);
            if (target !== _menuElement) {
                _menuElement.className = CLASS_MENU_OFF;
                _menuElement = null;
            }
        }
    };

    var onLoad = function() {
        var userMenuLink = document.getElementById("lbar-user-menu-link");
        var userMenuPopup = document.getElementById("lbar-user-menu-popup");

        var appMenuLink = document.getElementById("lbar-app-menu-link");
        var appMenuPopup = document.getElementById("lbar-app-menu-popup");

        var notificationMenuLink = document.getElementById("lbar-notifications-menu-link");
        var notificationMenuPopup = document.getElementById("lbar-notifications-menu-popup");

        if (_isTouch) {
            // Touch devices need to listen for the click action rather than mouseover/out events
            userMenuLink.onclick = function(e) {
                enterMenu(processEventTarget(e));
            };

            if (appMenuLink) {
                appMenuLink.onclick = userMenuLink.onclick;
            }

            if (notificationMenuLink) {
                notificationMenuLink.onclick = userMenuLink.onclick;
            }

            addEvent(document, "touchstart", menuTouchStart);
            addEvent(document, "touchmove", menuTouchMove);
            addEvent(document, "touchcancel", menuTouchCancel);
            addEvent(document, "touchend", menuTouchEnd);

        } else {
            var mouseOver = function(e) {
                enterMenu(processEventTarget(e));
            };

            userMenuLink.onmouseover = userMenuPopup.onmouseover = mouseOver;
            userMenuLink.onmouseout = userMenuPopup.onmouseout = exitMenu;

            if (appMenuLink && appMenuPopup) {
                appMenuLink.onmouseover = appMenuPopup.onmouseover = mouseOver;
                appMenuLink.onmouseout = appMenuPopup.onmouseout = exitMenu;
            }

            if (notificationMenuLink && notificationMenuPopup) {
                notificationMenuLink.onmouseover = notificationMenuPopup.onmouseover = mouseOver;
                notificationMenuLink.onmouseout = notificationMenuPopup.onmouseout = exitMenu;
            }
        }
    };

    var LinkClick = function(id) {
        var link = document.getElementById('lbl_' + id);
        return ((typeof MasterModuleClickHandler != 'undefined') ? MasterModuleClickHandler() && NavigateTo(link) : NavigateTo(link));
    };

    var ShouldJailbreakParent = function(parentWindow) {
        var hostName = window.location.hostname;
        var parentHostName;
        try {
            parentHostName = parentWindow.location.hostname;
        } catch (e) {
            parentHostName = null;
        }
        return (parentHostName && hostName.toLowerCase() == parentHostName.toLowerCase());
    };

    var NavigateTo = function(link) {
        var url = link.href;
        var target = link.target;
        if (target !== "") {
            window.open(url, target);
            return false;
        }
        if (top.frames.length != 0) {
            var parentWindow = window;
            while (parentWindow != window.top && ShouldJailbreakParent(parentWindow.parent)) {
                parentWindow = parentWindow.parent;
            }
            parentWindow.location.href = url;
        } else {
            window.location.href = url;
        }
        return false;
    };

    var getXMLHttpRequest = function() {
        if (window.XMLHttpRequest) {
            return new window.XMLHttpRequest;
        } else {
            try {
                return new ActiveXObject("MSXML2.XMLHTTP.3.0");
            } catch (ex) {
                return null;
            }
        }
    };

    var addEvent = function(obj, evType, fn) {
        if (obj.addEventListener) {
            obj.addEventListener(evType, fn, false);
            return true;
        } else if (obj.attachEvent) {
            var r = obj.attachEvent("on" + evType, fn);
            return r;
        } else {
            return false;
        }
    };

	var removeLaunchbar = function () {
		// remove zs launchbar
		var userBar = document.querySelector('.zs-user-bar ');
		if (userBar ) {
    		var profile = userBar.querySelector('[role="profile"]');
    		var apps = userBar.querySelector('[role="apps"]');
    		if (profile) { profile.parentNode.removeChild(profile); }
    		if (apps) { apps.parentNode.removeChild(apps); }
		}

		// remove legacy launchbar
		var legacy = document.querySelector('#lbr');
		if (legacy) {
			legacy.innerHTML = '';
		}
	}

    var displayLaunchBar = function (zsLbr, legacyLbr) {    	
    	var userBar = document.querySelector('.zs-user-bar');
    	var header = document.querySelector('.zs-header');

		// By default try to display ZS launch bar
    	var displayLegacy = false; 
    	var displayZs = true;

		
    	if (!zsLbr || (!header && !userBar)) { // Enforce legacy when we don't have zs-header or zs-user-bar
    		displayZs = false;
    		if (legacyLbr) { // But only if we have it 
    			displayLegacy = true;
    		}			
    	} 

		removeLaunchbar();

    	if (displayZs) {
    		displayZsLaunchbar(zsLbr);
    	} 

    	if (displayLegacy) {
    		displayLegacyLaunchbar(legacyLbr);
    	}
	}
	
    var error = function () {
    	console.log('launchbar.js error:', ex);
    	var error = document.createElement('div');
    	// error message copied from LaunchBar.vb
    	var header = document.querySelector('.zs-header');
    	if (header) {
    		$('.zs-header link').remove();
    		$('#zs-lbr').remove();
    		error.innerHTML = '<div id="lbr"><div class="zs-message-container"><div class="zs-message zs-error">An error occurred while rendering the launch bar.</div></div></div>';
    		header.insertBefore(error, header.children[0]);
    	} else {
    		error.innerHTML = '<div id="lbar" class="clearfix">An error occurred while rendering the launch bar.</div>';
    		var body = document.querySelector('body');
    		body.insertBefore(error, body.children[0]);
    	}
    }

    var handleLaunchbarResponse = function (request) {
        if (request.readyState == 4 /* complete */) {
        	if (request.status == 200) {
        		// This variable's value should be taken from the response
        		var tempContainer = document.createElement('div');
        		tempContainer.setAttribute('id', 'temporaryLbrContainer');
        		tempContainer.setAttribute('style', 'display:none');
        		tempContainer.innerHTML = request.responseText;
        		var zsLbr = tempContainer.querySelector('#zs-lbr');
        		var legacyLbr = tempContainer.querySelector('#lbr');
        		displayLaunchBar(zsLbr, legacyLbr);
        	}
        }
    };

    var loadLaunchBar = function (url, applicationId, userId, authenticatedUserId, returnUrl, logoutUrl, locale, isIdentityManager) {

    	function load() {
    		// Make a request
    		var request = getXMLHttpRequest();
    		if (request != null) {
    			request.open("POST", url, true);
    			request.onreadystatechange = function () { handleLaunchbarResponse(request); };

    			var params = "applicationId=" + applicationId + "&userId=" + userId + "&authenticatedUserId=" + authenticatedUserId +
					"&returnUrl=" + returnUrl + "&logoutUrl=" + logoutUrl + "&locale=" + locale + "&isIdentityManager=" + isIdentityManager;

    			// set the necessary request headers
    			request.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
    			request.send(params);
    		}
    	}

    	// On dom ready implementation
    	var isReady = false;
    	document.onreadystatechange = function () {
    		if (document.readyState === "interactive") { 
    			isReady = true;
    			load();
    		} else if (document.readyState === "complete") { // IE8 supports only this
    			if (!isReady) {
    				load();
    				isReady = true;
    			}
    		}
    	}
    	
    };
    return {
        enterMenu: enterMenu,
        exitMenu: exitMenu,
        menuTouchStart: menuTouchStart,
        menuTouchMove: menuTouchMove,
        menuTouchCancel: menuTouchCancel,
        menuTouchEnd: menuTouchEnd,
        onLoad: onLoad,
        processEventTarget: processEventTarget,
        LinkClick: LinkClick,
        ShouldJailbreakParent: ShouldJailbreakParent,
        NavigateTo: NavigateTo,
        getXMLHttpRequest: getXMLHttpRequest,
        addEvent: addEvent,
        handleLaunchbarResponse: handleLaunchbarResponse,
        loadLaunchBar: loadLaunchBar,
        registerZsEventHandlers: registerZsEventHandlers,
	displayZsLaunchbar: displayZsLaunchbar
    };

    
}();

// @todo modules actualy MUST NOT be global
if (typeof module !== 'undefined' && module.exports) {
    global.ZSLB = ZSLB;
}
