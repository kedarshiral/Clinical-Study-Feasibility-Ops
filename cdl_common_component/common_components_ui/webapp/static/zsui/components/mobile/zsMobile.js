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
			var rect = $li[0].getBoundingClientRect(), alignClass = '';
			if (window.innerWidth / 2 > (rect.left + rect.width/2)) {
				alignClass = 'zs-to-left';
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

			$li.on('click',function (event) {
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



(function ($) {
	'use strict';


	$(document).ready(function () {

		
		$('.zs-nav select').on('change', function () {
			var url = $(this).val();
			if (url) {
				window.location = url;
			}
		});	
		$('.zs-top-navigation .zs-icon-collapse, .zs-top-navigation .zs-icon-expand, .zs-expand').each(zs.subMenu);

	});

} (jQuery));