// Require jquery, jquery.zsPagination
var zs = (function(zs) {
	'use strict';
	
	// Pagination prototype
	zs.pagination = {
		events: {
			create: function() {
				//console.log('zs-pagination' , 'created');
			},
			attach: function() {
				this.classList.add('zs-pagination');
				this.configure();				
			},
			attributeChange: function(e) {
				if (e.detail) {
					var attributeName = e.detail.attributeName;
					if (attributeName == 'size' || attributeName == 'total' || attributeName == 'page') {
						this.configure();
					}
				}
			},
			detach: function() {
				$(this).zsPagination('destroy');
			}			
		},
		observedAttributes: ['size', 'total', 'page'],
		changePage: function(oldPage, newPage) {
			this.yelding = true; // Block attribute change handler
			this.setAttribute('page', newPage);
			this.yelding = false;
			
			// Trigger event
			var event = new CustomEvent('pagechange', {detail: {currentPage: newPage}});
			this.dispatchEvent(event);
		},
		configure: function(newValue, oldValue) {			
			var comp = this;
			
			// Yeld to let all attribte change trigger
			if (comp.yelding) {
				// console.log('zs-pagination', 'yelding');
				return;
			}			
			setTimeout(function() {			
				comp.yelding = false;
				//console.log('zs-pagination', 'configuring');
				
				var total = Number(comp.getAttribute('total')) || 0;
				var size = Number(comp.getAttribute('size')) || 0;
				var page = Number(comp.getAttribute('page')) || 0;
				
				$(comp).zsPagination({
					itemsCount: total,
					pageSize: size,
					currentPage: page,
					onPageChange: comp.changePage.bind(comp)
				});
			},0);
			comp.yelding = true;
		}
		
	}
	
	zs.paginationElement =  zs.customElement(HTMLElement, 'zs-pagination', null, [zs.pagination]);
	return zs;
})(window.zs || {});	