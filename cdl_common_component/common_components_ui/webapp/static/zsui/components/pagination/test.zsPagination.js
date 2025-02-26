describe("zsPagination", function () {
	
	it('is a custom element', function() {
		utils.isBehavior(expect, zs.pagination);
		utils.isCustomElement(expect, 'zs-pagination', null, zs.paginationElement, HTMLElement);
	});	

	it('configures jquery plugin on attach', function(done) {
		var elem = new zs.paginationElement();
		document.body.appendChild(elem);

		setTimeout(function() {
			var plugin = $(elem).data('zsPagination');
			expect(plugin.changePage).toBeTruthy();
			elem.parentNode.removeChild(elem);
			setTimeout(function() {
				var plugin = $(elem).data('zsPagination');
				expect(plugin).toBeFalsy();
				done();
			},100);			
		},100);
	});

	it('reflects attributes', function(done) {
		var elem = new zs.paginationElement();
		elem.setAttribute('page', 2);
		elem.setAttribute('total', 30);
		elem.setAttribute('size', 10);
		document.body.appendChild(elem);

		setTimeout(function() {
			expect(elem.children.length).toBe(5); // < 1 [2] 3 > 
			expect(elem.querySelector('[current]').innerHTML == '2');

			elem.setAttribute('page', 3);
			setTimeout(function() {				
				expect(elem.querySelector('[current]').innerHTML == '3');
				elem.parentNode.removeChild(elem);
				done();
			},100);			
		},100);
	});

	it('supports "pagechange" event', function(done) {
		var elem = new zs.paginationElement();
		var page = 2;
		elem.setAttribute('page', page);
		elem.setAttribute('total', 30);
		elem.setAttribute('size', 10);
		elem.addEventListener('pagechange', function(event) {
			page = event.detail.currentPage;
		})
		document.body.appendChild(elem);

		setTimeout(function() {
			var event = new CustomEvent('click', {
				'view': window,
				'bubbles': true,
				'cancelable': true
			  });

			elem.querySelector('[page="3"]').dispatchEvent(event);
			setTimeout(function() {				
				expect(page).toBe(3);
				elem.parentNode.removeChild(elem);
				done();
			},100);			
		},100);
	});



});
