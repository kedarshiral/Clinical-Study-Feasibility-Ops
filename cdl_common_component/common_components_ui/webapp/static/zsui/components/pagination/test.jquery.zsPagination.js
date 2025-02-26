describe("zsPagination", function () {
	var $container;

	function $isThing($elem) {
		if ($elem && $elem.length) {
			return true;
		}
		return false;
	}

	beforeEach(function () {
		$('<nav class="zs-pagination"></nav>').appendTo('body');
		$container = $('body > .zs-pagination');
	});
 
	it('Is a jQuery pluging', function () {
		utils.isPlugin(expect, 'zsPagination');
	})

	it('Creates a pagination links based on the provided options', function () {

		$container.zsPagination({
			itemsCount: 70,
			pageSize: 7,
			maxDisplayPages: 3,
			currentPage: 5,
		});
		var pagination = $container.data('zsPagination');
		  
		expect(pagination).toBeTruthy();
		expect(pagination.totalPages).toBe(10);
		expect(pagination.currentPage).toBe(5);
		expect(pagination.pagesToDisplay).toBe(3);
		expect(pagination.startPage).toBe(4);
		expect(pagination.firstPage).toBe(1);
		expect(pagination.prevPage).toBe(4);
		expect(pagination.nextPage).toBe(6);
		expect(pagination.lastPage).toBe(10);
		expect($container.find('a').length).toBe(7); // prev, 4, 5, 6, next
				
	});
	
	it('It can change parameters of an existing pagination', function () {
		$container.zsPagination({
			itemsCount: 100,
			pageSize: 5,
			currentPage: 1
		});		
		$container.zsPagination({
			currentPage: 10,
			itemsCount: 200,
			maxDisplayPages: 5
		});
		pagination = $container.data('zsPagination');
		expect(pagination.options.pageSize).toBe(5);
		expect(pagination.currentPage).toBe(10);
		expect(pagination.totalPages).toBe(40);
		expect(pagination.pagesToDisplay).toBe(5);
		expect(pagination.startPage).toBe(8); // 10 - floor(5/2)		
		
	});
	
	it('It can remove itself', function() {
		$container.zsPagination();
		var pagination = $container.data('zsPagination');
		$container.zsPagination('destroy');
		pagination = null;
		expect($container.data('zsPagination')).toBeFalsy();
		expect($container.html()).toBeFalsy();				
	});
	
	
	it('Highlights the current page', function () {
		$container.zsPagination({
			itemsCount: 35,
			pageSize: 7,
			maxDisplayPages: 3,
			currentPage: 2
		});
		var pagination = $container.data('zsPagination');
		var $current = $container.find('[current]');
		expect($isThing($current)).toBe(true);
		expect($current.attr('page')).toBe('2');
		
		$container.zsPagination({
			currentPage: null,
		});
		$current = $container.find('[current]');
		expect($current.attr('page')).toBe('1');
					
	});
	
	it('Can change the current page', function () {
		$container.zsPagination({
			itemsCount: 100,
		});
		var pagination = $container.data('zsPagination');
		expect(typeof pagination.changePage).toBe('function');
		pagination.changePage(2);
		expect(pagination.currentPage).toBe(2);
		var $current = $container.find('[current]');
		expect($isThing($current)).toBe(true);
		expect($current.attr('page')).toBe('2');		
	});
	
	it('Adjusts the starting page based on how many pages left to display', function () {
		$container.zsPagination({
			itemsCount: 150,
			currentPage: 9,
			pageSize: 15,
			maxDisplayPages: 5
		});
		var pagination = $container.data('zsPagination');		
		expect(pagination.startPage).toBe(6);  // <<,<, 6,7,8,[9],10		
		$container.zsPagination({
			itemsCount: 100,
			currentPage: 3,
			pageSize: 30,
			maxDisplayPages: 5
		});	
		expect(pagination.startPage).toBe(1);  // 1,2,[3]
		
	})
	
	it('Has a link to the next page', function () {
		$container.zsPagination({
			itemsCount: 100,
		});
		var pagination = $container.data('zsPagination');
		var $next = $container.find('[next]');
		expect($isThing($next)).toBeTruthy();
		$next.trigger('click');
		expect(pagination.currentPage).toBe(2);		
		//Not with ZS settings
		//pagination.changePage(pagination.totalPages);
		//$next = $container.find('[next]');
		//expect($isThing($next)).toBeFalsy();								
	});
	
	it('Has a link to the previous page', function () {
		$container.zsPagination({			
			itemsCount: 100,
		});
		var pagination = $container.data('zsPagination');
		var $prev = $container.find('[prev]');
		expect($prev.attr('disabled')).toBe('disabled');
		pagination.changePage(2);
		$prev = $container.find('[prev]');
		expect($isThing($prev)).toBeTruthy();
		$prev.trigger('click');
		expect(pagination.currentPage).toBe(1);								
	});

	it('Has a link to the first page', function () {
		$container.zsPagination({			
			itemsCount: 200, // Only if we hav more than 10 pages.
			maxDisplayPages: 3,
			currentPage: 2
		});
		var pagination = $container.data('zsPagination');
		
		var $first = $container.find('[first]');
		expect($isThing($first)).toBeTruthy();
		pagination.changePage(5);
		
		$first = $container.find('[first]');
		$first.trigger('click');
		$first = $container.find('[first]'); // Rendered		
		expect($first.attr('disabled')).toBe('disabled');
	});
	
	it('Has a link to the last page', function () {
		$container.zsPagination({			
			itemsCount: 200,
			maxDisplayPages: 4,
			pageSize: 15,
			currentPage: 4
		});
		var pagination = $container.data('zsPagination');
		
		var $last = $container.find('[last]');
		expect($isThing($last)).toBeTruthy();
		$last.trigger('click');
		expect(pagination.currentPage).toBe(Math.ceil(200/15));
		$last = $container.find('[last]');
		expect($last.attr('disabled')).toBe('disabled');
	});
	
		
	it('Supports customization', function () {
		$container.zsPagination({		
			currentPage: 5,
			itemsCount: 100,
			pageSize: 10,
			maxDisplayPages: 3,	
			format: {
				page: function(index) {
					if (index == this.currentPage) {
						return 'current:'+index;
					} else {
						return 'page:' + index;
					}	
				},
				next: function(index) {
					return 'next:' + index;
				},
				last: function(index) {
					return 'last:' + index;
				},
				prev: function(index) {
					return 'prev:' + index;
				},
				first: function(index) {
					return 'first:' + index;
				}				
			}
		});			
		expect($container.text()).toBe('first:1prev:4page:4current:5page:6next:6last:10');		
	});
	
	describe('Supports event handling', function () {
		var pagination, options, prevPage, currentPage, clicked = false;
		var events = {
			onClick: function (e) {
				console.log('clicked');
				clicked = true;		
			},
			onPageChange: function(oldPage, newPage) {
				prevPage = oldPage;
				currentPage = newPage;
			}
		}
		beforeEach( function () {
			prevPage = null, currentPage = null; 
			
			// Should be placed before initializing the plugin.
			spyOn(events,"onClick").and.callThrough();			
			spyOn(events,"onPageChange").and.callThrough();

			$container.zsPagination(events);
			pagination = $container.data('zsPagination');
			$container.find('a').eq(4).trigger('click'); // < 1 2 [3] >
		});
		
		
		it('onClick', function() {
			console.log('checked');
			expect(events.onClick).toHaveBeenCalled(); // issue
			expect(clicked).toBe(true);
			expect(events.onPageChange).toHaveBeenCalled()
			expect(prevPage).toBe(1);
			expect(currentPage).toBe(3);
		});
		
		afterEach(function () {
			$container.zsPagination('destroy');
		});		
	});
	
	xit('Supports smart rendering', function () {
		
	});

	afterEach(function () {
		$container.remove();
	});
});
