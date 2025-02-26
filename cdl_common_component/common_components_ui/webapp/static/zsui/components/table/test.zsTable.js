describe("table component", function () {
	it('is a custom element', function () {
		utils.isBehavior(expect, zs.table);
		utils.isCustomElement(expect, 'zs-table', 'div', zs.tableElement, HTMLDivElement);
	});

	it('Renders a table with data containing array of objects', function () {
		var elem = new zs.tableElement();
		document.body.appendChild(elem);
		var obj1 = { col1: 3, col2: 4 };
		elem.data = [{ col1: 1, col2: 2 }, Object.create(obj1)];
		elem.render();
		expect(elem.querySelector('table')).toBeTruthy(); // We have a table
		expect(elem.querySelectorAll('tr').length).toBe(3); // With 1 tbody row and 1 header row
		expect(elem.querySelectorAll('[key="col1"]').length).toBe(3); // 2 cols in each row including header
		expect(elem.querySelectorAll('[key="col2"]').length).toBe(3); // ... with a key 
		expect(elem.querySelector('[key="1"]')).toBeTruthy(); // Each row of tbody has a key too
		expect(elem.querySelector('[key="-1"]').parentNode.tagName).toBe('THEAD'); // First element is a table header	
		elem.parentNode.removeChild(elem);
	});

	it('Renders a table with  data containing array of arrays', function(){
		var elem = new zs.tableElement();
		document.body.appendChild(elem);
		elem.data = [[1,2],[3,4],[5,6]];
		elem.render();
		expect(elem.querySelector('table')).toBeTruthy(); // We have a table
		expect(elem.querySelectorAll('tr').length).toBe(4); // With 3 rows and 1 header row
		expect(elem.querySelectorAll('[key="0"]').length).toBe(5); // 3 cols in each row and two rows
		expect(elem.querySelectorAll('[key="1"]').length).toBe(5); // 3 cols in each row and two rows
		expect(elem.querySelectorAll('[key="2"]').length).toBe(1); // Only one 3rd row but no 3rd column
		expect(elem.querySelector('tr[key="1"]')).toBeTruthy(); // Each row has a key too
		expect(elem.querySelector('tr[key="2"]')).toBeTruthy(); // ...
		expect(elem.querySelector('[key="-1"]').parentNode.tagName).toBe('THEAD'); // First element is a table header
		expect(elem.querySelector('tr[key="0"]').parentNode.tagName).toBe('TBODY');	
		elem.parentNode.removeChild(elem);
	});

	
	it('Can clear a table', function () {
		var elem = new zs.tableElement();
		document.body.appendChild(elem);
		elem.data = [{ col1: 1, col2: 2 }, { col1: 3, col2: 4 }];
		elem.render();
		expect(elem.querySelector('tbody tr')).toBeTruthy();
		expect(elem.querySelector('thead tr')).toBeTruthy();
		elem.clearTable();
		expect(elem.querySelector('tbody tr')).toBeFalsy();
		expect(elem.querySelector('thead tr')).toBeFalsy();
		elem.parentNode.removeChild(elem);
	});
	
	
	describe('events', function () { // Because events could be asynchronous
		var elem = new zs.tableElement(), flag = false;

		beforeEach(function (done) {
			elem.addEventListener('render', function () {
				flag = true;
			});
			document.body.appendChild(elem);
			elem.data = [{ col1: 1, col2: 2 }, { col1: 3, col2: 4 }];
			elem.render();

			
			setTimeout(function () {
				done();
			},10); // IE11 requires delay, 0 ms - 5 ms doesn't work, weird 
			
		})


		it('render', function () {
			expect(flag).toBeTruthy();
		});

		afterEach(function () {
			elem.parentNode.removeChild(elem);
		});

	});


	describe('smart table', function() {
		var el,
			tableSmartElement = zs.customElement(HTMLElement, 'zs-smart-table', null, [zs.loading, zs.table, zs.tableSmartRender]);
		
		beforeEach(function() {
			el = new tableSmartElement();
		});

		it('is a behavior', function() {
			utils.isBehavior(expect, zs.tableSmartRender);

		});

		it('can detect newly added records', function() {
			el.data = [[1], [2], [3]];
			expect(el._changes).toBe(-1); // New data
			el.data = [[1], [2], [3], [4], [5]];
			expect(el._changes).toBeTruthy();
			var add = el._changes.add;
			expect(add).toBeTruthy();
			expect(add.length).toBe(2);
			expect(add[0].data).toBe(el.data[3]);
			expect(add[1].data).toBe(el.data[4]);
			expect(el._changes.remove).toBeFalsy();
			expect(el._changes.modify).toBeFalsy();
		});


		it('can detect changed records', function() {
			el.data = [[1], [2], [3]];
			expect(el._changes).toBe(-1); // New data
			el.data = [[1], [21], [31]];
			expect(el._changes).toBeTruthy();
			var modify = el._changes.modify;
			expect(modify).toBeTruthy();
			expect(modify.length).toBe(2);
			expect(modify[0].data).toBe(el.data[1]);
			expect(modify[1].data).toBe(el.data[2]);
			expect(el._changes.remove).toBeFalsy();
			expect(el._changes.add).toBeFalsy();
		});


		it('can detect delted records', function() {
			el.data = [[1], [2], [3]];
			expect(el._changes).toBe(-1); // New data
			var deleted = el.data.slice(1); // !!! splice doesn't trigger change
			el.data = [el.data[0]]; // delete [2], [3]		
			expect(el._changes).toBeTruthy();
			var del = el._changes.remove;
			expect(del).toBeTruthy();
			expect(del.length).toBe(2);
			expect(del[0].data).toBe(deleted[0]);
			expect(del[1].data).toBe(deleted[1]);
			expect(el._changes.add).toBeFalsy();
			expect(el._changes.modify).toBeFalsy();
		});


		it('delete first and move the rest', function() {
			el.getRowKey = function(rowIndex, rowData) { // Need to set a unique row key
				return rowData[0];
			}
			el.data = [[1], [2], [3]];			
			expect(el._changes).toBe(-1); // New data
			var deleted = el.data.slice(0,1); // !!! splice doesn't trigger change
			el.data = [el.data[1], el.data[2]]; // delete [1]
			expect(el._changes).toBeTruthy();
			var del = el._changes.remove;
			expect(del).toBeTruthy();
			expect(del.length).toBe(1);
			console.log('changes', el._changes, 'del' ,del, deleted);
			expect(del[0].data).toBe(deleted[0]);
			expect(el._changes.add).toBeFalsy();
			expect(el._changes.modify).toBeFalsy();
			var move = el._changes.move;
			expect(move).toBeTruthy();
			expect(move.length).toBe(2);
			expect(move[0].data).toBe(el.data[0]);
			expect(move[1].data).toBe(el.data[1]);
		});

		it('can detect change in order', function() {
			el.getRowKey = function(rowIndex, rowData) { // Need to set a unique row key
				return rowData[0];
			}
			el.data = [[1], [2], [3], [4], [5]];	
			expect(el._changes).toBe(-1); // New data		
			el.data = [[2], [1], [3], [5], [4]];
			expect(el._changes).toBeTruthy();
			var move = el._changes.move;
			expect(move).toBeTruthy();
			expect(move.length).toBe(4);
		});
	});


	describe('Supports sorting', function() {
		xit('todo');
	});

	describe('Sticky header', function() {
		xit('todo');
	});

	describe('Locked columns', function() {
		xit('todo');
	});

});