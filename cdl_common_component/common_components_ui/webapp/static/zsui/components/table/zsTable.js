var zs = (function (zs) {
	'use strict';


	// Fixed columns
	zs.tableFixedColumns = {
		fixedColumnCount: 0, // how many columns to lock
		scrollEl: null, // help to support virtual scrolling
		matchScroll: function (fixedContainer, scrollEl) {
			if (!fixedContainer._scrollTop || fixedContainer._scrollTop != scrollEl.scrollTop) {
				fixedContainer._scrollTop = scrollEl.scrollTop;
				fixedContainer.scrollTop = scrollEl.scrollTop;
			}
		},
		decorateFixedHeaders: function (cell) {
			cell.setAttribute('locked', true);
		},
		resizeFixed: function (fixedContainer) {
			if (!this.table || !fixedContainer) { return }

			var fixedTable = this.fixedTable || fixedContainer.firstChild;
			if (!fixedTable) { return; }

			var rows = this.table.querySelectorAll('tr');
			var fixedRows = fixedTable.querySelectorAll('tr');
			var comp = this;

			// Original table should be rendered before we can use that
			for (var i = 0; i < rows.length; i++) {
				var cell = rows[i].firstChild;
				var fixedCell = fixedRows[i].firstChild;
				var delta = rows[i].offsetHeight - fixedRows[i].offsetHeight;

				// We can't reduce row heights so we have to increase them only
				var newHeight = rows[i].clientHeight
				if (delta > 0) { // original row is higher
					newHeight = fixedRows[i].clientHeight + delta;
				} else if (delta < 0) { // fixed row is heigher
					newHeight = rows[i].clientHeight - delta; // double negative
				}

				// We need to fix the height of both rows to protect from further manipulation of content like sorting.
				rows[i].style.minHeight = newHeight + 'px';
				fixedRows[i].style.minHeight = newHeight + 'px';
				rows[i].style.maxHeight = newHeight + 'px';
				fixedRows[i].style.maxHeight = newHeight + 'px';
				rows[i].style.height = newHeight + 'px';
				fixedRows[i].style.height = newHeight + 'px';
			}
		},		
		scrollFix: function (elem) {

			//elem.style['-webkit-overflow-scrolling'] = 'touch';
			return;
			// Variables to track inputs
			var startY, startTopScroll;

			elem = elem || document.querySelector(elem);

			// If there is no element, then do nothing  
			if (!elem)
				return;

			// Handle the start of interactions
			elem.addEventListener('touchstart', function (event) {
				startY = event.touches[0].pageY;
				startTopScroll = elem.scrollTop;

				if (startTopScroll <= 0)
					elem.scrollTop = 1;

				if (startTopScroll + elem.offsetHeight >= elem.scrollHeight)
					elem.scrollTop = elem.scrollHeight - elem.offsetHeight - 1;
			}, false);
		},

		/**
		 * Override whenRendered to account for second table.
		 */
		whenRendered: function () {
			// Use this method to detect and trigger browser to finish rendering of our component
			var comp = this;
			return new Promise(function (resolve, reject) {
				setTimeout(function() { // Set timeout required for IE11
					if (!comp.table) { resolve(); return; }
					var tbody = comp.table.querySelector('tbody'); // Need to use querySelector for FF instead of just comp.tbody;
					if (!tbody) { resolve(); return; }
					var lastTr = tbody.lastChild;
					if (!lastTr) { resolve(); return; }
					var lastTd = lastTr.lastChild;
					var height1 = lastTd && lastTd.offsetHeight;
					var height2 = 0;
					var tableHeight1 = comp.table.offsetHeight;

					if (comp.fixedTable) {
						tbody = comp.fixedTable.querySelector('tbody');
						if (!tbody) {resolve(); return;}
						lastTr = tbody.lastChild;
						if (!lastTr) { resolve(); return; }
						var lastTd = lastTr.lastChild;					
						var height2 = lastTd && lastTd.offsetHeight;
						var tableHeight2 = comp.fixedTable.offsetHeight;						

					}				
					resolve(height1 +','+ height2 + ',' + tableHeight1 + ',' + tableHeight2);
				},0);
			});
		},

		/**
		 * Override to split locked columns in the header
		 */
		renderHeadRow: function (trFix) {
			var tr,
				rowData = this.headData || this.data[0],
				rowIndex = -1;

			tr = document.createElement('tr');
			tr.setAttribute('key', rowIndex);
			if (Array.isArray(rowData)) {
				for (var j = 0; j < rowData.length; j++) {
					var cell = this.renderHead(rowIndex, j, rowData[j]);
					if (this.fixedColumnCount > j) {
						trFix.appendChild(cell);
					} else {
						tr.appendChild(cell);
					}
				}
			} else {
				var jj = -1;
				for (var j in rowData) {
					jj++;
					var cell = this.renderHead(rowIndex, j, j);
					if (this.fixedColumnCount > jj) {
						trFix.appendChild(cell);
					} else {
						tr.appendChild(cell);
					}
				}
			}
			return tr;
		},

		/**
		 * Override to split locked columns in the table body
		 */
		renderRow: function (rowIndex, rowData, trFixed) {			
			if (rowData == null) {trFixed = null; return null;} // Skip row

			var j, tr = document.createElement('tr');			

			tr.setAttribute('key', rowIndex);

			if (Array.isArray(rowData)) {
				for (var j = 0; j < rowData.length; j++) {
					var cell = this.renderCell(rowIndex, j, rowData[j]);
					if (this.fixedColumnCount > j) {
						trFixed.appendChild(cell);
					} else {
						tr.appendChild(cell);
					}
				}
			} else {
				var jj=-1;
				for (var j in rowData) {
					jj++;
					var cell = this.renderCell(rowIndex, j, rowData[j]);
					if (this.fixedColumnCount > jj) {
						trFixed.appendChild(cell);
					} else {
						tr.appendChild(cell);
					}
				}
			}

			
			return tr;
		},

		/**
		 * Override renderTable to create a container for another table with locked columns
		 */
		renderTable: function (data, mode) {
			var i,	body = '', comp = this, tableContainer;

			

			// Original render
			comp.loadingSet(true, comp); // hide spinner
			if (!this.tableContainer) {				
				this.tableContainer = document.createElement('div');
				this.appendChild(this.tableContainer);
			}
			tableContainer = this.tableContainer;
			
			comp.scrollEl = tableContainer; // !!!
			
			// Empty table
			this.clearTable();

			// Create table
			if (!this.table) {
				this.table = this.querySelector('table') || document.createElement('table');
				this.table.classList.add('zs-data-table');
				this.table.style.tableLayout = 'fixed';
			}

			// Create thead and tbody
			if (!this.thead) {
				this.thead = this.table.querySelector('thead') || document.createElement('thead');
				this.table.appendChild(this.thead);
			}
			if (!this.tbody) {
				this.tbody = this.table.querySelector('tbody') || document.createElement('tbody');
				this.table.appendChild(this.tbody);
			}
		
			// Define fixed container
			if (this.fixedColumnCount) {

				if ( !this.fixedContainer) {
					this.fixedContainer = this.querySelector('.zs-fixed');
					if (!this.fixedContainer) {
						this.fixedContainer = document.createElement('div');
						this.appendChild(this.fixedContainer); // Important to append to overall container. Because we 
					}
				} else {
					this.fixedContainer.innerHTML = ''; // clear
				}


				// Create fixed table, head and body
				if (!this.fixedTable) {					
					this.fixedTable = document.createElement('table');
					this.fixedTable.setAttribute('class', this.table.getAttribute('class'));
					this.fixedTable.style.tableLayout = 'fixed';

					this.fixedContainer.appendChild(this.fixedTable);
				} else {
					this.fixedTable.innerHTML = '';
				}

				
				this.fixedTableHead = document.createElement('thead');
				this.fixedTableBody = document.createElement('tbody');


				// Create a table container and move the table inside it
				this.fixedContainer.setAttribute('class', 'zs-fixed');
				this.fixedContainer.style.position = 'absolute';
				this.considerPosition('relative');

				this.fixedContainer.style.bottom = '0px';
				this.fixedContainer.style.left = '0px';
				this.fixedContainer.style.overflowY = 'hidden';
				this.fixedContainer.style.top = '0px';	// to make it inline with tablecontainer

				// Syncornize scoll 
				var waiting = false;
				comp.scrollEl.addEventListener('scroll', function (event) {
					if (!waiting) {
						waiting = true;
					
						window.requestAnimationFrame(function () {
							waiting = false;
							comp.matchScroll(comp.fixedContainer, comp.scrollEl);
						});
					}
				});

				// Disable iOS elastic scroll
				this.scrollFix(comp.scrollEl);


				// Listen to resize
				this.addEventListener('resize', function () {
					comp.resizeFixed(comp.fixedContainer, comp.scrollEl);
				});
			}

			

			// Headers			
			if (this.fixedColumnCount) {
				var trFix = document.createElement('tr');
				this.thead.appendChild(this.renderHeadRow(trFix));
				this.fixedTableHead.appendChild(trFix);
			} else {
				this.thead.appendChild(this.renderHeadRow());
			}

			var trFixed, tr;
			for (i = 0; i < data.length; i++) {
				
				if (this.fixedColumnCount) {
					trFix = document.createElement('tr');
					tr = this.renderRow(i, data[i], trFix);					
				} 
				if (tr) {
					this.tbody.appendChild(tr);
				}
				if (trFix) {					
					this.fixedTableBody.appendChild(trFix);
				}
			}

			tableContainer.appendChild(this.table);	
			
			if (this.fixedColumnCount) {
				this.fixedTable.appendChild(this.fixedTableHead);
				this.fixedTable.appendChild(this.fixedTableBody);
			}
			
			// Wait till render
			this.whenRendered().then(function () {
				
				
				if (comp.fixedColumnCount) {
					comp.resizeFixed(comp.fixedContainer, comp.scrollEl);
					comp.matchScroll(comp.fixedContainer, comp.scrollEl);

					// Adjust width and height of locked columns table and main table
					tableContainer.style.marginLeft = comp.fixedContainer.offsetWidth + 'px';					
					comp.fixedContainer.firstChild.style.marginBottom = '100px';// compensate for scrollbar should 										
					comp.fixedContainer.style.height = tableContainer.offsetHeight + 'px';

				}
				comp.loadingSet(false, comp); // hide spinner
				comp.dispatchEvent(new CustomEvent('render'));

			});
		}
	}


	// Sticky header
	zs.tableStickyHeader = {
		decorateStickyCell: function (cell) {

		},
		resizeStickyHeader: function (targetTable, stickyContainer, scrollEl) {
			if (!stickyContainer || !targetTable || !scrollEl) { return; }
			// Need to match the width and height of headers in the sticky header and the table.
			// FF renderes headers first and then resizes them.
			var cells = targetTable.querySelector('thead').querySelectorAll('td,th');
			var stickyCells = stickyContainer.querySelectorAll('td,th');

			for (var i = 0; i < cells.length; i++) {
				var cell = cells[i];
				var stickyCell = stickyCells[i];
				if (stickyCell) {
					//stickyCell.style.display = 'inline-block';
					stickyCell.style.whiteSpace = 'normal'; // Because parent white-space is nowrap

					stickyCell.style.width = cell.clientWidth + (cell.offsetLeft - stickyCell.offsetLeft) + 'px'; // IE11 we need to compensate 
					stickyCell.style.height = cell.clientHeight + cell.offsetHeight - cell.clientHeight + 'px'; // Why?
					//alert(1);
					//console.log('cell', cell.clientWidth, cell.offsetLeft, stickyCell.offsetLeft);

					stickyCell.style.minWidth = stickyCell.style.width;
					stickyCell.style.maxWidth = stickyCell.style.width;

					this.decorateStickyCell(stickyCell);
				}
			}


			// Compensate for scrolling
			var delta = scrollEl.offsetWidth - scrollEl.clientWidth;
			var bar = stickyContainer.querySelector('[bar]');
			if (delta) {
				if (!bar) {
					bar = document.createElement('td');
					bar.setAttribute('bar', 'true');
					stickyContainer.firstChild.firstChild.firstChild.appendChild(bar);
				}
				bar.style.width = delta + 'px';

			} else if (bar) {
				stickyContainer.firstChild.firstChild.firstChild.removeChild(bar);
			}

			// Set postion
			//var rect = scrollEl.getBoundingClientRect();

			var stickyHeaderHeight = stickyContainer.offsetHeight;
			var headerHeight = targetTable.querySelector('thead td').offsetHeight;

			stickyContainer.style.left = '0px';
			stickyContainer.style.top = '0px';
			stickyContainer.style.marginLeft = scrollEl.style.marginLeft;

			if (stickyContainer.parentNode) {
                stickyContainer.parentNode.style.paddingTop = stickyHeaderHeight + 'px';
			}
			
			stickyContainer.style.width = scrollEl.offsetWidth + 'px';
			targetTable.style.marginTop = -headerHeight + 'px';


		},
		scrollStickyHeader: function (targetTable, stickyContainer, scrollEl) {
			if (!targetTable || !stickyContainer || !scrollEl) {
				return;
			}
			stickyContainer.scrollLeft = scrollEl.scrollLeft;
		},
		stickHeader: function (targetTable, stickyContainer, scrollEl) {
			// Can be applied to any target table			
			var comp = this;

			// Can't work without proper parameters
			if (!targetTable || !scrollEl) {
				return;
			}

			if (!stickyContainer) {
				stickyContainer = targetTable.stickyContainer;
				if (!stickyContainer) {
					stickyContainer = document.createElement('div');
					stickyContainer.style.visibility = 'hidden';
					targetTable.stickyContainer = stickyContainer;
				}
			}
			this.appendChild(stickyContainer);

			// Clear sticky container
			if (stickyContainer.firstChild) {
				stickyContainer.removeChild(stickyContainer.firstChild);
			}

			// Style sticky header
			stickyContainer.classList.add('zs-sticky');
			stickyContainer.style.overflowX = 'hidden';
			stickyContainer.style.position = 'absolute';
			stickyContainer.parentNode.style.position = 'relative';

			// Listen to scroll to shift the sticky header.
			var waiting = false;
			scrollEl.addEventListener('scroll', function (event) {
				if (!waiting) {
					waiting = true;
					window.requestAnimationFrame(function () {
						waiting = false;
						comp.scrollStickyHeader(targetTable, stickyContainer, scrollEl);
					});
				}
			});

			// Duplicate table headers
			var thead = targetTable.querySelector('thead').cloneNode(true);
			var table = document.createElement('table');
			table.classList.add('zs-data-table');
			table.appendChild(thead);
			targetTable.style.tableLayout = 'fixed';


			stickyContainer.appendChild(table);
			//stickyContainer.firstChild.style.display = 'block'; // table
			stickyContainer.firstChild.style.marginBottom = '0';


			//stickyContainer.firstChild.firstChild.style.display = 'block'; // thead
			if (stickyContainer.firstChild.firstChild.firstChild) { // If we have tr
				//stickyContainer.firstChild.firstChild.firstChild.style.display = 'block'; // tr
				stickyContainer.firstChild.firstChild.firstChild.style.whiteSpace = 'nowrap';
			}

			// Yeld to IE11
			setTimeout(function () {
				comp.resizeStickyHeader(targetTable, stickyContainer, scrollEl);
				comp.scrollStickyHeader(targetTable, stickyContainer, scrollEl);
				stickyContainer.style.visibility = 'visible';
				if (targetTable.parentNode && targetTable.parentNode.classList.contains("zs-fixed")) {
                    targetTable.parentNode.style.top = "";
                }
			}, 0); // Issue: IE11 might riquire longer delay

			return stickyContainer;
		}

	};

	// Custom data table prototype
	zs.table = {
		data: null,
		tableContainer: null,
		events: {
			create: function () {
				this.data = [];
			}
		},
		render: function () {
			this.renderTable(this.data);
		},
		refreshTable: function (params) {
			var comp = this;
			this.getTableData(params).then(function (data) {
				comp.data = data;
				comp.renderTable(data);
				comp.loadingSet(false, comp.querySelector('.zs-table'));
			});
		},
		getTableData: function (params) {
			var prom = new Promise(function (resolve, reject) {
				resolve([]);
			});
			return prom;
		},
		considerPosition: function (position) {
			if (!this.style.position || !this.style.position == 'auto') { this.style.position = 'relative'; }
		},
		renderRow: function (rowIndex, rowData) {
			var tr = document.createElement('tr');
			tr.setAttribute('key', rowIndex);
			if (Array.isArray(rowData)) {
				for (var j = 0; j < rowData.length; j++) {
					tr.appendChild(this.renderCell(rowIndex, j, rowData[j]));
				}
			} else {
				for (var j in rowData) {
					tr.appendChild(this.renderCell(rowIndex, j, rowData[j]));
				}
			}
			return tr;
		},
		renderHeadRow: function () {
			var tr,
				rowData = this.headData || this.data[0],
				rowIndex = -1;

			tr = document.createElement('tr');
			tr.setAttribute('key', rowIndex);
			if (Array.isArray(rowData)) {
				for (var j = 0; j < rowData.length; j++) {
					tr.appendChild(this.renderHead(rowIndex, j, rowData[j]));
				}
			} else {
				for (var j in rowData) {
					tr.appendChild(this.renderHead(rowIndex, j, j));
				}
			}
			return tr;
		},
		renderHead: function (rowIndex, colIndex, data) {
			var th = document.createElement('td');
			th.setAttribute('key', colIndex);
			th.innerHTML = data;
			return th;
		},
		renderCell: function (rowIndex, colIndex, data) {
			var td = document.createElement('td');
			td.setAttribute('key', colIndex);
			td.innerHTML = data;
			return td;
		},
		clearTable: function () {
			if (this.thead) {
				while (this.thead.firstChild) {
					this.thead.removeChild(this.thead.firstChild);
				}
				this.thead = null;
			}
			if (this.tbody) {
				while (this.tbody.firstChild) {
					this.tbody.removeChild(this.tbody.firstChild);
				}
				this.tbody = null;
			}
			if (this.table && this.table.parentNode) {
				this.table.parentNode.removeChild(this.table);
				this.table = null;
			}
		},
		whenRendered: function () {
			// Use this method to detect and trigger browser to finish rendering of our component
			var comp = this;
			return new Promise(function (resolve, reject) {
				if (!comp.table) { resolve(); return; }
				var tbody = comp.table.querySelector('tbody'); // Need to use querySelector for FF instead of just comp.tbody;
				if (!tbody) { resolve(); return; }
				var lastTr = tbody.lastChild;
				if (!lastTr) { resolve(); return; }
				var lastTd = lastTr.lastChild;
				resolve(lastTd && lastTd.offsetHeight);
			});
		},

		renderTable: function (data, mode) {
			var i,
				comp = this,
				body = '';

			var tableContainer = this.tableContainer || this;

			// Empty table
			this.clearTable();

			// Create table
			if (!this.table) {
				this.table = this.querySelector('table') || document.createElement('table');
				this.table.classList.add('zs-data-table');
				this.table.style.tableLayout = 'fixed';
			}

			// Create thead and tbody
			if (!this.thead) {
				this.thead = this.table.querySelector('thead') || document.createElement('thead');
				this.table.appendChild(this.thead);
			}
			if (!this.tbody) {
				this.tbody = this.table.querySelector('tbody') || document.createElement('tbody');
				this.table.appendChild(this.tbody);
			}

			// Headers
			this.thead.appendChild(this.renderHeadRow());

			for (i = 0; i < data.length; i++) {
				var tr = this.renderRow(i, data[i]);
				if (tr) { // We may render head row and return nothing
					this.tbody.appendChild(tr);
				}
			}

			tableContainer.appendChild(this.table);

			this.whenRendered().then(function () {
				comp.dispatchEvent(new CustomEvent('render'));
			});
		}
	};

	/**
	 * @class 		ZS TableSortable
	 * @classdesc 	Adds sortable behaviour to ZSTable. 
	 * @example 	Add sortable attribute to every element suppose to have sorting listener then simply listen to "sort" event of entire table. 
	 * 				Additionaly you can provide sorting-event (click by default) and sorting-elm ("a" by default) as attributes of your sorting elm.
	 */
	zs.tableSortable = {
		events: {
		},

		/**
		 * Adds sorting listener to provided element
		 * 
		 * @param {Element} td - Element td that should react as a sortable elm
		 * 
		 * @returns {Element}
		 */
		addSortableListener: function (td, sortOrder, strip) {
			var comp = this,
				tagName = td.getAttribute('sorting-elm') || 'a',
				eventName = td.getAttribute('sorting-event') || 'click',
				elm = document.createElement(tagName),
				event = new CustomEvent('sort', { detail: { elm: td } });

			if (!strip) {
				// Wrap contents
				while (td.firstChild) { // move all children
					elm.appendChild(td.firstChild);
				}
			} else {
				// Strip  contents
				elm.innerHTML = td.textContent;
				td.innerHTML = '';
			}

			elm.addEventListener(eventName, function (e) {
				comp.dispatchEvent(event);
			});

			td.appendChild(elm);

			if (sortOrder) {
				elm.setAttribute('sort', sortOrder);
			} else {
				elm.removeAttribute('sort');
			}

			return td;
		}
	};

	/**
	 * @class 		ZS TablePivot
	 * @classdesc 	Adds pivot behaviour to ZSTable. 
	 *  			Renders table in specific way according to pdovided dataset. 
	 * 				Main diffrence are styles and two headrs instead of one comparing w/ basic table
	 */
	zs.tablePivot = {
		/**
		 * Renders table in specific way
		 */
		renderPivotTable: function () {
			this.table = document.createElement('table');
			this.table.className = 'zs-data-table';
			this.table.appendChild(this.renderPivotHead());
			this.table.appendChild(this.renderPivotBody());
			this.appendChild(this.table);

			var comp = this;
			this.whenRendered().then(function () {
				comp.dispatchEvent(new CustomEvent('render'));
			});
		},

		/**
		 * Render thead element for pivot table
		 * 
		 * @returns {Element} thead
		 */
		renderPivotHead: function () {
			this.thead = document.createElement('thead');

			return this.thead;
		},

		/**
		 * Render tbody element for pivot table
		 * 
		 * @returns {Element} tbody
		 */
		renderPivotBody: function () {
			this.tbody = document.createElement('tbody');

			return this.tbody;
		},

		/**
		 * Create th element by provided params
		 * 
		 * @param {Object} params
		 * @returns {Element}
		 */
		createPivotItem: function (params) {
			params = params || {};

			var elm = document.createElement(params.name);

			if (params.text) {
				elm.innerText = params.text;
			}

			if (params.className) {
				elm.className += ' ' + params.className;
			}

			if (params.style) {
				for (var style in params.style) {
					elm.style[style] = params.style[style];
				}
			}

			delete params.name;
			delete params.text;
			delete params.className;
			delete params.style;

			for (var key in params) {
				elm.setAttribute(key, params[key]);
			}

			return elm;
		}

	};

	/**
	 * Smart render table
	 */
	zs.tableSmartRender = {
		_data: null,
		_hash: null,
		_rowHash: null,
		_changes: null,


		getRowKey: function (rowIndex, rowData) {
			return rowIndex;
		},
		getHash: function (data) {
			return JSON.stringify(data);
		},
		getRowHash: function (rowIndex, rowData) {
			return JSON.stringify(rowData);
		},
		properties: {
			data: {
				set: function (newValue) {

					if (this._data != null && this._rowHash) {
						this.detectChanges(newValue);
					}

					// Update hash to detect changes later
					var timer = performance.now();
					this._rowHash = {};
					this._hash = '';
					for (var i = 0; i < newValue.length; i++) {
						var rowData = newValue[i];
						var hash = this.getRowHash(i, rowData);
						this._rowHash[this.getRowKey(i, rowData)] = { hash: hash, data: rowData, index: i };
						this._hash += (this._hash == '' ? '' : ',') + hash;
					}
					this._hash = '[' + this._hash + ']';
					console.log('hashes created in', performance.now() - timer);


					this._data = newValue;
				},
				get: function () {
					return this._data;
				}
			}
		},

		/**
		 * Check for changes
		 * 
		 * @return {Object} - Object indicating changes in data like {add: [1, 2, 3], remove: [4], modify: [5]}
		 */
		detectChanges: function (newArray) {
			var timer = performance.now();
			//console.log('detect changes', newArray.slice(0));
			this._changes = {};
			var newHash = this.getHash(newArray);

			if (this._hash == newHash) {
				// Hasn't changed
				console.log('no changes');
				this._changes = false;
				return this._changes;
			}

			if ((this._data == null || this._data.length == 0) && newArray && newArray.length) {
				// We had empty data and got something 
				console.log('new data');
				this._changes = -1;
				return this._changes;
			}

			// Detect new and modified items
			var newRowHash = {};
			for (var i = 0; i < newArray.length; i++) {
				var rowData = newArray[i];
				var key = this.getRowKey(i, rowData);
				var hash = this.getRowHash(i, rowData);
				newRowHash[key] = { hash: hash, data: rowData, index: i };
				if (!this._rowHash[key]) { // new item
					if (!this._changes.add) { this._changes.add = []; }
					this._changes.add.push({ key: key, index: i, data: rowData });
					continue;
				}

				if (this._rowHash[key].hash != hash) { // modified
					if (!this._changes.modify) { this._changes.modify = []; }
					this._changes.modify.push({ key: key, index: i, data: rowData });
				}

				if (this._rowHash[key].index != i) { // new order
					if (!this._changes.move) { this._changes.move = []; }
					this._changes.move.push({ key: key, index: i, data: rowData });
				}
			}

			// Detect deleted items
			for (var i in this._rowHash) {
				if (!newRowHash[i]) {
					if (!this._changes.remove) { this._changes.remove = []; }
					this._changes.remove.push({ key: i, index: this._rowHash[i].index, data: this._rowHash[i].data });
				}
			}

			console.log('changes detected', this._changes, 'in ' + (performance.now() - timer));

			return this._changes;
		},
		renderChanges: function () {
			var i, data, key, rowData, tr, originalTr,
				tableContainer = this.tableContainer || this,
				comp = this;

			if (!this._changes) { return; }
			if (this._changes == -1) { this.renderTable(this.data); return; }
			if (this._changes.move) { this.renderTable(this.data); return; } // when sort order is change it would be too expansive to update that.

			// Remove items
			if (this._changes.remove) {
				data = this._changes.remove;
				for (i = 0; i < data.length; i++) {
					rowData = data[i].data;
					key = data[i].key;
					tr = this.tbody.querySelector('tr[key="' + key + '"]');
					if (tr) {
						tr.parentNode.removeChild(tr);
					}
				}
			}

			// Modify items
			if (this._changes.modify) {
				data = this._changes.modify;
				for (i = 0; i < data.length; i++) {
					rowData = data[i].data;
					key = data[i].key;
					tr = this.renderRow(key, rowData);
					originalTr = this.tbody.querySelector('tr[key="' + key + '"]');
					if (originalTr) {
						originalTr.parentNode.replaceChild(tr, originalTr);
					}
				}
			}

			// Add items 
			if (this._changes.add) {
				data = this._changes.add;
				for (i = 0; i < data.length; i++) {
					rowData = data[i].data;
					key = data[i].key;
					tr = this.renderRow(key, rowData);
					this.tbody.appendChild(tr);
				}
			}

			this.whenRendered().then(function () {
				comp.dispatchEvent(new CustomEvent('render'));
			});
		}

	}

	zs.tableElement = zs.customElement(HTMLDivElement, 'zs-table', 'div', [zs.loading, zs.table]);

	return zs;
})(window.zs || {});	