(function ($) {
	'use strict';
	var unknown = 9007199254740991,
		defaults = {
			itemsCount: 245,
			pageSize: 15,
			currentPage: 1,
			maxDisplayPages: 10, 
			format: {
				// The execution context is a zsPagination instance in the all members below 
				first: function (index) { 
					if (this.currentPage == 1) { // don't display a goto first link if the first page is already selected
						return '';
					}
					return '<a page="1" first="">first</a>';					
				},
				last: function (index) { 
					if (this.currentPage == this.totalPages) { // don't display a goto last link if the last page is already selected
						return '';
					} else if (this.totalPages == unknown) { // don't display a goto last link if total number of pages is unknown
						return '';
					}
					
					return '<a last="" page="'+ this.totalPages +'">last</a>';
				},
				prev: function (index) {
					if (this.currentPage == 1) {
						return '';
					}
					return '<a prev="">prev</a>'; // you want to be smart so you shouldn't set the page attribute for the prev link
				},
				next: function(index) {
					if (this.currentPage == this.totalPages) {
						return ''; 
					}
					return '<a  next="">next</a>'; // you want to be smart so you shouldn't set the page attribute for the next link
				},
				page: function (index) { // all other pages
					if (index == this.currentPage) {
						if (this.options.smartRender) { // you want to be smart so you need to set current attribute later to compare old and new html without current attribute in place
							return '<a page="' + index + '">' + index + '</a>';
						} else {
							return '<a page="' + index + '" current="">' + index + '</a>';
						}
					}
					return '<a page="' + index + '">' + index + '</a>';
				}			
			},		
			onClick: function (element) {
				
			},
			onPageChange: function(oldPage, newPage) {
			},
			smartRender: true // try to be smart with rendering
		};
	

	function $isThing($elem) {
		if ($elem && $elem.length) {
			return true;
		}
		return false;
	}
		
	function zsPagination(options, $container) {
		//console.log('zsPagination', 'constructor', options);		
		this.options = options;
		this.$container = $container;
		this.configure();
		this.render();		
	};
	
	zsPagination.prototype.render = function () {
		var html = '', self = this, i,  shouldRender = true;
			
		if (!$isThing(this.$container)) {
			return;
		}
		
		// Add pages links
		for(i=this.startPage;i<this.startPage + this.pagesToDisplay;i++) {
			if (i<= this.totalPages) {
				html += this.format('page', i);
			} else {
				break;
			}
		}	
		
		// Add navigation links 
		html = 	this.format('first', this.firstPage) +
				this.format('prev', this.prevPage) +  
				html +
				this.format('next', this.nextPage) +
				this.format('last', this.lastPage);
				
		// Trying to be smart
		if (this.options.smartRender && this.code) {
			this.$container.find('[current]').removeAttr('current');			
			if (html == this.code) { // what if we don't have to render
				shouldRender = false;
			}		
		}	
		
		// Render
		if (shouldRender) {
			
			this.$container.html(html);			
			this.code = html; // store the code			
			
			// Add event listeners
			this.$container.find('a').on('click', function() {
				var el = this;
				self.click(el);	
			});		
		}		
		
		// If you are smart than set the current page after
		if (this.options.smartRender) {
			this.$container.find('[page="' + this.currentPage +'"]').not('[last]').not('[first]').attr('current', '');
		}
	}
	zsPagination.prototype.click = function(element) {
		// The execution context here is a zsPagination instance
		var $element = $(element),			
			isCurrent = $element.is('[current]'),
			isLast = $element.is('[last]'),
			isFirst = $element.is('[first]'),
			isNext = $element.is('[next]'),
			isPrev = $element.is('[prev]'),			
			page = $element.attr('page'); 
			
		if (!page) {
			if (isPrev) {
				page = this.currentPage - 1 || 1;
			} else if (isNext) {
				page = this.currentPage + 1;					
			} else if (isFirst) {
				page = 1;
			} else if (isLast) {
				page = this.totalPages;
			}
		} else {
			page = Number(page);
		}
			
		this.changePage(page);			
		
		if (typeof this.options.onClick == 'function') {
			return this.options.onClick.call(this, element);	
		}
	}
	
	zsPagination.prototype.format = function(formatName, index) {
		if (typeof this.options.format[formatName] == 'function') {
			return this.options.format[formatName].call(this, index);	
		} else {
			return defaults.format[formatName].call(this, index);
		}		
	}
	
	zsPagination.prototype.destroy = function () {
		if ($isThing(this.$container)) {
			this.$container.removeData('zsPagination');
			this.$container.empty();
		}
	}
	
	zsPagination.prototype.configure = function (options) {
		var delta;
		
		if (options) {
			this.options = options;
		}
		
		// Calc total pages and how many to display
		if (this.options.itemsCount || this.options.itemsCount === 0) {
			this.totalPages = Math.ceil(this.options.itemsCount/this.options.pageSize);
		} else {
			this.totalPages = unknown;
		}
		this.pagesToDisplay = this.options.maxDisplayPages;
		this.currentPage = this.options.currentPage;
		if (!this.currentPage) {
			this.currentPage = 1;
		} else if (this.currentPage > this.totalPages) {
			this.currentPage = this.totalPages;
		}
		
		
		
		// Calc the start page
		this.startPage =  this.currentPage - Math.floor(this.pagesToDisplay/2);
		if (this.startPage<=0) {
			this.startPage = 1;
		} 
		
		// Adjust the start page if we have fewer to display
		if (this.totalPages != unknown) {
			delta = this.totalPages - this.pagesToDisplay - this.startPage;
			if (delta<0) {
				this.startPage += delta + 1;
				if (this.startPage<1) {
					this.startPage = 1;
				}
			}			 
		}	
		
		this.nextPage = this.currentPage + 1;
		this.prevPage = this.currentPage - 1;
		this.firstPage = 1;
		this.lastPage = this.totalPages;		
	}
	
	zsPagination.prototype.changePage = function (newPage) {
		var oldPage = this.currentPage;
		if (newPage > this.totalPages) {
			newPage = this.totalPages;
		} else if (newPage<=0) {
			newPage = 1;
		}
		this.options.currentPage = newPage;
		this.configure();
		this.render();
		if (typeof this.options.onPageChange == 'function') {
			this.options.onPageChange.call(this,oldPage, newPage);
		}
	}

	zsPagination.prototype.defaults = defaults;
	
	$.fn.zsPagination = function (opt) {
		// Override mode
		if (this == $.fn) {
			$.extend(zsPagination.prototype, opt);
			return;
		}
		
		var options = ($.isPlainObject(opt) || !opt) ? $.extend(true, {}, zsPagination.prototype.defaults , opt) : $.extend(true, {}, zsPagination.prototype.defaults);		
		
		return this.each(function () {
			var pagination = $(this).data('zsPagination');
			if (pagination) {
				if ($.type(opt) == 'string') {
					switch (opt) {						
						case 'destroy':
							pagination.destroy($(this));
							break;
					}
				} else {
					pagination.configure($.extend(true, pagination.options, opt));
					pagination.render();
				}
				return;
			}
			if ($.type(opt) != 'string') {
				$(this).data('zsPagination', new zsPagination(options, $(this)));
				return;
			}
		});
	}

	// Add zs settings to be default
	$.extend(zsPagination.prototype.defaults, {
			maxDisplayPages: 5,
			format: {
				page: function (index) {
						var i, html='', self= this;
						function onPageSelect() {
										
						}		
						if (index == this.currentPage) {
							if (this.totalPages >= 25 && this.totalPages != unknown) { // Add a drop down								
								// Could avoid recreation of the element
								html = '<select>'
								for (i=1; i<=this.totalPages; i++) {
									html += '<option value="'+i+'"' + (i == this.currentPage ? ' selected' : '') + '>'+ i +'</option>'											
								}			
								html += '</select>';
								
								// need to create a select container first
								setTimeout(function () {
									var $span = self.$container.find('span');
									
									// We need to add event listeners every time because select is removed from the dome
									$span.find('select').on('change', function () {
										var selectedPage = $(this).val();
										self.changePage(Number(selectedPage));
									});										
								});								
										
								return '<span current="" page="'+index+'">'+html+'</span>';
								//return html;						 
							} else {
								if (this.options.smartRender) { // you want to be smart so you need to set current attribute later to compare old and new html without current attribute in place
									return '<a page="' + index + '">' + index + '</a>';
								} else {
									return '<a page="' + index + '" current="">' + index + '</a>';
								}
							}
						}
						return '<a page="' + index + '">' + index + '</a>';
					},
				prev: function (index) {
					var disabled;
					if (this.currentPage == 1) {
						disabled = true;
					}
					return '<a href="javascript:void(0)" class="zs-icon zs-icon-large zs-icon-prev" page="' + (this.currentPage - 1) + '" prev=""'+(disabled && ' disabled') + '></a>';
				},
				next: function () {
					var disabled;
					if (this.currentPage >= this.totalPages) {
						disabled = true;
					}
					return '<a href="javascript:void(0)" class="zs-icon zs-icon-large zs-icon-next" page="' + (this.currentPage + 1) + '" next=""'+ (disabled && ' disabled') + '></a>';
				},
				first: function (index) {
					if (this.totalPages <10) {return '';} 
					if (this.currentPage == 1) { // don't display a goto first link if the first page is already selected
							return '<a page="1" first="" disabled="disabled" class="zs-icon zs-icon-large zs-icon-first"></a>';
						}
						return '<a page="1" first="" class="zs-icon zs-icon-large zs-icon-first"></a>';					
					},
				last: function (index) { 
					if (this.totalPages <10) {return '';}
					if (this.currentPage == this.totalPages) { // don't display a goto last link if the last page is already selected
						return '<a last="" page="'+ this.totalPages +'" disabled="disabled" class="zs-icon zs-icon-last zs-icon-large"></a>';;
					} else if (this.totalPages == unknown) { // don't display a goto last link if total number of pages is unknown
						return '';
					}							
					return '<a last="" page="'+ this.totalPages +'" class="zs-icon zs-icon-large zs-icon-last"></a>';
				}
			}
		});

} (jQuery));