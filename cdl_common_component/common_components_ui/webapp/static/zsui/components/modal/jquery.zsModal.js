// zsui jquery modal dialog components
(function () {
    var defaults = {    
        autoOpen: false,
        closeOnEscape: true,
		submitOnEnter: 'input[submit-on-enter]', // A selector for the elements to use save on enter logic
		autoFocus: 'input[focus]', // Selector for the element to take focus
		closeSelector: '[close]',	// Selector for elements to close the dialog when clicked
		submitSelector: '>footer button', // Selector for the submit button
		style: {
			position: 'fixed',
            display: 'none',
            top: '50%',
            left: '50%',
            marginTop: 0, // will be overwritten on ie8 mode
            marginLeft: 0, // will be overwritten on ie8 mode
            translateY: '-50%',
            translateX: '-50%',
		},        
		render: {
			closeIcon: '<a href="javascript:void(0)" class="zs-icon zs-icon-close zs-icon-large" close></a>',
			header: function() {
				return '<header><span>' + this.options.title + '</span></header>';
			},
			overlay: '<div class="zs-overlay" style="display:none"></div>'
		},
		events: {
			submit: function() {
				this.close();
			},
			open: function() {

			},
			close: function() {

			}
		}
    }

    function zsModalDialog(options, $container) {
        this.$container = $container;
        this.configure(options);
        this.render();

    }
	
	zsModalDialog.prototype.defaults = defaults;
    zsModalDialog.prototype.submitOnEnter = function (e) {
        if (e.keyCode == 13) {
            if (this.options.events.submit) {
				this.options.events.submit.apply(this);
			}
            return false;
        }
    }

    zsModalDialog.prototype.configure = function (options) {
        if (options) {
            this.options = options;
        }
    }

  
    zsModalDialog.prototype.renderOverlay = function () {
        if (!this.$overlay) {
            this.$overlay = $('body>.zs-overlay');
            if (!this.$overlay.length) {
                this.$overlay = $(this.renderElement('overlay'));
                this.$overlay.appendTo($('body'));
            }
        }
    }

	zsModalDialog.prototype.renderElement = function (name) {
		if (this.options.render && this.options.render[name]) {
			if (typeof this.options.render[name] == 'function') {
				return this.options.render[name].apply(this);
			} else {
				return this.options.render[name];
			}
		} else {
			return '';
		}
	}

    zsModalDialog.prototype.render = function () {
        this.renderOverlay();
		this.$container.css(this.options.style);

		// Header
        if (!this.$header) {
            this.$header = this.$container.find('>header');
            if (!this.$header.length) {
                this.$header = $(this.renderElement('header'));
                this.$container.prepend(this.$header);
            } else {
                this.$header.html('<span>' + this.$header.text() + '</span>');
            }
            this.$title = this.$header.find('>span');
        }

		// Close icon
        if (!this.$closeIcon && this.options.closeSelector) {
            this.$closeIcon = this.$header.find(this.options.closeSelector);
            if (!this.$closeIcon.length) {
                this.$closeIcon = $(this.renderElement('closeIcon'));
                this.$header.append(this.$closeIcon);
            }
        }

	     // Close on cancel click
		if (!this.closeBind) {
			this.closeBind = this.close.bind(this);
		}
        this.$container.find(this.options.closeSelector).click(this.closeBind);

		// Submit
		if (this.options.events.submit && this.options.submitSelector) {
			this.$container.find(this.options.submitSelector).click(this.options.events.submit.bind(this));
		}

		// Submit on enter
		if (this.options.submitOnEnter) {
			if (!this.submitOnEnterBind) {
				this.submitOnEnterBind = this.submitOnEnter.bind(this);
			}
			this.$container.find(this.options.submitOnEnter).bind('keyup.enter', this.submitOnEnterBind);
		}

		// Render
        if (this.options.autoOpen) {
            this.open();
        }

		// Update title
		if (this.options.title) {
			this.$title.html(this.options.title);
		}
    }

    zsModalDialog.prototype.close = function () {
        this.$overlay.hide();
        if (this.closeBind) {
            this.$overlay.off('click', this.closeBind);
        }
        this.$container.css('display','none');
        if (this.escapeClickBind) {
        	$(document).off('keyup', this.escapeClickBind);
        }
		if (typeof this.options.events.close == 'function') {
			this.options.events.close.apply(this);
		}
    }

	zsModalDialog.prototype.fixPosition = function () {
        if (document.documentMode === 8) {
            this.options.style.marginLeft = -1 * this.$container.width() / 2;
            this.options.style.marginTop = -1 * this.$container.height() / 2;
        }

        this.$container.css({
            top: this.options.style.top,
            left: this.options.style.left,
            marginLeft: this.options.style.marginLeft,
            marginTop: this.options.style.marginTop,
            transform: 'translate(' + this.options.style.translateX + ',' + this.options.style.translateY + ')'
        });

		var box = this.$container[0].getBoundingClientRect();

		//let's save window size
		var win = {
			h : $(window).height(),
			w : $(window).width()
		};

		var topEdgeInRange = box.top >= 0 && box.top <= win.h;
		var leftEdgeInRange = box.left >= 0 && box.left <= win.w;
		
        var top = this.options.style.top, 
            left = this.options.style.left, 
            marginTop = this.options.style.marginTop,
            marginLeft = this.options.style.marginLeft,
            translateY = this.options.style.translateY, 
            translateX = this.options.style.translateX;
        
		if (!topEdgeInRange) {
            top = 0;
            marginTop = 0,
            translateY = 0;
		}
		if (!leftEdgeInRange) {
            left = 0;
            marginLeft = 0;
            translateX = 0;
		}

        if(!topEdgeInRange || !leftEdgeInRange){
            this.$container.css({
                top: top,
                left: left,
                marginTop: marginTop,
                marginLeft: marginLeft,
                transform: 'translate('+translateX+', '+translateY+')'
            });
        }
	}	

    zsModalDialog.prototype.open = function () {
        if (!this.$overlay || !this.$overlay.length) { // We share the same overlay
            this.renderOverlay();
        }
        this.$overlay.show();
        this.$overlay.on('click', this.closeBind);		
        this.$container.css('display','inline-block');
		this.fixPosition();      
        var self = this;
        if (this.options.closeOnEscape) {
			if (!this.escapeClickBind) {
        		this.escapeClickBind = this.escapeClick.bind(this);
			}
			$(document).on('keyup', this.escapeClickBind);
        }     
		if (this.options.autoFocus) {
			this.$container.find(this.options.autoFocus).focus();
		}
        
		if (typeof this.options.events.open == 'function') {
			this.options.events.open.apply(this);
		}

	}
    
    zsModalDialog.prototype.escapeClick = function (e) {
    	if (e.keyCode == 27) {
    		this.close();
    	}
    }

    zsModalDialog.prototype.destroy = function () {
        this.close();
        this.$overlay.remove();
        this.$container.removeData('zsModalDialog');
    }

    $.fn.zsModalDialog = function (opt) {
        var options = ($.isPlainObject(opt) || !opt) ? $.extend(true, {}, zsModalDialog.prototype.defaults, opt) : $.extend(true, {}, zsModalDialog.prototype.defaults);
		
		// Override mode
		if (this == $.fn) {
			$.extend(zsModalDialog.prototype, opt);
			return;
		}

        return this.each(function () {
            var plugin = $(this).data('zsModalDialog');
            if (plugin) {
                if (typeof opt == 'string') {
                    switch (opt) {
                        case 'open':
                            plugin.open();
                            break;
                        case 'close':
                            plugin.close();
                            break;
                        case 'destroy':
                            plugin.destroy($(this));
                            break;
                        case 'update':
                            plugin.update();
                            break;

                    }
                } else {
                    plugin.configure($.extend(true, plugin.options, opt));
					plugin.render();
                }
                return;
            }
            if (typeof opt != 'string') {
                $(this).data('zsModalDialog', new zsModalDialog(options, $(this)));
                return;
            }
        });
	}
}(jQuery));