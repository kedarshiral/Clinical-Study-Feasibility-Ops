/*! rangeslider.js - v2.2.1 | (c) 2016 @andreruffert | MIT license | https://github.com/andreruffert/rangeslider.js */
(function($) {
    'use strict';

    // Polyfill Number.isNaN(value)
    // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number/isNaN
    Number.isNaN = Number.isNaN || function(value) {
        return typeof value === 'number' && value !== value;
    };

    /**
     * Range feature detection
     * @return {Boolean}
     */
    function supportsRange() {
        var input = document.createElement('input');
        input.setAttribute('type', 'range');
        return input.type !== 'text';
    }

    var zsSliderName = 'zsSlider',
        zsSliderIdentifier = 0,
        hasInputRangeSupport = supportsRange(),
        defaults = {
            multiple: false,
            polyfill: false,
            rangeMax: Infinity,
            rangeMin: -Infinity,
            base: null,
            orientation: 'horizontal',
            rulerPosition: 'top', // 'top' or 'bottom' for horizontal, 'left' or 'right' for vertical
            startEvent: ['mousedown', 'touchstart', 'pointerdown'],
            moveEvent: ['mousemove', 'touchmove', 'pointermove'],
            endEvent: ['mouseup', 'touchend', 'pointerup'],
            metric: '',
            prefix: '',
        },
        constants = {
            orientation: {
                horizontal: {
                    dimension: 'width',
                    direction: 'left',
                    directionStyle: 'left',
                    coordinate: 'x'
                },
                vertical: {
                    dimension: 'height',
                    direction: 'top',
                    directionStyle: 'bottom',
                    coordinate: 'y'
                }
            }
        };

    /**
     * Delays a function for the given number of milliseconds, and then calls
     * it with the arguments supplied.
     *
     * @param  {Function} fn   [description]
     * @param  {Number}   wait [description]
     * @return {Function}
     */
    function delay(fn, wait) {
        var args = Array.prototype.slice.call(arguments, 2);
        return setTimeout(function(){ return fn.apply(null, args); }, wait);
    }

    /**
     * Returns a debounced function that will make sure the given
     * function is not triggered too much.
     *
     * @param  {Function} fn Function to debounce.
     * @param  {Number}   debounceDuration OPTIONAL. The amount of time in milliseconds for which we will debounce the function. (defaults to 100ms)
     * @return {Function}
     */
    function debounce(fn, debounceDuration) {
        debounceDuration = debounceDuration || 100;
        return function() {
            if (!fn.debouncing) {
                var args = Array.prototype.slice.apply(arguments);
                fn.lastReturnVal = fn.apply(window, args);
                fn.debouncing = true;
            }
            clearTimeout(fn.debounceTimeout);
            fn.debounceTimeout = setTimeout(function(){
                fn.debouncing = false;
            }, debounceDuration);
            return fn.lastReturnVal;
        };
    }

    /**
     * Check if a `element` is visible in the DOM
     *
     * @param  {Element}  element
     * @return {Boolean}
     */
    function isHidden(element) {
        return (
            element && (
                element.offsetWidth === 0 ||
                element.offsetHeight === 0 ||
                // Also Consider native `<details>` elements.
                element.open === false
            )
        );
    }

    /**
     * Get hidden parentNodes of an `element`
     *
     * @param  {Element} element
     * @return {[type]}
     */
    function getHiddenParentNodes(element) {
        var parents = [],
            node    = element.parentNode;

        while (isHidden(node)) {
            parents.push(node);
            node = node.parentNode;
        }
        return parents;
    }

    /**
     * Returns dimensions for an element even if it is not visible in the DOM.
     *
     * @param  {Element} element
     * @param  {String}  key     (e.g. offsetWidth …)
     * @return {Number}
     */
    function getDimension(element, key) {
        var hiddenParentNodes       = getHiddenParentNodes(element),
            hiddenParentNodesLength = hiddenParentNodes.length,
            inlineStyle             = [],
            dimension               = element[key];

        // Used for native `<details>` elements
        function toggleOpenProperty(element) {
            if (typeof element.open !== 'undefined') {
                element.open = (element.open) ? false : true;
            }
        }

        if (hiddenParentNodesLength) {
            for (var i = 0; i < hiddenParentNodesLength; i++) {

                // Cache style attribute to restore it later.
                inlineStyle[i] = hiddenParentNodes[i].style.cssText;

                // visually hide
                if (hiddenParentNodes[i].style.setProperty) {
                    hiddenParentNodes[i].style.setProperty('display', 'block', 'important');
                } else {
                    hiddenParentNodes[i].style.cssText += ';display: block !important';
                }
                hiddenParentNodes[i].style.height = '0';
                hiddenParentNodes[i].style.overflow = 'hidden';
                hiddenParentNodes[i].style.visibility = 'hidden';
                toggleOpenProperty(hiddenParentNodes[i]);
            }

            // Update dimension
            dimension = element[key];

            for (var j = 0; j < hiddenParentNodesLength; j++) {

                // Restore the style attribute
                hiddenParentNodes[j].style.cssText = inlineStyle[j];
                toggleOpenProperty(hiddenParentNodes[j]);
            }
        }
        return dimension;
    }

    /**
     * Returns the parsed float or the default if it failed.
     *
     * @param  {String}  str
     * @param  {Number}  defaultValue
     * @return {Number}
     */
    function tryParseFloat(str, defaultValue) {
        var value = parseFloat(str);
        return Number.isNaN(value) ? defaultValue : value;
    }

    /**
     * Capitalize the first letter of string
     *
     * @param  {String} str
     * @return {String}
     */
    function ucfirst(str) {
        return str.charAt(0).toUpperCase() + str.substr(1);
    }

    /**
     * zsSlider
     * 
     * @param {String} element
     * @param {Object} options
     * 
     * @return ZSSlider
     */
    function zsSlider(element, options) {
        this.$window            = $(window);
        this.$document          = $(document);
        this.$element           = $(element);
        this.options            = $.extend( {}, defaults, options );
        this.polyfill           = this.options.polyfill;
        this.base               = this.$element[0].getAttribute('base') || this.options.base;
        this.rangeMax           = this.$element[0].getAttribute('range-max') || this.options.rangeMax;
        this.rangeMin           = this.$element[0].getAttribute('range-min') || this.options.rangeMin;
        this.orientation        = this.$element[0].getAttribute('orientation') || this.options.orientation;
        this.rulerPosition      = this.$element[0].getAttribute('ruler-position') || this.options.rulerPosition;
        this.metric             = this.$element[0].getAttribute('metric') || this.options.metric;
        this.prefix             = this.$element[0].getAttribute('prefix') || this.options.prefix;
        this.onInit             = this.options.onInit;
        this.onSlide            = this.options.onSlide;
        this.onSlideEnd         = this.options.onSlideEnd;
        this.DIMENSION          = constants.orientation[this.orientation].dimension;
        this.DIRECTION          = constants.orientation[this.orientation].direction;
        this.DIRECTION_STYLE    = constants.orientation[this.orientation].directionStyle;
        this.COORDINATE         = constants.orientation[this.orientation].coordinate;

        this.multiple = false;
        if(this.$element[0].hasAttribute('multiple') || this.options.multiple){
            this.multiple = true;
        }
        
        // zsSlider should only be used as a polyfill
        if (this.polyfill) {
            // Input range support?
            if (hasInputRangeSupport) { return this; }
        }

        this.identifier = zsSliderName + '-' +(zsSliderIdentifier++);
        this.startEvent = this.options.startEvent.join('.' + this.identifier + ' ') + '.' + this.identifier;
        this.moveEvent  = this.options.moveEvent.join('.' + this.identifier + ' ') + '.' + this.identifier;
        this.endEvent   = this.options.endEvent.join('.' + this.identifier + ' ') + '.' + this.identifier;
        this.toFixed    = (this.step + '').replace('.', '').length - 1;
        this.$fill      = $('<div fill />');
        this.$handle    = $('<button class="zs-buton" handle />');
        this.$handle2   = $('<button class="zs-buton" handle hh />');
        this.$ruler     = $('<div ruler />');
        this.$container  = $('<div class="zs-slider" ' + this.orientation + ' id="' + this.identifier + '"/>').append(this.$ruler).insertAfter(this.$element);
        this.$range     = $('<div range />').prepend(this.$fill, this.$handle).prependTo(this.$container);
        this.rulerUnits = {};
        this.$container.attr('ruler-position', this.rulerPosition);
        this.$currentHandle = this.$handle;

        if(this.multiple){
            this.$handle2.appendTo(this.$range);
        }

        // visually hide the inputs
        this.$element.css({
            'position': 'absolute',
            'width': '1px',
            'height': '1px',
            'overflow': 'hidden',
            'opacity': '0'
        });

        // Store context
        this.handleDown = $.proxy(this.handleDown, this);
        this.handleMove = $.proxy(this.handleMove, this);
        this.handleEnd  = $.proxy(this.handleEnd, this);

        this.init();

        // Attach Events
        var _this = this;
        this.$window.on('resize.' + this.identifier, debounce(function() {
            // Simulate resizeEnd event.
            delay(function() { _this.update(false, false); }, 300);
        }, 20));

        this.$document.on(this.startEvent, '#' + this.identifier + ':not([disabled])', this.handleDown);

        // Listen to programmatic value changes
        this.$element.on('change.' + this.identifier, function(e, data) {
            if (data && data.origin === _this.identifier) {
                return;
            }

            var value = e.target.getAttribute('value');
            if(this.multiple){
                value = value.split(';');
                var value1 = tryParseFloat(value[0], Math.round(_this.min + (_this.max-_this.min)/2));
                var value2 = tryParseFloat(value[1], Math.round(_this.min + (_this.max-_this.min)/2));
                
                value = [value1, value2]; 
                _this.$currentHandle = _this.$handle;
                _this.$currentGrabPos = _this.grabPos;
                _this.setPosition(_this.getPositionFromValue(value[1]));
    
                _this.$currentHandle = _this.$handle2;
                _this.$currentGrabPos = _this.grabPos2;
                _this.setPosition(_this.getPositionFromValue(value[0]));
            }else{
                _this.setPosition(_this.getPositionFromValue(value));
            }
        });

        this.$handle.on('click', function(e){
            e.preventDefault();
        });

        this.$handle2.on('click', function(e){
            e.preventDefault();
        });

        return this;
    }

    /**
     * Start point
     * 
     * @chainable
     * 
     * @return ZSSlider
     */ 
    zsSlider.prototype.init = function() {
        this.update(true, false);
        
        if (this.onInit && typeof this.onInit === 'function') {
            this.onInit();
        }

        return this;
    };

    /**
     * To be called when something changed
     * 
     * @chainable
     * 
     * @param {Bool} updateAttributes - Do we need to update attrs?
     * @param {Bool} triggerSlide - Should we call slide update
     * 
     * @return ZSSlider
     */
    zsSlider.prototype.update = function(updateAttributes, triggerSlide) {
        updateAttributes = updateAttributes || false;

        this.step  = tryParseFloat(this.$element[0].getAttribute('step'));
        if (updateAttributes) {
            this.min    = tryParseFloat(this.$element[0].getAttribute('min'), 0);
            this.max    = tryParseFloat(this.$element[0].getAttribute('max'), 100);
            
            if(this.multiple){
                var value;
                if(!this.$element[0].getAttribute('value') || !this.$element[0].getAttribute('value').split || !this.$element[0].getAttribute('value').split(';')[1]){
                    value = [this.min, this.max];
                }else{
                    value = this.$element[0].getAttribute('value').split(';');
                }
                var value1 = tryParseFloat(value[0], Math.round(this.min + (this.max-this.min)/2));
                var value2 = tryParseFloat(value[1], Math.round(this.min + (this.max-this.min)/2));

                if(value1 > value2){
                    this.value = [value2, value1];
                }else{
                    this.value = [value1, value2];
                }
                
            }else{
                this.value  = tryParseFloat(this.$element[0].value, Math.round(this.min + (this.max-this.min)/2));                
            }
            
        }
        this.handleDimension    = getDimension(this.$handle[0], 'offset' + ucfirst(this.DIMENSION));
        this.rangeDimension     = getDimension(this.$range[0], 'offset' + ucfirst(this.DIMENSION));
        this.maxHandlePos       = this.rangeDimension - this.handleDimension;
        this.grabPos            = this.handleDimension / 2;
        this.grabPos2           = this.handleDimension / 2;

        // Consider disabled state
        if (this.$element[0].disabled) {
            this.$container.attr('disabled', 'disabled');
        } else {
            this.$container.attr('disabled', false);
        }

        this.getRulerRange();
        
        this.minRangePosition = this.rulerUnits[this.rangeMin];
        this.maxRangePosition = this.rulerUnits[this.rangeMax];

        var minI = Infinity, maxI = -Infinity;
        for(var i in this.rulerUnits){
            i = +i; // convert to number
            if(i > maxI){
                maxI = i;
            }

            if(i < minI){
                minI = i;
            }
        }
        
        if(this.orientation === 'vertical'){
            this.adjustVerticalFill(maxI, minI);
        }else{
            this.adjustHorizontalFill(maxI, minI);
        }

        if(this.base){
            this.$fill.css({
                borderRadius:0
            });
        }
        
        if(this.multiple){
            this.$currentHandle = this.$handle;
            this.$currentGrabPos = this.grabPos;
            this.setPosition(this.getPositionFromValue(this.value[1]), triggerSlide);

            this.$currentHandle = this.$handle2;
            this.$currentGrabPos = this.grabPos2;
            this.setPosition(this.getPositionFromValue(this.value[0]), triggerSlide);
        }else{
            this.position = this.getPositionFromValue(this.value);
            this.$currentHandle = this.$handle;
            this.$currentGrabPos = this.grabPos;
            this.setPosition(this.position, triggerSlide);
        }
        
        this.$handle.textContent = this.value;    

        return this;    
    };

    zsSlider.prototype.adjustHorizontalFill = function(maxI, minI) {
        if(this.maxRangePosition){
            if(this.maxRangePosition >= this.rulerUnits[maxI]){
                this.$fill.css({
                    right: 0,
                    borderRadius: '0.3em'
                });
            }else{
                this.$fill.css({
                    right: this.rulerUnits[maxI] - this.maxRangePosition + this.$handle.width()/3
                });
            }
            
        }else{
            if(!this.minRangePosition || this.rangeMin <= minI){
                this.minRangePosition = -1;
                this.$fill.css({
                    borderRadius: '0.3em'
                });

            }    

            this.$fill.css({
                left: this.minRangePosition
            });       
        }
    };

    zsSlider.prototype.adjustVerticalFill = function(maxI, minI) {
        if(this.maxRangePosition){
            if(this.maxRangePosition >= this.rulerUnits[maxI]){
                this.$fill.css({
                    top: 0,
                    borderRadius: '0.3em'
                });
            }else{
                this.$fill.css({
                    top: this.rulerUnits[maxI] - this.maxRangePosition + this.$handle.height()/3
                });
            }
            
        }else{
            if(!this.minRangePosition || this.rangeMin <= minI){
                this.minRangePosition = -1;
                this.$fill.css({
                    borderRadius: '0.3em'
                });

            }    

            this.$fill.css({
                bottom: this.minRangePosition
            });       
        }
    };

    /**
     * When move started
     * 
     * @chainable
     * 
     * @param {Event} e
     * 
     * @return ZSSlider
     */ 
    zsSlider.prototype.handleDown = function(e) {
        e.preventDefault();
        
        this.$document.on(this.moveEvent, this.handleMove);
        this.$document.on(this.endEvent, this.handleEnd);

        // If we click on the handle don't set the new position
        if ((' ' + e.target.className + ' ').replace(/[\n\t]/g, ' ').indexOf(this.options.handleClass) > -1) {
            return this;
        }

        var params = this.chooseHandle(e);
        
        this.isMoving = true;

        var pos         = this.getRelativePosition(e),
            rangePos    = this.$range[0].getBoundingClientRect()[this.DIRECTION],
            handlePos   = this.getPositionFromNode(this.$currentHandle[0]) - rangePos,
            setPos      = (this.orientation === 'vertical') ? (this.maxHandlePos - (pos - this.$currentGrabPos)) : (pos - this.$currentGrabPos);
        
        this.setPosition(setPos);

        if (pos >= handlePos && pos < handlePos + this.handleDimension) {
            this.$currentGrabPos = pos - handlePos;
        }

        return this
    };

    /**
     * Helps determine which handler user probably wants to move. 
     * Makes sense only in multiple mode
     * 
     * @param {Event} e - current mouse/touch event
     * 
     */
    zsSlider.prototype.chooseHandle = function(e) {
        var $handle = this.$handle,
        grabPos = this.grabPos;
       
        var distanceBetweenEventAndHandle = Math.sqrt(Math.pow(e.pageX - this.$handle.offset().left, 2) + Math.pow(e.pageY - this.$handle.offset().top, 2));
        var distanceBetweenEventAndHandle2 = Math.sqrt(Math.pow(e.pageX - this.$handle2.offset().left, 2) + Math.pow(e.pageY - this.$handle2.offset().top, 2));
        
        if(this.multiple && distanceBetweenEventAndHandle2 < distanceBetweenEventAndHandle){
            $handle = this.$handle2;
            grabPos = this.grabPos2;
        }
         
        this.$currentHandle = $handle;
        this.$currentGrabPos = grabPos;
    }

    /**
     * When handle been moved
     * 
     * @chainable
     * 
     * @param {Event} e
     * 
     * @return ZSSlider
     */ 
    zsSlider.prototype.handleMove = function(e) {
        e.preventDefault();

        var pos = this.getRelativePosition(e);
        var setPos = (this.orientation === 'vertical') ? (this.maxHandlePos - (pos - this.$currentGrabPos)) : (pos - this.$currentGrabPos);
        this.setPosition(setPos);

        return this;
    };

    /**
     * When moving event stoped
     * 
     * @chainable
     * 
     * @param {Event} e
     * 
     * @return ZSSlider
     */ 
    zsSlider.prototype.handleEnd = function(e) {
        e.preventDefault();
        this.$document.off(this.moveEvent, this.handleMove);
        this.$document.off(this.endEvent, this.handleEnd);

        this.isMoving = false;

        // Ok we're done fire the change event
        this.$element.trigger('change', { origin: this.identifier });

        if (this.onSlideEnd && typeof this.onSlideEnd === 'function') {
            this.onSlideEnd(this.value);
        }

        return this;
    };

    /**
     * Check if the provided position is in the range of min-max
     * 
     * @chainable
     * 
     * @param {Number} pos
     * @param {Number} min
     * @param {Number} max
     * 
     * @return Number
     */ 
    zsSlider.prototype.cap = function(pos, min, max) {
        if (pos < min) { return min; }
        if (pos > max) { return max; }
        return pos;
    };

    /**
     * Set new posiiton 
     * 
     * @chainable
     * 
     * @param {Number} pos - New posiiton
     * @param {Bool} triggerSlide - do we have to trigger slide?
     * 
     * @return ZSSlider
     */ 
    zsSlider.prototype.setPosition = function(pos, triggerSlide) {
        var value, newPos;
        if (triggerSlide === undefined) {
            triggerSlide = true;
        }
        
        // Snapping steps
        value = this.getValueFromPosition(this.cap(pos, 0, this.maxHandlePos));
        newPos = this.getPositionFromValue(value);
        
        if(value < this.rangeMin){
            return this;
        }

        if(value > this.rangeMax){
            return this;
        }

        // Update ui
        var size = (newPos + this.$currentGrabPos) - this.minRangePosition;
        if(this.maxRangePosition){
            size = this.maxRangePosition - (newPos + this.$currentGrabPos) + this.$currentHandle.width()/3;
        }
        
        if(size < 0){
            size = 0;
        }
        
        if(this.multiple){
            var cachedPos = this.$currentHandle[0].style[this.DIRECTION_STYLE];

            this.$currentHandle[0].style[this.DIRECTION_STYLE] = newPos - 1 + 'px'; 

            var leftPosition = parseFloat(this.$handle2[0].style[this.DIRECTION_STYLE]);
            var rightPosition = parseFloat(this.$handle[0].style[this.DIRECTION_STYLE]);
            
            if(rightPosition < leftPosition && this.isMoving){
                this.$currentHandle[0].style[this.DIRECTION_STYLE] = cachedPos;
                return this;
            }
            
            size = leftPosition - rightPosition;

            this.$fill[0].style[this.DIRECTION_STYLE] =  leftPosition + 'px';
            this.$fill[0].style[this.DIMENSION] = Math.abs(size) + 'px';
        }else{
            if(this.base){
                size = (newPos + this.$currentGrabPos) - this.rulerUnits[this.base];
    
                if(size < 0){
                    this.$fill.css(this.DIRECTION_STYLE, this.rulerUnits[this.base] + size + 2);
                }else{
                    this.$fill.css(this.DIRECTION_STYLE, this.rulerUnits[this.base] + 2);
                }
            }

            this.$fill[0].style[this.DIMENSION] =  Math.abs(size) + 'px';
            this.$currentHandle[0].style[this.DIRECTION_STYLE] = newPos - 1 + 'px';
        }
        
        if(this.multiple){
            value = this.$currentHandle === this.$handle2 ? [value, this.value[1]] : [this.value[0], value];
        }
        
        this.setValue(value);
        
        // Update globals
        // this.position = newPos;
        this.value = value;

        if (triggerSlide && this.onSlide && typeof this.onSlide === 'function') {
            this.onSlide(newPos, value);
        }

        return this;
    };

    /**
     * Returns element position relative to the parent
     * 
     * @chainable
     * 
     * @param {DOMNode} node
     * 
     * @return Number
     */ 
    zsSlider.prototype.getPositionFromNode = function(node) {
        var i = 0;
        while (node) {
            i += node.offsetLeft;
            node = node.offsetParent;
        }
        return i;
    };

    /**
     * Find posiiton relative to the viewport
     * 
     * @chainable
     * 
     * @param {Event} e
     * 
     * @return Number
     */ 
    zsSlider.prototype.getRelativePosition = function(e) {
        // Get the offset DIRECTION relative to the viewport
        var ucCoordinate = ucfirst(this.COORDINATE),
            rangePos = this.$range[0].getBoundingClientRect()[this.DIRECTION],
            pageCoordinate = 0;

        if (typeof e['client' + ucCoordinate] !== 'undefined') {
            pageCoordinate = e['client' + ucCoordinate];
        }
        // IE8 support :)
        else if (typeof e.originalEvent['client' + ucCoordinate] !== 'undefined') {
            pageCoordinate = e.originalEvent['client' + ucCoordinate];
        }
        else if (e.originalEvent.touches && e.originalEvent.touches[0] && typeof e.originalEvent.touches[0]['page' + ucCoordinate] !== 'undefined') {
            pageCoordinate = e.originalEvent.touches[0]['page' + ucCoordinate] - window['scroll' + ucCoordinate];
        }
        else if(e.currentPoint && typeof e.currentPoint[this.COORDINATE] !== 'undefined') {
            pageCoordinate = e.currentPoint[this.COORDINATE];
        }

        return pageCoordinate - rangePos;
    };

    /**
     * Find a position by the value
     * 
     * @chainable
     * 
     * @param {Number} value
     * 
     * @return Number
     */ 
    zsSlider.prototype.getPositionFromValue = function(value) {
        var percentage, pos;
        percentage = (value - this.min)/(this.max - this.min);
        pos = (!Number.isNaN(percentage)) ? percentage * this.maxHandlePos : 0;
        return pos;
    };

    /**
     * Find a value by position
     * 
     * @chainable
     * 
     * @param {Number} pos
     * 
     * @return Number
     */ 
    zsSlider.prototype.getValueFromPosition = function(pos) {
        var percentage, value;
        percentage = ((pos) / (this.maxHandlePos || 1));
        value = this.step * Math.round(percentage * (this.max - this.min) / this.step) + this.min;
        return Number((value).toFixed(this.toFixed));
    };

    /**
     * Set value
     * 
     * @chainable
     * 
     * @return ZSSlider
     */ 
    zsSlider.prototype.setValue = function(value) {
        if (value === this.value && this.$element[0].value !== '') {
            return this;
        }

        if(this.multiple){
            value = value.join(';');
        }

        // Set the new value and fire the `input` event
        this.$element[0].setAttribute('value', value);     
        this.$element.trigger('input', { origin: this.identifier });
        
       return this;
    };

    /**
     * Get value
     * 
     * 
     * @return Number|String|Array
     */ 
    zsSlider.prototype.getValue = function() {
        return this.value;
    };

    /**
     * Before destroy zsSlider
     * 
     * @chainable
     * 
     * @return ZSSlider
     */ 
    zsSlider.prototype.destroy = function() {
        this.$document.off('.' + this.identifier);
        this.$window.off('.' + this.identifier);

        this.$element
            .off('.' + this.identifier)
            .removeAttr('style')
            .removeData('zsSlider_' + zsSliderName);
    
        this.$handle.off();
        // Remove the generated markup
        if (this.$range && this.$range.length) {
            this.$range[0].parentNode.removeChild(this.$range[0]);
        }

        return this;
    };

    /**
     * Prepare ruler
     * 
     * @chainable
     * 
     * @return ZSSlider
     */ 
    zsSlider.prototype.getRulerRange = function () {
        var $elm;
        var i = this.min;
        var step = this.step; 

        if(!this.step){
            this.step = 1;
            step = parseFloat((this.max - this.min)/2);
        }

        this.rulerUnits = {};
        this.$ruler.empty();
        while (i <= this.max) {
            $elm = $('<div />');
            $elm.html(this.prefix + i + this.metric) ;
            if(this.rulerPosition === 'bottom' || this.rulerPosition === 'right'){
                $elm.prepend('<div />');
            }else{
                $elm.append('<div />');
            }
            
            this.$ruler.append($elm);
            var position = this.getPositionFromValue(i);
            if(this.orientation == 'vertical'){
                $elm.css({
                    bottom: position,
                    marginBottom: -1 * $elm.height()/2
                });
            }else{
                $elm.css({
                    left: position,
                    marginLeft: -1 * $elm.width()/2
                });
            }

            this.rulerUnits[i] =  position + $elm.width()/3;
            if(step % 1 != 0){ // Floating point calc issue https://docs.oracle.com/cd/E19957-01/806-3568/ncg_goldberg.html
                var depth = ('' + step).split('.')[1].length; // calc the depth of number (how many digits are there after floating point)
                var multiplier = Math.pow(10, depth); // calc multiplier 
                i = ((i * multiplier + step * multiplier)/multiplier).toFixed(depth); 
            }else{
                i = i + step;
            }
            
        }

        return this;
    }

    // A really lightweight zsSlider wrapper around the constructor,
    // preventing against multiple instantiations
    $.fn[zsSliderName] = function(opt) {
        var options = ($.isPlainObject(opt) || !opt) ? $.extend(true, {}, zsSlider.prototype.defaults, opt) : $.extend(true, {}, zsSlider.prototype.defaults);

        var args = Array.prototype.slice.call(arguments, 1);

        // Override mode
		if (this == $.fn) {
			$.extend(zsSlider.prototype, opt);
			return;
		}

        return this.each(function() {
            var $this = $(this),
                data  = $this.data(zsSliderName);

            // Create a new instance.
            if (!data) {
                $this.data(zsSliderName, (data = new zsSlider(this, options)));
            }

            // Make it possible to access methods from public.
            // e.g `$element.rangeslider('method');`
            if (typeof opt === 'string') {
                data[opt].apply(data, args);
            }
        });
    };

    return 'zs slider.js is available in jQuery context e.g $(selector).zsSlider(options);';

}(jQuery));
