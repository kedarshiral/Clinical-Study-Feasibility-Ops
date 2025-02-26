(function (zs) {
	"use strict";

    /**
     * @class
     */
    zs.progressBar = {

        /**
         * @private
         * @type {Number}
         */
        _max: null,

        /**
         * @private
         * @type {Number}
         */
        _value: null,

        /**
         * @private
         * @type {Boolean}
         */
        _label: null,

        /**
         * @private
         * @type {Number}
         */
        _duration: null,

        /**
         * @private
         * @type {String}
         */
        _metrics: null,

        /**
         * @private
         * @type {Boolean}
         */
        _animate: null,

        /**
         * @private
         * @type {Boolean}
         */
        _norender: null,

        /**
         * Default params
         * 
         * @private
         * @type {Object}
         */
        _defaults: {
            max: 1,
            value: 1,
            label: false,
            animate: false,
            duration: 300,
            metrics: '%',
            norender: false
        },

        /**
         * If we already rendered this element?
         * 
         * @private
         * @type {Boolean}
         */
        _isRendered: null,

        /**
         * Ref requestAnimationFrame ID in order to cancel it
         * 
         * @private
         * @type {Object}
         */
        _currentRequestAnimationFrame: null,

        /**
         * Ref to update lable event handler. 
         * Will use it to remove event listener in the end
         * 
         * @private
         * @type {Function}
         */
        _updateLabelWidthHandler: null,

        /**
         * List of attributes to be listened for change event
         * 
         * @private
         * @type {String[]}
         */
        observedAttributes: ['max', 'value', 'label', 'duration', 'metrics', 'animate', 'norender'],
        
        /**
         * Reference to progress element
         *
         * @type {HTMLElement}
         */
        progressElm: null,
        
        /**
         * Reference to label element
         *
         * @type {HTMLElement}
         */
        labelElm: null,

        /**
         * Calculated properties
         * 
         * @private
         * @type {Object}
         */
        properties: {
            /**
             * Maximum value for the bar. Default is 1
             * 
             * @memberof zs.progressBar
             * @type {Number}
             */
            max: {
                set: function(value){
                    if(isNaN(value) || value < 0){
                        value = this._defaults.max;
                    }

                    this._checkAndUpdateAttrByProp('max', value);
                    this._max = value;
                },
                get: function(value){
                    return this._max;
                }
            },

            /**
             * Value for the bar. Default is 1
             * 
             * @memberof zs.progressBar
             * @type {Number}
             */
            value: {
                set: function(value){
                    if(isNaN(value)){
                        value = this._defaults.value
                    }
                    
                    this._checkAndUpdateAttrByProp('value', value);
                    this._value = value;
                },
                get: function(value){
                    return this._value;
                }
            },

            /**
             * Whether to show label or not. Default is false
             * 
             * @memberof zs.progressBar
             * @type {Boolean}
             */
            label: {
                set: function(value){
                    this._checkAndUpdateAttrByProp('label', value);
                    this._label = value;
                },
                get: function(value){
                    return this._label;
                }
            },

            /**
             * Animation duration in ms. Default is 300
             * 
             * @memberof zs.progressBar
             * @type {Number}
             */
            duration: {
                set: function(value){
                    if(isNaN(value)){
                        value = this._defaults.duration
                    }

                    this._checkAndUpdateAttrByProp('duration', value);
                    this._duration = value;
                },
                get: function(value){
                    return this._duration;
                }
            },

            /**
             * Metrics type: %, cm, in etc. Default %. Set empty string to keep blank
             * 
             * @memberof zs.progressBar
             * @type {String}
             */
            metrics: {
                set: function(value){
                    this._checkAndUpdateAttrByProp('metrics', value);
                    this._metrics = value;
                },
                get: function(value){
                    return this._metrics;
                }
            },
            
            /**
             * Whether to animate or not. Default is false
             * 
             * @memberof zs.progressBar
             * @type {Boolean}
             */
            animate: {
                set: function(value){
                    this._checkAndUpdateAttrByProp('animate', value);
                    this._animate = value;
                },
                get: function(value){
                    return this._animate;
                }
            },
            
            /**
             *  If you want to set a props first and then render elm only once set this for true and then ender manualy. Default is false
             * 
             * @memberof zs.progressBar
             * @type {Boolean}
             */
            norender: {
                set: function(value){
                    this._checkAndUpdateAttrByProp('norender', value);
                    this._norender = value;
                },
                get: function(value){
                    return this._norender;
                }
            }
        },

        /**
         * Events list
         */
        events: {
            create: function(){
                this._isRendered = false;
            },

            attach: function(){
                this._setDefaults();
                this.render();
            },

            attributeChange: function(event){
                this._checkAndUpdatePropByAttr(event.detail.attributeName, event.detail.newValue);
            },

            detach: function(){
                this.cleanUp();
            }
        },

        /**
         * Set default values to properties
         * 
         * @private
         * 
         * @returns {zs.progressBar}
         */
        _setDefaults: function(){
            for(var i in this.properties){   
                let value = this._defaults[i];
                if(!this.properties.hasOwnProperty(i)){ // that's even not ours property
                    continue;
                }

                if(this[i] !== null){ // value was already set
                    continue;
                }
                                
                if(this[i] == value){ // this particular value was alredy set
                    continue;
                }

                this[i] = value;
            }

            return this;
        },

        /**
         * Check if it is necessary to update attribute after 
         * property was changes and if so updates it
         * 
         * @private
         * 
         * @param {String} name - Name of attribute
         * @param {String} value - Value to be set
         * 
         * @returns {zs.progressBar}
         */
        _checkAndUpdateAttrByProp: function(name, value){
            if(value == this.getAttribute(name)){
                return this;
            }
            
            this.setAttribute(name, value);

            if(this._isRendered && name !== 'norender'){
                this.render();
            }

            return this;
        },

        /**
         * Check if it is necessary to update property after 
         * attribute was changes and if so updates it
         * 
         * @private
         * 
         * @param {String} name - Name of property
         * @param {String} value - Value to be set
         * 
         * @returns {zs.progressBar}
         */
        _checkAndUpdatePropByAttr: function(name, value){
            if(!this.properties.hasOwnProperty(name)){
                return this;
            }

            var currentVallue = '' + this[name];
            if(currentVallue == value){
                return this;
            }

            this[name] = value;
            
            if(this._isRendered && name !== 'norender'){
                this.render();
            }

            return this;
        },

        /**
         * Updates progrees's width on each frame
         * 
         * @chainable
         * 
         * @returns {zs.progressBar}
         */
        animationStep: function(timestamp){
            if(!this.progressElm){
                return;
            }

            if(!this._animationStartTime){
                this._animationStartTime = timestamp;
            }
            
            var requestAnimationFrame = window.requestAnimationFrame || window.mozRequestAnimationFrame || window.webkitRequestAnimationFrame || window.msRequestAnimationFrame;            
            var cancelAnimationFrame = window.cancelAnimationFrame || window.mozCancelAnimationFrame;

            var diff = timestamp - this._animationStartTime; // how much time passed since we started animation
            var progress = diff / this.duration; // the same but relativley
            var totalPath = this.getBoundingClientRect().width; // total length of the bar
            var path = (this.value/this.max) * totalPath; // actual path animation should take

            this.progressElm.style.width = (path * progress)/totalPath * 100 + '%';
            
            if(this._currentRequestAnimationFrame){ // stop current animation
                cancelAnimationFrame(this._currentRequestAnimationFrame);
                this._currentRequestAnimationFrame = null;
            }

            if(diff < this.duration){
                this._currentRequestAnimationFrame = requestAnimationFrame(this.animationStep.bind(this));

                return this;
            }
            
            this._animationStartTime = null;
            
            return this.calculateAndSetWidth();
        },

        /**
         * Clean up element, usually before render
         * 
         * @chainable
         * 
         * @returns {zs.progressBar}
         */
        cleanUp: function(){
            this.progressElm = null;
            this.labelElm = null;
            
            window.removeEventListener('resize', this._updateLabelWidthHandler);
            this._updateLabelWidthHandler = null;

            if(this.innerHTML != ''){
                this.innerHTML = '';
            }

            return this;
        },

        /**
         * Calculate and set width of progress element
         * 
         * @chainable
         * 
         * @returns {zs.progressBar}
         */
        calculateAndSetWidth: function(){
            var width = Math.abs(parseFloat(this.value/this.max) * 100);
            if(isNaN(width) || width < 0){
                width = 0;
            }
            
            if(width > 100){
                width = 100;
            }

            this.progressElm.style.width = width + '%';

            return this;
        },

        /**
         * All manipulations to render label
         * 
         * @chainable
         * 
         * @returns {zs.progressBar}
         */
        renderLabel: function(){
            this.labelElm = document.createElement('div');

            if(!this.label){
                return this;
            }

            this.innerText = this.value + this.metrics;
            this.labelElm.innerText  = this.value + this.metrics;
            this.progressElm.appendChild(this.labelElm);

            this._updateLabelWidthHandler = this.updateLabelWidth.bind(this);
            setTimeout(this._updateLabelWidthHandler, 50); // wait elm render
            window.addEventListener('resize', this._updateLabelWidthHandler);

            return this;
        },

        /**
         * Makes sure label's width equals the whole element's width
         * 
         * @chainable
         * 
         * @returns {zs.progressBar}
         */
        updateLabelWidth: function(){
            if(!this.progressElm){
                return this;
            }
            
            this.labelElm.style.width = this.getBoundingClientRect().width + 'px';

            return this;
        },

        /**
         * Render this element
         * 
         * @chainable
         * 
         * @returns {zs.progressBar}
         */
        render: function(){ 
            if(this.norender){
                return;
            }

            this.cleanUp();
            
            this.progressElm  = document.createElement('div');            
            this.progressElm.style.width = 0;
            this.renderLabel();

            this.appendChild(this.progressElm);

            if(this.animate){
                window.requestAnimationFrame(this.animationStep.bind(this));
            }else{
                this.calculateAndSetWidth();
            }            

            this._isRendered = true;            

            return this;
        }
    };

    /**
     * @constructor
     * @extends HTMLElement
     * @extends zs.progressBar
     */
    zs.progressBarElement = zs.customElement(HTMLElement, "zs-progress-bar", null, [zs.progressBar]);

    return zs;
})(window.zs || {});
    