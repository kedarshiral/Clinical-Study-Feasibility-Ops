(function (zs) {
	'use strict';

	/**
     * ZS Flow link
	 * 
     * @class zs.flowLink
     *
     * @property {Raphael.flow.paper} paper
     * @property {Boolean} isSelected
     * @property {zs.flowNode} from
     * @property {zs.flowNode} to
     * @property {Object} config
     *
     * @method configure
     * @method onMouseover
     * @method onMouseout
     * @method onClick
     * @method select
     * @method deselect
     * @method addShapeListeners
     * @method addArrowEndListeners
     * @method addArrowStartListeners
     * @method render
     * @method destroy
     *
     */
	zs.flowLink = function(){};

	/**
	 * Ref to Flow
	 *
	 * @property {HTMLElement}
	 */
	zs.flowLink.prototype.flow = null;

	/**
	 * Configuration
	 * 
	 * @type {Object}
	 */
	zs.flowLink.prototype.config = null;

	/**
	 * If this link was selected
	 * 
	 * @type {Boolean}
	 */
	zs.flowLink.prototype.isSelected = null;

	/**
	 * 'From' node ref
	 * 
	 * @type {zs.flowNode}
	 */
	zs.flowLink.prototype.from = null;

	/**
	 * 'To' node ref
	 * 
	 * @type {zs.flowNode}
	 */
	zs.flowLink.prototype.to = null;

	zs.flowLink.prototype.defaults = {
		arrowSize: 10,
		arrowStart: false,
		arrowEnd: false
	};

	/**
	 * Configure this element
	 * 
	 * @chainable
     * 
     * @returns {zs.flowLink}
	 */
	zs.flowLink.prototype.configure = function(params){
		if(!params.flow){
			throw Error('Reference to Flow MUST be provided');
		}

		if(!params.nodeFrom){
			throw Error('nodeFrom MUST be provided');
		}

		if(!params.nodeTo){
			throw Error('nodeTo MUST be provided');
		}

		if(!params.dotFrom){
			throw Error('dotFrom MUST be provided');
		}

		if(!params.dotTo){
			throw Error('dotTo MUST be provided');
		}
		
		this.config = {};
		this.isSelected = false;

		this.flow = params.flow;

		this.config.nodeFrom = params.nodeFrom;
		this.config.nodeTo   = params.nodeTo;
		
		this.config.dotFrom = params.dotFrom;
		this.config.dotTo   = params.dotTo;

		this.config.key = params.key;
		this.config.type = params.type;

		this.config.arrowSize  = params.arrowSize  || this.defaults.arrowSize;
		this.config.arrowEnd   = params.arrowEnd   || this.defaults.arrowEnd;
		this.config.arrowStart = params.arrowStart || this.defaults.arrowStart;

		this.from = this.config.nodeFrom[this.config.dotFrom];
        this.to = this.config.nodeTo[this.config.dotTo];

		this._nodeDraggedHandler = this.nodeDragged.bind(this);
		this._nodeDestroyHandler = this.destroy.bind(this);

		this.config.nodeFrom.shape.node.addEventListener('dragmove', this._nodeDraggedHandler);
		this.config.nodeTo.shape.node.addEventListener('dragmove', this._nodeDraggedHandler);

		this.config.nodeFrom.shape.node.addEventListener('dragend', this._nodeDraggedHandler);
		this.config.nodeTo.shape.node.addEventListener('dragend', this._nodeDraggedHandler);
		
		this.config.nodeFrom.shape.node.addEventListener('remove', this._nodeDestroyHandler);
		this.config.nodeTo.shape.node.addEventListener('remove', this._nodeDestroyHandler);
		
		return this;
	};

	/**
	 * Export this link
	 */
	zs.flowLink.prototype.export = function(){
		return {
			nodeFrom: this.config.nodeFrom.config.key,
			nodeTo: this.config.nodeTo.config.key,
			dotFrom: this.config.dotFrom,
			dotTo: this.config.dotTo,
			key: this.config.key,
			arrowSize: this.config.arrowSize,
			arrowEnd: this.config.arrowEnd,
			arrowStart: this.config.arrowStart,
			type: this.config.type
		};

	};

	/**
     * Render shape
     * 
     * @returns {Raphael.Element}
     */
    zs.flowLink.prototype.renderShape = function(dots){
        var shape = this.flow.paper.path(this.configureShapePath(dots));
        shape.toBack();

        return shape;
    };
    
    /**
     * Render start arrow
     * 
     * @returns {Raphael.Element}
     */
    zs.flowLink.prototype.renderStartArrow = function(dots){		
		var arrow = this.flow.paper.path(this.configureStartArrowPath(dots));
		arrow.node.setAttribute('arrow', '');
		arrow.toBack();

		return arrow;
	};

    /**
     * Render end arrow
     * 
     * @returns {Raphael.Element}
     */
    zs.flowLink.prototype.renderEndArrow = function(dots){	
		var arrow = this.flow.paper.path(this.configureEndArrowPath(dots));

		arrow.node.setAttribute('arrow', '');
		arrow.toBack();

		return arrow;
	};

	/**
	 * When node been dragged
	 * 
	 * @chainable
     * 
     * @returns {zs.flowLink}
	 */
	zs.flowLink.prototype.nodeDragged = function(e){
		var self = this;
		requestAnimationFrame(function(){
			var dots = self.flow.findClosestDots(self.config.nodeFrom, self.config.nodeTo);
			if(self.shape){
				self.shape.attr({ path: self.configureShapePath(dots) });
			}

			if(self.arrowEnd){
				self.arrowEnd.attr({ path: self.configureEndArrowPath(dots) });
			}

			if(self.arrowStart){
				self.arrowStart.attr({ path: self.configureStartArrowPath(dots) });
			}

		});

		return this;
	};

	/**
	 * When mouseover
	 * 
	 * @chainable
     * 
     * @returns {zs.flowLink}
	 */
	zs.flowLink.prototype.onMouseover = function(){
		if(this.config.arrowStart){
			this.arrowStart.node.setAttribute('hovered', '');
		}

		if(this.config.arrowEnd){
			this.arrowEnd.node.setAttribute('hovered', '');
		}

		this.shape.node.setAttribute('hovered', '');

		return this;
	};

	/**
	 * When mouseout 
	 * 
	 * @chainable
     * 
     * @returns {zs.flowLink}
	 */
	zs.flowLink.prototype.onMouseout = function(){
		if(this.config.arrowStart){
			this.arrowStart.node.removeAttribute('hovered');
		}

		if(this.config.arrowEnd){
			this.arrowEnd.node.removeAttribute('hovered');
		}

		this.shape.node.removeAttribute('hovered');

		return this;
	};

	/**
	 * When click occured
	 * 
	 * @chainable
     * 
     * @returns {zs.flowLink}
	 */
	zs.flowLink.prototype.onClick = function(){
		if(this.isSelected){
			return this.deselect();
		}

		return this.select();
	};

	/**
	 * Select this element
	 * 
	 * @chainable
     * 
     * @returns {zs.flowLink}
	 */
	zs.flowLink.prototype.select = function(){
		if(this.config.arrowStart){
			this.arrowStart.node.setAttribute('selected', '');
		}

		if(this.config.arrowEnd){
			this.arrowEnd.node.setAttribute('selected', '');
		}

		this.shape.node.setAttribute('selected', '');
		this.isSelected = true;

		this.shape.node.dispatchEvent(new CustomEvent("select", { detail: {
			link: this
		}}));

		return this;
	};

	/**
	 * Deselect this element
	 * 
	 * @chainable
     * 
     * @returns {zs.flowLink}
	 */
	zs.flowLink.prototype.deselect = function(){		
		if(this.config.arrowStart){
			this.arrowStart.node.removeAttribute('selected');
		}

		if(this.config.arrowEnd){
			this.arrowEnd.node.removeAttribute('selected');
		}
		
		this.shape.node.removeAttribute('selected');
		
		this.isSelected = false;

		return this;
	};

	 /**
     * Add event listeners to shape
     * 
     * @chainable
     * 
     * @returns {zs.flowLink}
     */
    zs.flowLink.prototype.addShapeListeners = function(){
        this.shape
            .click(this.onClick.bind(this))
            .mouseover(this.onMouseover.bind(this))
            .mouseout(this.onMouseout.bind(this))
            .node.setAttribute('link', '');

        return this;
    };

    /**
     * Add event listeners to end arrow
     * 
     * @chainable
     * 
     * @returns {zs.flowLink}
     */
    zs.flowLink.prototype.addArrowEndListeners = function(){
        this.arrowEnd
            .click(this.onClick.bind(this))
            .mouseover(this.onMouseover.bind(this))
            .mouseout(this.onMouseout.bind(this));

        return this;
    };

    /**
     * Add event listeners to start arrow
     * 
     * @chainable
     * 
     * @returns {zs.flowLink}
     */
    zs.flowLink.prototype.addArrowStartListeners = function(){
        this.arrowStart
            .click(this.onClick.bind(this))
            .mouseover(this.onMouseover.bind(this))
            .mouseout(this.onMouseout.bind(this));

        return this;
    };

	/**
	 * Render all the staff
	 * 
	 * @chainable
     * 
     * @returns {zs.flowLink}
	 */
	zs.flowLink.prototype.render = function(){
		var dots = this.flow.findClosestDots(this.config.nodeFrom, this.config.nodeTo);
		if(this.config.arrowStart){
			this.arrowStart = this.renderStartArrow(dots);
			this.addArrowStartListeners();
		}

		if(this.config.arrowEnd){
			this.arrowEnd = this.renderEndArrow(dots);
			this.addArrowEndListeners();
		}
        
        this.shape = this.renderShape(dots);   

        this.addShapeListeners();
        
        return this;
	};

	/**
	 * Remove this link
	 * 
	 * @chainable
     * 
     * @returns {zs.flowLink}
	 */
	zs.flowLink.prototype.destroy = function(){
		if(this.shape){
			this.shape.node.dispatchEvent(new CustomEvent("remove", { detail: {
				link: this
			}}));

			this.shape.remove();
		}

		if(this.arrowEnd){
			this.arrowEnd.node.dispatchEvent(new CustomEvent("remove", { detail: {
				link: this
			}}));

			this.arrowEnd.remove();
		}

		if(this.arrowStart){
			this.arrowStart.node.dispatchEvent(new CustomEvent("remove", { detail: {
				link: this
			}}));

			this.arrowStart.remove();
		}

		this.config.nodeFrom.shape.node.removeEventListener('dragmove', this._nodeDraggedHandler);
		this.config.nodeTo.shape.node.removeEventListener('dragmove', this._nodeDraggedHandler);

		this.config.nodeFrom.shape.node.removeEventListener('dragend', this._nodeDraggedHandler);
		this.config.nodeTo.shape.node.removeEventListener('dragend', this._nodeDraggedHandler);

		this.config.nodeFrom.shape.node.removeEventListener('remove', this._nodeDestroyHandler);
		this.config.nodeTo.shape.node.removeEventListener('remove', this._nodeDestroyHandler);

		return this;
	};

})(window.zs || {});