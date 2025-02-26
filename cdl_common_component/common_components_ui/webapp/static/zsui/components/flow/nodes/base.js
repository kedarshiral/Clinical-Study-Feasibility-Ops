(function (zs) {
	'use strict';

	/**
     * ZS Flow Node
	 * 
     * @class zs.flowNode
     *
     * @property {Raphael.Paper} paper
     * @property {Boolean} isSelected
     * @property {Raphael.Element} shape
     * @property {Raphael.Element} text
     * @property {Raphael.Set} glow
     * @property {Raphael.Element[]} dots
     * @property {Object} config
     *
     * @method configure
	 * @method render
	 * @method renderShape
	 * @method renderGlow
	 * @method renderDots
	 * @method renderText
	 * @method onShapeClick
	 * @method select
	 * @method deselect
	 * @method onDragStart
	 * @method onDragStartShape
	 * @method onDragStartGlow
	 * @method onDragStartDots
	 * @method onDragStartText
	 * @method onDragMove
	 * @method onDragMoveShape
	 * @method onDragMoveGlow
	 * @method onDragMoveDots
	 * @method onDragMoveText
	 * @method onDragEnd
	 * @method onDragEndShape
	 * @method onDragEndGlow
	 * @method onDragEndDots
	 * @method onDragEndText
	 * @method destroy
     *
     */
	zs.flowNode = function(){};

	/**
	 * Reference to flow
	 * 
	 * @type {HTMLElement}
	 */
	zs.flowNode.prototype.flow = null;

	/**
	 * If this link was selected
	 * 
	 * @type {Boolean}
	 */
	zs.flowNode.prototype.isSelected = false; 

	/**
	 * Shape
	 * 
	 * @type {Raphael.Element}
	 */
	zs.flowNode.prototype.shape = null; 

	/**
	 * Text 
	 * 
	 * @type {Raphael.Element}
	 */
	zs.flowNode.prototype.text = null; 

	/**
	 * Configuration
	 * 
	 * @type {Object}
	 */
	zs.flowNode.prototype.config = null; 

	zs.flowNode.prototype.defaults = {
		x: 0,
		y: 0,
		text: '',
		key: '',
		dotR: 2,
		draggable: false,
		subflow: null
	}; 

	/**
	 * Configure node
	 * 
	 * @chainable
	 * 
	 * @param {Object} params
	 * 
	 * @return {zs.flowNode}
	 */
	zs.flowNode.prototype.configure = function(params){
		if(!params.flow || !params.flow.paper){
			throw Error('Reference to Flow MUST be provided');
		}
		this.config = {};
		
		this.flow 				= params.flow;
		this.config.shapeW 		= params.shapeW;
		this.config.shapeH 		= params.shapeH;
		this.config.type 		= params.type;
		this.config.x 			= params.x || this.defaults.x;
		this.config.y 			= params.y || this.defaults.y;
		this.config.text 	    = params.text || this.defaults.text;
		this.config.key 	    = params.key || this.defaults.key;
		this.config.dotR 	    = params.dotR || this.defaults.dotR;
		this.config.subflow   	= params.subflow || this.defaults.subflow;

		return this;
	};

	/**
	 * Render node
	 * 
	 * @chainable
	 * 
	 * @return {zs.flowNode}
	 */
	zs.flowNode.prototype.render = function(){
		this.shape = this.renderShape();
		this.glow = this.renderGlow();
		this.text = this.renderText();
		this.dots = this.renderDots();		

		this.shape.click(this.onShapeClick.bind(this));

		this.shape.drag(this.onDragMove.bind(this), this.onDragStart.bind(this), this.onDragEnd.bind(this));

		this._checkDraggableHandler = this.checkDraggable.bind(this);

		this.flow.addEventListener('changedragable', this._checkDraggableHandler);

		return this.checkDraggable();
	};


	zs.flowNode.prototype.checkDraggable = function(){
		this.shape.node.removeAttribute('draggable');
		if(this.flow.config.draggable){
			this.shape.node.setAttribute('draggable', '');
		}

		return this;
	};

	zs.flowNode.prototype.getCenter = function(){
		return {};
	};

	zs.flowNode.prototype.hasPoint = function(x, y){
		return false;
	};

	zs.flowNode.prototype.getClosestDot = function(x, y){
		var dot, d = Infinity, nD = 0;
		for(var i in this.dots){
			nD = Math.sqrt(Math.pow(this.dots[i].attr('cx') - x, 2) + Math.pow(this.dots[i].attr('cy') - y, 2));               
			
			if(nD < d){
				dot = this.dots[i];
				d = nD;
			}
		}

		return dot;
	};

	/**
	 * Export this node
	 */
	zs.flowNode.prototype.export = function(){
		return this.config;

	};

	/**
	 * Render shape
	 * 
	 * @return {Raphael.Element}
	 */
	zs.flowNode.prototype.renderShape = function(){
		return this.shape;
	};

	/**
	 * Render shape
	 * 
	 * @return {Raphael.Element}
	 */
	zs.flowNode.prototype.renderGlow = function(){
		var glow = this.shape.glow({
			offsety: 1,
		});

		glow.items.forEach(function(glow){
			glow.node.setAttribute('glow', '');
		});

		return glow;
	};
	
	/**
	 * Render text
	 * 
	 * @return {Raphael.Element}
	 */
	zs.flowNode.prototype.renderText = function(){
		return this.text;
	};

	/**
	 * Render dots
	 * 
	 * @return {Raphael.Element[]}
	 */
	zs.flowNode.prototype.renderDots = function(){
		return [];
	};

	/**
	 * When shape beeb clicked
	 * 
	 * @chainable
	 * 
	 * @param {Event} e
	 * 
	 * @return {zs.flowNode}
	 */
	zs.flowNode.prototype.onShapeClick = function(e){
		if(this.isSelected){
			return this.deselect(e);
		}

		return this.select(e);	
	};

	/**
	 * Select this element
	 * 
	 * @chainable
     * 
     * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.select = function(e){
		this.shape.node.setAttribute('selected', '');
		this.isSelected = true;
		
		// var r = this.config.dotR * 2.5;
		// if(navigator.userAgent.match(/iPad/i)){
		// 	r = this.config.dotR * 1;
		// }

		// this.dots.forEach(function(dot){
		// 	dot.attr({ r: r });
		// });

		this.shape.node.dispatchEvent(new CustomEvent("select", { detail: {
			node: this
		}}));

		return this;
	};

	/**
	 * Select this element
	 * 
	 * @chainable
     * 
     * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.deselect = function(e){
		this.shape.node.removeAttribute('selected');
		this.isSelected = false;

		// var r = this.config.dotR ;
		// this.dots.forEach(function(dot){
		// 	dot.attr({ r: r });
		// });

		this.shape.node.dispatchEvent(new CustomEvent("deselect", { detail: {
			node: this
		}}));

		return this;
	};

	/**
	 * When user starts to drag node
	 *
	 * @chainable
	 *
	 * @param {Number} x - X posiiton of the mouse
	 * @param {Number} y - Y posiiton of the mouse
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragStart = function(x, y, e) {
		if(!this.flow.config.draggable){
			return this;
		}
		this.onDragStartShape(x, y, e);
		this.onDragStartGlow(x, y, e);
		this.onDragStartDots(x, y, e);
		this.onDragStartText(x, y, e);

		return this;
	};

	/**
	 * When user starts to drag node - what we should do w/ the shape
	 *
	 * @chainable
	 *
	 * @param {Number} x - X posiiton of the mouse
	 * @param {Number} y - Y posiiton of the mouse
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragStartShape = function(x, y, e){
		this.shape.odx = this.shape.attr("x");
		this.shape.ody = this.shape.attr("y");
		this.shape.toFront();

		this.shape.node.setAttribute('dragged', '');
		this.shape.node.dispatchEvent(new CustomEvent("dragstart"));

		return this;
	};

	/**
	 * When user starts to drag node - what we should do w/ the glow
	 *
	 * @chainable
	 *
	 * @param {Number} x - X posiiton of the mouse
	 * @param {Number} y - Y posiiton of the mouse
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragStartGlow = function(x, y, e){
		this.glow.remove();

		return this;
	};
	
	/**
	 * When user starts to drag node - what we should do w/ the dots
	 *
	 * @chainable
	 *
	 * @param {Number} x - X posiiton of the mouse
	 * @param {Number} y - Y posiiton of the mouse
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragStartDots = function(x, y, e){
		this.dots.forEach(function(dot){
			dot.odx = dot.attr("cx");
			dot.ody = dot.attr("cy");
			dot.toFront();
			dot.node.dispatchEvent(new CustomEvent("dragstart"));
		});

		return this;
	};

	/**
	 * When user starts to drag node - what we should do w/ the text
	 *
	 * @chainable
	 *
	 * @param {Number} x - X posiiton of the mouse
	 * @param {Number} y - Y posiiton of the mouse
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragStartText = function(x, y, e){
		this.text.odx = this.text.attr("x");
		this.text.ody = this.text.attr("y");
		this.text.toFront();

		this.text.node.dispatchEvent(new CustomEvent("dragstart"));

		return this;
	};

	/**
	 * When user drags node
	 *
	 * @chainable
	 *
	 * @param {Number} dx - X diff
	 * @param {Number} dy - Y diff
	 * @param {Number} x - X posiiton of the mouse
	 * @param {Number} y - Y posiiton of the mouse
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragMove = function(dx, dy, x, y, e) {
		if(!this.flow.config.draggable){
			return this;
		}

		this.onDragMoveShape(dx, dy, x, y, e);
		// this.onDragMoveGlow(dx, dy, x, y, e);
		this.onDragMoveDots(dx, dy, x, y, e);
		this.onDragMoveText(dx, dy, x, y, e);

		return this;
	};

	/**
	 * When user drags node - what we should do w/ the shape
	 *
	 * @chainable
	 *
	 * @param {Number} dx - X diff
	 * @param {Number} dy - Y diff
	 * @param {Number} x - X posiiton of the mouse
	 * @param {Number} y - Y posiiton of the mouse
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragMoveShape = function(dx, dy, x, y, e){
		this.shape.attr({
			x: dx + this.shape.odx,
			y: dy + this.shape.ody
		});

		this.shape.node.dispatchEvent(new CustomEvent("dragmove"));

		return this;
	};

	/**
	 * When user drags node - what we should do w/ the glow
	 *
	 * @chainable
	 *
	 * @param {Number} dx - X diff
	 * @param {Number} dy - Y diff
	 * @param {Number} x - X posiiton of the mouse
	 * @param {Number} y - Y posiiton of the mouse
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragMoveGlow = function(dx, dy, x, y, e){
		this.glow.items.forEach(function(glow){
			glow.node.dispatchEvent(new CustomEvent("dragmove"));
		});

		return this;
	};

	/**
	 * When user drags node - what we should do w/ the dots
	 *
	 * @chainable
	 *
	 * @param {Number} dx - X diff
	 * @param {Number} dy - Y diff
	 * @param {Number} x - X posiiton of the mouse
	 * @param {Number} y - Y posiiton of the mouse
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragMoveDots = function(dx, dy, x, y, e){
		this.dots.forEach(function(dot){
			dot.attr({
				cx: dx + dot.odx,
				cy: dy + dot.ody
			});
			dot.node.dispatchEvent(new CustomEvent("dragmove"));
		});

		return this;
	};

	/**
	 * When user drags node - what we should do w/ the text
	 *
	 * @chainable
	 *
	 * @param {Number} dx - X diff
	 * @param {Number} dy - Y diff
	 * @param {Number} x - X posiiton of the mouse
	 * @param {Number} y - Y posiiton of the mouse
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragMoveText = function(dx, dy, x, y, e){
		this.text.attr({
			x: dx + this.text.odx,
			y: dy + this.text.ody
		});

		this.text.node.dispatchEvent(new CustomEvent("dragmove"));

		return this;
	};

	/**
	 * When user ends to drag node
	 *
	 * @chainable
	 *
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragEnd = function(e) {
		if(!this.flow.config.draggable){
			return this;
		}

		this.onDragEndShape(e);
		this.onDragEndGlow(e);
		this.onDragEndDots(e);
		this.onDragEndText(e);

		// firefox the only one who proxied click event properly
		if(!~navigator.userAgent.toLowerCase().indexOf('firefox')){
			this.select(e);
		}
		

		return this;
	};
	

	/**
	 * When user ends to drag node - what we should do w/ the shape
	 *
	 * @chainable
	 *
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragEndShape = function(e){
		this.shape.toFront();

		this.shape.node.removeAttribute('dragged');
		this.shape.node.dispatchEvent(new CustomEvent("dragend"));

		return this;
	};

	/**
	 * When user ends to drag node - what we should do w/ the glow
	 *
	 * @chainable
	 *
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragEndGlow = function(e){
		this.glow.remove();
		this.glow = this.renderGlow();
		this.glow.items.forEach(function(glow){
			glow.node.dispatchEvent(new CustomEvent("dragend"));
		});

		return this;
	};	

	/**
	 * When user ends to drag node - what we should do w/ the dots
	 *
	 * @chainable
	 *
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragEndDots = function(e){
		this.dots.forEach(function(dot){
			dot.toFront();

			dot.node.dispatchEvent(new CustomEvent("dragend"));
		});

		return this;
	};

	/**
	 * When user ends to drag node - what we should do w/ the text
	 *
	 * @chainable
	 *
	 * @param {Event} e  - current event
	 *
	 * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.onDragEndText = function(e){
		this.text.toFront();

		this.text.node.dispatchEvent(new CustomEvent("dragend"));

		return this;
	};	

	/**
	 * Remove this node
	 * 
	 * @chainable
     * 
     * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.destroy = function(){
		if(this.shape){
			this.shape.node.dispatchEvent(new CustomEvent("remove", { detail: {
				node: this
			}}));

			this.shape.remove();
			this.shape = null;
			this.flow.removeEventListener('changedragable', this._checkDraggableHandler);
		}

		if(this.text){
			this.text.node.dispatchEvent(new CustomEvent("remove", { detail: {
				node: this
			}}));
			
			this.text.remove();
			this.text = null;
		}

		if(this.glow){
			this.glow.items.forEach(function(glow){
				glow.node.dispatchEvent(new CustomEvent("remove", { detail: {
					node: this
				}}));

				glow.remove();
			});

			this.glow = null;
		}

		if(this.dots){
			this.dots.forEach(function(dot){
				dot.node.dispatchEvent(new CustomEvent("remove", { detail: {
					node: this
				}}));
				dot.remove();
			});

			this.dots = null;
		}

		return this;
	};

	/**
	 * Gets and returns current text
	 * 
	 * @chainable
     * 
     * @returns {String}
	 */
	zs.flowNode.prototype.getText = function(){
		return this.config.text;
	};

	/**
	 * Update text
	 * 
	 * @chainable
     * 
	 * @param {String} v
	 * 
     * @returns {zs.flowNode}
	 */
	zs.flowNode.prototype.setText = function(v){
		this.config.text = v;
		this.text.attr({ text: v });

		this.text.node.dispatchEvent(new CustomEvent("change"));
	};

})(window.zs || {});