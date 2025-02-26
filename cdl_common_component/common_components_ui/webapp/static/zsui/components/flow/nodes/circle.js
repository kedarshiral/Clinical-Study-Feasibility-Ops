(function (zs) {
	'use strict';

    /**
     * ZS Flow Circle Node
	 * 
     * @class zs.flowCircleNode
	 * 
	 * @extends zs.flowNode
     *
	 * @method renderShape
	 * @method renderDots
	 * @method renderText
	 * @method onDragStartShape
	 * @method onDragMoveShape
     *
     */
	zs.flowCircleNode = function(){};

	/**
	 * @extends flowNode
	 */
	zs.flowCircleNode.prototype = Object.create(zs.flowNode.prototype);

	/**
	 * Prevent constructor hidden rewriting
	 */
	zs.flowCircleNode.prototype.constructor = zs.flowCircleNode;

	/**
	 * Render shape
	 * 
	 * @return {Raphael.Element}
	 */
	zs.flowCircleNode.prototype.renderShape = function(){
		var shape = this.flow.paper.circle(this.config.x, this.config.y, this.config.shapeW);

		shape.node.setAttribute('shape', '');

		return shape;
	};

	/**
	 * Render text
	 * 
	 * @return {Raphael.Element}
	 */
	zs.flowCircleNode.prototype.renderText = function(){
		return this.flow.paper.text(this.config.x, this.config.y, this.config.text);
	};

	zs.flowCircleNode.prototype.getCenter = function(){
		return {
			x: this.shape.attr('cx'),
			y: this.shape.attr('cy')
		};
	};

	/**
	 * If tprovided point is inside this node
	 * 
	 * @param {Number} x
	 * @param {Number} y
	 * 
	 * @return {Boolean}
	 */
	zs.flowCircleNode.prototype.hasPoint = function(x, y){
		var cx = this.shape.attr('cx'),
			cy = this.shape.attr('cy'),
			r  = this.shape.attr('r');

		var d = Math.sqrt(Math.pow(cx - x, 2) + Math.pow(cy - y, 2));
		if(d > r){
			return false;
		}
		
		return true;
	};

	/**
	 * Render dots
	 * 
	 * @return {Raphael.Element[]}
	 */
	zs.flowCircleNode.prototype.renderDots = function(){
		var left      = this.flow.paper.circle(this.config.x - this.config.shapeW, this.config.y, this.config.dotR),
			right 	  = this.flow.paper.circle(this.config.x + this.config.shapeW, this.config.y, this.config.dotR),
			top       = this.flow.paper.circle(this.config.x, this.config.y - this.config.shapeW, this.config.dotR),
			bottom    = this.flow.paper.circle(this.config.x, this.config.y + this.config.shapeW, this.config.dotR);
		
		top.nodeKey = left.nodeKey = right.nodeKey = bottom.nodeKey = this.config.key;

		this.top    = top;
		this.left   = left;
		this.right  = right;
		this.bottom = bottom;

		top.pos = 'top';
		left.pos = 'left';
		right.pos = 'right';
		bottom.pos = 'bottom';
		
		top.toFront();
		left.toFront();
		right.toFront();
		bottom.toFront();
		
		top.node.setAttribute('dot', '');
		left.node.setAttribute('dot', '');
		right.node.setAttribute('dot', '');
		bottom.node.setAttribute('dot', '');

		return [top, left, right, bottom];
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
	 * @returns {zs.flowCircleNode}
	 */
	zs.flowCircleNode.prototype.onDragStartShape = function(x, y, e){
		this.shape.odx = this.shape.attr("cx");
		this.shape.ody = this.shape.attr("cy");
		this.shape.toFront();

		this.shape.node.setAttribute('dragged', '');

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
	 * @returns {zs.flowCircleNode}
	 */
	zs.flowCircleNode.prototype.onDragMoveShape = function(dx, dy, x, y, e){
		this.shape.attr({
			cx: dx + this.shape.odx,
			cy: dy + this.shape.ody
		});

		this.shape.node.dispatchEvent(new CustomEvent("dragmove"));

		return this;
	};

})(window.zs || {});