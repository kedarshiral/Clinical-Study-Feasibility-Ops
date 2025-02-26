(function (zs) {
	'use strict';

    /**
     * ZS Flow Rectangle Node
	 * 
     * @class zs.flowRectNode
	 * 
	 * @extends zs.flowNode
     *
	 * @method renderShape
	 * @method renderDots
	 * @method renderText
     *
     */
	zs.flowRectNode = function(){};

	/**
	 * @extends flowNode
	 */
	zs.flowRectNode.prototype = Object.create(zs.flowNode.prototype);

	/**
	 * Prevent constructor hidden rewriting
	 */
	zs.flowRectNode.prototype.constructor = zs.flowRectNode;

	/**
	 * Render shape
	 * 
	 * @return {Raphael.Element}
	 */
	zs.flowRectNode.prototype.renderShape = function(){
		var shape = this.flow.paper.rect(this.config.x, this.config.y, this.config.shapeW, this.config.shapeH);

		shape.node.setAttribute('shape', '');
		
		return shape;
	};

	/**
	 * Render text
	 * 
	 * @return {Raphael.Element}
	 */
	zs.flowRectNode.prototype.renderText = function(){
		return this.flow.paper.text(this.config.x + this.config.shapeW / 2, this.config.y + this.config.shapeH / 2, this.config.text);
	};

	zs.flowRectNode.prototype.getCenter = function(){
		return {
			x: this.shape.attr('x') + this.shape.attr('width') / 2,
			y: this.shape.attr('y') + this.shape.attr('height') / 2
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
	zs.flowRectNode.prototype.hasPoint = function(x, y){
		var minX = this.shape.attr('x'),
			maxX = this.shape.attr('x') + this.shape.attr('width'),
			minY = this.shape.attr('y'),
			maxY = this.shape.attr('y') + this.shape.attr('height');
		
		if(x > maxX){
			return false;
		}

		if(x < minX){
			return false;
		}

		if(y > maxY){
			return false;
		}

		if(y < minY){
			return false;
		}

		return true;
	};

	/**
	 * Render dots
	 * 
	 * @return {Raphael.Element[]}
	 */
	zs.flowRectNode.prototype.renderDots = function(){
		var left      = this.flow.paper.circle(this.config.x, this.config.y + this.config.shapeH / 2, this.config.dotR),
			right 	  = this.flow.paper.circle(this.config.x + this.config.shapeW, this.config.y + this.config.shapeH / 2, this.config.dotR),
			top       = this.flow.paper.circle(this.config.x + this.config.shapeW / 2, this.config.y, this.config.dotR),
			bottom    = this.flow.paper.circle(this.config.x + this.config.shapeW / 2, this.config.y + this.config.shapeH, this.config.dotR);
		
		top.nodeKey = left.nodeKey = right.nodeKey = bottom.nodeKey = this.config.key;
		
		top.pos = 'top';
		left.pos = 'left';
		right.pos = 'right';
		bottom.pos = 'bottom';

		this.top    = top;
		this.left   = left;
		this.right  = right;
		this.bottom = bottom;

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
	
})(window.zs || {});