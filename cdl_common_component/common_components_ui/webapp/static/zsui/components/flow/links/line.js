(function (zs) {
	'use strict';

    /**
     * ZS Flow Line link
	 * 
     * @class zs.flowLineLink
     * 
     * @extends zs.flowLink
     *
     * @method renderShape
     * @method configureArrow
     * @method renderStartArrow
     * @method renderEndArrow
     *
     */
	zs.flowLinkLine = function(){};

    /**
	 * @extends flowNode
	 */
	zs.flowLinkLine.prototype = Object.create(zs.flowLink.prototype);

	/**
	 * Prevent constructor hidden rewriting
	 */
	zs.flowLinkLine.prototype.constructor = zs.flowLinkLine;

    /**
     * Render shape
     * 
     * @returns {Raphael.Element}
     */
    zs.flowLinkLine.prototype.configureArrow = function(dots){
        var x1 = dots.dot1.attr("cx");
        var y1 = dots.dot1.attr("cy");
        var x2 = dots.dot2.attr("cx");
        var y2 = dots.dot2.attr("cy");
        var size = this.config.arrowSize;
        
        var angle = Raphael.angle(x1, y1, x2, y2);
		var a45   = Raphael.rad(angle-30);
		var a45m  = Raphael.rad(angle+30);
		var a135  = Raphael.rad(angle-150);
		var a135m = Raphael.rad(angle+150);

		var x1a = x1 + Math.cos(a135) * size;
		var y1a = y1 + Math.sin(a135) * size;
		var x1b = x1 + Math.cos(a135m) * size;
		var y1b = y1 + Math.sin(a135m) * size;
		
		var x2a = x2 + Math.cos(a45) * size;
		var y2a = y2 + Math.sin(a45) * size;
		var x2b = x2 + Math.cos(a45m) * size;
		var y2b = y2 + Math.sin(a45m) * size;


        return {
            x1: x1,
            x2: x2,
            x1a: x1a,
            x2a: x2a,
            x1b: x1b,
            x2b: x2b,
            y1: y1,
            y2: y2,
            y1a: y1a,
            y1b: y1b,
            y2a: y2a,
            y2b: y2b
        };
    };

    zs.flowLinkLine.prototype.configureShapePath = function(dots){
        return "M" + dots.dot1.attr("cx") + " " + dots.dot1.attr("cy") + "L" + dots.dot2.attr("cx") + " " + dots.dot2.attr("cy");
    };

    zs.flowLinkLine.prototype.configureStartArrowPath = function(dots){
        var params = this.configureArrow(dots);
        return "M" + params.x1 + " " + params.y1 + "L" + params.x1a + " " + params.y1a +
				"M" + params.x1 + " " + params.y1 + "L" + params.x1b + " " + params.y1b;
        
    };

    zs.flowLinkLine.prototype.configureEndArrowPath = function(dots){
        var params = this.configureArrow(dots);
        return "M" + params.x2 + " " + params.y2 + "L" + params.x2a + " " + params.y2a + 
			"M" + params.x2 + " " + params.y2 + "L" + params.x2b + " " + params.y2b;
    };


})(window.zs || {});