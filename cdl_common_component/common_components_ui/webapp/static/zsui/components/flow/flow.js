(function (zs) {
	"use strict";

    /**
     * Creates FlowChart w/ Raphael's help
     *
     * @extends zs.customElement
     *
     * @property {Raphael.Paper} paper
     * @property {Object[]} nodes
     * @property {Object[]} links
     * @property {Object} config
     *
     * @method renderContainer
     * @method renderNodes
     * @method renderLinks
     * @method nodeSelected
     * @method linkSelected
     * @method nodeDestroyed
     * @method linkDestroyed
     * @method render
     *
     * @event render
     * @event rendernode
     * @event renderlink
     */
	zs.flow = {

        /**
         * Ref to Raphael paper
         *
         * @property {Raphael.Paper}
         */
        paper: null,

        /**
         * Links data
         *
         * @property {Object[]}
         */
        links: null,

        /**
         * Node elements
         *
         * @property {Object}
         */
        nodes: null,

        /**
         * Config
         *
         * @property {Object}
         */
        config: null,

        /**
         * Default params
         * 
         * @type {Object}
         */
        defaults: {
            shapeW: 120,
            shapeH: 60,
            containerWidth: 600,
            containerHeight: 400,
            draggable: false,
            newLinkType: 'line',
            newLinkArrowStart: false,
            newLinkArrowEnd: true,
        },

        /**
         * Configure
         *
         * @chainable
         *
         * @returns {HTMLElement}
         */
        configure: function (params) {
            this.config = {};
            
            this.nodes = {};
            this.links = {};

			this.config.nodes = params.nodes;
			this.config.links = params.links;
           
            this.config.containerWidth  = this.defaults.containerWidth;
            this.config.containerHeight = this.defaults.containerHeight;

            this.config.draggable = params.draggable || this.defaults.draggable;

            if(+params.containerWidth > 0){
                this.config.containerWidth = params.containerWidth;
            }
            
            if(+params.containerHeight > 0){
                this.config.containerHeight = params.containerHeight;
            }

            this.dispatchEvent(new CustomEvent('configure', {
                detail: { 
                    data: params
                }
            }));

            return this;
		},

        /**
         * Render paper
         *
         * @chainable
         *
         * @returns {HTMLElement}
         */
        renderContainer: function(){
            if(!this.paper){
                this.paper = Raphael(this, this.config.containerWidth, this.config.containerHeight);
            }

            this.paper.clear();

            return this;
        },

        /**
         * Render all nodes from data
         *
         * @chainable
         *
         * @returns {HTMLElement}
         */
        renderNodes: function(){
            for(var i in this.config.nodes){
                this.dispatchEvent(new CustomEvent("rendernode", {
                    detail: { 
                        data: this.config.nodes[i]
                    }
                }));
            }

            return this;
        },

        /**
         * Render all links from data
         *
         * @chainable
         *
         * @returns {HTMLElement}
         */
        renderLinks: function(){
            for(var i in this.config.links){
                this.dispatchEvent(new CustomEvent("renderlink", {
                    detail: { 
                        data: this.config.links[i]
                    }
                }));
            }

            return this;
        },

        /**
         * When node been selected
         * 
         * @chainable
         *
         * @returns {HTMLElement}
         */
        nodeSelected: function(e){
            for(var i in this.nodes){
                if(this.nodes[i] !== e.detail.node){
                    this.nodes[i].deselect();
                }
            }

            for(var i in this.links){
                this.links[i].deselect();
            }

            return this;
        },

        /**
         * When link been selected
         * 
         * @chainable
         *
         * @returns {HTMLElement}
         */
        linkSelected: function(e){
            for(var i in this.links){
                if(this.links[i] !== e.detail.link){
                    this.links[i].deselect();
                }
            }

            for(var i in this.nodes){
                this.nodes[i].deselect();
            }

            return this;
        },

        /**
         * When node been destroyed
         *
         * @chainable
         *
         * @returns {HTMLElement}
         */
        nodeDestroyed: function(e){
            delete this.nodes[e.detail.node.config.key];

            return this;
        },

        /**
         * When link been destroyed
         *
         * @chainable
         *
         * @returns {HTMLElement}
         */
        linkDestroyed: function(e){
            delete this.links[e.detail.link.config.key];

            return this;
        },

        /**
         * Render component
         *
         * @chainable
         *
         * @returns {HTMLElement}
         */
        render: function () {
			this
                .renderContainer()
                .renderNodes()
                .renderLinks();

            this.dispatchEvent(new CustomEvent("render"));

            return this;
		},

        findClosestDots: function(node1, node2){
            var d = Infinity, dot1, dot2, nD = 0;
            for(var i in node1.dots){
                for(var k in node2.dots){
                    nD = Math.sqrt(Math.pow(node1.dots[i].attr('cx') - node2.dots[k].attr('cx'), 2) + Math.pow(node1.dots[i].attr('cy') - node2.dots[k].attr('cy'), 2));               
                    
                    if(nD < d){
                        dot1 = node1.dots[i];
                        dot2 = node2.dots[k];
                        d = nD;
                    }
                }
            }

            return {
                dot1: dot1,
                dot2: dot2,
                d: d
            };
        }
    };

    /**
     * Adds ability to draw new links
     *
     * @property {Element} hovered
     *
     * @method {onMouseover}
     * @method {onMouseout}
     * @method {onDragStart}
     * @method {onDragMove}
     * @method {onDragEnd}
     *
     * @event render
     */
    zs.flowDrawableLinks = {

        /**
         * Current line
         *
         * @type {Element}
         */
        newLine: null,

        /**
         * Events list
         */
        events: {

            /**
             * When configured
             */
            configure: function(e){
                var params = e.detail.params || {};

                this.config.newLinkType = params.newLinkType || this.defaults.newLinkType;
                this.config.newLinkArrowStart   = params.newLinkArrowStart  || this.defaults.newLinkArrowStart;
                this.config.newLinkArrowEnd     = params.newLinkArrowEnd    || this.defaults.newLinkArrowEnd;
            },

            /**
             * When rendered
             */
            afterrendernode: function(e){
                var self        = this,
                    startNode   = e.detail.node, 
                    startDot    = null,
                    endNode     = null,
                    endDot      = null;
                
                startNode.shape.drag(
                    function(dx, dy, x, y, e){
                        
                        if(self.config.draggable){ // draw lines only if draggable is off
                            return;
                        }

                        if(this.newLine){
                            this.newLine.remove();
                        }
                        var rect = self.querySelector('svg').getBoundingClientRect();
                        var endX = (e.clientX || e.touches[0].clientX) - rect.left - 2;
                        var endY = (e.clientY || e.touches[0].clientY) - rect.top - 2;
                        
                        startDot = startNode.getClosestDot(endX, endY);
                        
                        this.newLine = this.paper.path("M" + startDot.attr('cx') + " " + startDot.attr('cy') + "L" + endX + " " + endY);

                        for(var i in self.nodes){
                            self.nodes[i].deselect();
                        }
                        
                        for(var i in self.nodes){
                            if(self.nodes[i] === startNode){
                                continue;
                            }

                            if(self.nodes[i].hasPoint(endX, endY)){
                                endNode = self.nodes[i];
                                endNode.select();
                                break;
                            }
                        }
                    }, 
                    function(x, y, e){}, 
                    function(e){
                        if(self.config.draggable){ // draw lines only if draggable is off
                            return;
                        }

                        if(this.newLine){
                            this.newLine.remove();
                        }

                        if(!endNode){
                            return this;
                        }

                        if(!startNode || !startDot){
                            return this;
                        }

                        if(startNode === endNode){
                            return this;
                        }
                        
                        var endDot = endNode.getClosestDot(startDot.attr('cx'), startDot.attr('cy'));
                        if(!endDot){
                            return this;
                        }

                        self.dispatchEvent(new CustomEvent("renderlink", {
                            detail: { 
                                data: {
                                    nodeFrom: startNode.config.key,
                                    nodeTo: endNode.config.key,
                                    dotFrom: startDot.pos,
                                    dotTo: endDot.pos,
                                    key: 'line' + Date.now(),
                                    type: self.config.newLinkType,
                                    arrowStart: self.config.newLinkArrowStart,
                                    arrowEnd: self.config.newLinkArrowEnd
                                }
                            }
                        }));
                    });   
            }
		}

    };

    zs.flowAddableLinks = {
        
        startNode: null,

        /**
         * Events list
         */
        events: {

            /**
             * When configured
             */
            configure: function(e){
                var params = e.detail.params || {};

                this.config.newLinkType         = params.newLinkType        || this.defaults.newLinkType;
                this.config.newLinkArrowStart   = params.newLinkArrowStart  || this.defaults.newLinkArrowStart;
                this.config.newLinkArrowEnd     = params.newLinkArrowEnd    || this.defaults.newLinkArrowEnd;

                this.config.linkDrawingMode = params.linkDrawingMode || false;
            },

            /**
             * When rendered
             */
            afterrendernode: function(e){
                var node = e.detail.node;
                
                node.shape.node.addEventListener('select', this.startOrEndLine.bind(this, node));
            }
		},

        startOrEndLine: function(node){
            if(this.startNode && this.config.linkDrawingMode){
               return this.endLine(node);
            }

            return this.startLine(node);
        },

        startLine: function(node){
            this.startNode = node;

            return this;
        },

        endLine: function(node){
            var startNode = this.startNode;

            this.startNode = node;
            if(startNode.config.key === node.config.key){
                return this;
            }

            var dots = this.findClosestDots(startNode, node);
            this.dispatchEvent(new CustomEvent("renderlink", {
                detail: { 
                    data: {
                        nodeFrom: startNode.config.key,
                        nodeTo: node.config.key,
                        dotFrom: dots.dot1.pos,
                        dotTo: dots.dot2.pos,
                        key: 'line' + Date.now(),
                        type: this.config.newLinkType,
                        arrowStart:this.config.newLinkArrowStart,
                        arrowEnd:this.config.newLinkArrowEnd
                    }
                }
            }));
            
            return this;
        }
    };

    /**
     * Adds ability to render nodes as rects
     *
     * @property {Element} hovered
     *
     * @event rendernode
     */
    zs.flowRectNodes = {

        /**
         * Events list
         */
        events:{

            /**
             * When node been rendered
             */
            rendernode: function(e){
                var data = e.detail.data;
                if(data.type !== 'rect'){
                    return null;
                }

                var node = new zs.flowRectNode()
                    .configure({
                        flow: this,
                        shapeW: data.shapeW || this.defaults.shapeW,
                        shapeH: data.shapeH || this.defaults.shapeH,
                        x: data.x,
                        y: data.y,
                        key: data.key,
                        text: data.text,
                        type: data.type,
                        subflow: data.subflow
                    })
                    .render();
               
                
                node.shape.node.addEventListener('select', this.nodeSelected.bind(this));
                node.shape.node.addEventListener('remove', this.nodeDestroyed.bind(this));

                this.nodes[data.key] = node;

                this.dispatchEvent(new CustomEvent("afterrendernode", {
                    detail: { 
                        node: node
                    }
                }));
            }
        }
    };

    /**
     * Adds ability to render nodes as rects
     *
     * @property {Element} hovered
     *
     * @event rendernode
     */
    zs.flowCircleNodes = {

        /**
         * Events list
         */
        events:{
            rendernode: function(e){
                var node, data = e.detail.data;

                if(data.type !== 'circle'){
                    return null;
                }
            
                node = new zs.flowCircleNode()
                    .configure({
                        flow: this,
                        shapeW: data.shapeW || this.defaults.shapeW,
                        x: data.x,
                        y: data.y,
                        key: data.key,
                        text: data.text,
                        type: data.type,
                        subflow: data.subflow
                    })
                    .render();

                node.shape.node.addEventListener('select', this.nodeSelected.bind(this));
                node.shape.node.addEventListener('remove', this.nodeDestroyed.bind(this));

                this.nodes[data.key] = node;

                this.dispatchEvent(new CustomEvent("afterrendernode", {
                    detail: { 
                        node: node
                    }
                }));
            }
        }
    };

    /**
     * Adds ability to render links as simple line
     *
     * @property {Element} hovered
     *
     * @event rendernode
     */
    zs.flowLineLinks = {

        /**
         * Events list
         */
        events:{
            renderlink: function(e){
                var link, data = e.detail.data;

                if(data.type !== 'line'){
                    return null;
                }

                link = new zs.flowLinkLine()
                    .configure({
                        flow: this,
                        nodeFrom: this.nodes[data.nodeFrom],
                        nodeTo: this.nodes[data.nodeTo],
                        dotFrom: data.dotFrom,
                        dotTo: data.dotTo,
                        key: data.key,
                        arrowEnd: data.arrowEnd,
                        arrowStart: data.arrowStart,
                        type: data.type
                    })
                    .render();
                
                link.shape.node.addEventListener('select', this.linkSelected.bind(this));
                link.shape.node.addEventListener('remove', this.linkDestroyed.bind(this));

                this.links[data.key] = link;

                this.dispatchEvent(new CustomEvent("afterrenderlink", {
                    detail: { 
                        link: link
                    }
                }));
            }
        }
    };

    /**
     * Allows to delete nodes on press DEL key
     *
     * @desc You can use any other behaviour - removing by clicking a button etc
     *  
     * @method removeNodeOnDel
     * 
     * @event render
     */
    zs.flowNodeRemovableOnDel = {
        events: {
            render: function(){
                document.addEventListener('keyup', this.removeNodeOnDel.bind(this));
            }
        },

        /**
         * Check if DEl key was pressed and delete node if there is any selected
         * 
         * @param {Event} e
         * 
         * @return {HTMLElement}
         */
        removeNodeOnDel: function(e){
            if(e.keyCode !== 46){
                return this;
            }

            for(var i in this.nodes){
                if(this.nodes[i].isSelected){
                    this.nodes[i].destroy();
                }
            }

            return this;
        }

    };

    /**
     * Allows to delete nodes on press DEL key
     *
     * @desc You can use any other behaviour - removing by clicking a button etc
     *  
     * @method removeNodeOnDel
     * 
     * @event render
     */
    zs.flowLinkRemovableOnDel = {
        events: {
            render: function(){
                document.addEventListener('keyup', this.removeLinkOnDel.bind(this));
            }
        },

        /**
         * Check if DEl key was pressed and delete node if there is any selected
         * 
         * @param {Event} e
         * 
         * @return {HTMLElement}
         */
        removeLinkOnDel: function(e){
            if(e.keyCode !== 46){
                return this;
            }

            for(var i in this.links){
                if(this.links[i].isSelected){
                    this.links[i].destroy();
                }
            }

            return this;
        }

    };

    zs.flowElement = zs.customElement(HTMLElement, "zs-flow", null, [zs.flow, zs.flowRectNodes, zs.flowCircleNodes, zs.flowLineLinks, zs.flowNodeRemovableOnDel, zs.flowLinkRemovableOnDel, zs.flowDrawableLinks]);

	return zs;
})(window.zs || {});
