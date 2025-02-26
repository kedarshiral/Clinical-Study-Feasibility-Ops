(function (zs) {
	"use strict";

    /**
     * Creates Flow Panel
     * 
     * This panel is for demo purposes only. It's not part of componentn itself. 
     * You can use it as an example how different nodes/links manipulations could be managed
     * You can use another approach if you wish
     *
     * @property 
     *
     * @method
     *
     * @event 
     */
	zs.flowPanel = {
        config: null,

        defaults:{
            newNodeW: 120,
            newNodeH: 60,
            newNodeX: 10,
            newNodeY: 10,
            newNodeType: 'rect',
            newNodeText: 'Hello'
        },

        events:{
            create: function(){},
            attach: function(){}
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

            if(!params.flow || !params.flow.addEventListener){ // flow absent or not a custom element
                throw Error('Flow MUST be provided and be a Custom element');
            }

            this.config.flow = params.flow;

            this.dispatchEvent(new CustomEvent('configure', {detail: { params: params }}));
            
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
            this.dispatchEvent(new CustomEvent("render"));

            return this;
		}
    };

    zs.flowPanelNodesEditable = {
        events: {
            render: function(){
                this.config.flow.addEventListener('afterrendernode', this.listenNodes.bind(this));
                this.querySelector('[open-sublow]').addEventListener('click', this.showSubflow.bind(this))

                this.querySelector('textarea').addEventListener('keyup', this.onChangeText.bind(this));
                this.querySelector('[remove-node]').addEventListener('click', this.removeNode.bind(this));
            }
        },

        listenNodes: function(e){
            var node = e.detail.node;

            node.shape.node.addEventListener('select', this.nodeSelected.bind(this));
            node.shape.node.addEventListener('deselect', this.nodeDeSelected.bind(this));
            node.shape.node.addEventListener('remove', this.nodeDestroyed.bind(this));


            return this;
        },

        showSubflow: function(){
            if(!this.currentNode){
                return this;
            }

            if(!this.currentNode.config.subflow){
                return this;
            }
            $('.zs-modal').zsModalDialog('open');

            var elm = document.querySelector('.zs-modal zs-flow');
            elm
                .configure(this.currentNode.config.subflow)
				.render();

            
        },

        nodeSelected: function(e){
            var node = e.detail.node;

            this.currentNode = node;
            
            this.querySelector('[property-panel]').classList.remove('zs-hidden');

            this.querySelector('textarea').value = this.currentNode.getText();

            this.querySelector('[open-sublow]').classList.add('zs-hidden');
            
            if(node.config.subflow){
                this.querySelector('[open-sublow]').classList.remove('zs-hidden');
            }
            return this;
        },

        nodeDeSelected: function(e){
            var node = e.detail.node;
            
            if(this.currentNode === node){
                this.querySelector('[property-panel]').classList.add('zs-hidden');
            }
        },

        nodeDestroyed: function(e){
            var node = e.detail.node;

            if(node !== this.currentNode){
                return this;
            }

            this.querySelector('textarea').value = '';
            
            this.currentNode = null;

            return this;
        },

        onChangeText: function(e){
            if(!this.currentNode){
                return this;
            }

            this.currentNode.setText(e.target.value);

            return this;
        },

        removeNode: function(e){
            if(!this.currentNode){
                return this;
            }

            this.currentNode.destroy();

            return this;
        }
    };

    zs.flowPanelLinksEditable = {
        events: {
            render: function(){
                this.config.flow.addEventListener('afterrenderlink', this.listenLinks.bind(this));
            }
        },

        listenLinks: function(e){
            var link = e.detail.link;

            link.shape.node.addEventListener('select', this.linkSelected.bind(this));
            link.shape.node.addEventListener('remove', this.linkDestroyed.bind(this));

            return this;
        },

        linkSelected: function(e){
            console.log('link selected');
            return this;
        },

                
        linkDestroyed: function(){

        }
    };

    zs.flowPanelNodesAddable = {
        events: {
            configure:function(e){
                var params = e.detail.params;
                this.config.newNodeW    = params.newNodeW    || this.defaults.newNodeW;
                this.config.newNodeH    = params.newNodeH    || this.defaults.newNodeH;
                this.config.newNodeX    = params.newNodeX    || this.defaults.newNodeX;
                this.config.newNodeY    = params.newNodeY    || this.defaults.newNodeY;
                this.config.newNodeType = params.newNodeType || this.defaults.newNodeType;
                this.config.newNodeText = params.newNodeText || this.defaults.newNodeText;
            },
            
            render: function(){
                var button = this.querySelector('[add-node]');
                if(!button){
                    throw Error('[add-node] DOM element MUST be present in panel');
                }

                button.addEventListener('click', this.addNode.bind(this));
            }
        },

        addNode: function(){
            this.config.flow.dispatchEvent(new CustomEvent("rendernode", {
                detail: { 
                    data: {
                        shapeW: this.config.newNodeW,
                        shapeH: this.config.newNodeH,
                        x: this.config.newNodeX,
                        y: this.config.newNodeY,
                        key: 'node' + Date.now(),
                        text: this.config.newNodeText,
                        type: this.config.newNodeType  
                    }
                }
            }));
        }
    };
    
    zs.flowPanelExport = {
        events: {
            render: function(){
                var button = this.querySelector('[flow-export]');
                if(!button){
                    throw Error('[flow-export] DOM element MUST be present in panel');
                }

                button.addEventListener('click', this.export.bind(this));
            }
        },

        export: function(){
            var i,
                data  = {},
                links = this.config.flow.links,
                nodes = this.config.flow.nodes;
            
            data.nodes = [];
            for(i in nodes){
                data.nodes.push(nodes[i].export());
            }

            data.links = [];
            for(i in links){
                data.links.push(links[i].export());
            }

            console.log(JSON.stringify(data));
        }
    };

    zs.flowPanelElement = zs.customElement(HTMLElement, "zs-flow-panel", null, [zs.flowPanel, zs.flowPanelNodesEditable, zs.flowPanelLinksEditable, zs.flowPanelNodesAddable, zs.flowPanelExport]);

	return zs;
})(window.zs || {});