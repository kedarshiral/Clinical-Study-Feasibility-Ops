describe("zsFlow", function () {
    var flow;
    beforeEach(function () {
        document.body.appendChild(document.createElement('zs-flow'));
		flow = document.querySelector('zs-flow');
	});

    it('should be a custom element', function () {
		expect(flow).not.toBe(null);
	});


    it('should be configureble', function () {
		expect(flow.configure).toBeDefined();
	});


    it('should be renderable', function () {
		expect(flow.render).toBeDefined();
	});

    it('should support diffrent types of node', function () {
		var data = {
			nodes: [{
				x: 380,
				y: 120,
				text: 'Node 1',
				key: 'node1',
				type: 'circle'
			}]
		};
		
		flow.configure(data);
        flow.render();

		expect(document.querySelector('zs-flow circle')).not.toBe(null);

		var data = {
			nodes: [{
				x: 380,
				y: 120,
				text: 'Node 1',
				key: 'node1',
				type: 'rect'
			}]
		};
		
		flow.configure(data);
        flow.render();

		expect(document.querySelector('zs-flow rect')).not.toBe(null);
	});

    it('should render links', function () {
		var data = {
			nodes: [{
				x: 20,
				y: 20,
				text: 'Node 1',
				key: 'node1',
				type: 'rect'
			},{
				x: 200,
				y: 20,
				text: 'Node 2',
				key: 'node2',
				type: 'rect'
			}],
			links: [{
				nodeFrom: 'node1', 
				nodeTo: 'node2',
				dotFrom: 'right',
				dotTo: 'left',
				type: 'line'
			}]
		};

        flow.configure(data);
        flow.render();
		expect(document.querySelector('zs-flow [link]')).not.toBe(null);
	});

	it('should be able to destroy node', function () {
		var data = {
			nodes: [{
				x: 20,
				y: 20,
				text: 'Node 1',
				key: 'node1',
				type: 'rect'
			}]
		};

        flow.configure(data);
        flow.render();
		expect(document.querySelector('zs-flow rect')).not.toBe(null);

		flow.nodes.node1.destroy();
		expect(document.querySelector('zs-flow rect')).toBe(null);
	});

	it('should be able to destroy links', function () {
		var data = {
			nodes: [{
				x: 20,
				y: 20,
				text: 'Node 1',
				key: 'node1',
				type: 'rect'
			},{
				x: 200,
				y: 20,
				text: 'Node 2',
				key: 'node2',
				type: 'rect'
			}],
			links: [{
				nodeFrom: 'node1', 
				nodeTo: 'node2',
				dotFrom: 'right',
				dotTo: 'left',
				key: 'link1',
				type: 'line'
			}]
		};

        flow.configure(data);
        flow.render();
		expect(document.querySelector('zs-flow [link]')).not.toBe(null);

		flow.links.link1.destroy();
		expect(document.querySelector('zs-flow [link]')).toBe(null);
	});

	it('should destroy links if node is destroyed', function () {
		var data = {
			nodes: [{
				x: 20,
				y: 20,
				text: 'Node 1',
				key: 'node1',
				type: 'rect'
			},{
				x: 200,
				y: 20,
				text: 'Node 2',
				key: 'node2',
				type: 'rect'
			}],
			links: [{
				nodeFrom: 'node1', 
				nodeTo: 'node2',
				dotFrom: 'right',
				dotTo: 'left',
				key: 'link1',
				type: 'line'
			}]
		};

        flow.configure(data);
        flow.render();
		expect(document.querySelector('zs-flow [link]')).not.toBe(null);

		flow.nodes.node1.destroy();
		expect(document.querySelector('zs-flow [link]')).toBe(null);
	});

    afterEach(function () {
        document.body.removeChild(flow);
        flow = null;
	});
});