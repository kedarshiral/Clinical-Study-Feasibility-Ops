describe("zsProgressBar", function () {
    var elm;
    var delay = 200;
    beforeEach(function () {
        elm = document.createElement('zs-progress-bar');
        document.body.appendChild(elm);
	});

    it('should be a custom element', function () {
		expect(elm).not.toBe(null);
    });

    it('should be renderable', function () {
		expect(elm.render).toBeDefined();
    });
    
    it('should create the component with default properties', function (done) {
        setTimeout(function(){
            // converting elm._defaults... to string for fair comparison
            expect(elm.max).toBe(elm._defaults.max);
            expect(elm.value).toBe(elm._defaults.value);
            expect('' + elm.label).toBe('' + elm._defaults.label);
            expect(elm.duration).toBe(elm._defaults.duration);
            expect(elm.metrics).toBe(elm._defaults.metrics);
            expect('' + elm.animate).toBe('' + elm._defaults.animate);
            done();
        }, delay); 
    });
    
    it('should create the component with default attributes', function (done) {
        setTimeout(function(){
            // converting elm._defaults... to string for fair comparison
            expect(elm.getAttribute('max')).toBe('' + elm._defaults.max);
            expect(elm.getAttribute('value')).toBe('' + elm._defaults.value);
            expect(elm.getAttribute('label')).toBe('' + elm._defaults.label);
            expect(elm.getAttribute('duration')).toBe('' + elm._defaults.duration);
            expect(elm.getAttribute('metrics')).toBe('' + elm._defaults.metrics);
            expect(elm.getAttribute('animate')).toBe('' + elm._defaults.animate);
            done();
        }, delay); 
    });
    
    it('should be possible to customize the component by setting properties', function (done) {
        var max = 10;
        var value = 2;
        var label = true;
        var duration = 1000;
        var metrics = 'hello';
        var animate = true;
        
        elm.max = max;
        elm.value = value;
        elm.label = label;
        elm.duration = duration;
        elm.metrics = metrics;
        elm.animate = animate;

        setTimeout(function(){
            expect(elm.max).toBe(max);
            expect(elm.value).toBe(value);
            expect('' + elm.label).toBe('' + label);
            expect(elm.duration).toBe(duration);
            expect(elm.metrics).toBe(metrics);
            expect('' + elm.animate).toBe('' + animate);
            done();
        }, delay); 
    });
    
    it('should be able to customize the component by setting attributes', function (done) {
        var max = 10;
        var value = 2;
        var label = true;
        var duration = 1000;
        var metrics = 'hello';
        var animate = true;

        setTimeout(function(){ // wait for init completed
            elm.setAttribute('max', max);
            elm.setAttribute('value', value);
            elm.setAttribute('label', label);
            elm.setAttribute('duration', duration);
            elm.setAttribute('metrics', metrics);
            elm.setAttribute('animate', animate);

            // converting to string for fair comparison
            expect(elm.getAttribute('max')).toBe('' + max);
            expect(elm.getAttribute('value')).toBe('' + value);
            expect(elm.getAttribute('label')).toBe('' + label);
            expect(elm.getAttribute('duration')).toBe('' + duration);
            expect(elm.getAttribute('metrics')).toBe('' + metrics);
            expect(elm.getAttribute('animate')).toBe('' + animate);
            done();
        }, 1000); 
    });
    
    it('should sync props when attrs set', function (done) {
        var max = 10;
        var value = 2;
        var label = true;
        var duration = 1000;
        var metrics = 'hello';
        var animate = true;
        
        setTimeout(function(){ // wait for init completed
            elm.setAttribute('max', max);
            elm.setAttribute('value', value);
            elm.setAttribute('label', label);
            elm.setAttribute('duration', duration);
            elm.setAttribute('metrics', metrics);
            elm.setAttribute('animate', animate);

        setTimeout(function(){ // wait for update completed
            // converting to string for fair comparison
                expect(elm.max).toBe('' + max);
                expect(elm.value).toBe('' + value);
                expect(elm.label).toBe('' + label);
                expect(elm.duration).toBe('' + duration);
                expect(elm.metrics).toBe('' + metrics);
                expect(elm.animate).toBe('' + animate);
                done();
            }, delay); 
        }, delay); 
    });
    
    it('should sync attrs when props set', function (done) {        
        var max = 11;
        var value = 8;
        var label = false;
        var duration = 1020;
        var metrics = 'hello there!';
        var animate = false;
        
        elm.max = max;
        elm.value = value;
        elm.label = label;
        elm.duration = duration;
        elm.metrics = metrics;
        elm.animate = animate;

        setTimeout(function(){
            
            // converting to string for fair comparison
            expect(elm.getAttribute('max')).toBe('' + max);
            expect(elm.getAttribute('value')).toBe('' + value);
            expect(elm.getAttribute('label')).toBe('' + label);
            expect(elm.getAttribute('duration')).toBe('' + duration);
            expect(elm.getAttribute('metrics')).toBe('' + metrics);
            expect(elm.getAttribute('animate')).toBe('' + animate);
            done();
        }, delay); 
	});
    
    it('should fallback to default values if crucial props/attr values set inccorrect', function (done) {        
        var max = 'not a number';
        var value = 'not a number';
        var duration = 'not a number';
        
        elm.max = max;
        elm.value = value;
        elm.duration = duration;

        setTimeout(function(){
            // converting to string for fair comparison
            expect(elm.getAttribute('max')).toBe('' + elm._defaults.max);
            expect(elm.getAttribute('value')).toBe('' + elm._defaults.value);
            expect(elm.getAttribute('duration')).toBe('' + elm._defaults.duration);
            done();
        }, delay); 
    });
    
    afterEach(function () {
        document.body.removeChild(elm);
        elm = null;
	});
});