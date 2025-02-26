describe("zsSlider", function () {
	var $slider, $parent;

	beforeEach(function () {
		$parent = $('<form><input type="range" min="-10" max="10" value="0"></form>');
		$parent.appendTo('body');
		$slider = $parent.find('input[type=range]');
	});

    it('Is a jQuery plugin', function () {
		utils.isPlugin(expect, 'zsSlider'); 
	});

    
    it('should be able to read several input params', function() {
        $slider[0].setAttribute('min', 10);
        $slider[0].setAttribute('max', 100);
        $slider[0].setAttribute('orientation', 'vertical');
        $slider[0].setAttribute('metric', 'px');
        $slider[0].setAttribute('prefix', '$');

        $slider.zsSlider();
        
        var plugin = $slider.data('zsSlider');

        expect(plugin.min).toEqual(10);
        expect(plugin.max).toEqual(100);
        expect(plugin.orientation).toEqual('vertical');
        expect(plugin.metric).toEqual('px');
        expect(plugin.prefix).toEqual('$');
    });
    
    it('should be able to set and get value', function() {
        $slider.prop({
            min: 0,
            max:100
        });

        $slider.zsSlider();

        $slider.zsSlider().val(11);
        
        expect($slider.zsSlider().val()).toEqual('11'); // value actualy converted to html param and this is string
    });
    
    it('should call onInit callback', function() {
        this.func = function(){};

        spyOn(this, 'func');
        $slider.zsSlider({
            onInit: this.func
        });

        expect(this.func).toHaveBeenCalled();
    });
        
    it('should call onSlide callback', function() {
        this.func = function(){};

        spyOn(this, 'func');
        $slider.zsSlider({
            onSlide: this.func
        });

        var plugin = $slider.data('zsSlider');
        spyOn(plugin, 'getRelativePosition'); // prevent calling

        $('#'+plugin.identifier).trigger('mousedown');
        plugin.$document.trigger('mousemove.'+plugin.identifier);
        expect(this.func).toHaveBeenCalled();
    });
    
    it('should call onSlideEnd callback', function() {
        this.func = function(){};

        spyOn(this, 'func');
        
        $slider.zsSlider({
            onSlideEnd: this.func
        });

        var plugin = $slider.data('zsSlider');
        spyOn(plugin, 'getRelativePosition'); // prevent calling

        $('#'+plugin.identifier).trigger('mousedown');
        plugin.$document.trigger('mouseup.'+plugin.identifier);
        expect(this.func).toHaveBeenCalled();
    });
        
    it('should support base param', function() {  
        var base = "-5";      
        $slider.zsSlider({
            base: base
        });

        var plugin = $slider.data('zsSlider');
        plugin.rulerUnits[base] = 5; // emulating to avoid different position issues
        plugin.setPosition(0);

        expect(parseInt(plugin.$fill[0].style.left)).toEqual(parseInt(plugin.rulerUnits[base]) + 2);
    });

    it('should support regular (not decimal) min/max with no step', function() {
        $slider.prop({
            min: 0,
            max: 10
        });

        $slider.zsSlider();
        var $plugin = $slider.data('zsSlider');
        expect(Object.keys($plugin.rulerUnits)).toEqual(['0', '5', '10']); 
    });

    it('should support regular (not decimal) min/max with step', function() {
        $slider.prop({
            min: 0,
            max: 10,
            step:1
        });

        $slider.zsSlider();
        var $plugin = $slider.data('zsSlider');
        expect(Object.keys($plugin.rulerUnits)).toEqual(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']); 
    });

    it('should support min:0 max: 1', function() {
        $slider.prop({
            min: 0,
            max: 1
        });

        $slider.zsSlider();
        var $plugin = $slider.data('zsSlider');
        expect(Object.keys($plugin.rulerUnits)).toEqual(['0', '0.5', '1.0']);
    });

    it('should support floating point step', function() {
        $slider.prop({
            min: 0,
            max: 1,
            step:0.25
        });

        $slider.zsSlider();
        var $plugin = $slider.data('zsSlider');
        expect(Object.keys($plugin.rulerUnits)).toEqual(['0', '0.25', '0.50', '0.75', '1.00']);
    });

    it('should support floating point min/max', function() {
        $slider.prop({
            min: 0.01,
            max: 0.1,
            step:0.01
        });

        $slider.zsSlider();
        var $plugin = $slider.data('zsSlider');
        expect(Object.keys($plugin.rulerUnits)).toEqual(['0.01', '0.02', '0.03', '0.04', '0.05', '0.06', '0.07', '0.08', '0.09', '0.10']); 
    });

    describe("in multiple mode", function () {
        beforeEach(function () {
            $parent.remove();
            $parent = $('<form><input type="range" min="-10" max="10" value="4;5" multiple></form>');
            $parent.appendTo('body');
            $slider = $parent.find('input[type=range]');
            $slider.zsSlider();
        });

        it('should be able to set and get value as array', function() {    
            $slider[0].setAttribute('value', '1;2');
            $slider.change();
            expect($slider.data('zsSlider').getValue()).toEqual([1, 2]);
        });

        it('should be filled from point A to point B', function() {
            var handle1Pos = parseFloat($slider.data('zsSlider').$handle[0].style.left);
            var handle2Pos = parseFloat($slider.data('zsSlider').$handle2[0].style.left);
            var width =  parseInt(Math.abs(handle1Pos - handle2Pos));
            var fillWidth = parseInt($slider.data('zsSlider').$fill.width());

            expect(fillWidth).toEqual(width);
        });

        it('should be filled from point A to point B in vertical mode', function() {
            $parent.remove();
            $parent = $('<form><input type="range" min="-10" max="10" value="4;5" orientation="vertical" multiple></form>');
            $parent.appendTo('body');
            $slider = $parent.find('input[type=range]');
            $slider.zsSlider();

            var handle1Pos = parseFloat($slider.data('zsSlider').$handle[0].style.bottom);
            var handle2Pos = parseFloat($slider.data('zsSlider').$handle2[0].style.bottom);
            var width =  parseInt(Math.abs(handle1Pos - handle2Pos));
            var fillWidth = parseInt($slider.data('zsSlider').$fill.height());
            
            expect(fillWidth).toEqual(width);
        });
    });

    afterEach(function () {
		if ($slider && $slider.length && $slider.data('zsSlider')) {
			$slider.zsSlider('destroy');
		}
		$parent.remove();	
	});

});
