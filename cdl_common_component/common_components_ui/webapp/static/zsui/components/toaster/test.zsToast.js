describe("zsToast", function () {
	beforeEach(function() {
        jQuery.fx.off = true;
       
        toast=new zs.toastElement();
        config={type:"error",timeout:"1",title:" ",body:" ",showClose:"true"};       
        
	});

	it('is a custom element', function () {
		utils.isBehavior(expect, zs.toast);
		utils.isCustomElement(expect, null, 'zs-toast', zs.toastElement, HTMLElement);
	});

    it('pops a toast message when proper config if passed', function() {
        spyOn(toast, 'create').and.callThrough();
       
        toast.create(config);
        
        document.querySelector("body").appendChild(toast);
        
        expect(toast.create).toHaveBeenCalled();

        expect(document.querySelector('.zs-toast')).toBeTruthy();
    });

    it('destroys a toast message when a user clicks over it', function (done) {
        spyOn(toast,'destroy').and.callThrough();
       
        toast.create(config);
        document.querySelector("body").appendChild(toast);
     
        document.querySelector('.zs-toast').dispatchEvent(new CustomEvent('click'));
        
        setTimeout(function(){expect(toast.destroy).toHaveBeenCalled();done();},1);    
    });

    it('destroys a toast message automatically after pre-configured time interval', function (done) {
        toast.create(config);
        document.querySelector("body").appendChild(toast);
        
        setTimeout(function () {
            expect(document.querySelector('zs-toast')).toBeFalsy();
            done();
        }, 10);
    });

    it('supports rendering of different types of messages', function () {
        config.type="success";
        toast.create(config);
        document.querySelector("body").appendChild(toast);
       
        expect(document.querySelector(".zs-toast.zs-success")).toBeTruthy();

        toast.destroy();

        config.type="warning";
        toast.create(config);
        document.querySelector("body").appendChild(toast);
       
        expect(document.querySelector(".zs-toast.zs-warning")).toBeTruthy();      
    });

    afterEach(function() {
       toast.destroy();       
    });
});