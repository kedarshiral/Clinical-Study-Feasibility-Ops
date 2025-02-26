describe("Select component", function () {
    var elm;
    beforeEach(function (done) {
        elm = new zs.selectElement();
        setTimeout(function () {
            done();
        },10); // IE11 requires delay, 0 ms - 5 ms doesn't work, weird 
        
    });
	it('should be a custom element', function () {
		utils.isBehavior(expect, zs.select);
		utils.isCustomElement(expect, 'zs-select', 'div', zs.selectElement, HTMLElement);
	});

    it('should call zsSearchDropdown if propriate parameter exists', function () {
        elm.setAttribute('searchableDropdown', '');
        elm.appendChild(document.createElement('select'));
        
        var event = document.createEvent("CustomEvent");
        event.initEvent("attach",true,true);
        elm.dispatchEvent(event);   
        
        expect(elm.querySelectorAll('.zs-search-dropdown').length).toEqual(1);
    });

    it('should not call zsSearchDropdown if propriate parameter absents', function () {
        elm.appendChild(document.createElement('select'));
        
        var event = document.createEvent("CustomEvent");
        event.initEvent("attach",true,true);
        elm.dispatchEvent(event);

        expect(elm.querySelectorAll('.zs-search-dropdown').length).toEqual(0);
    });



});