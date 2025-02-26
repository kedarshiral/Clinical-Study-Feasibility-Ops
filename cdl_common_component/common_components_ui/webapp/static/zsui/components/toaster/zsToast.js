var zs = (function(zs) {
	'use strict';
    zs.toast = {
		events: {
            click:function(){
                this.resetTimer();
                this.destroy();
            },
            mouseover:function(){
                if(this.lifetime){
                    this.resetTimer();
                }
            },
            mouseout:function(){
                if(this.lifetime){
                    this.setTimer(this.lifetime);
                }
            }
		},
        defaults:{
            type:"error",
            timeout:10000,
            title:"Toaster Title",
            body:"This toaster fades out in few seconds. Tap to dismiss.",
            showClose:false
        },
        create:function(message){
            var config={};
            Object.assign(config,this.defaults);
            Object.assign(config,message);

            this.setAttribute('class', 'zs-toast zs-message zs-' + config.type);

            var titleMarkup='<div class="zs-toast-title">'+config.title+'</div>';
            var bodyMarkup='<div class="zs-toast-body">'+config.body+'</div>';

            var closeButtonMarkup='<a href="javascript:void(0)" class="zs-icon zs-icon-rejected-approval zs-toast-close-button"></a>'
            
            if(config.showClose==true){this.innerHTML=closeButtonMarkup+titleMarkup+bodyMarkup;}
            else{this.innerHTML=titleMarkup+bodyMarkup;}
       
            requestAnimationFrame(function () {
		        //Timeout required to make transition work on FF.
                setTimeout(function(){this.classList.add("fadeIn");}.bind(this),0);
            }.bind(this));
       
            if(config.timeout) {this.lifetime=config.timeout;this.setTimer(this.lifetime);}
        },
        appendTo:function(parent){
            parent.appendChild(this);
        },
        setTimer:function(timeout){
            this.resetTimer();
            this.timerId=window.setTimeout(this.destroy.bind(this),this.lifetime);
        },
        resetTimer:function(){
            if(this.timerId){window.clearTimeout(this.timerId);}
        },
        destroy: function () {
            if (document.body.contains(this)) {
                this.classList.remove('fadeIn');
                setTimeout(function(){
                    if(this.parentNode){
                        this.parentNode.removeChild(this);
                    }
                }.bind(this),1000); //1000ms timeout needed to wait till CSS fadeOut transition ends.
            }
        }
	};
 
   	zs.toastElement = zs.customElement(HTMLElement, 'zs-toast','div', [zs.toast]);
	return zs;
})(window.zs || {});    
