/**
 * @deprecated
 */
 (function ($) {
	'use strict';
	var defaults = {
		showLabel:true,
		duration:300,
		onChange:function(){
			
		}
	};
	
	function zsProgress(options, $container) {
		this.$container = $container;
		this.configure(options);
		this.render();		
	};
	
	zsProgress.prototype.defaults = defaults;

	zsProgress.prototype.render = function () {
		var self = this;
		var progressContainer = $('<div class="progress-container">');
	    var progressDiv = $('<div class="current-progress">');
	    var progressValue = $('<div class="progress-value">');
		
		self.value=self.$container.data('percent');

		progressDiv.html(progressValue);
      	progressContainer.html(progressDiv);
		self.$container.html(progressContainer);
		
		self.$target = $(progressContainer);
		
		self.setValue.call(self,self.value);
	}


	zsProgress.prototype.destroy = function () {
		this.$container.find('.progress-container').remove();
	};

	
	zsProgress.prototype.update = function () {


	}
	
	zsProgress.prototype.setValue = function (value) {
		var duration=Number(this.options.duration) || 0;
		var value=Number(value) || 0;
		var value=value>100?100:value;
		var _this=this;

		this.value=value;
		
		this.$target.find(".current-progress").animate({width:value+'%'},duration,function(){
			if (_this.options.showLabel) {_this.$target.find(".progress-value").html(value+"%").hide();}
			
			if(parseInt(_this.$target.find(".progress-value").css("width"))<parseInt(_this.$target.find(".current-progress").css("width"))){
				_this.$target.find(".progress-value").show();
			}
		});		

		if(typeof this.options.onChange == 'function')
		{
			this.options.onChange.call(this);
		}
	}
	zsProgress.prototype.getValue = function () {
		return this.value;
	}
	
	zsProgress.prototype.configure = function (options) {
		if (options) {
			this.options = options;
		}	
	}
	
	$.fn.zsProgress = function (opt) {			
		// Override mode
		if (this == $.fn) {
			$.extend(zsProgress.prototype, opt);
			return;
		}		
		var options = ($.isPlainObject(opt) || !opt) ? $.extend(true, {}, zsProgress.prototype.defaults, opt) : $.extend(true, {}, zsProgress.prototype.defaults);
	
		return this.each(function () {
			var plugin = $(this).data('zsProgress');
			if (plugin) {
				if ($.type(opt) == 'string') {
					switch (opt) {						
						case 'destroy':
							plugin.destroy($(this));
							break;
						case 'update':
							plugin.update();
							break;					
					}
				} else {
					plugin.configure($.extend(true, plugin.options, opt));
					plugin.render();
				}
				return;
			}
			if ($.type(opt) != 'string') {
				$(this).data('zsProgress', new zsProgress(options, $(this)));
				return;
			}
		});
	}
	
} (jQuery));