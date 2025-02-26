var zs = (function(zs) {
	'use strict'		
	/**
	 * Highlight text fragment in the contents of DOM element using <mark></mark> tag
	 * @param {*} element - Element to hightlight in 
	 * @param {*} searchValue - Keywork to hightlight
	 * @param {*} caseSensitive - Case sensitive or not
	 */
	zs.highlight = {
		highlight: function (searchValue, caseSensitive) {
			var element = this;
			var keywordLower = caseSensitive ? searchValue : searchValue.toLowerCase();
			var stringToSearch = caseSensitive ? element.innerText : element.innerText.toLowerCase();
			var original = element.innerHTML;
			if (!original) { return; }
			var index = stringToSearch.indexOf(keywordLower);
			var offset = keywordLower.length;
			var text = '';
			while (index > -1) {
				text += original.substr(0, index) + '<mark>' + original.substr(index, offset) + '</mark>';
				stringToSearch = stringToSearch.substr(index + offset, stringToSearch.length - 1);
				original = original.substr(index + offset, original.length - 1);
				index = stringToSearch.indexOf(keywordLower);
			}
			text += original;
			element.innerHTML = text;
		},
		lowlight: function () {
			var element = this;
			var html = element.innerHTML;
			html = html.replace('<mark>','');
			html = html.replace('</mark>','');
			element.innerHTML = html;
		}
	} 
	return zs;
})(window.zs || {});	