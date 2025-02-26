// ZSUI Search Result Highlighting
 (function ($) {

    // zs namespace
    if (!$.zs) {
        $.zs = {};
    };

    function highlight($element, searchValue, caseSensitive) {
		
        var keywordLower = caseSensitive ? searchValue : searchValue.toLowerCase();
        var stringToSearch = $element.text().toLowerCase();
        var original = $element.text();
        if (!original) { return;}
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
        $element.html(text);
    }

    $.zs.highlight = function (keyword, scopeSelector) {
        if (!keyword) { // questionable feature
            $.zs.lowlight(scopeSelector);
            return;
        }
        $(scopeSelector).each(function () {
            var $element = $(this);
            highlight($element, keyword);
        });
    }

    $.zs.lowlight = function (scopeSelector, selector) {
        scopeSelector = scopeSelector || 'body';
        selector = selector || 'mark, .zs-highlight';

        $(scopeSelector).find(selector).each(function () {
            var $element = $(this);
            $element.replaceWith($element.text());
        })
    }
})(jQuery);