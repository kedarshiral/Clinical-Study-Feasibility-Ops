describe('idmLaunchbar', function () {
	var $launchbar, $menu, $a, $nav;
	var zsResponseText = '<div id="zs-lbr" style="display: none;"><div role="profile"><a href="javascript:void(0);"><span class="zs-icon zs-icon-user-permissions zs-hide zs-visible-lg-inline"><span class="zs-badge zs-error zs-badge-error zs-badge-rectangle" style="display: none;"></span></span><span class="zs-icon zs-icon-large zs-icon-user-permissions zs-hidden-lg"><span class="zs-badge zs-error zs-badge-error zs-badge-rectangle" style="display: none;"></span></span><label class="zs-visible-lg-inline zs-visible-md-inline zs-hide">Profile</label></a><nav class="zs-menu" style="display: none"><div class="zs-message zs-error" style="display: none;"><div>One or more roles are expiring</div><a href="javascript:void(0);">Manage roles to extend or renew access</a></div><a href="javascript:void(0);" class="zs-link">My Account</a><a href="javascript:void(0);" class="zs-link">Switch Account</a><a href="javascript:void(0);" class="zs-link"><span class="zs-icon zs-icon-logout"></span>Sign Out</a></nav></div><div role="apps"><a href="javascript:void(0);"><span class="zs-icon zs-icon-grid zs-hide zs-visible-lg-inline"></span><span class="zs-icon zs-icon-large zs-icon-grid zs-hidden-lg"></span><label class="zs-visible-lg-inline zs-hide">Applications</label></a><nav class="zs-menu" style="display: none"><a href="javascript:void(0);">App 1</a><a href="javascript:void(0);">App 2</a><a href="javascript:void(0);">App 3</a></nav></div><div role="help"><a href="javascript:void(0);"><span class="zs-icon zs-icon-help zs-hide zs-visible-lg-inline"></span><span class="zs-icon zs-icon-large zs-icon-help zs-hidden-lg"></span><label class="zs-visible-lg-inline zs-hide">Help</label></a></div></div>';

	var zsResponseNoApps = '<div id="zs-lbr" style="display: none;"><div role="profile"><a href="javascript:void(0);"><span class="zs-icon zs-icon-user-permissions zs-hide zs-visible-lg-inline"><span class="zs-badge zs-error zs-badge-error zs-badge-rectangle" style="display: none;"></span></span><span class="zs-icon zs-icon-large zs-icon-user-permissions zs-hidden-lg"><span class="zs-badge zs-error zs-badge-error zs-badge-rectangle" style="display: none;"></span></span><label class="zs-visible-lg-inline zs-visible-md-inline zs-hide">Profile</label></a><nav class="zs-menu" style="display: none"><div class="zs-message zs-error" style="display: none;"><div>One or more roles are expiring</div><a href="javascript:void(0);">Manage roles to extend or renew access</a></div><a href="javascript:void(0);" class="zs-link">My Account</a><a href="javascript:void(0);" class="zs-link">Switch Account</a><a href="javascript:void(0);" class="zs-link"><span class="zs-icon zs-icon-logout"></span>Sign Out</a></nav></div><div role="apps"><a href="javascript:void(0);"><span class="zs-icon zs-icon-grid zs-hide zs-visible-lg-inline"></span><span class="zs-icon zs-icon-large zs-icon-grid zs-hidden-lg"></span><label class="zs-visible-lg-inline zs-hide">Applications</label></a><nav class="zs-menu" style="display: none"></nav></div><div role="help"><a href="javascript:void(0);"><span class="zs-icon zs-icon-help zs-hide zs-visible-lg-inline"></span><span class="zs-icon zs-icon-large zs-icon-help zs-hidden-lg"></span><label class="zs-visible-lg-inline zs-hide">Help</label></a></div></div>';

	var legacyResponseText = '<div id="lbr"><div id="lbar-apps"><ul><li class="lbar-selected"><a class="lbar-link" href="javascript:void(0);" id="lbl_0" title="App1" onclick="return ZSLB.LinkClick(\'0\')">App1</a></li><li><a class="lbar-link" href="javascript:void(0);" id="lbl_1" title="App2" onclick="return ZSLB.LinkClick(\'1\')">App2</a></li><li><a class="lbar-link" href="javascript:void(0);" id="lbl_2" title="App3" onclick="return ZSLB.LinkClick(\'2\')">App3</a></li></ul></div><div id="lbar-logout"><a class="lbar-link" href="javascript:void(0);" id="lbl_so" onclick="return ZSLB.LinkClick(\'so\')">Sign Out</a></div><div id="lbar-user" class="lbar-menu-off"><div id="lbar-user-menu-popup" class="lbar-menu-popup"><ul><li><a href="javascript:void(0);" id="lbl_ma" class="lbar-link" onclick="return ZSLB.LinkClick(\'ma\')">My Account</a></li><li><a href="javascript:void(0);" id="lbl_sa" class="lbar-link" onclick="return ZSLB.LinkClick(\'sa\')">Switch Account</a></li></ul></div><a id="lbar-user-menu-link" class="lbar-menu-dropdown" href="javascript:void(0);"><span class="user-label">User: </span><span class="user-name">username</span></a></div></div>';
	
	beforeEach(function () {
		this.fakeRequest = {
			readyState: 4,
			status: 200,
		};

		$('body').append($('<div id="launch-bar" style="display: none;"><div id="1_HeaderLaunchbar"><div id="lbr"></div></div></div>'));
		$('body').append($('<div id="zs-lbr-container"><div id="zs-lbr"></div></div>'));
		$('body').append($('<div class="zs-user-bar"></div>'));		
	});

	afterEach(function () {
		delete this.fakeRequest;
		$('.zs-user-bar').remove();
		$('#zs-lbr-container').remove();
		$('#launch-bar').remove();
		$('.zs-app-message').remove();
		$('.zs-header').remove();
		// also need to deregister click handlers?
	});

	it('creates ZSLB global object', function () {
		expect(ZSLB).toBeDefined();
	});

	it('displays the proper launchbar based on settings', function () {
		
		// By default both launchbars are provided
		this.fakeRequest.responseText = zsResponseText + legacyResponseText;
		ZSLB.handleLaunchbarResponse(this.fakeRequest);
		var userBar = $('.zs-user-bar');
		expect(userBar.children().length).toBeGreaterThan(0);
		expect($('#lbr').children().length).toEqual(0);
		expect($('#zs-lbr').length).toEqual(1);		

		// No user bar
		userBar.remove();
		ZSLB.handleLaunchbarResponse(this.fakeRequest);
		expect($('#lbr').children().length).toBeGreaterThan(0);

		// No user bar but header
		$('body').append('<div class="zs-header"></div>');
		ZSLB.handleLaunchbarResponse(this.fakeRequest);
		expect($('#zs-lbr').length).toEqual(1);
		expect($('#lbr').html()).toBeFalsy(); // test for removal

		// Only legacy is provided 
		this.fakeRequest.responseText = legacyResponseText;
		ZSLB.handleLaunchbarResponse(this.fakeRequest);
		expect($('.zs-user-bar').find('[role="profile"]').length).toBe(0); // test for removal of zs launchbar
		expect($('.zs-user-bar').find('[role="apps"]').length).toBe(0);
	});

	it('Disables apps icon when no apps provided', function() {		
		this.fakeRequest.responseText = zsResponseNoApps;
		ZSLB.handleLaunchbarResponse(this.fakeRequest);
		var userBar = $('.zs-user-bar');
		expect(userBar.find('[role="apps"]').attr('disabled')).toBeTruthy();
	});
	
	xit('test the legacy launch bar',function() {
	});
	
	
	it('opens the profile menu', function (done) {
		this.fakeRequest.responseText = zsResponseText;
		ZSLB.handleLaunchbarResponse(this.fakeRequest);
		$menu = $('.zs-user-bar > [role="profile"]');
		$a = $menu.find('a:first');
		$nav = $menu.find('nav');
		$a.click();

		// Hide/show functionality contains a setTimeout, so this compensates for that
		setTimeout(function () {
			expect($menu.hasClass('zs-selected')).toBe(true);
			expect($nav.is(':visible')).toBe(true);

			done();
		});
	});

	it('closes the profile menu', function (done) {
		this.fakeRequest.responseText = zsResponseText;
		ZSLB.handleLaunchbarResponse(this.fakeRequest);
		$menu = $('.zs-user-bar > [role="profile"]');
		$a = $menu.find('a:first');
		$nav = $menu.find('nav');
		$a.click();
		setTimeout(function () {
			$a.click();
			expect($menu.hasClass('zs-selected')).toBe(false);
			expect($nav.is(':visible')).toBe(false);

			done();
		});
	});

	it('opens the applications menu', function (done) {
		this.fakeRequest.responseText = zsResponseText;
		ZSLB.handleLaunchbarResponse(this.fakeRequest);
		$menu = $('.zs-user-bar > [role="apps"]');
		$a = $menu.find('a:first');
		$nav = $menu.find('nav');
		$a.click();
		//expect($menu.hasClass('zs-selected')).toBe(true);

		setTimeout(function () {
			expect($menu.hasClass('zs-selected')).toBe(true);
			expect($nav.is(':visible')).toBe(true);

			done();
		});
	});

	it('closes the applications menu', function (done) {
		this.fakeRequest.responseText = zsResponseText;
		ZSLB.handleLaunchbarResponse(this.fakeRequest);
		$menu = $('.zs-user-bar > [role="apps"]');
		$a = $menu.find('a:first');
		$nav = $menu.find('nav');
		$a.click();
		$a.click();
		setTimeout(function () {			
			expect($menu.hasClass('zs-selected')).toBe(false);
			expect($nav.is(':visible')).toBe(false);
			done();
		});
	});

	it('displays and hides footer message', function () {
		var $appMessageBar = $('<div class="zs-app-message" style="display: none;"><span class="non-prod-msg" style="display: none;">Non-Production Environment</span><a href="javascript:void(0);" class="zs-icon zs-icon-rejected-approval zs-right"></a></div>');
		$('body').append($appMessageBar);
		this.fakeRequest.responseText = zsResponseText;
		ZSLB.handleLaunchbarResponse(this.fakeRequest);

		expect($appMessageBar.is(':visible')).toBe(true);
		expect($appMessageBar.find('.non-prod-msg').is(':visible')).toBe(true);

		$appMessageBar.find('a.zs-icon-rejected-approval').click();

		expect($appMessageBar.is(':visible')).toBe(false);
		expect($appMessageBar.find('.non-prod-msg').is(':visible')).toBe(false);
	});
	
	xit('test when we don\'t have a container for the message', function() {
	});
});