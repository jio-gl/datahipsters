'''
Created on 15/12/2014

@author: jose
'''

widget_template = '''(function() {

// Localize jQuery variable
    var jQuery;

/******** Load jQuery if not present *********/
    if (window.jQuery === undefined || window.jQuery.fn.jquery !== '1.4.2') {
    var script_tag = document.createElement('script');
    script_tag.setAttribute("type","text/javascript");
    script_tag.setAttribute("src",
                "http://ajax.googleapis.com/ajax/libs/jquery/1.4.2/jquery.min.js");
    if (script_tag.readyState) {
        script_tag.onreadystatechange = function () { // For old versions of IE
        if (this.readyState == 'complete' || this.readyState == 'loaded') {
            {{MAIN_NAME}}scriptLoadHandler();
        }
        };
    } else {
        script_tag.onload = {{MAIN_NAME}}scriptLoadHandler;
    }
    // Try to find the head, otherwise default to the documentElement
    (document.getElementsByTagName("head")[0] || document.documentElement).appendChild(script_tag);
    } else {
    // The jQuery version on the window is the one we want to use
    jQuery = window.jQuery;
    main();
    }

/******** Called once jQuery has loaded ******/
    function {{MAIN_NAME}}scriptLoadHandler() {
    // Restore $ and window.jQuery to their previous values and store the
    // new jQuery in our local jQuery variable
    jQuery = window.jQuery.noConflict(true);
    // Call our main function
    {{MAIN_NAME}}(); 
    }

/******** Our main function ********/
    function {{MAIN_NAME}}() { 
    jQuery(document).ready(function($) { 
        
        var self_script = document.querySelector('script[src="http://api.datahipsters.com{{PATH}}"]');
        /******* Load CSS *******/
            var css_link = $("", { 
        rel: "stylesheet", 
        type: "text/css", 
        href: self_script.getAttribute('css_href') 
            });
            css_link.appendTo('head');          

        /******* Load HTML *******/
        var api_method = self_script.getAttribute('api_method');
        var container_id = self_script.getAttribute('container_id');
        var params = {};
        if (self_script.getAttribute('client_id')!=null) {
            params["client_id"] = encodeURIComponent( self_script.getAttribute('client_id') );
        }
        if (self_script.getAttribute('user_id')!=null) {
            params["user_id"] = encodeURIComponent( self_script.getAttribute('user_id') );
        }
        if (self_script.getAttribute('item_id')!=null) {
            params["item_id"] = encodeURIComponent( self_script.getAttribute('item_id') );
        }
        if (self_script.getAttribute('randomized')!=null) {
            params["randomized"] = encodeURIComponent( self_script.getAttribute('randomized') );
        }
        if (self_script.getAttribute('bucket_id')!=null) {
            params["bucket_id"] = encodeURIComponent( self_script.getAttribute('bucket_id') );
        }
        if (self_script.getAttribute('n')!=null) {
            params["n"] = encodeURIComponent( self_script.getAttribute('n') );
        }
        if (self_script.getAttribute('reco_token')!=null) {
            params["reco_token"] = encodeURIComponent( self_script.getAttribute('reco_token') );
        }
        if (self_script.getAttribute('action_token')!=null) {
            params["action_token"] = encodeURIComponent( self_script.getAttribute('action_token') );
        }
        if (self_script.getAttribute('container_id')!=null) {
            container_id = encodeURIComponent( self_script.getAttribute('container_id') );
        } else {
            container_id = null;
        }
        var url_param = jQuery.param( params );
        var jsonp_url = "http://api.datahipsters.com/"+api_method+"?"+url_param+"&widget=yes&callback=?";
        $.getJSON(jsonp_url, function(data) {
            {{WIDGET_LOADER}}
            });
            
    });
    }

})(); // We call our anonymous function immediately'''
