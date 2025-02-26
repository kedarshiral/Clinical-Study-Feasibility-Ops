from flask import make_response, jsonify
import utilities.CommonServicesConstants as CommonServicesConstants
import gzip
import json

class Response():
    def make_response_json(self, idm_response=None, response_json=None, compression=False):
        if compression:
            content = gzip.compress(json.dumps(response_json).encode('utf-8'), compresslevel=8)

            response = make_response(content)
            response.headers["Content-Encoding"] = "gzip"
            response.headers["Content-Type"] = "gzip"
            response.headers["Accept-Encoding"] = "gzip"
        else:
            response = make_response(jsonify(response_json))
        self.set_cookies(idm_response=idm_response, response=response)
        return response

    def set_cookies(self, idm_response=None, response=None):
        session_details = idm_response[CommonServicesConstants.SESSION_DETAIL] if idm_response is not None else None
        oauth_cookie = idm_response[CommonServicesConstants.OAUTH_COOKIE] if idm_response is not None else None
        if session_details is not None:
            if "session_alive_cookie" in session_details:
                session_cookie = session_details["session_alive_cookie"]
                response.set_cookie(session_cookie.get("session_alive_cookie_name", None),
                                    session_cookie.get("idm_session_alive_token", None),
                                    expires=session_cookie.get("expiry_time", None),
                                    domain=session_cookie.get("domain", None),
                                    secure=True, httponly=True)
            if "form_cookie" in session_details:
                form_cookie = session_details["form_cookie"]
                response.set_cookie(form_cookie.get("name", None),
                                    form_cookie.get("value", None),
                                    domain=form_cookie.get("domain", None),
                                    secure=True, httponly=True)
        if oauth_cookie is not None:
            response.set_cookie(oauth_cookie.get("name", None),
                                oauth_cookie.get("value", None),
                                expires=oauth_cookie.get("expiration_time", None),
                                domain=oauth_cookie.get("domain", None),
                                secure=True, httponly=True)
