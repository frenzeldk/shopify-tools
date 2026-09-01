"""Behavioural tests for the fail-closed access gate and browser defenses.

    python -m unittest discover -s web_tools/tests
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

os.environ.setdefault("FLASK_SECRET_KEY", "x" * 48)
os.environ.setdefault("SESSION_COOKIE_SECURE", "0")

from flask import Blueprint, Flask, jsonify  # noqa: E402

import security  # noqa: E402


def build_app(*, authenticated: bool, extra_route: bool = False) -> Flask:
    app = Flask(__name__)
    security.configure(app)

    # Stand-in for the flask-oidc blueprint so url_for("oidc_auth.login") resolves.
    oidc_auth = Blueprint("oidc_auth", __name__)
    oidc_auth.add_url_rule("/login", "login", lambda: "login")
    oidc_auth.add_url_rule("/authorize", "authorize", lambda: "authorize")
    oidc_auth.add_url_rule("/logout", "logout", lambda: "logout")
    app.register_blueprint(oidc_auth)

    @app.route("/page/")
    def page():
        return "<html></html>"

    @app.post("/api/")
    def api():
        return jsonify({"ok": True})

    if extra_route:
        @app.get("/forgotten/")
        def forgotten():
            return jsonify({"ok": True})

    policies = {
        "page": security.RoutePolicy(security.ROLE_READ, security.LIMIT_READ, html=True),
        "api": security.RoutePolicy(security.ROLE_READ, security.LIMIT_WRITE),
    }
    security.install(app, policies=policies, is_authenticated=lambda: authenticated)
    app.config["_policies"] = policies
    return app


class AnonymousAccess(unittest.TestCase):
    def setUp(self):
        self.client = build_app(authenticated=False).test_client()

    def test_html_page_redirects_to_login(self):
        response = self.client.get("/page/")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers["Location"])

    def test_json_api_gets_401_not_a_redirect(self):
        response = self.client.post("/api/", json={})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["error"], "Authentication required.")

    def test_unknown_path_is_also_closed(self):
        # Fail closed: an unmatched path must not reveal that it is a 404.
        self.assertEqual(self.client.get("/no-such-route/").status_code, 401)

    def test_static_and_login_stay_public(self):
        self.assertEqual(self.client.get("/login").status_code, 200)
        self.assertEqual(self.client.get("/authorize").status_code, 200)


class AuthenticatedAccess(unittest.TestCase):
    def setUp(self):
        self.client = build_app(authenticated=True).test_client()

    def test_post_without_origin_is_rejected(self):
        response = self.client.post("/api/", json={})
        self.assertEqual(response.status_code, 403)

    def test_post_from_a_foreign_origin_is_rejected(self):
        response = self.client.post(
            "/api/", json={}, headers={"Origin": "https://evil.example"}
        )
        self.assertEqual(response.status_code, 403)

    def test_same_origin_post_succeeds(self):
        response = self.client.post(
            "/api/", json={}, headers={"Origin": "https://localhost"}
        )
        self.assertEqual(response.status_code, 200)

    def test_referer_is_accepted_as_a_fallback(self):
        response = self.client.post(
            "/api/", json={}, headers={"Referer": "https://localhost/page/"}
        )
        self.assertEqual(response.status_code, 200)

    def test_get_needs_no_origin(self):
        self.assertEqual(self.client.get("/page/").status_code, 200)

    def test_rate_limit_returns_429_with_retry_after(self):
        headers = {"Origin": "https://localhost"}
        statuses = [
            self.client.post("/api/", json={}, headers=headers).status_code
            for _ in range(security.LIMIT_WRITE.count + 2)
        ]
        self.assertEqual(statuses[0], 200)
        self.assertEqual(statuses[-1], 429)
        response = self.client.post("/api/", json={}, headers=headers)
        self.assertIn("Retry-After", response.headers)


class SecurityHeaders(unittest.TestCase):
    def test_headers_are_present_on_every_response(self):
        client = build_app(authenticated=True).test_client()
        response = client.get("/page/")
        self.assertIn("frame-ancestors 'none'", response.headers["Content-Security-Policy"])
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(response.headers["X-Frame-Options"], "DENY")
        self.assertEqual(response.headers["Referrer-Policy"], "same-origin")


class SessionConfiguration(unittest.TestCase):
    def test_weak_secret_key_is_refused(self):
        app = Flask(__name__)
        saved = os.environ.get("FLASK_SECRET_KEY")
        os.environ["FLASK_SECRET_KEY"] = "short"
        try:
            with self.assertRaises(RuntimeError):
                security.configure(app)
        finally:
            os.environ["FLASK_SECRET_KEY"] = saved

    def test_cookie_flags(self):
        app = Flask(__name__)
        os.environ["SESSION_COOKIE_SECURE"] = "1"
        try:
            security.configure(app)
        finally:
            os.environ["SESSION_COOKIE_SECURE"] = "0"
        self.assertTrue(app.config["SESSION_COOKIE_SECURE"])
        self.assertTrue(app.config["SESSION_COOKIE_HTTPONLY"])
        self.assertEqual(app.config["SESSION_COOKIE_SAMESITE"], "Lax")
        self.assertEqual(app.config["MAX_CONTENT_LENGTH"], security.max_request_bytes())


class OidcEndpointNames(unittest.TestCase):
    """PUBLIC_ENDPOINTS is a literal list of flask-oidc's endpoint names.

    If an upgrade renames or adds one, the handshake would start returning 401
    (or a new route would become anonymous), so pin the names here.
    """

    def test_public_endpoints_match_the_extension(self):
        from flask_oidc import OpenIDConnect

        app = Flask(__name__)
        app.config["OIDC_ENABLED"] = False
        security.configure(app)
        OpenIDConnect(app)
        registered = {rule.endpoint for rule in app.url_map.iter_rules()}
        self.assertEqual(registered - security.PUBLIC_ENDPOINTS, set())
        # And nothing in the allowlist has silently disappeared.
        self.assertEqual(security.PUBLIC_ENDPOINTS - registered, set())


class RouteAudit(unittest.TestCase):
    def test_unpoliced_route_stops_startup(self):
        app = build_app(authenticated=True, extra_route=True)
        with self.assertRaises(RuntimeError) as ctx:
            security.audit_routes(app, app.config["_policies"])
        self.assertIn("forgotten", str(ctx.exception))

    def test_fully_policed_app_passes(self):
        app = build_app(authenticated=True)
        security.audit_routes(app, app.config["_policies"])


if __name__ == "__main__":
    unittest.main()
