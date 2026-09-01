"""Regression check: every route in app.py declares an access policy.

This is the ST-01 guard rail in CI form.  ``create_app()`` performs the same
check at startup, but that needs the whole dependency tree and a live Shopify
session; this parses ``app.py`` instead, so it runs anywhere:

    python -m unittest discover -s web_tools/tests
"""
from __future__ import annotations

import ast
import unittest
from pathlib import Path

APP_PATH = Path(__file__).resolve().parent.parent / "app.py"

_ROUTE_DECORATORS = {"route", "get", "post", "put", "patch", "delete"}


def _module() -> ast.Module:
    return ast.parse(APP_PATH.read_text(encoding="utf-8"), filename=str(APP_PATH))


def _is_route_decorator(node: ast.expr) -> bool:
    """Whether a decorator is an ``@application.<method>(...)`` route."""
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _ROUTE_DECORATORS
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "application"
    )


def route_handlers() -> set[str]:
    """Names of every function registered as a route in app.py."""
    handlers: set[str] = set()
    for node in ast.walk(_module()):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
            _is_route_decorator(d) for d in node.decorator_list
        ):
            handlers.add(node.name)
    return handlers


def declared_policies() -> set[str]:
    """Keys of the module-level ROUTE_POLICIES mapping."""
    for node in _module().body:
        targets = (
            [node.target] if isinstance(node, ast.AnnAssign) else getattr(node, "targets", [])
        )
        if any(isinstance(t, ast.Name) and t.id == "ROUTE_POLICIES" for t in targets):
            value = node.value
            if isinstance(value, ast.Dict):
                return {
                    key.value
                    for key in value.keys
                    if isinstance(key, ast.Constant) and isinstance(key.value, str)
                }
    raise AssertionError("ROUTE_POLICIES was not found in app.py")


class RoutePolicyCoverage(unittest.TestCase):
    def test_every_route_has_a_policy(self):
        missing = sorted(route_handlers() - declared_policies())
        self.assertEqual(
            missing,
            [],
            "These routes have no ROUTE_POLICIES entry and would ship without an "
            f"access decision: {missing}",
        )

    def test_no_stale_policies(self):
        stale = sorted(declared_policies() - route_handlers())
        self.assertEqual(stale, [], f"ROUTE_POLICIES entries with no route: {stale}")

    def test_routes_were_actually_found(self):
        # Guards against the AST matcher silently going blind after a refactor.
        self.assertGreater(len(route_handlers()), 50)


if __name__ == "__main__":
    unittest.main()
