# =============================================================
# SECURITY TESTS — BI connection authority guard (Priority 1)
# =============================================================
# assert_role_allowed() is the pure, network-free half of the BI
# least-privilege enforcement: it decides whether a configured
# role is even permitted before any socket opens. Testing it here
# means the POLICY (never run the BI surface as an admin role) is
# verified in CI with no credentials; the live single-role session
# assertion is demonstrated separately against Snowflake.
# =============================================================


import pytest

from fraud_platform.bi_dashboard.connection import assert_role_allowed, InsecureConnectionError  # noqa: E402


class TestRoleRefusal:
    @pytest.mark.parametrize("role", [
        "ACCOUNTADMIN", "accountadmin", "AccountAdmin",
        "ORGADMIN", "SECURITYADMIN", "SYSADMIN",
    ])
    def test_admin_roles_refused(self, role):
        # a misconfigured BI_SNOWFLAKE_ROLE=ACCOUNTADMIN must fail
        # loudly BEFORE connecting, regardless of casing
        with pytest.raises(InsecureConnectionError):
            assert_role_allowed(role)

    @pytest.mark.parametrize("role", ["", "   ", None])
    def test_empty_role_refused(self, role):
        with pytest.raises(InsecureConnectionError):
            assert_role_allowed(role)

    def test_bi_role_allowed(self):
        # the sanctioned least-privilege role passes the guard
        assert_role_allowed("BI_ROLE")

    def test_arbitrary_readonly_role_allowed(self):
        # non-admin custom roles are permitted — the guard blocks
        # privilege escalation, not role naming
        assert_role_allowed("ANALYST_ROLE")
