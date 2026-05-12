"""
Project Horizon — tenant ID resolution.

Phase 0.4 per ADR-002. Resolves the tenant_id for each incoming request
in priority order:

  1. Host header → tenant_id lookup against the hardcoded fallback map
  2. HORIZON_TENANT_ID environment variable
  3. Default: 'ams-demo'

Operational code does NOT yet consume tenant_id — this module exists so
that subsequent Phase 0 steps (audit emission, session events, etc.)
have a canonical resolver to call. Beta 10 behaviour is unchanged in
this step: the resolved value is attached to the request handler and
optionally logged at DEBUG, but no other code path uses it yet.

A later Phase 0 step will optionally augment the hardcoded map with
config.tenant.allowed_hostnames once DATABASE_URL is operational. The
hardcoded map is the safe baseline that works without Postgres.
"""

import logging
import os

log = logging.getLogger("horizon.tenant")


# Phase 0.4 hardcoded host → tenant mapping.
#
# Both current production hosts map to the AMS demo tenant. This matches
# the deployment shape: Beta 10 is the AMS demo tenant's running
# instance, served on both app.horizon.ams.group (operational) and
# horizon.ams.group (marketing — also routes through this server via
# the _serve_site path).
_HOST_TO_TENANT: dict[str, str] = {
    "app.horizon.ams.group": "ams-demo",
    "horizon.ams.group":     "ams-demo",
}

# Default fallback when neither Host nor env var resolves.
_DEFAULT_TENANT = "ams-demo"

# Optional environment override for deployments not yet in the host map
# (e.g. AMS-internal QA tenants, future per-customer deployments).
# Read once at module import.
_ENV_TENANT: str | None = os.environ.get("HORIZON_TENANT_ID", "").strip() or None


def resolve_tenant_id(host_header: str) -> str:
    """
    Return the tenant_id for this request.

    Resolution order:
      1. Host header lookup against _HOST_TO_TENANT (port stripped,
         hostname lowercased)
      2. HORIZON_TENANT_ID environment variable, if set
      3. _DEFAULT_TENANT ('ams-demo')

    Parameters
    ----------
    host_header : str
        The raw Host header value from the request (may include port).

    Returns
    -------
    str
        The resolved tenant_id. Never None; never empty.
    """
    if host_header:
        host = host_header.split(":")[0].strip().lower()
        if host in _HOST_TO_TENANT:
            return _HOST_TO_TENANT[host]
    if _ENV_TENANT:
        return _ENV_TENANT
    return _DEFAULT_TENANT


def log_startup_mapping() -> None:
    """
    Log the configured tenant resolution mapping at server startup.

    Called once from server.py's __main__ block. Produces a single
    INFO line listing the host map size, env var state, and fallback.
    Provides operational visibility into what mappings are active
    without per-request log noise.
    """
    log.info(
        "tenant resolution configured: %d host(s) mapped, env=%s, fallback=%s",
        len(_HOST_TO_TENANT),
        _ENV_TENANT or "(unset)",
        _DEFAULT_TENANT,
    )


def get_host_mapping() -> dict[str, str]:
    """
    Return a copy of the host → tenant_id mapping.

    Used by tests and by future code paths that may want to enumerate
    valid hosts. Returns a copy so callers cannot mutate the source.
    """
    return dict(_HOST_TO_TENANT)
