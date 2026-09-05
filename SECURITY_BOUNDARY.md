# Security / IP Boundary

Public review is intentionally bounded.

Allowed: source control flow, identities, queues, durable accounting, authority contracts, lifecycle and projection logic needed to investigate defects.

Excluded: trained model implementation/artifacts, private feature recipe/calibration, secret values, credentials, account configuration, live evidence and user-specific data.

A reviewer finding does not require access to these excluded assets unless the suspected defect is specifically about model quality or an external provider/account boundary.
