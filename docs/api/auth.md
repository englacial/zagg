# Auth

Authentication helper for NASA Earthdata S3 access. Call [`get_nsidc_s3_credentials`][zagg.auth.get_nsidc_s3_credentials] once in your orchestrator before invoking workers --- credentials are valid for approximately 1 hour.

::: zagg.auth.get_nsidc_s3_credentials
