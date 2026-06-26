# Changelog
## 1.1.1
  * Streams that return HTTP 403 during discovery are now excluded from the catalog, and discovery only fails when no streams are accessible.
  * Added unit tests for discovery access checks and exclusion behavior.

## 1.1.0
  * Upgraded to Python 3.12, added integration tests, and updated sync logic. [#26](https://github.com/singer-io/tap-taboola/pull/26)
  * Renamed streams (`campaign` → `campaigns`, `Campaign Performance` → `campaign_performance`), fixed replication keys/methods, removed phantom `id`/`created_at` fields not returned by the API, fixed 429 retry handling, and upgraded dependencies.

## 1.0.0
  * Add discovery support [#22](https://github.com/singer-io/tap-taboola/pull/22)

## 0.3.2
  * Bump dependency versions for twistlock compliance [#23](https://github.com/singer-io/tap-taboola/pull/23)

## 0.3.1
  * add campaign_name and conversions_value in campaign_performance stream  [#13](https://github.com/singer-io/tap-taboola/pull/13)
  * update singer-python, backoff, and requests package [#15](https://github.com/singer-io/tap-taboola/pull/15)
