# Changelog

## v0.1.0

Initial release.

- `Commanded.Registration.SynRegistry`, a Commanded registry adapter that
  keeps the names of aggregates, event handlers and process managers in syn,
  one scope per Commanded application.
- Each event handler and process manager runs on one node of the cluster at
  a time. The other nodes proxy it and take over when it stops.
- When a network partition heals and a name turns up on two nodes, the older
  registration keeps it.
- Requires Elixir 1.15 or later, OTP 26 or later, `commanded` 1.4.11 or later
  within 1.x and `syn` 3.4.2 or later within 3.x. Tested on Elixir 1.15 to
  1.20 and OTP 26 to 29.
