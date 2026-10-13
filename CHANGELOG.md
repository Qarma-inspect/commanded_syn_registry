# Changelog

## Unreleased

- The adapter installs `Commanded.Registration.SynRegistry.ConflictResolution`
  as the syn event handler of each Commanded application's scope, with
  `:syn.set_event_handler/2`, and sets no other handler in syn.
- Breaking: `ConflictResolution.adapter_scope?/1` is removed. syn no longer
  sends the adapter's scopes to a syn event handler of your own, so delete
  the branch that passed them back to the adapter. Remove
  `Commanded.Registration.SynRegistry.ConflictResolution` from
  `config :syn, event_handler:` if you set it there. It no longer checks the
  scope, so it would decide conflicts in your own scopes: by the adapter's
  rule when both registrations carry `:started_at` in a metadata map, and
  otherwise by freeing the clashing name on both nodes and stopping both
  processes. `resolve_registry_conflict/4` and
  `on_process_unregistered/5` apply the adapter's rule to whatever scope
  they are called with.
- Requires a syn with `:syn.set_event_handler/2`, proposed in
  [ostinelli/syn#93](https://github.com/ostinelli/syn/pull/93). Until it is
  released, this branch takes syn from
  [Qarma-inspect/syn](https://github.com/Qarma-inspect/syn) at commit
  260ea0b.

## v0.1.0

Initial release.

- `Commanded.Registration.SynRegistry`, a Commanded registry adapter that
  keeps the names of aggregates, event handlers and process managers in syn,
  one scope per Commanded application.
- Each event handler and process manager runs on one node of the cluster at
  a time. The other nodes proxy it and take it over when its node stops or
  is lost; its own exits reach the supervisors on every node.
- When a network partition heals and a name turns up on two nodes, the older
  registration keeps it.
- Requires Elixir 1.15 or later, OTP 26 or later, `commanded` 1.4.11 or later
  within 1.x and `syn` 3.4.2 or later within 3.x. Tested on Elixir 1.15 to
  1.20 and OTP 26 to 29.
