# Commanded logs every command and event at debug level, which buries the
# results, and its processes log again while the supervision tree shuts down
# after a test. Tests capture the info logs they assert on themselves, and the
# one test that asserts on a debug line raises the level for its own run.
Logger.configure(level: :info)
ExUnit.start(capture_log: true)
