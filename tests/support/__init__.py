"""Test support: the fakes, the fixture-lake builder, the enforcement scanners, and the
child processes the suite spawns.

Nothing here ships in the ``lake`` package. Most of it is the seams the suite injects
and the guards that keep the seams from being bypassed. ``compaction_child`` is the one
exception in kind: it is a runnable entry point that drives production code in a separate
process. Two integration tests need one, and for different reasons. The kill test needs a
real process to die partway, which no monkeypatch inside the pytest process can produce.
The lock test needs a second process because the race the lake-root lock settles is
between processes. That module also holds the parent's side of the arrangement, the spawn
and the reader for the child's milestone lines, so the two tests start the child the same
way and read it the same way.
"""
