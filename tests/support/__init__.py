"""Test support: the fakes, the fixture-lake builder, the enforcement scanners, and the
child processes the suite spawns.

Nothing here ships in the ``lake`` package. Most of it is the seams the suite injects
and the guards that keep the seams from being bypassed. ``compaction_child`` is the one
exception in kind: it is a runnable entry point that drives production code in a separate
process, because the failure it serves is a real process dying partway and no monkeypatch
inside the pytest process can produce that.
"""
