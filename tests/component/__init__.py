"""Component tier.

One subsystem across exactly one real boundary. These run on every commit. In D0 the
real boundary is one of two things.

1. The real filesystem plus the Parquet and Arrow engines, exercised by the
   fixture-lake builder.
2. The real dependency behind a seam, exercised by the seam's real adapter: the
   operating system clock behind ``SystemClock``, and the ``exchange_calendars``
   library behind ``ExchangeCalendar``.

The placement rule does not name the second case, since production reaches those
dependencies only through the seam. The adapter's own test must use the real
dependency, so it crosses one real boundary and sits here rather than in the unit
tier.
"""
