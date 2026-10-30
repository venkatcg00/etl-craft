"""Business calendars skip or move closed-day ticks and find business days of the month."""

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import pytest

from etl_craft.core.calendars import BusinessCalendar, NonBusinessDay, Schedule, parse_weekend
from etl_craft.core.cron import business_rule, next_after, parse
from etl_craft.core.errors import MetadataError

pytestmark = pytest.mark.unit

LONDON = ZoneInfo("Europe/London")
UK = BusinessCalendar(
    "UK",
    frozenset({5, 6}),
    {date(2026, 12, 25): "Christmas Day", date(2026, 12, 28): "Boxing Day (substitute)"},
)


def ticks(schedule, start, count):
    found, instant = [], start
    for _ in range(count):
        tick = schedule.next_tick(instant)
        found.append(tick)
        instant = tick.instant
    return found


def utc(*parts):
    return datetime(*parts, tzinfo=UTC)


def test_skip_records_closed_days_with_the_calendar_and_date():
    schedule = Schedule("0 6 * * *", LONDON, UK, NonBusinessDay.SKIP)
    found = ticks(schedule, utc(2026, 12, 24, 7), 5)
    assert [(t.run_date, t.skipped) for t in found] == [
        (date(2026, 12, 25), "calendar UK: 2026-12-25 is a holiday (Christmas Day)"),
        (date(2026, 12, 26), "calendar UK: 2026-12-26 is a weekend day (SAT)"),
        (date(2026, 12, 27), "calendar UK: 2026-12-27 is a weekend day (SUN)"),
        (date(2026, 12, 28), "calendar UK: 2026-12-28 is a holiday (Boxing Day (substitute))"),
        (date(2026, 12, 29), None),
    ]
    assert found[-1].instant == utc(2026, 12, 29, 6)


def test_next_business_day_moves_closed_ticks_and_merges_them_with_that_days_own():
    schedule = Schedule("0 6 * * *", LONDON, UK, NonBusinessDay.NEXT_BUSINESS_DAY)
    found = ticks(schedule, utc(2026, 12, 24, 7), 2)
    assert [(t.instant, t.run_date, t.skipped) for t in found] == [
        (utc(2026, 12, 29, 6), date(2026, 12, 29), None),
        (utc(2026, 12, 30, 6), date(2026, 12, 30), None),
    ]


def test_moved_sub_daily_ticks_stay_in_order():
    schedule = Schedule("0 */6 * * *", LONDON, UK, NonBusinessDay.NEXT_BUSINESS_DAY)
    found = ticks(schedule, utc(2026, 12, 24, 19), 5)
    assert [t.instant for t in found] == [
        utc(2026, 12, 29, 0),
        utc(2026, 12, 29, 6),
        utc(2026, 12, 29, 12),
        utc(2026, 12, 29, 18),
        utc(2026, 12, 30, 0),
    ]


def test_a_weekday_schedule_with_next_business_day_does_not_run_on_the_moved_day_twice():
    schedule = Schedule("0 5 * * 1-5", LONDON, UK, NonBusinessDay.NEXT_BUSINESS_DAY)
    found = ticks(schedule, utc(2026, 12, 24, 6), 3)
    assert [t.run_date for t in found] == [
        date(2026, 12, 29),
        date(2026, 12, 30),
        date(2026, 12, 31),
    ]


def test_nth_and_last_business_days_of_the_month():
    third = Schedule("0 6 3BD * *", LONDON, UK)
    last = Schedule("0 6 LBD * *", LONDON, UK)
    # 1-2 Jan 2027 are Fri and the 4th is Mon: business days 1, 4, 5.
    january = BusinessCalendar("UK", frozenset({5, 6}), {date(2027, 1, 1): "New Year's Day"})
    assert Schedule("0 6 3BD * *", LONDON, january).next_tick(utc(2027, 1, 1)).run_date == date(
        2027, 1, 6
    )
    assert third.next_tick(utc(2026, 12, 1)).run_date == date(2026, 12, 3)
    # 31 Dec 2026 is a Thursday: the last business day.
    assert last.next_tick(utc(2026, 12, 1)).run_date == date(2026, 12, 31)
    # 31 Jan 2027 is a Sunday: the last business day is Friday the 29th.
    assert last.next_tick(utc(2027, 1, 1)).run_date == date(2027, 1, 29)
    assert all(t.skipped is None for t in ticks(last, utc(2026, 1, 1), 12))


def test_a_tick_moved_into_a_daylight_saving_gap_fires_at_the_next_valid_minute():
    new_york = ZoneInfo("America/New_York")
    calendar = BusinessCalendar("US", frozenset(), {date(2026, 3, 7): "closed"})
    schedule = Schedule("30 2 * * *", new_york, calendar, NonBusinessDay.NEXT_BUSINESS_DAY)
    tick = schedule.next_tick(utc(2026, 3, 6, 8))
    # 02:30 on 8 March 2026 does not exist in New York; 03:00 EDT is 07:00 UTC.
    assert (tick.run_date, tick.instant) == (date(2026, 3, 8), utc(2026, 3, 8, 7))


def test_a_schedule_whose_every_tick_is_skipped_has_no_first_run():
    christmas = Schedule("0 6 25 12 *", LONDON, UK)
    assert christmas.first_run(utc(2026, 1, 1), 366) is None
    assert Schedule("0 6 24 12 *", LONDON, UK).first_run(utc(2026, 1, 1), 366) is not None


@pytest.mark.parametrize(
    ("expr", "message"),
    [
        ("0 6 3BD * MON", "needs day-of-week"),
        ("0 6 24BD * *", "1BD to 23BD or LBD"),
        ("0 6 3BD,5 * *", "1BD to 23BD or LBD"),
    ],
)
def test_bad_business_day_rules_are_refused(expr, message):
    with pytest.raises(MetadataError, match=message):
        business_rule(expr)
    with pytest.raises(MetadataError, match=message):
        parse(expr)


def test_business_day_rules_need_a_calendar():
    assert business_rule("0 6 2BD,LBD * *") == (frozenset({2}), True)
    with pytest.raises(MetadataError, match="name the pipeline's CALENDAR"):
        next_after("0 6 LBD * *", utc(2026, 1, 1), "UTC")


@pytest.mark.parametrize(
    ("written", "weekend"),
    [(None, frozenset()), ("", frozenset()), ("sat|SUN", frozenset({5, 6})), ("FRI", {4})],
)
def test_weekend_days(written, weekend):
    assert parse_weekend("UK", written) == weekend


@pytest.mark.parametrize("written", ["SAT|SAT", "SATURDAY", "MON|TUE|WED|THU|FRI|SAT|SUN"])
def test_bad_weekend_days_are_refused(written):
    with pytest.raises(MetadataError, match="calendar UK: WEEKEND_DAYS"):
        parse_weekend("UK", written)
