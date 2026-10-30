-- A business calendar: SALES_DAILY runs on weekdays that are not UK bank holidays. A tick on a
-- holiday is recorded as SKIPPED, naming the calendar and the date (NON_BUSINESS_DAY=SKIP).
--
-- The holiday list must reach at least 30 days ahead, or `etl-craft validate` fails; `doctor`
-- reports one that ends within 60 days. Add each next year's holidays well before then.

INSERT INTO CFG_CALENDARS (PROJECT_ID, CALENDAR_CODE, CALENDAR_NAME, WEEKEND_DAYS)
SELECT PROJECT_ID, 'UK', 'England and Wales bank holidays', 'SAT|SUN' FROM ETL_CRAFT_PROJECT;

INSERT INTO CFG_CALENDAR_HOLIDAYS (CALENDAR_ID, HOLIDAY_DATE, HOLIDAY_NAME)
SELECT c.CALENDAR_ID, date(v.column1), v.column2
FROM CFG_CALENDARS c
CROSS JOIN (VALUES
    ('2026-12-25', 'Christmas Day'),
    ('2026-12-28', 'Boxing Day (substitute day)'),
    ('2027-01-01', 'New Year''s Day'),
    ('2027-03-26', 'Good Friday'),
    ('2027-03-29', 'Easter Monday'),
    ('2027-05-03', 'Early May bank holiday'),
    ('2027-05-31', 'Spring bank holiday'),
    ('2027-08-30', 'Summer bank holiday'),
    ('2027-12-27', 'Christmas Day (substitute day)'),
    ('2027-12-28', 'Boxing Day (substitute day)'),
    ('2028-12-25', 'Christmas Day'),
    ('2028-12-26', 'Boxing Day'),
    ('2029-12-25', 'Christmas Day'),
    ('2029-12-26', 'Boxing Day'),
    ('2030-12-25', 'Christmas Day'),
    ('2030-12-26', 'Boxing Day')
) v
WHERE c.PROJECT_ID = (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT) AND c.CALENDAR_CODE = 'UK';

UPDATE CFG_PIPELINES
SET CALENDAR_ID = (
        SELECT c.CALENDAR_ID FROM CFG_CALENDARS c
        WHERE c.PROJECT_ID = (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT) AND c.CALENDAR_CODE = 'UK'
    ),
    NON_BUSINESS_DAY = 'SKIP'
WHERE PROJECT_ID = (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT) AND PIPELINE_CODE = 'SALES_DAILY';
