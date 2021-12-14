{{ config(
    pre_hook = "set st_date = '2021-08-01';
      set en_date = '2021-09-02';
      set dist_id = '7aaa92f2-c3f8-4e82-b48b-a62c011c6323';"
)}}

with
tbl_roster as
(
  SELECT
    dist.name           as district_name,
    dist.organizationID as district_guid,
    sch.name            as school_name,
    sch.organizationID  as school_guid,
    CASE
        WHEN info.grade_level = 'PreK' THEN -1
        WHEN info.grade_level = 'Kindergarten' THEN 0
        WHEN info.grade_level = 'First' THEN 1
        WHEN info.grade_level = 'Second' THEN 2
        WHEN info.grade_level = 'Third' THEN 3
        WHEN info.grade_level = 'Fourth' THEN 4
        WHEN info.grade_level = 'Fifth' THEN 5
        WHEN info.grade_level = 'Sixth' THEN 6
        WHEN info.grade_level = 'Seventh' THEN 7
        WHEN info.grade_level = 'Eighth' THEN 8
        WHEN info.grade_level = 'Ninth' THEN 9
        WHEN info.grade_level = 'Tenth' THEN 10
        WHEN info.grade_level = 'Eleventh' THEN 11
        WHEN info.grade_level = 'Twelfth' THEN 12
        WHEN info.grade_level = 'Other' THEN 13
        ELSE NULL
    END as grade,
    stud.studentid      as student_id,
    info.first_name     as student_firstname,
    info.last_name      as student_lastname,
    info.first_language as student_language,
    info.student_number as student_number,
    info.sync_id        as alternate_student_number,
    info.tag            as student_tag

  FROM "WELD_NORTH_PROD"."IMAGINE_LEARNING"."REDSHIFT_REPORTSDB_TBL_ORGANIZATION" dist
    join "WELD_NORTH_PROD"."IMAGINE_LEARNING"."REDSHIFT_REPORTSDB_TBL_ORGANIZATION" sch  on sch.parent = dist.organizationID
    join "WELD_NORTH_PROD"."IMAGINE_LEARNING"."REDSHIFT_REPORTSDB_TBL_STUDENTORGANIZATIONS" stud on stud.organizationID = sch.organizationID
    join "WELD_NORTH_PROD"."IMAGINE_LEARNING"."MONGO_MANAGER_STUDENT" info on info.id = stud.studentid
  WHERE
    dist.organizationID = $dist_id
    and sch.orgtype = 'School'
),
tbl_usage as
(
  SELECT
          a.student_id,
          MIN(usage_date) as first_sessiondate,
          MAX(usage_date) as last_sessiondate,
          sum(a.session_seconds)
            /60.0             as total_session_minutes
    FROM "WELD_NORTH_PROD"."IMAGINE_LEARNING"."ELASTICSEARCH_ESREPORTS_DAILY_USAGE" a
    join tbl_roster r on r.student_id = a.student_id
    WHERE usage_date between $st_date and $en_date
        AND PRODUCT_TAG = 'ILE'
    GROUP BY 1
),
tbl_lessons as
(
  SELECT
      a.student_id,
      a.progress_area,
      a.lesson_grade_level,
      a.lesson_name,
      a.lesson_order,
      a.completed,
      a.passed,
      a.date_updated,
      ROW_NUMBER() OVER (PARTITION BY a.student_id, a.progress_area ORDER BY a.date_updated) as taken_order,
      ROW_NUMBER() OVER (PARTITION BY a.student_id, a.progress_area ORDER BY a.date_updated DESC) as rev_taken_order,
      CASE
            WHEN a.lesson_grade_level = 'PreK' THEN -1
            WHEN a.lesson_grade_level = 'Kindergarten' THEN 0
            WHEN a.lesson_grade_level = 'First' THEN 1
            WHEN a.lesson_grade_level = 'Second' THEN 2
            WHEN a.lesson_grade_level = 'Third' THEN 3
            WHEN a.lesson_grade_level = 'Fourth' THEN 4
            WHEN a.lesson_grade_level = 'Fifth' THEN 5
            WHEN a.lesson_grade_level = 'Sixth' THEN 6
            WHEN a.lesson_grade_level = 'Seventh' THEN 7
            WHEN a.lesson_grade_level = 'Eighth' THEN 8
            WHEN a.lesson_grade_level = 'Ninth' THEN 9
            WHEN a.lesson_grade_level = 'Tenth' THEN 10
            WHEN a.lesson_grade_level = 'Eleventh' THEN 11
            WHEN a.lesson_grade_level = 'Twelfth' THEN 12
            WHEN a.lesson_grade_level = 'Other' THEN 13
            ELSE NULL
         END as grade_numeric
  FROM "WELD_NORTH_PROD"."IMAGINE_LEARNING"."ELASTICSEARCH_ESREPORTS_PROGRESS_BY_LESSON" a
  JOIN tbl_roster r on r.student_id = a.student_id
  WHERE
      a.date_updated between $st_date and $en_date AND
      NOT a.skipped
),
tbl_lessons_stud as
(
  SELECT
    a.student_id,
    // Even though each of these only returns a single value, an aggregation is required for the group by
    SUM(CASE WHEN a.taken_order = 1 AND a.progress_area = 'Reading' THEN a.lesson_order ELSE NULL END) as literacy_first,
    SUM(CASE WHEN a.rev_taken_order = 1 AND a.progress_area = 'Reading' THEN a.lesson_order ELSE NULL END) as literacy_last,
    SUM(CASE WHEN a.taken_order     = 1 AND a.progress_area = 'Reading' THEN a.grade_numeric ELSE NULL END) as grade_level_first,
    SUM(CASE WHEN a.rev_taken_order = 1 AND a.progress_area = 'Reading' THEN a.grade_numeric ELSE NULL END) as grade_level_latest,
    COUNT(*) as lessons_worked,
    1.0 * SUM(CASE WHEN a.passed = 'TRUE' THEN 1 ELSE 0 END) as lessons_passed,
    1.0 * SUM(CASE WHEN a.passed = 'TRUE' THEN 1 ELSE 0 END) / COUNT(*) as pass_rate,
    1.0 * SUM(CASE WHEN a.passed = 'TRUE' and a.progress_area = 'Reading' THEN 1 ELSE 0 END) as Literacy_Lessons_Passed,
    1.0 * SUM(CASE WHEN a.passed = 'TRUE' and a.progress_area = 'Vocabulary' THEN 1 ELSE 0 END) as Language_Lessons_Passed,
    1.0 * SUM(CASE WHEN a.passed = 'TRUE' and a.progress_area = 'Grammar' THEN 1 ELSE 0 END) as Grammar_Lessons_Passed,
    1.0 * SUM(CASE WHEN a.progress_area = 'Reading' THEN 1 ELSE 0 END) as Literacy_Lessons_Attempted,
    1.0 * SUM(CASE WHEN a.progress_area = 'Vocabulary' THEN 1 ELSE 0 END) as Language_Lessons_Attempted,
    1.0 * SUM(CASE WHEN a.progress_area = 'Grammar' THEN 1 ELSE 0 END) as Grammar_Lessons_Attempted
  FROM tbl_lessons a
  GROUP BY 1
)
SELECT
   a.*,
    b.first_sessiondate,
    b.last_sessiondate,
    b.total_session_minutes,
    c.lessons_worked,
    c.lessons_passed,
    c.pass_rate,
    c.literacy_first,
    c.literacy_last,
    c.grade_level_first,
    c.grade_level_latest,
    c.Literacy_Lessons_Passed,
    c.Language_Lessons_Passed,
    c.Grammar_Lessons_Passed,
    c.Literacy_Lessons_Attempted,
    c.Language_Lessons_Attempted,
    c.Grammar_Lessons_Attempted
      
  FROM tbl_roster a 
  JOIN tbl_usage b on b.student_id = a.student_id
  LEFT JOIN tbl_lessons_stud c on c.student_id = a.student_id

ORDER BY
    a.district_name,
    a.school_name,
    a.grade
