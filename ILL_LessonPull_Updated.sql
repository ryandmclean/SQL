DECLARE
	@StartDate_Name VARCHAR(MAX),
	@EndDate_Name VARCHAR(MAX),
	@DistrictGuid_Name VARCHAR(MAX)

SET @StartDate_Name = '2021-11-10';
SET @EndDate_Name = '2022-11-20';
SET @DistrictGuid_Name = 'f414d81a-caff-4c46-8e82-a658017382f2';


with 
tbl_roster as (
	SELECT DISTINCT
		District_GUID = LOWER(dist._id),
		School_GUID = LOWER(sch._id),
		Student_GUID = LOWER(studs._id),
		e.SqlStudentDocumentId

	FROM info..Mongo_OrgTree dist 
		INNER JOIN info..Mongo_OrgTree d2 on d2._id = dist.ParentID 
		INNER JOIN info..Mongo_OrgTree sch on sch.ParentID = dist._id
		INNER JOIN info..Mongo_StudentOrganizations StudOrgs on LOWER(StudOrgs.OrganizationID) = LOWER(sch._id)
		INNER JOIN info..Mongo_Students studs on LOWER(studs._id) = LOWER(StudOrgs.StudentID) 
		INNER JOIN  IlCloud..StudentDocument e on e._id = studs._id
	WHERE
		d2.OrgType = 2 and
		dist.OrgType = 0 and
		sch.OrgType = 1 and
		LOWER(dist._id) = @DistrictGuid_Name 
),
tbl_scores as (
	SELECT 
		SqlStudentDocumentId,
		Lesson,
		Activity,
		Activity_Score = 1.0 * SUM(
				CASE 
					WHEN NumPossible = 0 THEN Mastered 
					ELSE NumCorrect
				END
			) / SUM(
				CASE 
					WHEN NumPossible = 0 THEN 1 
					ELSE NumPossible
				END
			)
	FROM ILCloud..Scores
	WHERE 
	[DateTime] >= @StartDate_Name
	and [DateTime] <= DATEADD(dd, 1, @EndDate_Name)  
	and Product = 'ILE'

	GROUP BY SqlStudentDocumentId, Lesson, Activity
),
tbl_lessons as (
	SELECT 
	studs.Student_GUID,
	LData.SqlStudentDocumentId,
	LData.Lesson,
	LData.SatisfiedReason,
	LData.LessonState,
	Lesson_Number = CASE WHEN Strand.StrandID = 4 THEN LNode.OrderID ELSE 16+LNode.OrderID END,
	Lesson_Date = LData.[DateTime],
	Order_Num = ROW_NUMBER() OVER (PARTITION BY studs.Student_GUID, CASE WHEN
			Strand.StrandID in (13,7) THEN 'Language' WHEN
			Strand.StrandID in (4, 8) THEN 'Literacy' WHEN
			Strand.StrandID = 14 THEN 'Grammar' ELSE
			'Other' END ORDER BY LData.[DateTime] DESC),
	Lesson_Type = CASE WHEN
			Strand.StrandID in (13,7) THEN 'Language' WHEN
			Strand.StrandID in (4, 8) THEN 'Literacy' WHEN
			Strand.StrandID = 14 THEN 'Grammar' ELSE
			'Other' END,
	Lesson_Accuracy = AVG(sc.Activity_Score)
	
	FROM tbl_roster studs
	INNER JOIN ILCloud..StudentLessonStateHistoryV2Collection LData on LData.SqlStudentDocumentId = studs.SqlStudentDocumentId
	INNER JOIN Imagine_Helix..SequenceNode sNode on lower(sNode.NodeGUID) = lower(LData.Lesson)
	INNER JOIN Imagine_Helix..LessonNode LNode on LNode.NodeID = sNode.NodeID
	INNER JOIN Imagine_Helix..Strand on Strand.StrandID = LNode.StrandID
	LEFT JOIN tbl_scores sc on sc.SqlStudentDocumentId = studs.SqlStudentDocumentId and sc.Lesson = LData.Lesson
	
	WHERE LData.[DateTime] >= @StartDate_Name 
	and LData.[DateTime] <= DATEADD(dd, 1, @EndDate_Name)   
	and LData.LessonState NOT LIKE '%Skip%' 
	and LData.SatisfiedReason NOT LIKE '%Skip%' 
	and LData.SatisfiedReason NOT LIKE 'SatisfiedByAlternateLesson'
	and Strand.StrandID in (4, 8, 13, 7, 14)

	GROUP BY 
	studs.Student_GUID,
	LData.SqlStudentDocumentId,
	LData.Lesson,
	LData.SatisfiedReason,
	LData.LessonState,
	LNode.OrderID,
	LData.[DateTime],
	Strand.StrandID
)

SELECT 
	a.Student_GUID,
	Last_literacy_lesson = SUM(CASE WHEN b.Lesson_Type = 'Literacy' AND b.Order_Num = 1 THEN b.Lesson_Number ELSE 0 END),
	Overall_Accuracy = AVG(b.Lesson_Accuracy),
	Lessons_Passed_Current = SUM(
		CASE WHEN
			b.Lesson_Accuracy >= .70 THEN
		1 ELSE
		0 END
	),
	Literacy_Lessons_Passed_Current = SUM(
		CASE WHEN
			b.SatisfiedReason = 'StudentAchievement' AND b.Lesson_Type = 'Literacy' THEN
		1 ELSE
		0 END
	),
	Language_Lessons_Passed_Current = SUM(
		CASE WHEN
			b.SatisfiedReason = 'StudentAchievement' AND b.Lesson_Type = 'Language' THEN
		1 ELSE
		0 END
	),
	Grammar_Lessons_Passed_Current = SUM(
		CASE WHEN
			b.SatisfiedReason = 'StudentAchievement' AND b.Lesson_Type = 'Grammar' THEN
		1 ELSE
		0 END
	),
	Literacy_Lessons_Attempted_Current = SUM(
		CASE WHEN
			b.Lesson_Type = 'Literacy' THEN
		1 ELSE
		0 END
	),
	Language_Lessons_Attempted_Current = SUM(
		CASE WHEN
			b.Lesson_Type = 'Language' THEN
		1 ELSE
		0 END
	),
	Grammar_Lessons_Attempted_Current = SUM(
		CASE WHEN
			b.Lesson_Type = 'Grammar' THEN
		1 ELSE
		0 END
	)
	
FROM tbl_roster a
INNER JOIN tbl_lessons b on b.Student_GUID = a.Student_GUID

GROUP BY a.Student_GUID
