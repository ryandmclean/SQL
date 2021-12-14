DECLARE
	@StartDate_Name VARCHAR(MAX),
	@EndDate_Name VARCHAR(MAX),
	@DistrictGuid_Name VARCHAR(MAX)

SET @StartDate_Name = '2021-10-10';
SET @EndDate_Name = '2021-10-20';
SET @DistrictGuid_Name = '26975237-66d7-486e-a344-abd701468b54';

-- Sum up each activity score
-- Average each activity to get the lesson score

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
		dist._id = @DistrictGuid_Name 
		--and e.SqlStudentDocumentId = '122716218'
),
tbl_scores as (
	SELECT *
	FROM ILCloud..Scores
	WHERE 
	[DateTime] >= @StartDate_Name
	and [DateTime] <= @EndDate_Name 
	and Product = 'Spanish' and
	Skill NOT LIKE '%assessment%' and
	ActivitySkippedType IS NULL and
	LessonBranch = 'Main'
),

tbl_lessons as (
	SELECT 
	studs.Student_GUID,
	LData.SqlStudentDocumentId,
	LData.Lesson,
	LData.SatisfiedReason,
	LData.LessonState,
	Lesson_Number = LNode.OrderID,
	LData.[DateTime],
	Order_Num = ROW_NUMBER() OVER (PARTITION BY LData.SqlStudentDocumentId ORDER BY LData.[DateTime] DESC),
	LNode.[Name],
	sc.Skill,
	sc.NumPossible,
	sc.NumCorrect,
	sc.Mastered,
	sc.[DateTime] as sc_date,
	sc.Activity
	
	FROM tbl_roster studs
	INNER JOIN ILCloud..StudentLessonStateHistoryV2Collection LData on LData.SqlStudentDocumentId = studs.SqlStudentDocumentId
	INNER JOIN Imagine_Helix..SequenceNode sNode on lower(sNode.NodeGUID) = lower(LData.Lesson)
	INNER JOIN Imagine_Helix..LessonNode LNode on LNode.NodeID = sNode.NodeID
	INNER JOIN Imagine_Helix..Strand on Strand.StrandID = LNode.StrandID
	INNER JOIN tbl_scores sc on sc.SqlStudentDocumentId = studs.SqlStudentDocumentId and sc.Lesson = LData.Lesson
	
	WHERE LData.[DateTime] >= @StartDate_Name 
	and LData.[DateTime] <= @EndDate_Name 
	and LData.LessonState NOT LIKE '%Skip%' 
	and LData.SatisfiedReason NOT LIKE '%Skip%' 
	and LData.SatisfiedReason NOT LIKE 'SatisfiedByAlternateLesson'
	and Strand.StrandID = 26
	and Strand.Enum like '%Spanish%' and
	LNode.[Name] NOT LIKE '%review%'

)

SELECT 
	b.*
FROM tbl_roster a
INNER JOIN tbl_lessons b on b.Student_GUID = a.Student_GUID

ORDER BY b.[Name], b.NumCorrect
