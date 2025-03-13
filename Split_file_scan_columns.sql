WITH SplitData AS (
    SELECT 
        [file_scan],
        CHARINDEX('.', [file_scan]) AS Pos1,
        CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan]) + 1) AS Pos2,
        CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan]) + 1) + 1) AS Pos3,
        CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan]) + 1) + 1) + 1) AS Pos4,
        CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan]) + 1) + 1) + 1) + 1) AS Pos5,
        CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan]) + 1) + 1) + 1) + 1) + 1) AS Pos6,
        CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan], CHARINDEX('.', [file_scan]) + 1) + 1) + 1) + 1) + 1) + 1) AS Pos7
    FROM AFS_Volumes_Info
)
SELECT [file_scan],
    CASE WHEN Pos1 > 0 THEN LEFT([file_scan], Pos1 - 1) ELSE [file_scan] END AS Part1,
    CASE WHEN Pos1 > 0 AND Pos2 > Pos1 THEN SUBSTRING([file_scan], Pos1 + 1, Pos2 - Pos1 - 1) ELSE NULL END AS Part2,
    CASE WHEN Pos2 > 0 AND Pos3 > Pos2 THEN SUBSTRING([file_scan], Pos2 + 1, Pos3 - Pos2 - 1) ELSE NULL END AS Part3,
    CASE WHEN Pos3 > 0 AND Pos4 > Pos3 THEN SUBSTRING([file_scan], Pos3 + 1, Pos4 - Pos3 - 1) ELSE NULL END AS Part4,
    CASE 
        WHEN Pos4 > 0 AND Pos5 > Pos4 AND LEN(SUBSTRING([file_scan], Pos4 + 1, Pos5 - Pos4 - 1)) = 2 THEN SUBSTRING([file_scan], Pos4 + 1, Pos6 - Pos4 - 1)
        WHEN Pos4 > 0 AND Pos5 > Pos4 THEN SUBSTRING([file_scan], Pos4 + 1, Pos5 - Pos4 - 1)
        ELSE NULL 
    END AS Part5,
    CASE 
        WHEN Pos4 > 0 AND Pos5 > Pos4 AND LEN(SUBSTRING([file_scan], Pos4 + 1, Pos5 - Pos4 - 1)) = 2 THEN SUBSTRING([file_scan], Pos6 + 1, Pos7 - Pos6 - 1)
        WHEN Pos5 > 0 AND Pos6 > Pos5 THEN SUBSTRING([file_scan], Pos5 + 1, Pos6 - Pos5 - 1)
        ELSE NULL 
    END AS Part6,
    CASE 
        WHEN Pos4 > 0 AND Pos5 > Pos4 AND LEN(SUBSTRING([file_scan], Pos4 + 1, Pos5 - Pos4 - 1)) = 2 THEN SUBSTRING([file_scan], Pos7 + 1, LEN([file_scan]) - Pos7)
        WHEN Pos6 > 0 AND Pos7 > Pos6 THEN SUBSTRING([file_scan], Pos6 + 1, LEN([file_scan]) - Pos6)
        ELSE NULL 
    END AS Part7
FROM SplitData;