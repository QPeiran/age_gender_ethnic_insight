
# “raw_main_table”(Data8277.csv) has attribute “Count” but some of them are not a number but shown as “..C” 
# – better to delete this tuple rather tha replace the to 0
DELETE FROM `countdownintervewtest`.CDI_test.raw_main_table WHERE Count = "..C"


#Filter census data from 2018. Choose Age, Ethnic, Area as analysis factors
SELECT * FROM `countdownintervewtest.CDI_test.raw_main_table`
WHERE (Area IN ("01", "02", "03", "04", "05", "06", "07", "08", "09", "12", "13", "14", "15", "16", "17", "18")
    AND (Age IN ("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21"))
    AND (Sex = "9") # total sex
    AND (Ethnic = "9999")   # total ethnic
    AND (Year = "2018") # year 2018

#Join raw_main_tabel with look_up tables
SELECT Description_age, Description_ethnic, Description_area, Count_NEW FROM `countdownintervewtest.data_model.prepared_main_table` a
LEFT JOIN `countdownintervewtest`.CDI_test.lookup_age_table b
ON (a.Age = b.Code_age)
LEFT JOIN `countdownintervewtest`.CDI_test.lookup_ethnic_table c
ON (a.Ethnic = c.Code_ethnic)
LEFT JOIN `countdownintervewtest`.CDI_test.lookup_area_table d
ON (a.Area = d.Code_area)

# Align income table's "partial key" with prepared table's "partial key"

SELECT *,
  CASE Age
    WHEN '15 to 19' THEN '15-19 years'
    WHEN '20 to 24' THEN '20-24 years'
    WHEN '25 to 29' THEN '25-29 years'
    WHEN '30 to 34' THEN '30-34 years'
    WHEN '40 to 44' THEN '40-44 years'
    WHEN '45 to 49' THEN '45-49 years'
    WHEN '50 to 54' THEN '50-54 years'
    WHEN '55 to 59' THEN '55-59 years'
    WHEN '60 plus' THEN '65+ years'
  END NEW_Age
FROM countdownintervewtest.CDI_test.income_table






#SELECT DISTINCT Description_area
#FROM `countdownintervewtest.data_model.prepared_main` 



#only choose data from 15 big regions
SELECT * FROM `countdownintervewtest`.data_model.prepare_data_table_main 
WHERE (Area = "01" OR Area = "02" OR Area = "03" OR Area = "04" OR Area = "05" OR Area = "06" OR Area = "07" OR Area = "08" OR Area = "09" OR Area = "10" OR Area = "11" OR Area = "12" OR Area = "13" OR Area = "14" OR Area = "15" )
#only choose data from people able to work
SELECT * FROM `countdownintervewtest`.data_model.prepare_data_table_main
WHERE (Age = "01" OR Age = "02" OR Age = "03" OR Age = "04" OR Age = "05" OR Age = "06" OR Age = "07" OR Age = "08" OR Age = "09" OR Age = "10" OR Age = "11" OR Age = "12" OR Age = "13" OR Age = "14" OR Age = "15" OR Age = "16" OR Age = "17" OR Age = "18" OR Age = "19" OR Age = "20" OR Age = "21") 

SELECT * #SUM(CAST(Count AS FLOAT64))
FROM `countdownintervewtest.CDI_test.raw_main_table`
WHERE Year = "2018" AND Area = "15" AND Age = "999999" AND Sex = "9" AND Ethnic = "9999"
#total 4699755
#01 179076
#02 1571718
#03 458202
#04 308499
#05 47517
#06 166368
#07 117561
#08 238797
#09 506814
#12 31575
#13 599694
#14 225186
#15 97467
                   
### same region granulity
SELECT Age, sum(count) AS count, "56" AS Area_code
FROM `scanning-database.test_data_set.test_data_table` 
Where Area_code IN ("05","06")
Group by Age
UNION ALL
SELECT Age, sum(count) AS count, "12161718" AS Area_code
FROM `scanning-database.test_data_set.test_data_table` 
Where Area_code IN ("12","16","17","18")
Group by Age
UNION ALL
SELECT Age, count, Area_code
FROM `scanning-database.test_data_set.test_data_table` 
Where Area_code NOT IN ("05","06","12","16","17","18")
