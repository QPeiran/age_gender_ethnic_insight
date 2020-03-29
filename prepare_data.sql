
# “raw_main_table”(Data8277.csv) has attribute “Count” but some of them are not a number but shown as “..C” 
# – better to delete this tuple rather tha replace the to 0
DELETE FROM `countdownintervewtest`.CDI_test.raw_main_table WHERE Count = "..C"


#Filter census data from 2018. Choose 

SELECT
  Age, Ethnic, Area, SUM(CAST(Count AS FLOAT64)) AS Count_NEW
 FROM
   `countdownintervewtest`.CDI_test.raw_main_table
 WHERE
  Year = "2018"
GROUP BY Age, Ethnic, Area


#replace "values" to "lookup reference" in the income table








#only choose data from 15 big regions
SELECT * FROM `countdownintervewtest`.data_model.prepared_main_table 
WHERE (Area = "01" OR Area = "02" OR Area = "03" OR Area = "04" OR Area = "05" OR Area = "06" OR Area = "07" OR Area = "08" OR Area = "09" OR Area = "10" OR Area = "11" OR Area = "12" OR Area = "13" OR Area = "14" OR Area = "15" )
#only choose data from people able to work
SELECT * FROM `countdownintervewtest`.data_model.prepare_data_table_main
WHERE (Age = "01" OR Age = "02" OR Age = "04" OR Age = "04" OR Age = "05" OR Age = "06" OR Age = "07" OR Age = "08" OR Age = "09" OR Age = "10" OR Age = "11" OR Age = "12" OR Age = "13" OR Age = "14") 