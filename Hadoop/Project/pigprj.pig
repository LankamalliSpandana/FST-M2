--Load the input file
line1= LOAD 'hdfs:///user/spandana/Inputs/episodeIV_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
line2= LOAD 'hdfs:///user/spandana/Inputs/episodeV_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
line3= LOAD 'hdfs:///user/spandana/Inputs/episodeVI_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
lines= UNION line1,line2,line3;

--Group by character names
group_lines = GROUP lines BY name;

--Count the number of lines spoken by each character
char_line_count = FOREACH group_lines GENERATE $0 AS character, COUNT($1) AS line_count;

--Order the results in descending order
order_count = ORDER char_line_count BY line_count DESC;

--Store the result
STORE order_count INTO 'hdfs:///user/spandana/pigprojectoutput' USING PigStorage('\t');
