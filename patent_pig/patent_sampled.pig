-- Sample data
pats = LOAD 'hdfs://localhost:9000/user/user/cache/apat63_99.txt' using PigStorage(',')
  as (ipatent : long, gyear : int, gdate : int, appyear : int, 
      country : chararray , postate : chararray,
      assignee, asscode,
      claims : int, nclass, cat, subcat, 
      cmade : int, creceive : int, ratiocit : float ,
      general, original, fwdaplag, bckgtlag, 
      selfctub, selfctlb, secdupbd, secdlwbd);

--
-- Eliminate the header row
--
frawpat = FILTER pats by ipatent is not null;
lrawpat = SAMPLE frawpat 0.05; -- sample 0.01% of the patents

STORE lrawpat into 'hdfs://localhost:9000/user/user/littlepig/patents.csv' using PigStorage(',');

--
-- Now, load citations
--
cites = load 'hdfs://localhost:9000/user/user/input/cite75_99.txt' using PigStorage(',') 
  as (citing: long, cited:long);
frawcite = FILTER cites by citing is not null; -- eliminate header

--
-- Pull all citations that match the first column
--
jj1 = COGROUP frawcite by citing INNER, lrawpat by ipatent INNER;
gg1 = foreach jj1 generate FLATTEN(frawcite);

--
-- then for second column
--
jj2 = COGROUP frawcite by cited INNER, lrawpat by ipatent INNER;
gg2 = foreach jj2 generate FLATTEN(frawcite);

ugg = UNION gg1,gg2;

STORE ugg into 'hdfs://localhost:9000/user/user/littlepig/citations.csv' using PigStorage(',');

-- Main Program Stuff
rawpat = LOAD 'hdfs://localhost:9000/user/user/littlepig/patents.csv' using PigStorage(',')
  as (ipatent : long, gyear : int, gdate : int, appyear : int, 
      country : chararray , postate : chararray,
      assignee, asscode,
      claims : int, nclass, cat, subcat, 
      cmade : int, creceive : int, ratiocit : float ,
      general, original, fwdaplag, bckgtlag, 
      selfctub, selfctlb, secdupbd, secdlwbd);

rawcite = load 'hdfs://localhost:9000/user/user/littlepig/citations.csv' using PigStorage(',') 
  as (citing: long, cited:long);

--Switch citing, cited to cited, citing
cited_citing_all = FOREACH rawcite GENERATE cited, citing;

--Remove the first empty record (row)
cited_citing = FILTER cited_citing_all BY cited is not null;

--Subset the rawpatent data
raw_pat_all = FOREACH rawpat GENERATE ipatent, country, postate;

--Remove the record where patent is null
pats = FILTER raw_pat_all BY ipatent is not null;

--Get all US patents only
us_pats = FILTER pats BY country matches '.*US.*'; 

-- Join patents info with cited_citing
first_join = join us_pats by ipatent, cited_citing by cited;

second_join = join first_join by cited_citing::citing, us_pats by ipatent;

--dump first_join;
--dump second_join;

-- Get to Cited_State , Citing State
cited_state_citing_state = FOREACH second_join GENERATE first_join::us_pats::postate,  us_pats::postate;

-- Group by cited state
grouped_data = group cited_state_citing_state by first_join::us_pats::postate;

--dump grouped_data;

-- Generate state, total count;
-- $0 = refers to state in grouped_data
-- $1 = refers to tuple of cited state, citing state 
total_count = FOREACH grouped_data GENERATE $0, COUNT($1);

-- For grouped_data, generate a bag if it has same state
same_state_citations = FOREACH grouped_data{
  temp = FILTER $1 BY (first_join::us_pats::postate == us_pats::postate);
  GENERATE $0, temp;
}

-- Total counts of same state citations
same_count = FOREACH same_state_citations GENERATE $0, COUNT($1);

-- Join total_count and same_count
joined_count = join total_count by $0, same_count by $0;

final_count = FOREACH joined_count GENERATE $0, $1, $3;
dump final_count;

-- Get the percentage
percentage = FOREACH final_count GENERATE $0, ((double)$2/$1) * 100;

-- Store the result
STORE percentage into 'hdfs://localhost:9000/user/user/output/output.csv' using PigStorage(',');



