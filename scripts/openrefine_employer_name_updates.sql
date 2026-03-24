-- OpenRefine cluster export -> employer_name updates
-- source: openrefine_employer_name_clusters.json
-- column: employer_name
-- clusters: 22
BEGIN TRANSACTION;

-- cluster 1: merge -> spire global subsidiary inc
UPDATE h1b_wages SET employer_name = 'spire global subsidiary inc' WHERE employer_name = 'spire subsidiary global inc';

-- cluster 2: merge -> the industrial company tic
UPDATE h1b_wages SET employer_name = 'the industrial company tic' WHERE employer_name = 'tic the industrial company';

-- cluster 3: merge -> logistics plus inc
UPDATE h1b_wages SET employer_name = 'logistics plus inc' WHERE employer_name = 'plus logistics inc';

-- cluster 4: merge -> centers for disease control and prevention cdc dhhs
UPDATE h1b_wages SET employer_name = 'centers for disease control and prevention cdc dhhs' WHERE employer_name = 'dhhs centers for disease control and prevention cdc';

-- cluster 5: merge -> aurora east school district 131
UPDATE h1b_wages SET employer_name = 'aurora east school district 131' WHERE employer_name = 'east aurora school district 131';

-- cluster 6: merge -> society of roman catholic church diocese of lake charles
UPDATE h1b_wages SET employer_name = 'society of roman catholic church diocese of lake charles' WHERE employer_name = 'society roman catholic church diocese of lake charles';

-- cluster 7: merge -> university of las vegas nevada
UPDATE h1b_wages SET employer_name = 'university of las vegas nevada' WHERE employer_name = 'university of nevada las vegas';

-- cluster 8: merge -> guild systems inc
UPDATE h1b_wages SET employer_name = 'guild systems inc' WHERE employer_name = 'systems guild inc';

-- cluster 9: merge -> lane associates of nc pllc
UPDATE h1b_wages SET employer_name = 'lane associates of nc pllc' WHERE employer_name = 'lane lane associates of nc pllc';

-- cluster 10: merge -> university of vermont health network medical group inc
UPDATE h1b_wages SET employer_name = 'university of vermont health network medical group inc' WHERE employer_name = 'university of vermont medical health network medical group inc';

-- cluster 11: merge -> prisma health medical group university medical group
UPDATE h1b_wages SET employer_name = 'prisma health medical group university medical group' WHERE employer_name = 'prisma health university medical group';

-- cluster 12: merge -> nucor corporation nucor steel berkeley
UPDATE h1b_wages SET employer_name = 'nucor corporation nucor steel berkeley' WHERE employer_name = 'nucor steel berkeley nucor corporation';

-- cluster 13: merge -> research foundation for the state university of new york
UPDATE h1b_wages SET employer_name = 'research foundation for the state university of new york' WHERE employer_name = 'the research foundation for the state university of new york';

-- cluster 14: merge -> azzur group dba azzur consulting llc
UPDATE h1b_wages SET employer_name = 'azzur group dba azzur consulting llc' WHERE employer_name = 'azzur group llc dba azzur consulting llc';

-- cluster 15: merge -> everywhere wireless llc
UPDATE h1b_wages SET employer_name = 'everywhere wireless llc' WHERE employer_name = 'wireless everywhere llc';

-- cluster 16: merge -> tech vista llc
UPDATE h1b_wages SET employer_name = 'tech vista llc' WHERE employer_name = 'vista tech llc';

-- cluster 17: merge -> state of west virginia department of health office of the chief medical examiner
UPDATE h1b_wages SET employer_name = 'state of west virginia department of health office of the chief medical examiner' WHERE employer_name = 'west virginia state health department office of the chief medical examiner';

-- cluster 18: merge -> adams county 14 school district
UPDATE h1b_wages SET employer_name = 'adams county 14 school district' WHERE employer_name = 'adams county school district 14';

-- cluster 19: merge -> sri varadavinayaka inc dba innovant tech
UPDATE h1b_wages SET employer_name = 'sri varadavinayaka inc dba innovant tech' WHERE employer_name = 'sri varadavinayaka inc dba innovant tech inc';

-- cluster 20: merge -> health level inc
UPDATE h1b_wages SET employer_name = 'health level inc' WHERE employer_name = 'level health inc';

-- cluster 21: merge -> the university of texas health center science at tyler
UPDATE h1b_wages SET employer_name = 'the university of texas health center science at tyler' WHERE employer_name = 'the university of texas health science center at tyler';

-- cluster 22: merge -> teleplan services texas inc
UPDATE h1b_wages SET employer_name = 'teleplan services texas inc' WHERE employer_name = 'teleplan texas services inc';

UPDATE h1b_wages SET employer_name = 'government employees insurance company geico' WHERE employer_name = 'government employee insurance company geico';

COMMIT;

-- Total UPDATE statements: 22