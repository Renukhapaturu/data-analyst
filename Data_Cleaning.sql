use world_layoffs;
  
 select * 
 from layoffs;  

/* 1. remove duplicates
2. standardize the data
3. null values or blank values
4. removes any columns or rows

/*---Create Table (like is used to create as it is table)---*/
create table layoffs_staging
like layoffs;

/*----Insert data from one table to another---------*/
INSERT INTO layoffs_staging
SELECT * FROM layoffs;


/*                 Add a row number to each record in layoffs_staging, 
		    partitioned by all specified columns to identify duplicate entries          */

select *,
     row_number() over(
	 partition by company, location, industry, stage, country, 
     total_laid_off,percentage_laid_off,'date',funds_raised_millions) as row_num
	 from layoffs_staging; 
 
 
 /*          finding duplicates                */
 
with duplicate_cte as
(
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location,  industry, total_laid_off, 
                        percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM layoffs_staging
)
select *
from duplicate_cte
where row_num > 1; 
 
 select *
 from layoffs_staging
 where company = 'Casper';
 
 
 /*---                                      Create a new Table                               ----*/
 
CREATE TABLE  `layoffs_staging2`  (
   `company` text,
   `location` text,
   `industry` text,
   `total_laid_off` int default null,
   `percentage_laid_off` text,
   `date` text,
   `stage` text,
   `country` text,
   `funds_raised_millions` int default null,
   `row_num` int
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_0900_ai_ci;


select *
from layoffs_staging2;


/*-----                                     Insert data into new table                          -----*/

insert into layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location,  industry, total_laid_off, 
                        percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM layoffs_staging;

#-----                            Retrive Table            -----

select *
from layoffs_staging2;

--                      display duplicates,  > 1  is show a duplicates i.e; 2nd row_num is duplicates

select *
from layoffs_staging2
where row_num > 1 ;    

  
--                        Delete Duplicates               --
delete 
from layoffs_staging2
where row_num > 1 ;    

--                      here it shows only original data
select *
from layoffs_staging2;                 


--                    2. standarding data  --

select distinct(company)
from layoffs_staging2;

select distinct(trim(company))
from layoffs_staging2;

select company,trim(company)
from layoffs_staging2;

--       Update table : Now remove white space before text 

update layoffs_staging2
set company = trim(company);
 --         or
SET SQL_SAFE_UPDATES = 0;

UPDATE layoffs_staging2
SET company = TRIM(company);

SET SQL_SAFE_UPDATES = 1;  -- (optional) re-enable after the update

/*                        Retrieve a distinct, alphabetically sorted list of 
                            industries from the layoffs_staging2 table.                   */
 
select distinct industry
from layoffs_staging2
order by 1;      

--            Fetch all records from table where the industry starts with 'crypto'.

select *
from layoffs_staging2
where industry like 'crypto%';

--              Update the industry to 'crypto'  where the industry is 'crypto%'

SET SQL_SAFE_UPDATES = 0;

update layoffs_staging2
set industry = 'crypto'
where industry = 'crypto%';

--              Retrieve a unique list of industries

select distinct industry
from layoffs_staging2;

--              Retrieve a unique list of industries

select distinct location
from layoffs_staging2;

/*            This extracts a list of unique locations to check 
                for duplicates, inconsistencies, or anomalies       */

select distinct location
from layoffs_staging2
order by 1;

/*                            filtering by country
               Looks for variations like 'united states' that need cleanup          */

select *
from layoffs_staging2
where country like 'united states%'
order by 1;

/*              Identifies and trims whitespace or punctuation inconsistencies                */

select distinct country, trim(country)
from layoffs_staging2
order by 1;

select distinct country, trim(trailing'.' from country)
from layoffs_staging2
order by 1;

--                    Cleans country names like "united states"

update layoffs_staging2
set country = trim(trailing'.' from country)
where country like 'united states%';

--                      Tests date conversion before update

select `date`,
str_to_date(`date`, '%m/%d/%y' )
from layoffs_staging2;

--                    updates the date column

SET SQL_SAFE_UPDATES = 0;
update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y' );

--            retrive tha data         

select `date`
from layoffs_staging2;

--                       Changes column data type to DATE

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2;

--         -- Selects records with missing data for both total and percentage_laid_off       

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

--           Normalizes empty strings to NULL

update layoffs_staging2
set industry = null
where industry = '';

--      Verifies changes

select *
from layoffs_staging2
where industry is null
or industry ='';

select *
from layoffs_staging2
where company = 'Airbnb';

/*                    Identifies rows with missing industry and finds matching 
                             known industries for the same company                         */


select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

/*               Fills missing industry values by copying from other 
					records of the same company with known industry                        */
                                                                                     
update layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

select *
from layoffs_staging2;

--           Retrieves rows where both total and percentage_laid_off are missing   

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

--         Deletes rows lacking both key values, assuming they're non-informative

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

--                Removes staging metadata

alter table layoffs_staging2
drop column row_num;

--                        Verifies the final cleaned table

select *
from layoffs_staging2;