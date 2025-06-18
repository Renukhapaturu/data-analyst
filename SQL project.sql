--     Exploratory Data Analyst

use world_layoffs;

-- Shows all records from the layoffs_staging2 table

select *
from layoffs_staging2;

/*                    Returns the max number of layoffs and 
                 the highest percentage laid off in any single record               */

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

/*                Companies that laid off 100% of their employees, 
                     sorted by how much funding they raised                  */

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

--                         Ranks companies by total layoffs          
select company, sum(total_laid_off) 
from layoffs_staging2
group by company
order by 2 desc;

--                   Returns the first and last dates of layoffs recorded       
        
select min(`date`) min_date, max(`date`) max_date
from layoffs_staging2;

--             Industry-wise summary of layoffs

select industry, sum(total_laid_off) 
from layoffs_staging2
group by industry
order by 2 desc;

--              Country-wise summary of layoffs

select country, sum(total_laid_off) 
from layoffs_staging2
group by country
order by 2 desc;

--                    Total layoffs per day

select `date`, sum(total_laid_off) 
from layoffs_staging2
group by `date`
order by 1 desc;

--                   Annual trend of layoffs

select year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by year(`date`)
order by 1 desc;


select `date`, sum(total_laid_off) 
from layoffs_staging2
group by `date`
order by 1 desc;

--               Layoffs by funding/operational stage of the company

select stage, sum(total_laid_off) 
from layoffs_staging2
group by stage
order by 1 desc;

--             Sums up the percentage laid off per company       
           
select company, sum(percentage_laid_off) 
from layoffs_staging2
group by company
order by 2 desc;

--             Monthly aggregated layoffs (YYYY-MM format)
select substring(`date`,1,7) as `month` , sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;


select *
from layoffs_staging2;

--    Computes a cumulative sum of layoffs by month
               
with rolling_table as
(
   select substring(`date`,1,7) as `month` , sum(total_laid_off) as total_off
   from layoffs_staging2
   where substring(`date`,1,7) is not null
   group by `month`
   order by 1 asc 
)
select `month`,total_off,
		sum(total_off) over(order by `month`) as rolling_table
from rolling_table;

--                               Year-wise layoffs for each company

select company, year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by company, year(`date`)
order by company asc;

--                   Year-wise layoffs for each company & Sorted by Layoff Count

select company, year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;

--                       For each year, shows top 5 companies with the most layoffs

with company_year as (
    select company, year(`date`) as years, sum(total_laid_off) as total_laid_off
    from layoffs_staging2
    group by company, year(`date`)
),
company_year_rank as (
    select *,
           dense_rank() over(partition by years order by total_laid_off desc) as ranking
    from company_year
    where years is not null
)
select *
from company_year_rank
where ranking <= 5;


