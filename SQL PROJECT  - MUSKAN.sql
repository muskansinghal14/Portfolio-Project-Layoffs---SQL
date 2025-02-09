-- Data Cleaning 

SELECT *
FROM layoffs;  

-- 1. Remove duplicates if there are any
-- 2. Standardise the data (that means if there are issues with the data like spellings or other things) 
-- 3. Null Values or Blank values 
-- 4. Remove unecessary col. and rows. 

CREATE TABLE layoffs_staging
LIKE layoffs; 

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;  

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
ORDER BY company) AS row_num 
FROM layoffs_staging;                                -- we've written date with backticks like '   ' as date is a keyword in sql  


WITH duplicate_cte AS 
( 
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions
ORDER BY company) AS row_num 
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1; 

WITH duplicate_cte AS 
( 
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions
ORDER BY company) AS row_num 
FROM layoffs_staging
)
DELETE FROM layoffs_staging
WHERE row_num > 1;
----------------------------------------------------------------

-- now we'll make the temporary table 

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions
ORDER BY company) AS row_num 
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;                     -- DISABLE THE SAFE MODE WITH THIS QUERY 

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;                 -- Enable safe mode again 

SELECT *
FROM layoffs_staging2;

-- STANDARDIZING THE DATA

SELECT DISTINCT(TRIM(company))
FROM layoffs_staging2; 

UPDATE layoffs_staging2                      -- This step permanently updates this mistake in the table 
SET company = TRIM(company); 

SELECT DISTINCT(industry)
FROM layoffs_staging2 
ORDER BY 1;               -- Here 1 means the first column 

SELECT *
FROM layoffs_staging2 
WHERE industry LIKE 'crypto%' ;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%' ; 

SELECT DISTINCT(location)
FROM layoffs_staging2 
ORDER BY 1;  

SELECT DISTINCT(country)
FROM layoffs_staging2 
ORDER BY 1;      

SELECT *
FROM layoffs_staging2 
WHERE country LIKE 'United States%'
ORDER BY 1;     

SELECT TRIM(TRAILING '.' FROM country)                 -- trailing removes characters specified from the end of a string
FROM layoffs_staging2 
ORDER BY 1;     

UPDATE layoffs_staging2                                -- Updating the mistake corrected 
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%' ; 

SELECT date                                   -- Checking date column 
FROM layoffs_staging2;

SELECT `date`, str_to_date(`date`, '%m/%d/%Y')                   -- formatting date column in date format
FROM layoffs_staging2;

UPDATE layoffs_staging2                            -- Updating the date format  
SET date = str_to_date(`date`, '%m/%d/%Y'); 

ALTER TABLE layoffs_staging2                 -- changing data type 
MODIFY COLUMN `date` DATE; 

-- Checking for null and blank values

SELECT *                         
FROM layoffs_staging2
WHERE total_laid_off IS NULL;           -- IS NULLis used to check the null values in the specific column 

SELECT *                         
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL; 

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
     ON t1.company = t2.company 
WHERE t1.industry IS NULL OR t1.industry = '' AND t2.industry IS NOT NULL;

SELECT t1.industry, t2.industry                       -- same query as above its just to make the query more clear
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
     ON t1.company = t2.company 
WHERE t1.industry IS NULL OR t1.industry = '' AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
     ON t1.company = t2.company 
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;              -- (OR t1.industry = '') - we've removed this part from where clause as neeche we've updated those blank values to null values so while doing we no longer needed to write this 

-- Lets update those blank values to null value from T1 then our JOIN & UPDATE statement will act on. 

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '' ;

SELECT *
FROM layoffs_staging2;

-- Remove col. & rows if necessary 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL; 

DELETE FROM layoffs_staging2                       -- Removing those rows that contains null values together in total laid offand percentage laid off column
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL; 

ALTER TABLE layoffs_staging2               -- Removing row_num column as its no longer needed 
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;

----------------------------------------------------------------------------------------------------------
                                                -- Exploratory Data Analysis 
                                                
SELECT MAX(total_laid_off)
FROM layoffs_staging2;
                                                
SELECT MAX(percentage_laid_off)
FROM layoffs_staging2;
                                                
SELECT company, percentage_laid_off
FROM layoffs_staging2
WHERE percentage_laid_off = 1;
                                                
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1                                              
ORDER BY total_laid_off DESC;
                                                
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;                                                
                                                
SELECT company, SUM(total_laid_off)        -- Through this query we can see which company hit the most layoff 
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;             -- Here 2 stands for second column which is sum of total_laid_off                                                 
                                                
SELECT MIN(`date`), MAX(`date`) 
FROM layoffs_staging2;                                                

SELECT industry, SUM(total_laid_off)        -- Through this query we can see which industry hit the most layoff 
FROM layoffs_staging2               -- In this case consumer and retail hit very hard lay off
GROUP BY industry
ORDER BY 2 DESC;  

SELECT country, SUM(total_laid_off)        -- Through this query we can see which country hit the most layoff 
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;  

SELECT `date`, SUM(total_laid_off)        -- Through this query we can see which recent date hit what no. layoff 
FROM layoffs_staging2                     -- but it is showing the layoff on individual dates instead of doing group by as there are many different dates so we'll now do group by by year from the date
GROUP BY `date`
ORDER BY 1 DESC;  

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2                -- Here we can see that in 2022 there is a whole lot of layoff but jese jese 2023 aaya layoff kam hota gya so it's kind of rapping up
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;  

SELECT stage, SUM(total_laid_off)        -- Through this query we can see which stage of the company hit the most layoff 
FROM layoffs_staging2               -- stage tells us the stage of the company like post-ipo is kind of the large companies and they had the most layoff 
GROUP BY stage
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`, 6, 2) AS `MONTH`, SUM(total_laid_off)            -- This rolling total is not that great becuase of month as in single month we have all years
FROM layoffs_staging2
GROUP BY `MONTH`;

SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)           
FROM layoffs_staging2
GROUP BY `MONTH`
ORDER BY 1; 

SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_of)         -- I write the above query again with the WHERE clause as I wanted to get rid of that NULL date row  
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL             -- Here I wrote substring poora bcoz WHERE clause mai woh `MONTH` nhi accept kr rha tha
GROUP BY `MONTH`
ORDER BY 1; 

WITH Rolling_Total AS 
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1
)
SELECT `MONTH`, total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total; 

SELECT company, YEAR(`date`), SUM(total_laid_off)        
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS   -- Here in bracket we have written the column name with the name we want, so in order to change the column names while using CTE we can write like this 
(
SELECT company, YEAR(`date`), SUM(total_laid_off)        
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY Ranking ASC;

-- NOW WE WANT TO FILTER ON THE RANKING COLUMN LIKE IF WE WANT TOP 5 RANK PER YEAR THEN WE WILL CREATE 2 CTEs AND WRITE THE DIFFERENT WHERE CONDITION 

WITH Company_Year (company, years, total_laid_off) AS   
(
SELECT company, YEAR(`date`), SUM(total_laid_off)        
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS           -- we created this another cte becuase humme uss rank pe condition lagani thi jahape window function use kia so where clause would not have supported it 
(SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;

--------------------------------------------------------- THANK YOU ----------------------------------------------------------------------










