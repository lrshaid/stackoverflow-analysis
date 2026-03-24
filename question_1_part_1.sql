-- q1 part 1: top/bottom 20 individual tags by avg answers (latest year)
-- tags with 75+ questions only to filter out noise

with questions_latest_year as (                                                                                           
      select                                                                                                              
id,                                                                                                               
lower(tags) as tags, -- normalize to avoid case duplicates
answer_count,                                                                                                     
case when accepted_answer_id is not null then 1 else 0 end as has_accepted_answer                               
from `bigquery-public-data.stackoverflow.posts_questions`
where extract(year from creation_date) = (                                                                            
select extract(year from max(creation_date))
from `bigquery-public-data.stackoverflow.posts_questions`                                                         
)                                                                                                                     
)
  , individual_tags as (
-- explode pipe-delimited tags into rows and aggregate per tag
select                                                                                                              
tag,
count(*) as total_questions,
sum(answer_count) as total_answers,
avg(answer_count) as avg_answers_per_question,
sum(has_accepted_answer) as accepted_count,
safe_divide(sum(has_accepted_answer), count(*)) as accepted_rate                                                  
from questions_latest_year
          , unnest(split(tags, '|')) as tag                                                                                 
group by tag 
having count(*)>75                                                                                                       
)
-- top 20
(
select                                                                                                                
'top' as category,
tag,                                                                                                              
total_questions,                                                                                                
total_answers,
round(avg_answers_per_question, 2) as avg_answers,
accepted_count,
round(accepted_rate, 4) as accepted_rate
from individual_tags                                                                                                  
order by avg_answers_per_question desc
limit 20                                                                                                              
)                                                                                                                       
union all
-- bottom 20
(
select
'bottom' as category,
tag,
total_questions,
total_answers,
round(avg_answers_per_question, 2) as avg_answers,
accepted_count,                                                                                                   
round(accepted_rate, 4) as accepted_rate
from individual_tags                                                                                                  
order by avg_answers_per_question asc                                                                               
limit 20
)
order by category, avg_answers desc
