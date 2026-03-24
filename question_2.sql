-- q2: python vs dbt — yoy change in avg answers per question and accepted rate
-- regex matches tags that are exactly 'python' or 'dbt' (not substrings)

with yearly_stats as (                                                                                                    
      select                                                                                                              
extract(year from creation_date) as year,                                                                         
case                                                                                                              
when regexp_contains(lower(tags), r'(^|\|)python(\||$)') then 'python'                                        
when regexp_contains(lower(tags), r'(^|\|)dbt(\||$)') then 'dbt'                                              
end as tag,                                                                                                       
count(*) as total_questions,                                                                                      
sum(answer_count) as total_answers,                                                                               
round(safe_divide(sum(answer_count), count(*)), 2) as avg_answers_per_question,                                 
sum(case when accepted_answer_id is not null then 1 else 0 end) as accepted_count,                                
round(safe_divide(sum(case when accepted_answer_id is not null then 1 else 0 end), count(*)), 4) as accepted_rate
from `bigquery-public-data.stackoverflow.posts_questions`                                                             
where regexp_contains(lower(tags), r'(^|\|)python(\||$)')                                                           
or regexp_contains(lower(tags), r'(^|\|)dbt(\||$)')                                                               
group by year, tag                                                                                                    
having tag is not null
)                                                                                                                         
  , with_yoy as (
-- lag to get previous year's value, then compute the difference
select                                                                                                                
year,
tag,                                                                                                              
total_questions,                                                                                                
total_answers,
avg_answers_per_question,
round(avg_answers_per_question - lag(avg_answers_per_question) over (partition by tag order by year), 2) as
yoy_change_avg_answers,                                                                                                   
accepted_count,
accepted_rate,                                                                                                    
round(accepted_rate - lag(accepted_rate) over (partition by tag order by year), 4) as yoy_change_accepted_rate  
from yearly_stats                                                                                                     
)
select                                                                                                                    
year,                                                                                                               
tag,
total_questions,
total_answers,
avg_answers_per_question,
yoy_change_avg_answers,
accepted_count,
accepted_rate,
yoy_change_accepted_rate                                                                                              
from with_yoy
order by tag, year
