-- q1 part 2: top/bottom 20 tag combinations by avg answers (latest year)
-- pairs with 75+ questions only to filter out noise

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
  , tag_pairs as (
-- self-join exploded tags, t1 < t2 to get unique pairs without dupes
      select                                                                                                                
          t1 as tag_1,
          t2 as tag_2,                                                                                                      
          q.answer_count,                                                                                                 
          q.has_accepted_answer                                                                                             
      from questions_latest_year q
          , unnest(split(q.tags, '|')) as t1                                                                                
          , unnest(split(q.tags, '|')) as t2                                                                                
      where t1 < t2
  )                                                                                                                         
  , pair_stats as (                                                                                                       
      select                                                                                                                
          tag_1,                                                                                                          
          tag_2,
          count(*) as total_questions,
          sum(answer_count) as total_answers,
          avg(answer_count) as avg_answers_per_question,                                                                    
          sum(has_accepted_answer) as accepted_count,
          safe_divide(sum(has_accepted_answer), count(*)) as accepted_rate                                                  
      from tag_pairs                                                                                                        
      group by tag_1, tag_2
      having count(*) >= 75                                                                                                 
  )                                                                                                                       
  -- top 20
  (
      select                                                                                                                
          'top' as category,
          concat(tag_1, ' + ', tag_2) as tag_combination,                                                                   
          total_questions,                                                                                                  
          total_answers,
          round(avg_answers_per_question, 2) as avg_answers,                                                                
          accepted_count,                                                                                                 
          round(accepted_rate, 4) as accepted_rate
      from pair_stats                                                                                                       
      order by avg_answers_per_question desc
      limit 20                                                                                                              
  )                                                                                                                         
  union all
  -- bottom 20
  (                                                                                                                         
      select                                                                                                              
          'bottom' as category,
          concat(tag_1, ' + ', tag_2) as tag_combination,
          total_questions,
          total_answers,
          round(avg_answers_per_question, 2) as avg_answers,
          accepted_count,                                                                                                   
          round(accepted_rate, 4) as accepted_rate
      from pair_stats                                                                                                       
      order by avg_answers_per_question asc                                                                               
      limit 20
  )
  order by category, avg_answers desc
