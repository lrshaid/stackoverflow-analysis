-- q3: what post qualities (beyond tags) correlate with more answers / higher accepted rate?
-- bucketed analysis across 7 features, all unioned into one result set

with all_questions as (
-- extract features per question, join users for reputation
      select
          q.id,
          q.answer_count,
          case when q.accepted_answer_id is not null then 1 else 0 end as has_accepted_answer,
          array_length(split(q.title, ' ')) as title_word_count,
          length(q.body) as body_length,
          case when q.body like '%<code>%' then 1 else 0 end as has_code,
          array_length(split(q.tags, '|')) as tag_count,
          extract(hour from q.creation_date) as hour_posted,
          extract(dayofweek from q.creation_date) as day_of_week,
          u.reputation as author_reputation
      from `bigquery-public-data.stackoverflow.posts_questions` q
      left join `bigquery-public-data.stackoverflow.users` u
          on q.owner_user_id = u.id
  )

  -- Title length
  (
      select
          'title_word_count' as feature,
          case
              when title_word_count <= 5 then '01. ≤5 words'
              when title_word_count between 6 and 10 then '02. 6-10 words'
              when title_word_count between 11 and 15 then '03. 11-15 words'
              when title_word_count between 16 and 20 then '04. 16-20 words'
              when title_word_count > 20 then '05. 20+ words'
          end as bucket,
          count(*) as total_questions,
          round(avg(answer_count), 2) as avg_answers,
          round(safe_divide(sum(has_accepted_answer), count(*)), 4) as accepted_rate
      from all_questions
      group by feature, bucket
  )
  union all

  -- Body length
  (
      select
          'body_length' as feature,
          case
              when body_length < 500 then '01. <500 chars'
              when body_length between 500 and 1000 then '02. 500-1k'
              when body_length between 1001 and 2000 then '03. 1k-2k'
              when body_length between 2001 and 5000 then '04. 2k-5k'
              when body_length > 5000 then '05. 5k+'
          end as bucket,
          count(*) as total_questions,
          round(avg(answer_count), 2) as avg_answers,
          round(safe_divide(sum(has_accepted_answer), count(*)), 4) as accepted_rate
      from all_questions
      group by feature, bucket
  )
  union all

  -- Has code
  (
      select
          'has_code' as feature,
          cast(has_code as string) as bucket,
          count(*) as total_questions,
          round(avg(answer_count), 2) as avg_answers,
          round(safe_divide(sum(has_accepted_answer), count(*)), 4) as accepted_rate
      from all_questions
      group by feature, bucket
  )
  union all

  -- Tag count
  (
      select
          'tag_count' as feature,
          cast(tag_count as string) as bucket,
          count(*) as total_questions,
          round(avg(answer_count), 2) as avg_answers,
          round(safe_divide(sum(has_accepted_answer), count(*)), 4) as accepted_rate
      from all_questions
      group by feature, bucket
  )
  union all

  -- Hour posted
  (
      select
          'hour_posted' as feature,
          case
              when hour_posted between 0 and 5 then '01. 0-5 (night)'
              when hour_posted between 6 and 11 then '02. 6-11 (morning)'
              when hour_posted between 12 and 17 then '03. 12-17 (afternoon)'
              when hour_posted between 18 and 23 then '04. 18-23 (evening)'
          end as bucket,
          count(*) as total_questions,
          round(avg(answer_count), 2) as avg_answers,
          round(safe_divide(sum(has_accepted_answer), count(*)), 4) as accepted_rate
      from all_questions
      group by feature, bucket
  )
  union all

  -- Day of week
  (
      select
          'day_of_week' as feature,
          case day_of_week
              when 1 then '1. Sun'
              when 2 then '2. Mon'
              when 3 then '3. Tue'
              when 4 then '4. Wed'
              when 5 then '5. Thu'
              when 6 then '6. Fri'
              when 7 then '7. Sat'
          end as bucket,
          count(*) as total_questions,
          round(avg(answer_count), 2) as avg_answers,
          round(safe_divide(sum(has_accepted_answer), count(*)), 4) as accepted_rate
      from all_questions
      group by feature, bucket
  )
  union all

  -- Author reputation
  (
      select
          'author_reputation' as feature,
          case
              when author_reputation is null then '00. unknown'
              when author_reputation < 10 then '01. <10'
              when author_reputation between 10 and 100 then '02. 10-100'
              when author_reputation between 101 and 1000 then '03. 101-1k'
              when author_reputation between 1001 and 10000 then '04. 1k-10k'
              when author_reputation > 10000 then '05. 10k+'
          end as bucket,
          count(*) as total_questions,
          round(avg(answer_count), 2) as avg_answers,
          round(safe_divide(sum(has_accepted_answer), count(*)), 4) as accepted_rate
      from all_questions
      group by feature, bucket
  )

  order by feature, bucket

-- author reputation: poor correlation
-- body length: inverse — shorter posts get higher accepted rate
-- day of the week: more or less stable
-- has code: stable-ish
-- title word count: similar to body length — shorter title, higher accepted rate
