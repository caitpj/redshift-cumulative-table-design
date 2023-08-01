select 
    snapshot_date,
    sum(is_monthly_active) as monthly_active_users,
    sum(is_weekly_active) as weekly_active_users,
    sum(is_daily_active) as daily_active_users,
    sum(num_likes_7d) as num_likes_7d,
    sum(num_likes_30d) as num_likes_30d,
    sum(num_comments_7d) as num_comments_7d,
    sum(num_comments_30d) as num_comments_30d,
    sum(num_shares_7d) as num_shares_7d,
    sum(num_shares_30d) as num_shares_30d
from database_name.schema_name.active_users_cumulated
where snapshot_date between '2023-01-01' and '2023-01-31'
group by snapshot_date
order by snapshot_date
