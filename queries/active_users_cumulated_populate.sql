drop table if exists temp_combined
;

-- Need to create a (temporary) table to unnest arrays in Redshift
create temp table temp_combined(
    user_id integer,
    activity_array super,
    like_array super,
    comment_array super,
    share_array super,
    snapshot_date date
)
;

insert into temp_combined (
    -- Read in yesterday from the cumulated table
    with yesterday AS (
        SELECT * FROM database_name.schema_name.active_users_cumulated
        WHERE snapshot_date = '2023-01-30'
    ),
    -- Read in the daily active user numbers for just today from the daily table
    today AS (
        SELECT * FROM database_name.schema_name.active_users_daily
        WHERE snapshot_date = '2023-01-31'
    ),

    -- we FULL OUTER JOIN today and yesterday. We need to do some nvl both because
    -- activity_array may not exist yet for a given user (i.e. they are brand new) and
    -- is_active_today may be null as well since it's null on days when a user didn't generate an event
    combined AS (
        SELECT
            -- We need to nvl here since y.user_id may be NULL if user is new
            nvl(y.user_id, t.user_id) AS user_id,
            -- if y.activity_array is null (indicating a brand new user), we have to nvl with an array of size 1
            -- this array just holds the value for today since that's the only history we have
            nvl(
                case 
                    when get_array_length(y.activity_array) < 30
                    then array_concat(array(nvl(t.is_active_today, 0)), y.activity_array)
                    else array_concat(array(nvl(t.is_active_today, 0)), subarray(y.activity_array, 0, 29))
                end
                , array(t.is_active_today)
            ) as activity_array,
            nvl(
                case
                    when get_array_length(y.like_array) < 30
                    then array_concat(array(nvl(t.num_likes, 0)), y.like_array)
                    else array_concat(array(nvl(t.num_likes, 0)), subarray(y.like_array, 0, 29))
                end
                , array(t.num_likes)
            ) as like_array,
            nvl(
                case 
                    when get_array_length(y.comment_array) < 30
                    then array_concat(array(nvl(t.num_comments, 0)), y.comment_array)
                    else array_concat(array(nvl(t.num_comments, 0)), subarray(y.comment_array, 0, 29))
                end
                , array(t.num_comments)
            ) as comment_array,
            nvl(
                case
                    when get_array_length(y.share_array) < 30
                    then array_concat(array(nvl(t.num_shares, 0)), y.share_array)
                    else array_concat(array(nvl(t.num_shares, 0)), subarray(y.share_array, 0, 29))
                end
                , array(t.num_shares)
            ) as share_array,
            t.snapshot_date
        FROM yesterday y
            FULL OUTER JOIN today t ON y.user_id = t.user_id
    )
    select * from combined
)
;

insert into database_name.schema_name.active_users_cumulated (
    -- Dealing with arrays is a bit of a pain in Redshift, but possible.
    -- Here I need to create a CTE for each array calculation I need to make.
    -- Since I want to aggragate the like, comment, and share arrays for both 
    -- the last 7 days and last 30 days I will need to unnest these arrays twice.
    -- Notice the array unnesting CTEs are very similar, a templating language like
    -- Jinja may be a good idea to use here, especially if you have a lot of arrays.
    with active_unnest as (
        select
            user_id,
            cast(activity_array[0] as integer) AS is_daily_active,
            case when sum(array) > 0 then 1 else 0 end as is_monthly_active,
            case 
                when activity_array[0] > 0
                    or activity_array[1] > 0
                    or activity_array[2] > 0
                    or activity_array[3] > 0
                    or activity_array[4] > 0
                    or activity_array[5] > 0
                    or activity_array[6] > 0
                then 1 else 0 
            end as is_weekly_active,
            activity_array,
            snapshot_date
        from temp_combined a,
            a.activity_array array
        group by
            user_id,
            activity_array,
            snapshot_date
    ),
    arrays_7d as (
        select  
            user_id,
            subarray(like_array, 0, 7) like_array_7d,
            subarray(comment_array, 0, 7) comment_array_7d,
            subarray(share_array, 0, 7) share_array_7d,
            snapshot_date
        from temp_combined
    ),
    like_unnest_30 as (
        select
            user_id,
            cast(sum(array) as integer) num_likes_30d,
            like_array,
            snapshot_date
        from temp_combined a,
            a.like_array array
        group by
            user_id,
            like_array,
            snapshot_date
    ),
    like_unnest_7 as (
        select
            user_id,
            cast(sum(array) as integer) num_likes_7d,
            snapshot_date
        from arrays_7d a,
            a.like_array_7d array
        group by
            user_id,
            like_array_7d,
            snapshot_date
    ),
    comment_unnest_30 as (
        select
            user_id,
            cast(sum(array) as integer) num_comments_30d,
            comment_array,
            snapshot_date
        from temp_combined a,
            a.comment_array array
        group by
            user_id,
            comment_array,
            snapshot_date
    ),
    comment_unnest_7 as (
        select
            user_id,
            cast(sum(array) as integer) num_comments_7d,
            snapshot_date
        from arrays_7d a,
            a.comment_array_7d array
        group by
            user_id,
            comment_array_7d,
            snapshot_date
    ),
    share_unnest_30 as (
        select
            user_id,
            cast(sum(array) as integer) num_shares_30d,
            share_array,
            snapshot_date
        from temp_combined a,
            a.share_array array
        group by
            user_id,
            share_array,
            snapshot_date
    ),
    share_unnest_7 as (
        select
            user_id,
            cast(sum(array) as integer) num_shares_7d,
            snapshot_date
        from arrays_7d a,
            a.share_array_7d array
        group by
            user_id,
            share_array_7d,
            snapshot_date
    )

    select
        user_id,
        is_daily_active,
        is_monthly_active,
        is_weekly_active,
        activity_array,
        like_array,
        share_array,
        comment_array,
        num_likes_7d,
        num_comments_7d,
        num_shares_7d,
        num_likes_30d,
        num_comments_30d,
        num_shares_30d,
        snapshot_date
    from active_unnest
        inner join like_unnest_30 using (user_id, snapshot_date) 
        inner join comment_unnest_30 using (user_id, snapshot_date) 
        inner join share_unnest_30 using (user_id, snapshot_date) 
        inner join like_unnest_7 using (user_id, snapshot_date) 
        inner join comment_unnest_7 using (user_id, snapshot_date) 
        inner join share_unnest_7 using (user_id, snapshot_date) 
)
;

select * 
from database_name.schema_name.active_users_cumulated
order by snapshot_date
;
