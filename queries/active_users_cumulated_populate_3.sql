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
