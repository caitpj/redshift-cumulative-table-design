insert into scratchpad.data_eng.cai_test_active_users_daily (
    select
        user_id,
        -- If the user_id has at least 1 event, they are daily active
        1 as is_active_today,
        count(case when event_type = 'like' then 1 END) as num_likes, -- Use 'count' rather than 'sum' to get 0 instead of NULL
        count(case when event_type = 'comment' then 1 END) as num_comments,
        count(case when event_type = 'share' then 1 END) as num_shares,
        '2023-01-31' as snapshot_date
    from scratchpad.data_eng.cai_test_events
    where event_date = '2023-01-31'
    group by user_id
)
;

select * from scratchpad.data_eng.cai_test_active_users_daily
;
