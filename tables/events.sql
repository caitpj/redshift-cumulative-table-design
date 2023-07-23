create table database_name.schema_name.events(
    user_id Integer,
    event_type VARCHAR,
    event_date Date
)
-- Sort key is important since we only want to scan the most recent event_date for each run
sortkey
    (event_date)
;

insert into database_name.schema_name.events values
    (1, 'like', '2023-01-01'),
    (1, 'like', '2023-01-02'),
    (1, 'like', '2023-01-03'),
    (1, 'share', '2023-01-04'),
    (1, 'comment', '2023-01-05'),
    (1, 'comment', '2023-01-30'),
    (1, 'comment', '2023-01-31'),
    (2, 'like', '2023-01-30'),
    (2, 'share', '2023-01-31'),
    (3, 'like', '2023-01-31')
;

select * from database_name.schema_name.events
;
