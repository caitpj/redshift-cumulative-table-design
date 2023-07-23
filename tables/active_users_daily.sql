create table database_name.schema_name.active_users_daily (
    user_id integer,
    is_active_today integer,
    num_likes integer,
    num_comments integer,
    num_shares integer,
    snapshot_date date
)
-- Sort key is important since we only want to scan the most recent event_date for each run
sortkey
    (event_date)
;

-- Inserting data post 2023-01-30 run, and pre 2023-01-31 run 
insert into database_name.schema_name.active_users_daily values
(1, 1, 1, 0, 0, '2023-01-01'),
(1, 1, 1, 0, 0, '2023-01-02'),
(1, 1, 1, 0, 0, '2023-01-03'),
(1, 1, 0, 0, 1, '2023-01-04'),
(1, 1, 0, 1, 0, '2023-01-05'),
(1, 0, 0, 0, 0, '2023-01-06'),
(1, 0, 0, 0, 0, '2023-01-07'),
(1, 0, 0, 0, 0, '2023-01-08'),
(1, 0, 0, 0, 0, '2023-01-09'),
(1, 0, 0, 0, 0, '2023-01-10'),
(1, 0, 0, 0, 0, '2023-01-11'),
(1, 0, 0, 0, 0, '2023-01-12'),
(1, 0, 0, 0, 0, '2023-01-13'),
(1, 0, 0, 0, 0, '2023-01-14'),
(1, 0, 0, 0, 0, '2023-01-15'),
(1, 0, 0, 0, 0, '2023-01-16'),
(1, 0, 0, 0, 0, '2023-01-17'),
(1, 0, 0, 0, 0, '2023-01-18'),
(1, 0, 0, 0, 0, '2023-01-19'),
(1, 0, 0, 0, 0, '2023-01-20'),
(1, 0, 0, 0, 0, '2023-01-21'),
(1, 0, 0, 0, 0, '2023-01-22'),
(1, 0, 0, 0, 0, '2023-01-23'),
(1, 0, 0, 0, 0, '2023-01-24'),
(1, 0, 0, 0, 0, '2023-01-25'),
(1, 0, 0, 0, 0, '2023-01-26'),
(1, 0, 0, 0, 0, '2023-01-27'),
(1, 0, 0, 0, 0, '2023-01-28'),
(1, 0, 0, 0, 0, '2023-01-29'),
(1, 1, 0, 1, 0, '2023-01-30'),
(2, 1, 1, 0, 0, '2023-01-30')
;

select * from database_name.schema_name.active_users_daily
;
