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
