WITH first_insert AS (
    INSERT INTO hub_user
    (user_bk, user_pk, load_dts, rec_src)
    VALUES
    ((MD5(%(user_pk)s || %(rec_src)s)), %(user_pk)s, %(load_dts)s, %(rec_src)s)
    RETURNING user_pk
),
second_insert AS (
    INSERT INTO so_sat_user
    (user_h_fk, load_dts, display_name, profile_image, user_type, user_link, rec_src, hash_diff)
    VALUES
    ( (SELECT user_pk from first_insert), %(load_dts)s, %(display_name)s, %(profile_image)s,
      %(user_type)s, %(user_link)s, %(rec_src)s,
      (MD5((SELECT user_pk from first_insert) || %(load_dts)s || %(display_name)s
    || %(profile_image)s || %(user_type)s || %(user_link)s || %(rec_src)s)) )
)
INSERT INTO so_sat_user_score
(user_h_fk, load_dts, score, accept_rate, post_count, reputation, rec_src, hash_diff)
VALUES
( (SELECT user_pk from first_insert), %(load_dts)s, %(score)s,  %(accept_rate)s,
  %(post_count)s, %(reputation)s, %(rec_src)s,
  (MD5(CONCAT( (SELECT user_pk from first_insert), %(load_dts)s, %(score)s,  %(accept_rate)s,
  %(post_count)s, %(reputation)s, %(rec_src)s)))
)
;
