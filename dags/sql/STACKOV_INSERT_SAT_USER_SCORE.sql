INSERT INTO so_sat_user_score
(user_h_fk, load_dts, score, accept_rate, post_count, reputation, rec_src, hash_diff)
VALUES
( MD5(%(user_bk)s || %(rec_src)s), %(load_dts)s, %(score)s,  %(accept_rate)s, %(post_count)s, %(reputation)s, %(rec_src)s,
( MD5(CONCAT( (MD5(%(user_bk)s || %(rec_src)s)), %(load_dts)s, %(score)s,  %(accept_rate)s,
%(post_count)s, %(reputation)s, %(rec_src)s)))
)
;