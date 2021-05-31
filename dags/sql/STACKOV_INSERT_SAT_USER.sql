INSERT INTO so_sat_user
(user_h_fk, load_dts, display_name, profile_image, user_type, user_link, rec_src, hash_diff)
VALUES
( MD5(%(user_bk)s || %(rec_src)s), %(load_dts)s, %(display_name)s, %(profile_image)s, %(user_type)s, %(user_link)s, %(rec_src)s,
( MD5((MD5(%(user_bk)s || %(rec_src)s)) || %(load_dts)s || %(display_name)s
|| %(profile_image)s || %(user_type)s || %(user_link)s || %(rec_src)s)
))
;