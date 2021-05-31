INSERT INTO hub_user
(user_pk, user_bk, load_dts, rec_src)
VALUES
((MD5(%(user_bk)s || %(rec_src)s)), %(user_bk)s, %(load_dts)s, %(rec_src)s);