-- ************************************** HUB_POST

CREATE TABLE IF NOT EXISTS HUB_POST
(
 post_id  varchar NOT NULL,
 load_dts timestamp NOT NULL,
 rec_src  varchar NOT NULL,
 CONSTRAINT PK_hub_post PRIMARY KEY ( post_id )
);


-- ************************************** HUB_USER

CREATE TABLE IF NOT EXISTS HUB_USER
(
 user_id  varchar NOT NULL,
 load_dts timestamp NOT NULL,
 rec_src  varchar NOT NULL,
 CONSTRAINT PK_user_h PRIMARY KEY ( user_id )
);


-- ************************************** LINK_USER_POSTS

CREATE TABLE IF NOT EXISTS LINK_USER_POSTS
(
 user_post_link_pk varchar NOT NULL,
 user_id_pk        varchar NOT NULL,
 post_id_pk        varchar NOT NULL,
 load_dts          timestamp NOT NULL,
 rec_src           varchar NOT NULL,
 CONSTRAINT PK_link_user_posts PRIMARY KEY ( user_post_link_pk ),
 CONSTRAINT FK_43 FOREIGN KEY ( user_id_pk ) REFERENCES HUB_USER ( user_id ),
 CONSTRAINT FK_46 FOREIGN KEY ( post_id_pk ) REFERENCES HUB_POST ( post_id )
);

CREATE INDEX IF NOT EXISTS fkIdx_44 ON LINK_USER_POSTS
(
 user_id_pk
);

CREATE INDEX IF NOT EXISTS fkIdx_47 ON LINK_USER_POSTS
(
 post_id_pk
);


-- ************************************** SO_SAT_POST

CREATE TABLE IF NOT EXISTS SO_SAT_POST
(
 post_id_h_fk varchar NOT NULL,
 load_dts     timestamp NOT NULL,
 post_name    varchar NOT NULL,
 content      text NULL,
 rec_src      varchar NOT NULL,
 hash_diff    varchar NOT NULL,
 CONSTRAINT PK_sat_post PRIMARY KEY ( post_id_h_fk, load_dts ),
 CONSTRAINT FK_33 FOREIGN KEY ( post_id_h_fk ) REFERENCES HUB_POST ( post_id )
);

CREATE INDEX IF NOT EXISTS fkIdx_34 ON SO_SAT_POST
(
 post_id_h_fk
);



-- ************************************** SO_SAT_USER

CREATE TABLE IF NOT EXISTS SO_SAT_USER
(
 user_id_h_fk  varchar NOT NULL,
 load_dts      timestamp NOT NULL,
 display_name  varchar(50) NULL,
 profile_image varchar NULL,
 user_type     varchar NOT NULL,
 user_link          varchar NOT NULL,
 rec_src       varchar NOT NULL,
 hash_diff     varchar NOT NULL,
 CONSTRAINT PK_sat_user PRIMARY KEY ( user_id_h_fk, load_dts ),
 CONSTRAINT FK_22 FOREIGN KEY ( user_id_h_fk ) REFERENCES HUB_USER ( user_id )
);

CREATE INDEX IF NOT EXISTS fkIdx_23 ON SO_SAT_USER
(
 user_id_h_fk
);



-- ************************************** SO_SAT_USER_SCORE

CREATE TABLE IF NOT EXISTS SO_SAT_USER_SCORE
(
 user_id_h_fk varchar NOT NULL,
 load_dts     timestamp NOT NULL,
 score        numeric NULL,
 accept_rate  numeric NULL,
 post_count   numeric NOT NULL,
 reputation   numeric NOT NULL,
 rec_src      varchar NOT NULL,
 hash_diff    varchar NOT NULL,
 CONSTRAINT PK_sat_user_score PRIMARY KEY ( user_id_h_fk, load_dts ),
 CONSTRAINT FK_52 FOREIGN KEY ( user_id_h_fk ) REFERENCES HUB_USER ( user_id )
);

CREATE INDEX IF NOT EXISTS fkIdx_53 ON SO_SAT_USER_SCORE
(
 user_id_h_fk
);




