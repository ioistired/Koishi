CREATE TABLE koi_test.names(
	uid BIGINT NOT NULL,
	name TEXT NOT NULL NOT NULL,
	first_seen TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP);

CREATE INDEX ON names (uid);

CREATE TABLE koi_test.avatars(
	uid BIGINT NOT NULL,
	avatar TEXT NOT NULL,
	avatar_url TEXT NOT NULL,
	first_seen TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP);

CREATE INDEX ON avatars (uid);

CREATE TABLE koi_test.avy_urls(
	hash TEXT PRIMARY KEY,
	url TEXT NOT NULL,
	msgid BIGINT NOT NULL,
	id BIGINT NOT NULL,
	size BIGINT NOT NULL,
	-- these are allowed to be null in case the embed server fails to detect the image size
	height BIGINT,
	width BIGINT);

CREATE TABLE koi_test.discrims(
	uid BIGINT NOT NULL,
	discrim TEXT NOT NULL,
	first_seen TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP);

CREATE INDEX ON discrims (uid);

CREATE TABLE koi_test.nicks(
	uid BIGINT NOT NULL,
	sid BIGINT NOT NULL,
	nick TEXT,
	first_seen TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP);

CREATE INDEX ON nicks (uid, sid);

CREATE TABLE koi_test.statuses(
	uid BIGINT NOT NULL,
	status TEXT NOT NULL,
	first_seen TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP);

CREATE INDEX ON statuses (uid);	

CREATE TABLE koi_test.games(
	uid BIGINT NOT NULL,
	game TEXT,
	first_seen TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP);

CREATE INDEX ON games (uid);
