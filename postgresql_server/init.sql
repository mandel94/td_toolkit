CREATE DATABASE "td_db";

\c td_db


CREATE TABLE "article" (
  "id" int PRIMARY KEY,
  "title" varchar(1000) NOT NULL,
  "link" varchar,
  "pagepath" varchar,
  "pubDate" timestamp,
  "category" varchar(50),
  "publication_tags" varchar,
  "content" text,
  "post_id" int,
  "post_views" int,
  "yoast_focus_keyword" varchar(50),
  "yoast_metadesc" varchar,
  "yoast_seo_score" smallint,
  "yoast_content_readability_score" smallint,
  "yoast_keyword_synonyms" varchar,
  "yoast_estimated_reading_time" smallint
);
