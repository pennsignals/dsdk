create schema if not exists test;
grant usage on test to public;
grant create on test to public;
set search_path = test,public;
