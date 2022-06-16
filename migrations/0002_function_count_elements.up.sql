CREATE FUNCTION count_elements(text[]) RETURNS text AS $BODY$
select string_agg(i || ':' || c, ',' ) from (select i, count(*) c FROM (select unnest($1::text[]) i) i group by i order by c desc) foo;
$BODY$
LANGUAGE SQL;