CREATE OR REPLACE FUNCTION public.json_append(data json, insert_data json)
RETURNS json
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT ('{'||string_agg(to_json(key)||':'||value, ',')||'}')::json
    FROM (
        SELECT * FROM json_each(data)
        UNION ALL
        SELECT * FROM json_each(insert_data)
    ) t;
$$;

UPDATE computation_strategies
SET parameters = json_append(parameters::json, '{ "primary_key" : "_feature_id" }')
WHERE (parameters::json->'primary_key')::text is null;