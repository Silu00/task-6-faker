CREATE SCHEMA task_6;

CREATE TABLE task_6.lookup_data (
    id SERIAL PRIMARY KEY,
    category VARCHAR(64) NOT NULL,
    locale VARCHAR(64) NOT NULL,
    value TEXT NOT NULL,
    CONSTRAINT lookup_entry UNIQUE (category, locale, value)
);
CREATE INDEX idx_lookup_cat_loc ON task_6.lookup_data (category, locale);
CREATE TABLE task_6.formatting (
    id SERIAL PRIMARY KEY,
    category VARCHAR(64) NOT NULL,
    locale VARCHAR(64) NOT NULL,
    pattern TEXT NOT NULL
);
CREATE INDEX idx_formatting_cat_loc ON task_6.formatting (category, locale);

CREATE OR REPLACE FUNCTION task_6.get_deterministic_float(seed_text text)
RETURNS float as $$
DECLARE
    hash_val bigint;
    max_int constant float := 2147483647.0;
BEGIN
    hash_val := hashtext(seed_text);
    RETURN abs(hash_val)::float / max_int;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION task_6.get_normal_dist(
    mu float,
    sigma float,
    seed_text text
)
RETURNS float AS $$
DECLARE
    u1 float;
    u2 float;
    z0 float;
BEGIN
    u1 := task_6.get_deterministic_float(seed_text || '_norm_u1');
    u2 := task_6.get_deterministic_float(seed_text || '_norm_u2');
    IF u1 < 0.0000001 THEN
        u1 := 0.0000001;
    END IF;
    --Box-Muller: Z = sqrt(-2 * ln(u1)) * cos(2 * PI * u2)
    z0 := sqrt(-2.0 * ln(u1)) * cos(2.0 * 3.14159265359 * u2);
    RETURN mu + (z0 * sigma);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION task_6.get_geo_location(seed_text text)
RETURNS float[] AS $$
DECLARE
    u float;
    v float;
    lon float;
    lat float;
BEGIN
    u := task_6.get_deterministic_float(seed_text || '_geo_u');
    v := task_6.get_deterministic_float(seed_text || '_geo_v');
    lon := (360.0 * u) - 180.0;
    lat := degrees(asin(2.0 * v - 1.0));
    RETURN ARRAY[lat, lon];
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION task_6.pick_lookup_value(
    p_category text,
    p_locale text,
    seed_text text
)
RETURNS text AS $$
DECLARE
    total_count int;
    random_idx int;
    result_val text;
BEGIN
    SELECT count(*) INTO total_count
    FROM task_6.lookup_data
    WHERE category = p_category AND locale = p_locale;

    random_idx := floor(task_6.get_deterministic_float(seed_text) * total_count)::int;

    SELECT value INTO result_val
    FROM task_6.lookup_data
    WHERE category = p_category AND locale = p_locale
    ORDER BY value
    OFFSET random_idx LIMIT 1;
    RETURN result_val;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION task_6.apply_format_pattern(
    pattern text,
    seed_text text
)
RETURNS text AS $$
DECLARE
    result_str text := '';
    i int;
    curr_char text;
    random_digit int;
BEGIN
    IF pattern IS NULL THEN RETURN ''; END IF;

    FOR i IN 1..length(pattern) LOOP
        curr_char := substr(pattern, i, 1);

        IF curr_char = '#' THEN
            random_digit := floor(task_6.get_deterministic_float(seed_text || '_char_' || i) * 10)::int;
            result_str := result_str || random_digit::text;
        ELSE
            result_str := result_str || curr_char;
        END IF;
    END LOOP;

    RETURN result_str;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION task_6.get_random_formatted(
    p_category text,
    p_locale text,
    seed_text text
)
RETURNS text AS $$
DECLARE
    total_count int;
    random_idx int;
    chosen_pattern text;
BEGIN
    SELECT count(*) INTO total_count
    FROM task_6.formatting
    WHERE category = p_category AND locale = p_locale;

    IF total_count = 0 THEN RETURN NULL; END IF;

    random_idx := floor(task_6.get_deterministic_float(seed_text || '_fmt_idx') * total_count)::int;

    SELECT pattern INTO chosen_pattern
    FROM task_6.formatting
    WHERE category = p_category AND locale = p_locale
    ORDER BY pattern
    OFFSET random_idx LIMIT 1;

    RETURN task_6.apply_format_pattern(chosen_pattern, seed_text || '_fmt_val');
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION task_6.generate_users_batch(
    p_global_seed text,
    p_locales text[],
    p_batch_id int,
    p_count int
)
RETURNS TABLE (
    user_id text,
    full_name text,
    gender text,
    email_address text,
    phone_number text,
    address_full text,
    geo_lat float,
    geo_lon float,
    height_cm int,
    weight_kg numeric(5,1),
    eye_color text,
    hair_color text,
    skin_tone text,
    body_build text
) AS $$
BEGIN
    RETURN QUERY
    WITH basis AS (
        SELECT generate_series(1, p_count) as idx
    ),
    user_config AS (
        SELECT
            idx,
            p_global_seed || '_b' || p_batch_id || '_u' || idx as u_seed,
            CASE
                WHEN task_6.get_deterministic_float(p_global_seed || '_b' || p_batch_id || '_u' || idx || '_sex') < 0.5 THEN 'M'
                ELSE 'F'
            END as sex_code,
            p_locales[
                floor(
                    task_6.get_deterministic_float(p_global_seed || '_b' || p_batch_id || '_u' || idx || '_loc')
                    * array_length(p_locales, 1)
                )::int + 1
            ] as assigned_locale
        FROM basis
    ),
    raw_data AS (
        SELECT
            uc.idx,
            uc.u_seed,
            uc.sex_code,
            uc.assigned_locale,
            CASE
                WHEN uc.sex_code = 'M' THEN task_6.pick_lookup_value('fname_male', uc.assigned_locale, uc.u_seed || '_fn')
                ELSE task_6.pick_lookup_value('fname_female', uc.assigned_locale, uc.u_seed || '_fn')
            END as first_name,
            CASE
                WHEN task_6.get_deterministic_float(uc.u_seed || '_has_mn') < 0.35 THEN
                    CASE
                        WHEN uc.sex_code = 'M' THEN task_6.pick_lookup_value('middlename_male', uc.assigned_locale, uc.u_seed || '_mn')
                        ELSE task_6.pick_lookup_value('middlename_female', uc.assigned_locale, uc.u_seed || '_mn')
                    END
                END as middle_name,
            task_6.pick_lookup_value('last_name', uc.assigned_locale, uc.u_seed || '_ln') as last_name,
            CASE
                WHEN task_6.get_deterministic_float(uc.u_seed || '_has_title') < 0.2 THEN
                    CASE
                        WHEN uc.sex_code = 'M' THEN task_6.pick_lookup_value('title_male', uc.assigned_locale, uc.u_seed || '_tl')
                        ELSE task_6.pick_lookup_value('title_female', uc.assigned_locale, uc.u_seed || '_tl')
                    END
                END as title,
            task_6.pick_lookup_value('city', uc.assigned_locale, uc.u_seed || '_city') as city,
            task_6.pick_lookup_value('street', uc.assigned_locale, uc.u_seed || '_st') as street,
            task_6.pick_lookup_value('email_domain', uc.assigned_locale, uc.u_seed || '_em') as email_domain,
            task_6.pick_lookup_value('eye_color', uc.assigned_locale, uc.u_seed || '_eye') as eyes,
            task_6.pick_lookup_value('hair_color', uc.assigned_locale, uc.u_seed || '_hair') as hair,
            task_6.pick_lookup_value('skin_tone', uc.assigned_locale, uc.u_seed || '_skin') as skin,
            task_6.pick_lookup_value('build', uc.assigned_locale, uc.u_seed || '_build') as body_build
        FROM user_config uc
    )
    SELECT
        (p_batch_id || '_' || rd.idx) as user_id,

        TRIM(CONCAT_WS(' ', rd.title, rd.first_name, rd.middle_name, rd.last_name)) as full_name,

        rd.sex_code as gender,

        LOWER(rd.first_name || '.' || rd.last_name || '@' || rd.email_domain) as email,
        task_6.get_random_formatted('phone_format', rd.assigned_locale, rd.u_seed || '_ph') as phone,

        CONCAT(
            rd.street, ' ',
            floor(task_6.get_deterministic_float(rd.u_seed || '_hn') * 200 + 1)::int, ', ',
            task_6.get_random_formatted('postcode_format', rd.assigned_locale, rd.u_seed || '_pc'), ' ',
            rd.city
        ) as address_full,

        (task_6.get_geo_location(rd.u_seed || '_geo'))[1] as geo_lat,
        (task_6.get_geo_location(rd.u_seed || '_geo'))[2] as geo_lon,

        ROUND(task_6.get_normal_dist(
            CASE WHEN rd.sex_code='M' THEN 178 ELSE 165 END,
            CASE WHEN rd.sex_code='M' THEN 10 ELSE 8 END,
            rd.u_seed || '_h'
        )::numeric)::int as height_cm,

        ROUND(task_6.get_normal_dist(
            CASE WHEN rd.sex_code='M' THEN 85 ELSE 65 END,
            CASE WHEN rd.sex_code='M' THEN 15 ELSE 12 END,
            rd.u_seed || '_w'
        )::numeric, 1) as weight_kg,

        rd.eyes,
        rd.hair,
        rd.skin,
        rd.body_build

    FROM raw_data rd;
END;
$$ LANGUAGE plpgsql;
