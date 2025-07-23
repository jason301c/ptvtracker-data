-- Performance Optimization Migration
-- This migration implements various performance improvements for the GTFS database

-- =====================================================
-- STEP 1: QUERY OPTIMIZATION
-- =====================================================

-- 1.1 Optimize gtfs.get_scheduled_stop_times function
CREATE OR REPLACE FUNCTION gtfs.get_scheduled_stop_times(
    p_source_id text,
    p_stop_id text,
    p_from_time text DEFAULT NULL,
    p_to_time text DEFAULT NULL,
    p_route_types integer[] DEFAULT NULL,
    p_limit integer DEFAULT 100
)
RETURNS TABLE (
    arrival_time_with_offset interval,
    departure_time_with_offset interval,
    trip_id text,
    trip_headsign text,
    trip_short_name text,
    stop_headsign text,
    route_id text,
    route_short_name text,
    route_long_name text,
    route_type integer,
    route_color text,
    route_text_color text,
    agency_id text,
    agency_name text,
    agency_timezone text,
    service_id text,
    direction_id smallint,
    stop_sequence integer,
    relative_position text
) AS $$
DECLARE
    v_current_time time;
    v_from_seconds integer;
    v_to_seconds integer;
    v_today date;
    v_service_date date;
    v_24h_from_seconds integer;
    v_route_type_filter text;
BEGIN
    -- Initialize current time and date
    v_current_time := CURRENT_TIME;
    v_today := CURRENT_DATE;
    
    -- Convert times to seconds with 24-hour handling
    IF p_from_time IS NOT NULL THEN
        v_from_seconds := EXTRACT(HOUR FROM p_from_time::time) * 3600 + 
                         EXTRACT(MINUTE FROM p_from_time::time) * 60 + 
                         EXTRACT(SECOND FROM p_from_time::time);
        v_24h_from_seconds := v_from_seconds + 86400; -- For trips after midnight
    ELSE
        v_from_seconds := 0;
        v_24h_from_seconds := 86400;
    END IF;
    
    IF p_to_time IS NOT NULL THEN
        v_to_seconds := EXTRACT(HOUR FROM p_to_time::time) * 3600 + 
                       EXTRACT(MINUTE FROM p_to_time::time) * 60 + 
                       EXTRACT(SECOND FROM p_to_time::time);
    ELSE
        v_to_seconds := 172800; -- 48 hours to handle overnight trips
    END IF;
    
    -- Build route type filter for dynamic SQL
    IF p_route_types IS NOT NULL AND array_length(p_route_types, 1) > 0 THEN
        v_route_type_filter := format(' AND r.route_type = ANY(ARRAY[%s]::integer[])', 
                                     array_to_string(p_route_types, ','));
    ELSE
        v_route_type_filter := '';
    END IF;
    
    -- Determine which service date to check
    -- If current time is before 3 AM, also check yesterday's services
    IF EXTRACT(HOUR FROM v_current_time) < 3 THEN
        v_service_date := v_today - INTERVAL '1 day';
    ELSE
        v_service_date := v_today;
    END IF;
    
    RETURN QUERY
    WITH active_services AS (
        -- Pre-filter active services for today
        SELECT DISTINCT c.service_id
        FROM gtfs.calendar c
        WHERE c.source_id = p_source_id
          AND v_today BETWEEN c.start_date AND c.end_date
          AND (
            (EXTRACT(DOW FROM v_today) = 0 AND c.sunday = true) OR
            (EXTRACT(DOW FROM v_today) = 1 AND c.monday = true) OR
            (EXTRACT(DOW FROM v_today) = 2 AND c.tuesday = true) OR
            (EXTRACT(DOW FROM v_today) = 3 AND c.wednesday = true) OR
            (EXTRACT(DOW FROM v_today) = 4 AND c.thursday = true) OR
            (EXTRACT(DOW FROM v_today) = 5 AND c.friday = true) OR
            (EXTRACT(DOW FROM v_today) = 6 AND c.saturday = true)
          )
        
        UNION
        
        -- Include services added by calendar_dates
        SELECT DISTINCT cd.service_id
        FROM gtfs.calendar_dates cd
        WHERE cd.source_id = p_source_id
          AND cd.date = v_today
          AND cd.exception_type = 1
        
        EXCEPT
        
        -- Exclude services removed by calendar_dates
        SELECT DISTINCT cd.service_id
        FROM gtfs.calendar_dates cd
        WHERE cd.source_id = p_source_id
          AND cd.date = v_today
          AND cd.exception_type = 2
    ),
    stop_times_filtered AS (
        -- Get stop times for the specific stop with minimal columns
        SELECT 
            st.trip_id,
            st.arrival_time_seconds,
            st.departure_time_seconds,
            st.stop_headsign,
            st.stop_sequence
        FROM gtfs.stop_times st
        WHERE st.source_id = p_source_id
          AND st.stop_id = p_stop_id
          AND (
            -- Normal times within range
            (st.arrival_time_seconds >= v_from_seconds AND st.arrival_time_seconds <= v_to_seconds)
            OR
            -- Times after midnight (when arrival_seconds > 86400)
            (st.arrival_time_seconds >= v_24h_from_seconds AND st.arrival_time_seconds <= v_to_seconds)
          )
    )
    SELECT DISTINCT ON (st.arrival_time_seconds, t.trip_id)
        make_interval(secs => st.arrival_time_seconds) as arrival_time_with_offset,
        make_interval(secs => st.departure_time_seconds) as departure_time_with_offset,
        t.trip_id,
        t.trip_headsign,
        t.trip_short_name,
        st.stop_headsign,
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        r.route_color,
        r.route_text_color,
        a.agency_id,
        a.agency_name,
        a.agency_timezone,
        t.service_id,
        t.direction_id,
        st.stop_sequence,
        CASE 
            WHEN st.arrival_time_seconds - EXTRACT(EPOCH FROM v_current_time) < 0 THEN 'past'
            WHEN st.arrival_time_seconds - EXTRACT(EPOCH FROM v_current_time) <= 300 THEN 'imminent'
            ELSE 'future'
        END as relative_position
    FROM stop_times_filtered st
    INNER JOIN gtfs.trips t 
        ON st.trip_id = t.trip_id 
        AND t.source_id = p_source_id
    INNER JOIN active_services acs 
        ON t.service_id = acs.service_id
    INNER JOIN gtfs.routes r 
        ON t.route_id = r.route_id 
        AND r.source_id = p_source_id
    INNER JOIN gtfs.agency a 
        ON r.agency_id = a.agency_id 
        AND a.source_id = p_source_id
    WHERE 1=1
        -- Apply dynamic route type filter if provided
        AND (p_route_types IS NULL OR r.route_type = ANY(p_route_types))
    ORDER BY st.arrival_time_seconds, t.trip_id
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql STABLE;

-- 1.2 Optimize gtfs.get_stop_routes function
CREATE OR REPLACE FUNCTION gtfs.get_stop_routes(
    p_source_id text,
    p_stop_id text
)
RETURNS TABLE (
    route_id text,
    route_short_name text,
    route_long_name text,
    route_type integer,
    route_color text,
    route_text_color text,
    agency_id text,
    agency_name text,
    trip_count bigint
) AS $$
BEGIN
    RETURN QUERY
    WITH route_trips AS (
        SELECT DISTINCT
            t.route_id,
            t.trip_id
        FROM gtfs.trips t
        INNER JOIN gtfs.stop_times st 
            ON t.trip_id = st.trip_id 
            AND t.source_id = st.source_id
        WHERE st.source_id = p_source_id
          AND st.stop_id = p_stop_id
    )
    SELECT 
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        r.route_color,
        r.route_text_color,
        a.agency_id,
        a.agency_name,
        COUNT(DISTINCT rt.trip_id) as trip_count
    FROM route_trips rt
    INNER JOIN gtfs.routes r 
        ON rt.route_id = r.route_id 
        AND r.source_id = p_source_id
    INNER JOIN gtfs.agency a 
        ON r.agency_id = a.agency_id 
        AND a.source_id = p_source_id
    GROUP BY 
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        r.route_color,
        r.route_text_color,
        a.agency_id,
        a.agency_name
    ORDER BY 
        r.route_type,
        r.route_short_name,
        r.route_long_name;
END;
$$ LANGUAGE plpgsql STABLE;

-- 1.3 Add optimized get_stop_departures function used by API
CREATE OR REPLACE FUNCTION gtfs.get_stop_departures(
    p_stop_id TEXT,
    p_source_id INTEGER,
    p_limit INTEGER DEFAULT 50,
    p_version_id INTEGER DEFAULT NULL
)
RETURNS TABLE (
    trip_id VARCHAR,
    route_id VARCHAR,
    route_short_name VARCHAR,
    route_long_name VARCHAR,
    route_type SMALLINT,
    route_color VARCHAR,
    route_text_color VARCHAR,
    trip_headsign VARCHAR,
    stop_id VARCHAR,
    stop_name VARCHAR,
    scheduled_departure_time_seconds INTEGER,
    scheduled_departure_time TIME,
    stop_sequence INTEGER,
    pickup_type SMALLINT,
    drop_off_type SMALLINT,
    source_id INTEGER,
    version_id INTEGER,
    platform_id VARCHAR,
    direction_id SMALLINT
) AS $$
DECLARE
    v_version_id INTEGER;
    v_platform_ids TEXT[];
    v_current_time_seconds INTEGER;
    v_current_date_melbourne DATE;
    v_yesterday_date_melbourne DATE;
BEGIN
    -- Get active version if not specified
    IF p_version_id IS NULL THEN
        SELECT av.version_id INTO v_version_id 
        FROM active_version av 
        LIMIT 1;
    ELSE
        v_version_id := p_version_id;
    END IF;

    -- Resolve stop to all relevant platform/stop IDs
    v_platform_ids := gtfs.resolve_stop_platforms(p_stop_id, p_source_id, v_version_id);

    IF v_platform_ids IS NULL OR array_length(v_platform_ids, 1) = 0 THEN
        RETURN;
    END IF;

    -- Get current time context
    v_current_time_seconds := EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'Australia/Melbourne')::TIME);
    v_current_date_melbourne := (NOW() AT TIME ZONE 'Australia/Melbourne')::DATE;
    v_yesterday_date_melbourne := v_current_date_melbourne - INTERVAL '1 day';

    RETURN QUERY
    WITH active_services AS (
        -- Use the optimized service check
        SELECT DISTINCT c.service_id
        FROM gtfs.calendar c
        WHERE c.source_id = p_source_id
          AND c.version_id = v_version_id
          AND v_current_date_melbourne BETWEEN c.start_date AND c.end_date
          AND (
            (EXTRACT(ISODOW FROM v_current_date_melbourne) = 1 AND c.monday = 1) OR
            (EXTRACT(ISODOW FROM v_current_date_melbourne) = 2 AND c.tuesday = 1) OR
            (EXTRACT(ISODOW FROM v_current_date_melbourne) = 3 AND c.wednesday = 1) OR
            (EXTRACT(ISODOW FROM v_current_date_melbourne) = 4 AND c.thursday = 1) OR
            (EXTRACT(ISODOW FROM v_current_date_melbourne) = 5 AND c.friday = 1) OR
            (EXTRACT(ISODOW FROM v_current_date_melbourne) = 6 AND c.saturday = 1) OR
            (EXTRACT(ISODOW FROM v_current_date_melbourne) = 7 AND c.sunday = 1)
          )
        
        UNION
        
        SELECT DISTINCT cd.service_id
        FROM gtfs.calendar_dates cd
        WHERE cd.source_id = p_source_id
          AND cd.version_id = v_version_id
          AND cd.date = v_current_date_melbourne
          AND cd.exception_type = 1
        
        EXCEPT
        
        SELECT DISTINCT cd.service_id
        FROM gtfs.calendar_dates cd
        WHERE cd.source_id = p_source_id
          AND cd.version_id = v_version_id
          AND cd.date = v_current_date_melbourne
          AND cd.exception_type = 2
    ),
    combined_departures AS (
        -- Today's departures
        SELECT
            st.trip_id, t.trip_headsign, t.direction_id,
            r.route_id, r.route_short_name, r.route_long_name, r.route_type,
            r.route_color, r.route_text_color,
            st.stop_id, s.stop_name,
            st.departure_time_seconds,
            st.stop_sequence, st.pickup_type, st.drop_off_type,
            st.source_id, st.version_id,
            0 as day_offset
        FROM gtfs.stop_times st
        JOIN gtfs.stops s ON st.stop_id = s.stop_id 
            AND st.version_id = s.version_id 
            AND st.source_id = s.source_id
        JOIN gtfs.trips t ON st.trip_id = t.trip_id 
            AND st.version_id = t.version_id 
            AND st.source_id = t.source_id
        JOIN gtfs.routes r ON t.route_id = r.route_id 
            AND t.version_id = r.version_id 
            AND t.source_id = r.source_id
        JOIN active_services acs ON t.service_id = acs.service_id
        WHERE st.version_id = v_version_id
          AND st.source_id = p_source_id
          AND st.stop_id = ANY(v_platform_ids)
          AND st.departure_time_seconds >= v_current_time_seconds
          AND EXISTS (
              SELECT 1 FROM gtfs.stop_times st2
              WHERE st2.trip_id = st.trip_id
                AND st2.version_id = st.version_id
                AND st2.source_id = st.source_id
                AND st2.stop_sequence > st.stop_sequence
          )

        UNION ALL

        -- Yesterday's overnight departures  
        SELECT
            st.trip_id, t.trip_headsign, t.direction_id,
            r.route_id, r.route_short_name, r.route_long_name, r.route_type,
            r.route_color, r.route_text_color,
            st.stop_id, s.stop_name,
            st.departure_time_seconds,
            st.stop_sequence, st.pickup_type, st.drop_off_type,
            st.source_id, st.version_id,
            1 as day_offset
        FROM gtfs.stop_times st
        JOIN gtfs.stops s ON st.stop_id = s.stop_id 
            AND st.version_id = s.version_id 
            AND st.source_id = s.source_id
        JOIN gtfs.trips t ON st.trip_id = t.trip_id 
            AND st.version_id = t.version_id 
            AND st.source_id = t.source_id
        JOIN gtfs.routes r ON t.route_id = r.route_id 
            AND t.version_id = r.version_id 
            AND t.source_id = r.source_id
        JOIN (
            -- Yesterday's active services
            SELECT DISTINCT c.service_id
            FROM gtfs.calendar c
            WHERE c.source_id = p_source_id
              AND c.version_id = v_version_id
              AND v_yesterday_date_melbourne BETWEEN c.start_date AND c.end_date
              AND (
                (EXTRACT(ISODOW FROM v_yesterday_date_melbourne) = 1 AND c.monday = 1) OR
                (EXTRACT(ISODOW FROM v_yesterday_date_melbourne) = 2 AND c.tuesday = 1) OR
                (EXTRACT(ISODOW FROM v_yesterday_date_melbourne) = 3 AND c.wednesday = 1) OR
                (EXTRACT(ISODOW FROM v_yesterday_date_melbourne) = 4 AND c.thursday = 1) OR
                (EXTRACT(ISODOW FROM v_yesterday_date_melbourne) = 5 AND c.friday = 1) OR
                (EXTRACT(ISODOW FROM v_yesterday_date_melbourne) = 6 AND c.saturday = 1) OR
                (EXTRACT(ISODOW FROM v_yesterday_date_melbourne) = 7 AND c.sunday = 1)
              )
            UNION
            SELECT DISTINCT cd.service_id
            FROM gtfs.calendar_dates cd
            WHERE cd.source_id = p_source_id
              AND cd.version_id = v_version_id
              AND cd.date = v_yesterday_date_melbourne
              AND cd.exception_type = 1
            EXCEPT
            SELECT DISTINCT cd.service_id
            FROM gtfs.calendar_dates cd
            WHERE cd.source_id = p_source_id
              AND cd.version_id = v_version_id
              AND cd.date = v_yesterday_date_melbourne
              AND cd.exception_type = 2
        ) yesterday_services ON t.service_id = yesterday_services.service_id
        WHERE st.version_id = v_version_id
          AND st.source_id = p_source_id
          AND st.stop_id = ANY(v_platform_ids)
          AND st.departure_time_seconds >= 86400
          AND (st.departure_time_seconds - 86400) >= v_current_time_seconds
          AND EXISTS (
              SELECT 1 FROM gtfs.stop_times st2
              WHERE st2.trip_id = st.trip_id
                AND st2.version_id = st.version_id
                AND st2.source_id = st.source_id
                AND st2.stop_sequence > st.stop_sequence
          )
    )
    SELECT
        cd.trip_id::VARCHAR,
        cd.route_id::VARCHAR,
        cd.route_short_name::VARCHAR,
        cd.route_long_name::VARCHAR,
        cd.route_type::SMALLINT,
        cd.route_color::VARCHAR,
        cd.route_text_color::VARCHAR,
        cd.trip_headsign::VARCHAR,
        p_stop_id::VARCHAR as stop_id,
        cd.stop_name::VARCHAR,
        CASE 
            WHEN cd.day_offset = 1 THEN cd.departure_time_seconds - 86400
            ELSE cd.departure_time_seconds
        END::INTEGER as scheduled_departure_time_seconds,
        (TIME '00:00' + (CASE 
            WHEN cd.day_offset = 1 THEN cd.departure_time_seconds - 86400
            ELSE cd.departure_time_seconds
        END) * INTERVAL '1 second')::TIME as scheduled_departure_time,
        cd.stop_sequence::INTEGER,
        cd.pickup_type::SMALLINT,
        cd.drop_off_type::SMALLINT,
        cd.source_id::INTEGER,
        cd.version_id::INTEGER,
        CASE
            WHEN p_source_id = 2 THEN 
                COALESCE((SELECT pn.platform_number::VARCHAR 
                          FROM gtfs.platform_numbers pn 
                          WHERE pn.platform_id = cd.stop_id), cd.stop_id::VARCHAR)
            ELSE cd.stop_id::VARCHAR
        END as platform_id,
        cd.direction_id::SMALLINT
    FROM combined_departures cd
    ORDER BY 
        CASE 
            WHEN cd.day_offset = 1 THEN cd.departure_time_seconds - 86400
            ELSE cd.departure_time_seconds
        END
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- 1.4 Optimize service date check queries
CREATE OR REPLACE FUNCTION gtfs.is_service_active(
    p_source_id text,
    p_service_id text,
    p_date date DEFAULT CURRENT_DATE
)
RETURNS boolean AS $$
DECLARE
    v_is_active boolean;
BEGIN
    -- Check if service is active on the given date
    SELECT EXISTS (
        -- Check regular calendar
        SELECT 1
        FROM gtfs.calendar c
        WHERE c.source_id = p_source_id
          AND c.service_id = p_service_id
          AND p_date BETWEEN c.start_date AND c.end_date
          AND (
            (EXTRACT(DOW FROM p_date) = 0 AND c.sunday = true) OR
            (EXTRACT(DOW FROM p_date) = 1 AND c.monday = true) OR
            (EXTRACT(DOW FROM p_date) = 2 AND c.tuesday = true) OR
            (EXTRACT(DOW FROM p_date) = 3 AND c.wednesday = true) OR
            (EXTRACT(DOW FROM p_date) = 4 AND c.thursday = true) OR
            (EXTRACT(DOW FROM p_date) = 5 AND c.friday = true) OR
            (EXTRACT(DOW FROM p_date) = 6 AND c.saturday = true)
          )
          -- Not excluded by calendar_dates
          AND NOT EXISTS (
            SELECT 1
            FROM gtfs.calendar_dates cd
            WHERE cd.source_id = p_source_id
              AND cd.service_id = p_service_id
              AND cd.date = p_date
              AND cd.exception_type = 2
          )
        
        UNION
        
        -- Check calendar_dates additions
        SELECT 1
        FROM gtfs.calendar_dates cd
        WHERE cd.source_id = p_source_id
          AND cd.service_id = p_service_id
          AND cd.date = p_date
          AND cd.exception_type = 1
    ) INTO v_is_active;
    
    RETURN v_is_active;
END;
$$ LANGUAGE plpgsql STABLE;

-- =====================================================
-- STEP 2: INDEX OPTIMIZATION
-- =====================================================

-- 2.1 Create missing indexes for stop_times queries
CREATE INDEX IF NOT EXISTS idx_stop_times_lookup 
ON gtfs.stop_times(source_id, stop_id, arrival_time_seconds);

CREATE INDEX IF NOT EXISTS idx_stop_times_trip_lookup 
ON gtfs.stop_times(source_id, trip_id, stop_sequence);

-- 2.2 Create composite indexes for common join patterns
CREATE INDEX IF NOT EXISTS idx_trips_composite 
ON gtfs.trips(source_id, trip_id, route_id, service_id);

CREATE INDEX IF NOT EXISTS idx_routes_composite 
ON gtfs.routes(source_id, route_id, agency_id);

-- 2.3 Create indexes for calendar and calendar_dates
CREATE INDEX IF NOT EXISTS idx_calendar_service_dates 
ON gtfs.calendar(source_id, service_id, start_date, end_date);

CREATE INDEX IF NOT EXISTS idx_calendar_dates_lookup 
ON gtfs.calendar_dates(source_id, service_id, date, exception_type);

-- 2.4 Create indexes for active services
-- Note: We can't use CURRENT_DATE in partial indexes as it's not immutable
-- Instead, create a regular index that will be used for date range queries
CREATE INDEX IF NOT EXISTS idx_calendar_active_services 
ON gtfs.calendar(source_id, service_id, start_date, end_date);

-- 2.5 Create indexes for route type filtering
CREATE INDEX IF NOT EXISTS idx_routes_type 
ON gtfs.routes(source_id, route_type);

-- 2.6 Create indexes for real-time data queries (used by SSE endpoint)
CREATE INDEX IF NOT EXISTS idx_trip_updates_trip_id 
ON gtfs_rt.trip_updates(trip_id);

CREATE INDEX IF NOT EXISTS idx_stop_time_updates_stop_id 
ON gtfs_rt.stop_time_updates(stop_id);

CREATE INDEX IF NOT EXISTS idx_stop_time_updates_trip_update 
ON gtfs_rt.stop_time_updates(trip_update_id, stop_id, departure_delay);

CREATE INDEX IF NOT EXISTS idx_trip_updates_feed_message 
ON gtfs_rt.trip_updates(feed_message_id, trip_id, route_id);

CREATE INDEX IF NOT EXISTS idx_feed_messages_source_received 
ON gtfs_rt.feed_messages(source_id, received_at DESC);

-- 2.7 Note: platform_numbers is a view, not a table, so no index needed

-- =====================================================
-- STEP 3: MATERIALIZED VIEWS
-- =====================================================

-- 3.1 Create materialized view for stop-route relationships
CREATE MATERIALIZED VIEW IF NOT EXISTS gtfs.mv_stop_routes AS
WITH stop_route_trips AS (
    SELECT DISTINCT
        st.source_id,
        st.stop_id,
        t.route_id,
        t.service_id,
        t.trip_id
    FROM gtfs.stop_times st
    INNER JOIN gtfs.trips t 
        ON st.trip_id = t.trip_id 
        AND st.source_id = t.source_id
)
SELECT 
    srt.source_id,
    srt.stop_id,
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    r.route_color,
    r.route_text_color,
    a.agency_id,
    a.agency_name,
    COUNT(DISTINCT srt.trip_id) as trip_count,
    COUNT(DISTINCT srt.service_id) as service_count,
    ARRAY_AGG(DISTINCT srt.service_id) as service_ids
FROM stop_route_trips srt
INNER JOIN gtfs.routes r 
    ON srt.route_id = r.route_id 
    AND srt.source_id = r.source_id
INNER JOIN gtfs.agency a 
    ON r.agency_id = a.agency_id 
    AND r.source_id = a.source_id
GROUP BY 
    srt.source_id,
    srt.stop_id,
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    r.route_color,
    r.route_text_color,
    a.agency_id,
    a.agency_name;

-- Create indexes on materialized view
CREATE INDEX IF NOT EXISTS idx_mv_stop_routes_lookup 
ON gtfs.mv_stop_routes(source_id, stop_id);

CREATE INDEX IF NOT EXISTS idx_mv_stop_routes_route 
ON gtfs.mv_stop_routes(source_id, route_id);

-- 3.2 Create materialized view for active services
CREATE MATERIALIZED VIEW IF NOT EXISTS gtfs.mv_active_services AS
WITH date_range AS (
    SELECT 
        generate_series(
            CURRENT_DATE - INTERVAL '1 day',
            CURRENT_DATE + INTERVAL '7 days',
            '1 day'::interval
        )::date as service_date
),
active_services AS (
    SELECT DISTINCT
        dr.service_date,
        c.source_id,
        c.service_id
    FROM date_range dr
    CROSS JOIN gtfs.calendar c
    WHERE dr.service_date BETWEEN c.start_date AND c.end_date
      AND (
        (EXTRACT(DOW FROM dr.service_date) = 0 AND c.sunday = true) OR
        (EXTRACT(DOW FROM dr.service_date) = 1 AND c.monday = true) OR
        (EXTRACT(DOW FROM dr.service_date) = 2 AND c.tuesday = true) OR
        (EXTRACT(DOW FROM dr.service_date) = 3 AND c.wednesday = true) OR
        (EXTRACT(DOW FROM dr.service_date) = 4 AND c.thursday = true) OR
        (EXTRACT(DOW FROM dr.service_date) = 5 AND c.friday = true) OR
        (EXTRACT(DOW FROM dr.service_date) = 6 AND c.saturday = true)
      )
    
    UNION
    
    SELECT 
        cd.date as service_date,
        cd.source_id,
        cd.service_id
    FROM gtfs.calendar_dates cd
    WHERE cd.date IN (SELECT service_date FROM date_range)
      AND cd.exception_type = 1
    
    EXCEPT
    
    SELECT 
        cd.date as service_date,
        cd.source_id,
        cd.service_id
    FROM gtfs.calendar_dates cd
    WHERE cd.date IN (SELECT service_date FROM date_range)
      AND cd.exception_type = 2
)
SELECT 
    service_date,
    source_id,
    service_id
FROM active_services;

-- Create indexes on materialized view
CREATE INDEX IF NOT EXISTS idx_mv_active_services_lookup 
ON gtfs.mv_active_services(service_date, source_id);

CREATE INDEX IF NOT EXISTS idx_mv_active_services_service 
ON gtfs.mv_active_services(source_id, service_id, service_date);

-- 3.3 Create function to refresh materialized views
CREATE OR REPLACE FUNCTION gtfs.refresh_materialized_views()
RETURNS void AS $$
BEGIN
    -- Refresh stop routes (concurrently to avoid blocking)
    REFRESH MATERIALIZED VIEW CONCURRENTLY gtfs.mv_stop_routes;
    
    -- Refresh active services (concurrently to avoid blocking)
    REFRESH MATERIALIZED VIEW CONCURRENTLY gtfs.mv_active_services;
    
    RAISE NOTICE 'Materialized views refreshed successfully';
END;
$$ LANGUAGE plpgsql;