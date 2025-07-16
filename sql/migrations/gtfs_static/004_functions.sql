-- GTFS Static Functions for Search and Data Management
-- Functions for searching, version management, and data operations

SET search_path TO gtfs, public;

-- Function to activate a specific version (for blue-green deployment)
CREATE OR REPLACE FUNCTION activate_version(target_version_id INTEGER)
RETURNS VOID AS $$
BEGIN
    -- Deactivate all versions
    UPDATE versions SET is_active = FALSE WHERE is_active = TRUE;
    
    -- Activate the target version
    UPDATE versions SET is_active = TRUE WHERE version_id = target_version_id;
    
    -- Update the updated_at timestamp
    UPDATE versions SET updated_at = CURRENT_TIMESTAMP WHERE version_id = target_version_id;
    
    RAISE NOTICE 'Version % has been activated', target_version_id;
END;
$$ LANGUAGE plpgsql;

-- Function to create a new version
CREATE OR REPLACE FUNCTION create_new_version(
    p_version_name VARCHAR(100),
    p_source_url TEXT DEFAULT NULL,
    p_description TEXT DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    new_version_id INTEGER;
BEGIN
    INSERT INTO versions (version_name, source_url, description)
    VALUES (p_version_name, p_source_url, p_description)
    RETURNING version_id INTO new_version_id;
    
    RETURN new_version_id;
END;
$$ LANGUAGE plpgsql;

-- Enhanced search function for stops (version-aware)
CREATE OR REPLACE FUNCTION search_stops(
    query_text TEXT, 
    max_results INTEGER DEFAULT 10,
    use_version_id INTEGER DEFAULT NULL
)
RETURNS TABLE (
    stop_id VARCHAR(50),
    source_id INTEGER,
    version_id INTEGER,
    source_name VARCHAR(50),
    stop_name VARCHAR(255),
    stop_lat NUMERIC(10,7),
    stop_lon NUMERIC(10,7),
    relevance_score REAL
) AS $$
BEGIN
    RETURN QUERY
    WITH target_version AS (
        SELECT COALESCE(use_version_id, (SELECT v.version_id FROM versions v WHERE v.is_active = TRUE LIMIT 1)) as vid
    )
    SELECT 
        s.stop_id,
        s.source_id,
        s.version_id,
        ts.source_name,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        (
            -- Exact match bonus
            CASE WHEN lower(s.stop_name) = lower(query_text) THEN 1.0
            -- Starts with bonus  
            WHEN lower(s.stop_name) LIKE lower(query_text) || '%' THEN 0.8
            -- Contains match
            WHEN lower(s.stop_name) LIKE '%' || lower(query_text) || '%' THEN 0.6
            -- Trigram similarity
            ELSE similarity(s.stop_name, query_text)
            END
        )::REAL as relevance_score
    FROM stops s
    JOIN transport_sources ts ON s.source_id = ts.source_id
    JOIN target_version tv ON s.version_id = tv.vid
    WHERE 
		s.location_type = 0
		AND
        -- Full text search OR trigram similarity
        (to_tsvector('english', s.stop_name) @@ plainto_tsquery('english', query_text)
         OR similarity(s.stop_name, query_text) > 0.3)
    ORDER BY relevance_score DESC, s.stop_name
    LIMIT max_results;
END;
$$ LANGUAGE plpgsql;

-- Function to get stops near a location (version-aware)
CREATE OR REPLACE FUNCTION stops_near_location(
    lat NUMERIC,
    lon NUMERIC,
    radius_meters INTEGER DEFAULT 500,
    max_results INTEGER DEFAULT 10,
    use_version_id INTEGER DEFAULT NULL
)
RETURNS TABLE (
    stop_id VARCHAR(50),
    source_id INTEGER,
    version_id INTEGER,
    source_name VARCHAR(50),
    stop_name VARCHAR(255),
    stop_lat NUMERIC(10,7),
    stop_lon NUMERIC(10,7),
    distance_meters NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH target_version AS (
        SELECT COALESCE(use_version_id, (SELECT v.version_id FROM versions v WHERE v.is_active = TRUE LIMIT 1)) as vid
    )
    SELECT 
        s.stop_id,
        s.source_id,
        s.version_id,
        ts.source_name,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        earth_distance(ll_to_earth(s.stop_lat, s.stop_lon), ll_to_earth(lat, lon))::NUMERIC as distance_meters
    FROM stops s
    JOIN transport_sources ts ON s.source_id = ts.source_id
    JOIN target_version tv ON s.version_id = tv.vid
    WHERE 
    	s.location_type = 0
		AND
        s.stop_lat IS NOT NULL 
        AND s.stop_lon IS NOT NULL
        AND earth_box(ll_to_earth(lat, lon), radius_meters) @> ll_to_earth(s.stop_lat, s.stop_lon)
    ORDER BY distance_meters
    LIMIT max_results;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up old versions (keep N most recent)
CREATE OR REPLACE FUNCTION cleanup_old_versions(keep_count INTEGER DEFAULT 3)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    WITH versions_to_delete AS (
        SELECT version_id
        FROM versions
        WHERE is_active = FALSE
        ORDER BY created_at DESC
        OFFSET keep_count
    )
    DELETE FROM versions
    WHERE version_id IN (SELECT version_id FROM versions_to_delete);
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RAISE NOTICE 'Deleted % old versions', deleted_count;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to copy data between versions (for testing/rollback)
CREATE OR REPLACE FUNCTION copy_version_data(
    source_version_id INTEGER,
    target_version_id INTEGER,
    source_filter INTEGER DEFAULT NULL -- optional source_id filter
)
RETURNS VOID AS $$
BEGIN
    -- Copy agency data
    INSERT INTO agency (agency_id, source_id, version_id, agency_name, agency_url, agency_timezone, agency_lang, agency_fare_url)
    SELECT agency_id, source_id, target_version_id, agency_name, agency_url, agency_timezone, agency_lang, agency_fare_url
    FROM agency
    WHERE version_id = source_version_id
    AND (source_filter IS NULL OR source_id = source_filter);
    
    -- Copy calendar data
    INSERT INTO calendar (service_id, source_id, version_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date)
    SELECT service_id, source_id, target_version_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date
    FROM calendar
    WHERE version_id = source_version_id
    AND (source_filter IS NULL OR source_id = source_filter);
    
    -- Continue with other tables...
    -- (Add similar INSERT statements for all other tables)
    
    RAISE NOTICE 'Data copied from version % to version %', source_version_id, target_version_id;
END;
$$ LANGUAGE plpgsql;