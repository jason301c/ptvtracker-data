package processor

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/ptvtracker-data/internal/common/db"
	"github.com/ptvtracker-data/internal/common/logger"
	"github.com/ptvtracker-data/internal/common/maintenance"
	"github.com/ptvtracker-data/internal/gtfs-realtime/consumer"
	gtfs_proto "github.com/ptvtracker-data/pkg/gtfs-realtime/proto"
)

type Processor struct {
	db             *sql.DB
	dbWrapper      *db.DB
	logger         logger.Logger
	maintenance    *maintenance.Maintenance
	sourceMapping  map[string]int // maps source name to source_id
	versionMapping map[string]int // maps version info to version_id
}

type ProcessorStats struct {
	ProcessedMessages int64
	ProcessedEntities int64
	VehiclePositions  int64
	TripUpdates       int64
	ServiceAlerts     int64
	DatabaseErrors    int64
	ProcessingErrors  int64
	LastProcessedTime time.Time
}

func NewProcessor(dbWrapper *db.DB, log logger.Logger) *Processor {
	return &Processor{
		db:             dbWrapper.DB(),
		dbWrapper:      dbWrapper,
		logger:         log,
		maintenance:    maintenance.New(dbWrapper, log),
		sourceMapping:  make(map[string]int),
		versionMapping: make(map[string]int),
	}
}

func (p *Processor) Start(ctx context.Context, feedChan <-chan *consumer.FeedResult) error {
	p.logger.Info("Starting GTFS-realtime processor")

	// Initialize source mappings
	if err := p.initializeSourceMappings(); err != nil {
		return fmt.Errorf("failed to initialize source mappings: %w", err)
	}

	// Start cleanup goroutine for old realtime data
	go p.runCleanupJob(ctx)

	// Process incoming feeds
	go p.processFeedResults(ctx, feedChan)

	return nil
}

func (p *Processor) initializeSourceMappings() error {
	// Query transport sources
	rows, err := p.db.Query(`
		SELECT source_id, source_name 
		FROM gtfs.transport_sources 
		ORDER BY source_id
	`)
	if err != nil {
		return fmt.Errorf("failed to query transport sources: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var sourceID int
		var sourceName string
		if err := rows.Scan(&sourceID, &sourceName); err != nil {
			return fmt.Errorf("failed to scan transport source: %w", err)
		}
		p.sourceMapping[sourceName] = sourceID
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating transport sources: %w", err)
	}

	p.logger.Info("Initialized source mappings", "count", len(p.sourceMapping))
	return nil
}

func (p *Processor) processFeedResults(ctx context.Context, feedChan <-chan *consumer.FeedResult) {
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Processor context cancelled")
			return
		case result, ok := <-feedChan:
			if !ok {
				p.logger.Info("Feed channel closed")
				return
			}

			if result.Error != nil {
				p.logger.Error("Feed fetch error", "endpoint", result.Endpoint.Name, "error", result.Error)
				continue
			}

			if result.Message == nil {
				p.logger.Warn("Received nil feed message", "endpoint", result.Endpoint.Name)
				continue
			}

			if err := p.processFeedMessage(result); err != nil {
				p.logger.Error("Failed to process feed message",
					"endpoint", result.Endpoint.Name,
					"error", err)
			}
		}
	}
}

func (p *Processor) processFeedMessage(result *consumer.FeedResult) error {
	sourceID, exists := p.sourceMapping[result.Endpoint.Source]
	if !exists {
		return fmt.Errorf("unknown source: %s", result.Endpoint.Source)
	}

	versionID, err := p.getOrCreateVersion(result.Message.Header)
	if err != nil {
		return fmt.Errorf("failed to get version: %w", err)
	}

	// Note: Realtime data cleanup is now handled by a separate cleanup goroutine
	// This ensures consistent retention across all feed types

	// Start timing the transaction
	startTime := time.Now()

	p.logger.Debug("Starting transaction for feed processing", 
		"endpoint", result.Endpoint.Name, 
		"feedType", result.Endpoint.FeedType)

	tx, err := p.dbWrapper.BeginTx(context.Background())
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Test database connection and schema visibility
	var schemaTest string
	if err := tx.QueryRow("SELECT current_database()").Scan(&schemaTest); err != nil {
		p.logger.Error("Failed to query current database", "error", err)
	} else {
		p.logger.Debug("Database connection confirmed", "database", schemaTest)
	}

	var searchPath string
	if err := tx.QueryRow("SHOW search_path").Scan(&searchPath); err != nil {
		p.logger.Error("Failed to query search_path", "error", err)
	} else {
		p.logger.Debug("Current search_path", "searchPath", searchPath)
	}

	// Check if gtfs_rt.vehicle_positions table exists
	var tableExists bool
	if err := tx.QueryRow("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'gtfs_rt' AND table_name = 'vehicle_positions')").Scan(&tableExists); err != nil {
		p.logger.Error("Failed to check table existence", "error", err)
	} else {
		p.logger.Debug("Table existence check", "table", "gtfs_rt.vehicle_positions", "exists", tableExists)
	}

	feedMessageID, err := p.insertFeedMessage(tx, result.Message.Header, sourceID, versionID, result.Endpoint.FeedType)
	if err != nil {
		return fmt.Errorf("failed to insert feed message: %w", err)
	}

	switch result.Endpoint.FeedType {
	case "vehicle_positions":
		err = p.processVehiclePositionsBulk(tx, feedMessageID, result.Message.Entity)
	case "trip_updates":
		_, err = p.processTripUpdatesBulk(tx, feedMessageID, result.Message.Entity)
	case "service_alerts":
		err = p.processServiceAlertsBulk(tx, feedMessageID, result.Message.Entity)
	default:
		err = fmt.Errorf("unknown feed type: %s", result.Endpoint.FeedType)
	}

	if err != nil {
		return fmt.Errorf("failed to process entities: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Log transaction performance
	duration := time.Since(startTime)
	p.logger.Info("Processed feed message",
		"endpoint", result.Endpoint.Name,
		"entities", len(result.Message.Entity),
		"feed_message_id", feedMessageID,
		"duration_ms", duration.Milliseconds())

	// All notifications are handled by database triggers
	// No manual notification sending needed from the Go backend
	
	return nil
}

func (p *Processor) getOrCreateVersion(header *gtfs_proto.FeedHeader) (int, error) {
	// For GTFS-realtime, we always use the currently active GTFS-static version
	// since realtime data relates to the static schedule
	
	cacheKey := "active_version"
	
	// Check if we have the active version cached
	if versionID, exists := p.versionMapping[cacheKey]; exists {
		return versionID, nil
	}

	// Query database for the active version
	var versionID int
	err := p.db.QueryRow(`
		SELECT version_id 
		FROM gtfs.versions 
		WHERE is_active = TRUE
		LIMIT 1
	`).Scan(&versionID)

	if err == sql.ErrNoRows {
		// No active version found, this shouldn't happen in normal operation
		return 0, fmt.Errorf("no active GTFS version found")
	} else if err != nil {
		return 0, fmt.Errorf("failed to query active version: %w", err)
	}

	// Cache the active version
	p.versionMapping[cacheKey] = versionID
	
	// Log the feed version if provided (for debugging)
	if header.FeedVersion != nil && *header.FeedVersion != "" {
		p.logger.Debug("GTFS-RT feed version", "feed_version", *header.FeedVersion, "using_version_id", versionID)
	}
	
	return versionID, nil
}

func (p *Processor) insertFeedMessage(tx *sql.Tx, header *gtfs_proto.FeedHeader, sourceID, versionID int, feedType string) (int, error) {
	var feedMessageID int
	var timestamp time.Time

	// Convert timestamp
	if header.Timestamp != nil {
		timestamp = time.Unix(int64(*header.Timestamp), 0).UTC()
	} else {
		timestamp = time.Now().UTC()
	}

	// Determine incrementality
	incrementality := 0 // FULL_DATASET
	if header.Incrementality != nil && *header.Incrementality == gtfs_proto.FeedHeader_DIFFERENTIAL {
		incrementality = 1
	}

	// Insert feed message - each fetch gets a unique received_at timestamp
	receivedAt := time.Now().UTC()
	err := tx.QueryRow(`
		INSERT INTO gtfs_rt.feed_messages (
			timestamp, gtfs_realtime_version, incrementality, 
			received_at, source_id, version_id, feed_type
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING feed_message_id
	`, timestamp, header.GtfsRealtimeVersion, incrementality,
		receivedAt, sourceID, versionID, feedType).Scan(&feedMessageID)

	if err != nil {
		return 0, fmt.Errorf("failed to insert feed message: %w", err)
	}

	return feedMessageID, nil
}

// processVehiclePositionsBulk uses PostgreSQL COPY for high-performance bulk inserts
func (p *Processor) processVehiclePositionsBulk(tx *sql.Tx, feedMessageID int, entities []*gtfs_proto.FeedEntity) error {
	p.logger.Debug("Preparing COPY statement for vehicle_positions", 
		"tableName", "gtfs_rt.vehicle_positions",
		"entityCount", len(entities))

	// Double-check table exists before CopyIn
	var tableExists bool
	if err := tx.QueryRow("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'gtfs_rt' AND table_name = 'vehicle_positions')").Scan(&tableExists); err != nil {
		p.logger.Error("Failed to verify table existence before CopyIn", "error", err)
	} else {
		p.logger.Debug("Pre-CopyIn table check", "exists", tableExists)
	}

	// Try COPY statement without schema qualification, relying on search_path
	// Some versions of pq.CopyIn have issues with schema-qualified names
	stmt, err := tx.Prepare(pq.CopyIn("vehicle_positions",
		"feed_message_id", "entity_id", "is_deleted", "trip_id", "route_id",
		"start_time", "start_date", "schedule_relationship", "vehicle_id",
		"vehicle_label", "license_plate", "latitude", "longitude", "bearing",
		"current_status", "stop_id", "timestamp"))
	if err != nil {
		p.logger.Error("CopyIn preparation failed", 
			"error", err,
			"tableName", "gtfs_rt.vehicle_positions")
		return fmt.Errorf("failed to prepare vehicle positions copy: %w", err)
	}
	defer stmt.Close()

	count := 0
	for _, entity := range entities {
		if entity.Vehicle == nil || entity.Vehicle.Position == nil {
			continue
		}

		vp := entity.Vehicle
		var tripID, routeID, startDate, vehicleID, vehicleLabel, licensePlate, stopID sql.NullString
		var scheduleRelationship, currentStatus, startTime sql.NullInt32
		var bearing sql.NullFloat64
		var timestamp time.Time

		if vp.Trip != nil {
			if vp.Trip.TripId != nil {
				tripID = sql.NullString{String: *vp.Trip.TripId, Valid: true}
			}
			if vp.Trip.RouteId != nil {
				routeID = sql.NullString{String: *vp.Trip.RouteId, Valid: true}
			}
			if vp.Trip.StartTime != nil {
				if parsedTime, err := parseGTFSTime(*vp.Trip.StartTime); err == nil && parsedTime != nil {
					startTime = sql.NullInt32{Int32: *parsedTime, Valid: true}
				}
			}
			if vp.Trip.StartDate != nil {
				startDate = sql.NullString{String: *vp.Trip.StartDate, Valid: true}
			}
			if vp.Trip.ScheduleRelationship != nil {
				scheduleRelationship = sql.NullInt32{Int32: int32(*vp.Trip.ScheduleRelationship), Valid: true}
			}
		}

		if vp.Vehicle != nil {
			if vp.Vehicle.Id != nil {
				vehicleID = sql.NullString{String: *vp.Vehicle.Id, Valid: true}
			}
			if vp.Vehicle.Label != nil {
				vehicleLabel = sql.NullString{String: *vp.Vehicle.Label, Valid: true}
			}
			if vp.Vehicle.LicensePlate != nil {
				licensePlate = sql.NullString{String: *vp.Vehicle.LicensePlate, Valid: true}
			}
		}

		latitude := vp.Position.Latitude
		longitude := vp.Position.Longitude
		if vp.Position.Bearing != nil {
			bearing = sql.NullFloat64{Float64: float64(*vp.Position.Bearing), Valid: true}
		}

		if vp.CurrentStatus != nil {
			currentStatus = sql.NullInt32{Int32: int32(*vp.CurrentStatus), Valid: true}
		}
		if vp.StopId != nil {
			stopID = sql.NullString{String: *vp.StopId, Valid: true}
		}

		if vp.Timestamp != nil {
			timestamp = time.Unix(int64(*vp.Timestamp), 0).UTC()
		} else {
			timestamp = time.Now().UTC()
		}

		_, err = stmt.Exec(feedMessageID, entity.Id, entity.IsDeleted,
			tripID, routeID, startTime, startDate, scheduleRelationship,
			vehicleID, vehicleLabel, licensePlate, latitude, longitude,
			bearing, currentStatus, stopID, timestamp)
		if err != nil {
			return fmt.Errorf("failed to add vehicle position to batch: %w", err)
		}
		count++
	}

	// Execute the COPY
	if _, err = stmt.Exec(); err != nil {
		return fmt.Errorf("failed to execute vehicle positions copy: %w", err)
	}

	p.logger.Debug("Bulk inserted vehicle positions", "count", count)
	return nil
}

// processTripUpdatesBulk uses PostgreSQL COPY for high-performance bulk inserts
func (p *Processor) processTripUpdatesBulk(tx *sql.Tx, feedMessageID int, entities []*gtfs_proto.FeedEntity) ([]string, error) {
	// Track affected stops for notifications
	affectedStopsMap := make(map[string]bool)

	// First, bulk insert trip updates
	tripStmt, err := tx.Prepare(pq.CopyIn("trip_updates",
		"feed_message_id", "entity_id", "is_deleted", "trip_id", "route_id",
		"direction_id", "start_time", "start_date", "schedule_relationship",
		"vehicle_id", "vehicle_label", "timestamp", "delay"))
	if err != nil {
		return nil, fmt.Errorf("failed to prepare trip updates copy: %w", err)
	}
	defer tripStmt.Close()

	// Collect trip updates for bulk insert
	tripUpdateMapping := make(map[string]int) // entity_id -> trip_update_id
	tripCount := 0

	for _, entity := range entities {
		if entity.TripUpdate == nil || entity.TripUpdate.Trip == nil || entity.TripUpdate.Trip.TripId == nil {
			continue
		}
		tu := entity.TripUpdate

		var routeID, startDate, vehicleID, vehicleLabel sql.NullString
		var directionID, scheduleRelationship, startTime, delay sql.NullInt32
		var timestamp sql.NullTime

		tripID := *tu.Trip.TripId
		if tu.Trip.RouteId != nil {
			routeID = sql.NullString{String: *tu.Trip.RouteId, Valid: true}
		}
		if tu.Trip.DirectionId != nil {
			directionID = sql.NullInt32{Int32: int32(*tu.Trip.DirectionId), Valid: true}
		}
		if tu.Trip.StartTime != nil {
			if parsedTime, err := parseGTFSTime(*tu.Trip.StartTime); err == nil && parsedTime != nil {
				startTime = sql.NullInt32{Int32: *parsedTime, Valid: true}
			}
		}
		if tu.Trip.StartDate != nil {
			startDate = sql.NullString{String: *tu.Trip.StartDate, Valid: true}
		}
		if tu.Trip.ScheduleRelationship != nil {
			scheduleRelationship = sql.NullInt32{Int32: int32(*tu.Trip.ScheduleRelationship), Valid: true}
		}
		if tu.Vehicle != nil {
			if tu.Vehicle.Id != nil {
				vehicleID = sql.NullString{String: *tu.Vehicle.Id, Valid: true}
			}
			if tu.Vehicle.Label != nil {
				vehicleLabel = sql.NullString{String: *tu.Vehicle.Label, Valid: true}
			}
		}
		if tu.Timestamp != nil {
			timestamp = sql.NullTime{Time: time.Unix(int64(*tu.Timestamp), 0).UTC(), Valid: true}
		}
		if tu.Delay != nil {
			delay = sql.NullInt32{Int32: *tu.Delay, Valid: true}
		}

		_, err = tripStmt.Exec(feedMessageID, entity.Id, entity.IsDeleted,
			tripID, routeID, directionID, startTime, startDate,
			scheduleRelationship, vehicleID, vehicleLabel, timestamp, delay)
		if err != nil {
			return nil, fmt.Errorf("failed to add trip update to batch: %w", err)
		}
		tripCount++

		// Track stops for this trip update
		for _, stu := range tu.StopTimeUpdate {
			if stu.StopId != nil {
				affectedStopsMap[*stu.StopId] = true
			}
		}
	}

	// Execute trip updates COPY
	if _, err = tripStmt.Exec(); err != nil {
		return nil, fmt.Errorf("failed to execute trip updates copy: %w", err)
	}

	// Get the inserted trip_update_ids
	rows, err := tx.Query(`
		SELECT entity_id, trip_update_id 
		FROM gtfs_rt.trip_updates 
		WHERE feed_message_id = $1
	`, feedMessageID)
	if err != nil {
		return nil, fmt.Errorf("failed to query trip update ids: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var entityID string
		var tripUpdateID int
		if err := rows.Scan(&entityID, &tripUpdateID); err != nil {
			return nil, fmt.Errorf("failed to scan trip update id: %w", err)
		}
		tripUpdateMapping[entityID] = tripUpdateID
	}

	// Now bulk insert stop time updates
	stopStmt, err := tx.Prepare(pq.CopyIn("stop_time_updates",
		"trip_update_id", "stop_sequence", "stop_id", "arrival_delay",
		"arrival_time", "arrival_uncertainty", "departure_delay",
		"departure_time", "departure_uncertainty", "schedule_relationship"))
	if err != nil {
		return nil, fmt.Errorf("failed to prepare stop time updates copy: %w", err)
	}
	defer stopStmt.Close()

	stopCount := 0
	for _, entity := range entities {
		if entity.TripUpdate == nil {
			continue
		}

		tripUpdateID, exists := tripUpdateMapping[*entity.Id]
		if !exists {
			continue
		}

		for _, stu := range entity.TripUpdate.StopTimeUpdate {
			var stopSequence, arrivalDelay, arrivalUncertainty, departureDelay, departureUncertainty, scheduleRel sql.NullInt32
			var arrivalTime, departureTime sql.NullInt64
			var stopID sql.NullString

			if stu.StopSequence != nil {
				stopSequence = sql.NullInt32{Int32: int32(*stu.StopSequence), Valid: true}
			}
			if stu.StopId != nil {
				stopID = sql.NullString{String: *stu.StopId, Valid: true}
			}
			if stu.Arrival != nil {
				if stu.Arrival.Delay != nil {
					arrivalDelay = sql.NullInt32{Int32: *stu.Arrival.Delay, Valid: true}
				}
				if stu.Arrival.Time != nil {
					arrivalTime = sql.NullInt64{Int64: *stu.Arrival.Time, Valid: true}
				}
				if stu.Arrival.Uncertainty != nil {
					arrivalUncertainty = sql.NullInt32{Int32: *stu.Arrival.Uncertainty, Valid: true}
				}
			}
			if stu.Departure != nil {
				if stu.Departure.Delay != nil {
					departureDelay = sql.NullInt32{Int32: *stu.Departure.Delay, Valid: true}
				}
				if stu.Departure.Time != nil {
					departureTime = sql.NullInt64{Int64: *stu.Departure.Time, Valid: true}
				}
				if stu.Departure.Uncertainty != nil {
					departureUncertainty = sql.NullInt32{Int32: *stu.Departure.Uncertainty, Valid: true}
				}
			}
			if stu.ScheduleRelationship != nil {
				scheduleRel = sql.NullInt32{Int32: int32(*stu.ScheduleRelationship), Valid: true}
			}

			_, err = stopStmt.Exec(tripUpdateID, stopSequence, stopID,
				arrivalDelay, arrivalTime, arrivalUncertainty,
				departureDelay, departureTime, departureUncertainty,
				scheduleRel)
			if err != nil {
				return nil, fmt.Errorf("failed to add stop time update to batch: %w", err)
			}
			stopCount++
		}
	}

	// Execute stop time updates COPY
	if _, err = stopStmt.Exec(); err != nil {
		return nil, fmt.Errorf("failed to execute stop time updates copy: %w", err)
	}

	p.logger.Debug("Bulk inserted trip updates", "trips", tripCount, "stops", stopCount)

	// Convert affected stops map to slice
	affectedStops := make([]string, 0, len(affectedStopsMap))
	for stopID := range affectedStopsMap {
		affectedStops = append(affectedStops, stopID)
	}

	return affectedStops, nil
}

// processServiceAlertsBulk uses PostgreSQL COPY for high-performance bulk inserts
func (p *Processor) processServiceAlertsBulk(tx *sql.Tx, feedMessageID int, entities []*gtfs_proto.FeedEntity) error {
	// First pass: insert alerts and collect their IDs
	alertStmt, err := tx.Prepare(pq.CopyIn("alerts",
		"feed_message_id", "entity_id", "is_deleted", "cause", "effect", "severity"))
	if err != nil {
		return fmt.Errorf("failed to prepare alerts copy: %w", err)
	}
	defer alertStmt.Close()

	alertCount := 0
	for _, entity := range entities {
		if entity.Alert == nil {
			continue
		}

		alert := entity.Alert
		var cause, effect, severity sql.NullInt32

		if alert.Cause != nil {
			cause = sql.NullInt32{Int32: int32(*alert.Cause), Valid: true}
		}
		if alert.Effect != nil {
			effect = sql.NullInt32{Int32: int32(*alert.Effect), Valid: true}
		}
		if alert.SeverityLevel != nil {
			severity = sql.NullInt32{Int32: int32(*alert.SeverityLevel), Valid: true}
		}

		_, err = alertStmt.Exec(feedMessageID, entity.Id, entity.IsDeleted,
			cause, effect, severity)
		if err != nil {
			return fmt.Errorf("failed to add alert to batch: %w", err)
		}
		alertCount++
	}

	// Execute alerts COPY
	if _, err = alertStmt.Exec(); err != nil {
		return fmt.Errorf("failed to execute alerts copy: %w", err)
	}

	// Get the inserted alert_ids
	alertMapping := make(map[string]int) // entity_id -> alert_id
	rows, err := tx.Query(`
		SELECT entity_id, alert_id 
		FROM gtfs_rt.alerts 
		WHERE feed_message_id = $1
	`, feedMessageID)
	if err != nil {
		return fmt.Errorf("failed to query alert ids: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var entityID string
		var alertID int
		if err := rows.Scan(&entityID, &alertID); err != nil {
			return fmt.Errorf("failed to scan alert id: %w", err)
		}
		alertMapping[entityID] = alertID
	}

	// Now bulk insert related data using the alert IDs
	// We'll still do individual inserts for these as they're typically fewer
	for _, entity := range entities {
		if entity.Alert == nil {
			continue
		}

		alertID, exists := alertMapping[*entity.Id]
		if !exists {
			continue
		}

		alert := entity.Alert

		// Insert active periods
		for _, period := range alert.ActivePeriod {
			var startTime, endTime sql.NullInt64
			if period.Start != nil {
				startTime = sql.NullInt64{Int64: int64(*period.Start), Valid: true}
			}
			if period.End != nil {
				endTime = sql.NullInt64{Int64: int64(*period.End), Valid: true}
			}

			_, err = tx.Exec(`
				INSERT INTO gtfs_rt.alert_active_periods (alert_id, start_time, end_time)
				VALUES ($1, $2, $3)
			`, alertID, startTime, endTime)
			if err != nil {
				return fmt.Errorf("failed to insert active period: %w", err)
			}
		}

		// Insert informed entities
		for _, informedEntity := range alert.InformedEntity {
			var agencyID, routeID, tripID, tripRouteID, tripStartDate, stopID sql.NullString
			var directionID, tripStartTime sql.NullInt32

			if informedEntity.AgencyId != nil {
				agencyID = sql.NullString{String: *informedEntity.AgencyId, Valid: true}
			}
			if informedEntity.RouteId != nil {
				routeID = sql.NullString{String: *informedEntity.RouteId, Valid: true}
			}
			if informedEntity.DirectionId != nil {
				directionID = sql.NullInt32{Int32: int32(*informedEntity.DirectionId), Valid: true}
			}
			if informedEntity.Trip != nil {
				if informedEntity.Trip.TripId != nil {
					tripID = sql.NullString{String: *informedEntity.Trip.TripId, Valid: true}
				}
				if informedEntity.Trip.RouteId != nil {
					tripRouteID = sql.NullString{String: *informedEntity.Trip.RouteId, Valid: true}
				}
				if informedEntity.Trip.StartTime != nil {
					if parsedTime, err := parseGTFSTime(*informedEntity.Trip.StartTime); err == nil && parsedTime != nil {
						tripStartTime = sql.NullInt32{Int32: *parsedTime, Valid: true}
					}
				}
				if informedEntity.Trip.StartDate != nil {
					tripStartDate = sql.NullString{String: *informedEntity.Trip.StartDate, Valid: true}
				}
			}
			if informedEntity.StopId != nil {
				stopID = sql.NullString{String: *informedEntity.StopId, Valid: true}
			}

			_, err = tx.Exec(`
				INSERT INTO gtfs_rt.alert_informed_entities (
					alert_id, agency_id, route_id, direction_id, trip_id,
					trip_route_id, trip_start_time, trip_start_date, stop_id
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			`, alertID, agencyID, routeID, directionID, tripID,
				tripRouteID, tripStartTime, tripStartDate, stopID)
			if err != nil {
				return fmt.Errorf("failed to insert informed entity: %w", err)
			}
		}

		// Insert translations
		fieldTypes := map[string]*gtfs_proto.TranslatedString{
			"url":              alert.Url,
			"header_text":      alert.HeaderText,
			"description_text": alert.DescriptionText,
		}

		for fieldType, translatedString := range fieldTypes {
			if translatedString == nil {
				continue
			}

			for _, translation := range translatedString.Translation {
				language := ""
				if translation.Language != nil {
					language = *translation.Language
				}

				_, err = tx.Exec(`
					INSERT INTO gtfs_rt.alert_translations (
						alert_id, field_type, language, text
					) VALUES ($1, $2, $3, $4)
				`, alertID, fieldType, language, translation.Text)
				if err != nil {
					return fmt.Errorf("failed to insert translation: %w", err)
				}
			}
		}
	}

	p.logger.Debug("Bulk inserted service alerts", "count", alertCount)
	return nil
}

// Notification handling has been moved to database triggers
// The sendStopNotifications function is no longer needed

// parseGTFSTime converts GTFS time string (HH:MM:SS) to seconds since midnight
// Supports times > 24:00:00 for trips continuing past midnight
func parseGTFSTime(timeStr string) (*int32, error) {
	if timeStr == "" {
		return nil, nil
	}

	parts := strings.Split(timeStr, ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid time format: %s", timeStr)
	}

	hours, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid hours: %s", parts[0])
	}

	minutes, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid minutes: %s", parts[1])
	}

	seconds, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid seconds: %s", parts[2])
	}

	totalSeconds := int32(hours*3600 + minutes*60 + seconds)
	return &totalSeconds, nil
}

// runCleanupJob runs a periodic cleanup to remove old realtime data
// This maintains a 15-minute retention window for all realtime data
func (p *Processor) runCleanupJob(ctx context.Context) {
	// Run cleanup every 5 minutes
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	p.logger.Info("Starting realtime data cleanup job", "retention_minutes", 15, "cleanup_interval_minutes", 5)

	// Run initial cleanup
	p.performCleanup(ctx)

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Cleanup job stopped")
			return
		case <-ticker.C:
			p.performCleanup(ctx)
		}
	}
}

// performCleanup removes all realtime data older than 15 minutes
// Uses CASCADE to automatically clean child tables
func (p *Processor) performCleanup(ctx context.Context) {
	startTime := time.Now()
	
	// Single query to clean all old data - CASCADE handles relationships
	result, err := p.db.ExecContext(ctx, `
		DELETE FROM gtfs_rt.feed_messages 
		WHERE received_at < NOW() - INTERVAL '15 minutes'
	`)
	
	if err != nil {
		p.logger.Error("Failed to cleanup old realtime data", "error", err)
		return
	}

	rowsDeleted, _ := result.RowsAffected()
	duration := time.Since(startTime)

	if rowsDeleted > 0 {
		p.logger.Info("Cleaned up old realtime data",
			"feed_messages_deleted", rowsDeleted,
			"duration_ms", duration.Milliseconds(),
			"retention_minutes", 15)
	} else {
		p.logger.Debug("No old realtime data to cleanup",
			"duration_ms", duration.Milliseconds())
	}
}