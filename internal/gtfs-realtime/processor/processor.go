package processor

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ptvtracker-data/internal/common/logger"
	"github.com/ptvtracker-data/internal/gtfs-realtime/consumer"
	gtfs_proto "github.com/ptvtracker-data/pkg/gtfs-realtime/proto"
)

type Processor struct {
	db             *sql.DB
	logger         logger.Logger
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

func NewProcessor(db *sql.DB, log logger.Logger) *Processor {
	return &Processor{
		db:             db,
		logger:         log,
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

	// Get or create version
	versionID, err := p.getOrCreateVersion(result.Message.Header)
	if err != nil {
		return fmt.Errorf("failed to get version: %w", err)
	}

	// Start transaction
	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert feed message
	feedMessageID, err := p.insertFeedMessage(tx, result.Message.Header, sourceID, versionID, result.Endpoint.FeedType)
	if err != nil {
		return fmt.Errorf("failed to insert feed message: %w", err)
	}

	// Process entities based on feed type
	switch result.Endpoint.FeedType {
	case "vehicle_positions":
		err = p.processVehiclePositions(tx, feedMessageID, result.Message.Entity)
	case "trip_updates":
		err = p.processTripUpdates(tx, feedMessageID, result.Message.Entity)
	case "service_alerts":
		err = p.processServiceAlerts(tx, feedMessageID, result.Message.Entity)
	default:
		err = fmt.Errorf("unknown feed type: %s", result.Endpoint.FeedType)
	}

	if err != nil {
		return fmt.Errorf("failed to process entities: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Send batch update notification after successful commit
	entityCount := len(result.Message.Entity)
	if entityCount > 0 {
		if err := p.sendBatchNotification(result.Endpoint.FeedType, result.Endpoint.Source, versionID, entityCount); err != nil {
			// Log error but don't fail the processing
			p.logger.Error("Failed to send batch notification",
				"endpoint", result.Endpoint.Name,
				"error", err)
		}
	}

	p.logger.Info("Processed feed message",
		"endpoint", result.Endpoint.Name,
		"entities", entityCount,
		"feed_message_id", feedMessageID)

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

func (p *Processor) processVehiclePositions(tx *sql.Tx, feedMessageID int, entities []*gtfs_proto.FeedEntity) error {
	stmt, err := tx.Prepare(`
		INSERT INTO gtfs_rt.vehicle_positions (
			feed_message_id, entity_id, is_deleted, trip_id, route_id, 
			start_time, start_date, schedule_relationship, vehicle_id, 
			vehicle_label, license_plate, latitude, longitude, bearing, 
			current_status, stop_id, timestamp
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare vehicle positions statement: %w", err)
	}
	defer stmt.Close()

	for _, entity := range entities {
		if entity.Vehicle == nil {
			continue
		}

		vp := entity.Vehicle
		var tripID, routeID, startDate, vehicleID, vehicleLabel, licensePlate, stopID *string
		var scheduleRelationship, currentStatus, startTime *int32
		var bearing *float32
		var timestamp time.Time

		// Extract trip information
		if vp.Trip != nil {
			if vp.Trip.TripId != nil {
				tripID = vp.Trip.TripId
			}
			if vp.Trip.RouteId != nil {
				routeID = vp.Trip.RouteId
			}
			if vp.Trip.StartTime != nil {
				var err error
				startTime, err = parseGTFSTime(*vp.Trip.StartTime)
				if err != nil {
					p.logger.Warn("Invalid start time in vehicle position", "entity_id", entity.Id, "start_time", *vp.Trip.StartTime, "error", err)
				}
			}
			if vp.Trip.StartDate != nil {
				startDate = vp.Trip.StartDate
			}
			if vp.Trip.ScheduleRelationship != nil {
				val := int32(*vp.Trip.ScheduleRelationship)
				scheduleRelationship = &val
			}
		}

		// Extract vehicle information
		if vp.Vehicle != nil {
			if vp.Vehicle.Id != nil {
				vehicleID = vp.Vehicle.Id
			}
			if vp.Vehicle.Label != nil {
				vehicleLabel = vp.Vehicle.Label
			}
			if vp.Vehicle.LicensePlate != nil {
				licensePlate = vp.Vehicle.LicensePlate
			}
		}

		// Extract position (required)
		if vp.Position == nil {
			continue // Skip if no position data
		}

		latitude := vp.Position.Latitude
		longitude := vp.Position.Longitude
		if vp.Position.Bearing != nil {
			bearing = vp.Position.Bearing
		}

		// Extract status and stop
		if vp.CurrentStatus != nil {
			val := int32(*vp.CurrentStatus)
			currentStatus = &val
		}
		if vp.StopId != nil {
			stopID = vp.StopId
		}

		// Extract timestamp
		if vp.Timestamp != nil {
			timestamp = time.Unix(int64(*vp.Timestamp), 0).UTC()
		} else {
			timestamp = time.Now().UTC()
		}

		// Insert vehicle position
		_, err = stmt.Exec(
			feedMessageID, entity.Id, entity.IsDeleted,
			tripID, routeID, startTime, startDate, scheduleRelationship,
			vehicleID, vehicleLabel, licensePlate,
			latitude, longitude, bearing,
			currentStatus, stopID, timestamp,
		)
		if err != nil {
			return fmt.Errorf("failed to insert vehicle position %s: %w", entity.Id, err)
		}
	}

	return nil
}

func (p *Processor) processTripUpdates(tx *sql.Tx, feedMessageID int, entities []*gtfs_proto.FeedEntity) error {
	// Insert trip updates
	tripStmt, err := tx.Prepare(`
		INSERT INTO gtfs_rt.trip_updates (
			feed_message_id, entity_id, is_deleted, trip_id, route_id, 
			direction_id, start_time, start_date, schedule_relationship, 
			vehicle_id, vehicle_label, timestamp, delay
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING trip_update_id
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare trip updates statement: %w", err)
	}
	defer tripStmt.Close()

	// Insert stop time updates
	stopStmt, err := tx.Prepare(`
		INSERT INTO gtfs_rt.stop_time_updates (
			trip_update_id, stop_sequence, stop_id, arrival_delay, 
			arrival_time, arrival_uncertainty, departure_delay, 
			departure_time, departure_uncertainty, schedule_relationship
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare stop time updates statement: %w", err)
	}
	defer stopStmt.Close()

	for _, entity := range entities {
		if entity.TripUpdate == nil {
			continue
		}

		tu := entity.TripUpdate
		var routeID, startDate, vehicleID, vehicleLabel *string
		var directionID, scheduleRelationship, startTime *int32
		var timestamp *time.Time
		var delay *int32

		// Extract trip information (required)
		if tu.Trip == nil {
			continue // Skip if no trip data
		}

		tripID := tu.Trip.TripId
		if tu.Trip.RouteId != nil {
			routeID = tu.Trip.RouteId
		}
		if tu.Trip.DirectionId != nil {
			val := int32(*tu.Trip.DirectionId)
			directionID = &val
		}
		if tu.Trip.StartTime != nil {
			var err error
			startTime, err = parseGTFSTime(*tu.Trip.StartTime)
			if err != nil {
				p.logger.Warn("Invalid start time in trip update", "entity_id", entity.Id, "start_time", *tu.Trip.StartTime, "error", err)
			}
		}
		if tu.Trip.StartDate != nil {
			startDate = tu.Trip.StartDate
		}
		if tu.Trip.ScheduleRelationship != nil {
			val := int32(*tu.Trip.ScheduleRelationship)
			scheduleRelationship = &val
		}

		// Extract vehicle information
		if tu.Vehicle != nil {
			if tu.Vehicle.Id != nil {
				vehicleID = tu.Vehicle.Id
			}
			if tu.Vehicle.Label != nil {
				vehicleLabel = tu.Vehicle.Label
			}
		}

		// Extract timestamp and delay
		if tu.Timestamp != nil {
			ts := time.Unix(int64(*tu.Timestamp), 0).UTC()
			timestamp = &ts
		}
		if tu.Delay != nil {
			delay = tu.Delay
		}

		// Insert trip update
		var tripUpdateID int
		err = tripStmt.QueryRow(
			feedMessageID, entity.Id, entity.IsDeleted,
			tripID, routeID, directionID, startTime, startDate,
			scheduleRelationship, vehicleID, vehicleLabel,
			timestamp, delay,
		).Scan(&tripUpdateID)
		if err != nil {
			return fmt.Errorf("failed to insert trip update %s: %w", entity.Id, err)
		}

		// Process stop time updates
		for _, stu := range tu.StopTimeUpdate {
			var stopSequence *int32
			var stopID *string
			var arrivalDelay, arrivalUncertainty, departureDelay, departureUncertainty *int32
			var arrivalTime, departureTime *int64
			var scheduleRel *int32

			if stu.StopSequence != nil {
				val := int32(*stu.StopSequence)
				stopSequence = &val
			}
			if stu.StopId != nil {
				stopID = stu.StopId
			} else {
				// Use empty string as default for missing stop_id
				emptyString := ""
				stopID = &emptyString
			}

			// Extract arrival information
			if stu.Arrival != nil {
				if stu.Arrival.Delay != nil {
					arrivalDelay = stu.Arrival.Delay
				}
				if stu.Arrival.Time != nil {
					arrivalTime = stu.Arrival.Time
				}
				if stu.Arrival.Uncertainty != nil {
					arrivalUncertainty = stu.Arrival.Uncertainty
				}
			}

			// Extract departure information
			if stu.Departure != nil {
				if stu.Departure.Delay != nil {
					departureDelay = stu.Departure.Delay
				}
				if stu.Departure.Time != nil {
					departureTime = stu.Departure.Time
				}
				if stu.Departure.Uncertainty != nil {
					departureUncertainty = stu.Departure.Uncertainty
				}
			}

			// Extract schedule relationship
			if stu.ScheduleRelationship != nil {
				val := int32(*stu.ScheduleRelationship)
				scheduleRel = &val
			}

			// Insert stop time update
			_, err = stopStmt.Exec(
				tripUpdateID, stopSequence, stopID,
				arrivalDelay, arrivalTime, arrivalUncertainty,
				departureDelay, departureTime, departureUncertainty,
				scheduleRel,
			)
			if err != nil {
				return fmt.Errorf("failed to insert stop time update for trip %s: %w", entity.Id, err)
			}
		}
	}

	return nil
}

// sendBatchNotification calls the PostgreSQL function to send NOTIFY messages for batch updates
func (p *Processor) sendBatchNotification(feedType, source string, versionID, recordCount int) error {
	// Map feed type to table name
	tableName := ""
	switch feedType {
	case "vehicle_positions":
		tableName = "vehicle_positions"
	case "trip_updates":
		tableName = "trip_updates"
	case "service_alerts":
		tableName = "alerts"
	default:
		return fmt.Errorf("unknown feed type for notification: %s", feedType)
	}

	// Call the PostgreSQL function to send NOTIFY
	_, err := p.db.Exec(`SELECT gtfs_rt.notify_batch_update($1, $2, $3, $4)`,
		tableName, source, versionID, recordCount)
	
	if err != nil {
		return fmt.Errorf("failed to send batch notification: %w", err)
	}

	p.logger.Debug("Sent batch notification",
		"table", tableName,
		"source", source,
		"version_id", versionID,
		"record_count", recordCount)

	return nil
}

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

func (p *Processor) processServiceAlerts(tx *sql.Tx, feedMessageID int, entities []*gtfs_proto.FeedEntity) error {
	// Insert alerts
	alertStmt, err := tx.Prepare(`
		INSERT INTO gtfs_rt.alerts (
			feed_message_id, entity_id, is_deleted, cause, effect, severity
		) VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING alert_id
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare alerts statement: %w", err)
	}
	defer alertStmt.Close()

	// Insert active periods
	periodStmt, err := tx.Prepare(`
		INSERT INTO gtfs_rt.alert_active_periods (
			alert_id, start_time, end_time
		) VALUES ($1, $2, $3)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare active periods statement: %w", err)
	}
	defer periodStmt.Close()

	// Insert informed entities
	entityStmt, err := tx.Prepare(`
		INSERT INTO gtfs_rt.alert_informed_entities (
			alert_id, agency_id, route_id, direction_id, trip_id, 
			trip_route_id, trip_start_time, trip_start_date, stop_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare informed entities statement: %w", err)
	}
	defer entityStmt.Close()

	// Insert translations
	translationStmt, err := tx.Prepare(`
		INSERT INTO gtfs_rt.alert_translations (
			alert_id, field_type, language, text
		) VALUES ($1, $2, $3, $4)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare translations statement: %w", err)
	}
	defer translationStmt.Close()

	for _, entity := range entities {
		if entity.Alert == nil {
			continue
		}

		alert := entity.Alert
		var cause, effect, severity *int32

		if alert.Cause != nil {
			val := int32(*alert.Cause)
			cause = &val
		}
		if alert.Effect != nil {
			val := int32(*alert.Effect)
			effect = &val
		}
		if alert.SeverityLevel != nil {
			val := int32(*alert.SeverityLevel)
			severity = &val
		}

		// Insert alert
		var alertID int
		err = alertStmt.QueryRow(
			feedMessageID, entity.Id, entity.IsDeleted,
			cause, effect, severity,
		).Scan(&alertID)
		if err != nil {
			return fmt.Errorf("failed to insert alert %s: %w", entity.Id, err)
		}

		// Insert active periods
		for _, period := range alert.ActivePeriod {
			var startTime, endTime *int64
			if period.Start != nil {
				val := int64(*period.Start)
				startTime = &val
			}
			if period.End != nil {
				val := int64(*period.End)
				endTime = &val
			}

			_, err = periodStmt.Exec(alertID, startTime, endTime)
			if err != nil {
				return fmt.Errorf("failed to insert active period for alert %s: %w", entity.Id, err)
			}
		}

		// Insert informed entities
		for _, informedEntity := range alert.InformedEntity {
			var agencyID, routeID, tripID, tripRouteID, tripStartDate, stopID *string
			var directionID, tripStartTime *int32

			if informedEntity.AgencyId != nil {
				agencyID = informedEntity.AgencyId
			}
			if informedEntity.RouteId != nil {
				routeID = informedEntity.RouteId
			}
			if informedEntity.DirectionId != nil {
				val := int32(*informedEntity.DirectionId)
				directionID = &val
			}
			if informedEntity.Trip != nil {
				if informedEntity.Trip.TripId != nil {
					tripID = informedEntity.Trip.TripId
				}
				if informedEntity.Trip.RouteId != nil {
					tripRouteID = informedEntity.Trip.RouteId
				}
				if informedEntity.Trip.StartTime != nil {
					var err error
					tripStartTime, err = parseGTFSTime(*informedEntity.Trip.StartTime)
					if err != nil {
						p.logger.Warn("Invalid trip start time in alert", "entity_id", entity.Id, "start_time", *informedEntity.Trip.StartTime, "error", err)
					}
				}
				if informedEntity.Trip.StartDate != nil {
					tripStartDate = informedEntity.Trip.StartDate
				}
			}
			if informedEntity.StopId != nil {
				stopID = informedEntity.StopId
			}

			_, err = entityStmt.Exec(
				alertID, agencyID, routeID, directionID, tripID,
				tripRouteID, tripStartTime, tripStartDate, stopID,
			)
			if err != nil {
				return fmt.Errorf("failed to insert informed entity for alert %s: %w", entity.Id, err)
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

				_, err = translationStmt.Exec(
					alertID, fieldType, language, translation.Text,
				)
				if err != nil {
					return fmt.Errorf("failed to insert translation for alert %s: %w", entity.Id, err)
				}
			}
		}
	}

	return nil
}
