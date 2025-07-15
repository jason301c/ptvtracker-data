package importer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
	
	"github.com/ptvtracker-data/pkg/gtfs/models"
	"github.com/ptvtracker-data/internal/gtfs-static/parser"
	"github.com/ptvtracker-data/internal/common/db"
)

type Importer struct {
	db        *db.DB
	sourceID  int
	versionID int
	batchSize int
}

func NewImporter(database *db.DB, sourceID, versionID int) *Importer {
	return &Importer{
		db:        database,
		sourceID:  sourceID,
		versionID: versionID,
		batchSize: 1000,
	}
}

func (i *Importer) Import(ctx context.Context, zipPath string) error {
	p := parser.New(i.db.Logger())
	
	// Create batch inserters
	agencyBatch := i.newBatchInserter("agency", 6)
	stopBatch := i.newBatchInserter("stops", 8)
	routeBatch := i.newBatchInserter("routes", 7)
	calendarBatch := i.newBatchInserter("calendar", 11)
	calendarDateBatch := i.newBatchInserter("calendar_dates", 5)
	shapeBatch := i.newBatchInserter("shapes", 6)
	tripBatch := i.newBatchInserter("trips", 8)
	stopTimeBatch := i.newBatchInserter("stop_times", 10)
	levelBatch := i.newBatchInserter("levels", 5)
	pathwayBatch := i.newBatchInserter("pathways", 8)
	transferBatch := i.newBatchInserter("transfers", 10)
	
	callbacks := parser.ParseCallbacks{
		OnAgency: func(agency *models.Agency) error {
			return agencyBatch.Add(
				agency.AgencyID,
				i.sourceID,
				i.versionID,
				agency.AgencyName,
				sql.NullString{String: agency.AgencyURL, Valid: agency.AgencyURL != ""},
				agency.AgencyTimezone,
				sql.NullString{String: agency.AgencyLang, Valid: agency.AgencyLang != ""},
				sql.NullString{String: agency.AgencyFareURL, Valid: agency.AgencyFareURL != ""},
			)
		},
		OnStop: func(stop *models.Stop) error {
			return stopBatch.Add(
				stop.StopID,
				i.sourceID,
				i.versionID,
				stop.StopName,
				sql.NullFloat64{Float64: stop.StopLat, Valid: stop.StopLat != 0},
				sql.NullFloat64{Float64: stop.StopLon, Valid: stop.StopLon != 0},
				stop.LocationType,
				sql.NullString{String: stop.ParentStation, Valid: stop.ParentStation != ""},
				stop.WheelchairBoarding,
				sql.NullString{String: stop.LevelID, Valid: stop.LevelID != ""},
			)
		},
		OnRoute: func(route *models.Route) error {
			return routeBatch.Add(
				route.RouteID,
				i.sourceID,
				i.versionID,
				sql.NullString{String: route.AgencyID, Valid: route.AgencyID != ""},
				sql.NullString{String: route.RouteShortName, Valid: route.RouteShortName != ""},
				sql.NullString{String: route.RouteLongName, Valid: route.RouteLongName != ""},
				route.RouteType,
				sql.NullString{String: route.RouteColor, Valid: route.RouteColor != ""},
				sql.NullString{String: route.RouteTextColor, Valid: route.RouteTextColor != ""},
			)
		},
		OnCalendar: func(calendar *models.Calendar) error {
			return calendarBatch.Add(
				calendar.ServiceID,
				i.sourceID,
				i.versionID,
				calendar.Monday,
				calendar.Tuesday,
				calendar.Wednesday,
				calendar.Thursday,
				calendar.Friday,
				calendar.Saturday,
				calendar.Sunday,
				calendar.StartDate,
				calendar.EndDate,
			)
		},
		OnCalendarDate: func(calendarDate *models.CalendarDate) error {
			return calendarDateBatch.Add(
				calendarDate.ServiceID,
				i.sourceID,
				i.versionID,
				calendarDate.Date,
				calendarDate.ExceptionType,
			)
		},
		OnShape: func(shape *models.Shape) error {
			return shapeBatch.Add(
				shape.ShapeID,
				i.sourceID,
				i.versionID,
				shape.ShapePtLat,
				shape.ShapePtLon,
				shape.ShapePtSequence,
				sql.NullFloat64{Float64: shape.ShapeDistTraveled, Valid: shape.ShapeDistTraveled != 0},
			)
		},
		OnTrip: func(trip *models.Trip) error {
			return tripBatch.Add(
				trip.TripID,
				i.sourceID,
				i.versionID,
				trip.RouteID,
				trip.ServiceID,
				sql.NullString{String: trip.ShapeID, Valid: trip.ShapeID != ""},
				sql.NullString{String: trip.TripHeadsign, Valid: trip.TripHeadsign != ""},
				sql.NullInt64{Int64: int64(trip.DirectionID), Valid: true},
				sql.NullString{String: trip.BlockID, Valid: trip.BlockID != ""},
				trip.WheelchairAccessible,
			)
		},
		OnStopTime: func(stopTime *models.StopTime) error {
			// Convert time strings to sql.NullTime
			var arrivalTime, departureTime sql.NullTime
			if stopTime.ArrivalTime != "" {
				if t, err := parseGTFSTime(stopTime.ArrivalTime); err == nil {
					arrivalTime = sql.NullTime{Time: t, Valid: true}
				}
			}
			if stopTime.DepartureTime != "" {
				if t, err := parseGTFSTime(stopTime.DepartureTime); err == nil {
					departureTime = sql.NullTime{Time: t, Valid: true}
				}
			}
			
			return stopTimeBatch.Add(
				stopTime.TripID,
				i.sourceID,
				i.versionID,
				stopTime.StopID,
				stopTime.StopSequence,
				arrivalTime,
				departureTime,
				sql.NullString{String: stopTime.StopHeadsign, Valid: stopTime.StopHeadsign != ""},
				stopTime.PickupType,
				stopTime.DropOffType,
				sql.NullFloat64{Float64: stopTime.ShapeDistTraveled, Valid: stopTime.ShapeDistTraveled != 0},
			)
		},
		OnLevel: func(level *models.Level) error {
			return levelBatch.Add(
				level.LevelID,
				i.sourceID,
				i.versionID,
				sql.NullFloat64{Float64: level.LevelIndex, Valid: level.LevelIndex != 0},
				sql.NullString{String: level.LevelName, Valid: level.LevelName != ""},
			)
		},
		OnPathway: func(pathway *models.Pathway) error {
			return pathwayBatch.Add(
				pathway.PathwayID,
				i.sourceID,
				i.versionID,
				pathway.FromStopID,
				pathway.ToStopID,
				pathway.PathwayMode,
				sql.NullInt64{Int64: int64(pathway.IsBidirectional), Valid: true},
				sql.NullInt64{Int64: int64(pathway.TraversalTime), Valid: pathway.TraversalTime != 0},
			)
		},
		OnTransfer: func(transfer *models.Transfer) error {
			fromTripID := transfer.FromTripID
			toTripID := transfer.ToTripID
			if fromTripID == "" {
				fromTripID = ""
			}
			if toTripID == "" {
				toTripID = ""
			}
			
			return transferBatch.Add(
				transfer.FromStopID,
				transfer.ToStopID,
				i.sourceID,
				i.versionID,
				sql.NullString{String: transfer.FromRouteID, Valid: transfer.FromRouteID != ""},
				sql.NullString{String: transfer.ToRouteID, Valid: transfer.ToRouteID != ""},
				fromTripID,
				toTripID,
				transfer.TransferType,
				sql.NullInt64{Int64: int64(transfer.MinTransferTime), Valid: transfer.MinTransferTime != 0},
			)
		},
	}
	
	// Begin transaction
	tx, err := i.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()
	
	// Set transaction for all batch inserters
	batches := []*batchInserter{
		agencyBatch, levelBatch, stopBatch, routeBatch, calendarBatch,
		calendarDateBatch, shapeBatch, tripBatch, stopTimeBatch,
		pathwayBatch, transferBatch,
	}
	
	for _, batch := range batches {
		batch.tx = tx
	}
	
	// Parse the zip file
	if err := p.ParseZip(ctx, zipPath, callbacks); err != nil {
		return fmt.Errorf("parsing zip: %w", err)
	}
	
	// Flush all remaining batches
	for _, batch := range batches {
		if err := batch.Flush(); err != nil {
			return fmt.Errorf("flushing %s batch: %w", batch.tableName, err)
		}
	}
	
	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	
	i.db.Logger().Info("Import completed successfully", 
		"source_id", i.sourceID,
		"version_id", i.versionID)
	
	return nil
}

type batchInserter struct {
	tableName   string
	columns     []string
	values      []interface{}
	valueCount  int
	batchSize   int
	tx          *sql.Tx
	fieldCount  int
}

func (i *Importer) newBatchInserter(tableName string, fieldCount int) *batchInserter {
	columns := getColumnsForTable(tableName)
	return &batchInserter{
		tableName:  tableName,
		columns:    columns,
		values:     make([]interface{}, 0, i.batchSize*fieldCount),
		batchSize:  i.batchSize,
		fieldCount: fieldCount,
	}
}

func (b *batchInserter) Add(values ...interface{}) error {
	b.values = append(b.values, values...)
	b.valueCount++
	
	if b.valueCount >= b.batchSize {
		return b.Flush()
	}
	
	return nil
}

func (b *batchInserter) Flush() error {
	if b.valueCount == 0 {
		return nil
	}
	
	query := b.buildInsertQuery()
	_, err := b.tx.Exec(query, b.values...)
	if err != nil {
		return fmt.Errorf("executing batch insert: %w", err)
	}
	
	// Reset
	b.values = b.values[:0]
	b.valueCount = 0
	
	return nil
}

func (b *batchInserter) buildInsertQuery() string {
	var sb strings.Builder
	
	sb.WriteString(fmt.Sprintf("INSERT INTO gtfs.%s (%s) VALUES ",
		b.tableName,
		strings.Join(b.columns, ", ")))
	
	for i := 0; i < b.valueCount; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for j := 0; j < b.fieldCount; j++ {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("$%d", i*b.fieldCount+j+1))
		}
		sb.WriteString(")")
	}
	
	sb.WriteString(" ON CONFLICT DO NOTHING")
	
	return sb.String()
}

func getColumnsForTable(tableName string) []string {
	switch tableName {
	case "agency":
		return []string{"agency_id", "source_id", "version_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_fare_url"}
	case "stops":
		return []string{"stop_id", "source_id", "version_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station", "wheelchair_boarding", "level_id"}
	case "routes":
		return []string{"route_id", "source_id", "version_id", "agency_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color"}
	case "calendar":
		return []string{"service_id", "source_id", "version_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"}
	case "calendar_dates":
		return []string{"service_id", "source_id", "version_id", "date", "exception_type"}
	case "shapes":
		return []string{"shape_id", "source_id", "version_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "shape_dist_traveled"}
	case "trips":
		return []string{"trip_id", "source_id", "version_id", "route_id", "service_id", "shape_id", "trip_headsign", "direction_id", "block_id", "wheelchair_accessible"}
	case "stop_times":
		return []string{"trip_id", "source_id", "version_id", "stop_id", "stop_sequence", "arrival_time", "departure_time", "stop_headsign", "pickup_type", "drop_off_type", "shape_dist_traveled"}
	case "levels":
		return []string{"level_id", "source_id", "version_id", "level_index", "level_name"}
	case "pathways":
		return []string{"pathway_id", "source_id", "version_id", "from_stop_id", "to_stop_id", "pathway_mode", "is_bidirectional", "traversal_time"}
	case "transfers":
		return []string{"from_stop_id", "to_stop_id", "source_id", "version_id", "from_route_id", "to_route_id", "from_trip_id", "to_trip_id", "transfer_type", "min_transfer_time"}
	default:
		return nil
	}
}

func parseGTFSTime(timeStr string) (time.Time, error) {
	// GTFS times can be in format HH:MM:SS and can exceed 24:00:00
	parts := strings.Split(timeStr, ":")
	if len(parts) != 3 {
		return time.Time{}, fmt.Errorf("invalid time format: %s", timeStr)
	}
	
	// For now, we'll use a base date and parse as a regular time
	// In production, you might want to handle times > 24:00:00 differently
	baseDate := "2006-01-02 "
	normalizedTime := timeStr
	
	// Handle times >= 24:00:00 by converting to next day
	// This is a simplified approach - you may need more sophisticated handling
	t, err := time.Parse("2006-01-02 15:04:05", baseDate+normalizedTime)
	if err != nil {
		return time.Time{}, err
	}
	
	return t, nil
}