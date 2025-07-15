package parser

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	
	"github.com/ptvtracker-data/pkg/gtfs/models"
	"github.com/ptvtracker-data/internal/common/logger"
)

type Parser struct {
	logger logger.Logger
}

func New(logger logger.Logger) *Parser {
	return &Parser{logger: logger}
}

type ParseCallbacks struct {
	OnAgency      func(agency *models.Agency) error
	OnStop        func(stop *models.Stop) error
	OnRoute       func(route *models.Route) error
	OnTrip        func(trip *models.Trip) error
	OnStopTime    func(stopTime *models.StopTime) error
	OnCalendar    func(calendar *models.Calendar) error
	OnCalendarDate func(calendarDate *models.CalendarDate) error
	OnShape       func(shape *models.Shape) error
	OnLevel       func(level *models.Level) error
	OnPathway     func(pathway *models.Pathway) error
	OnTransfer    func(transfer *models.Transfer) error
	OnFileComplete func(fileName string) error
}

func (p *Parser) ParseZip(ctx context.Context, zipPath string, callbacks ParseCallbacks) error {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("opening zip file: %w", err)
	}
	defer reader.Close()
	
	p.logger.Info("Parsing GTFS zip file", "path", zipPath, "files", len(reader.File))
	
	// Check if this is a Victorian nested GTFS structure
	// Look for numbered folders with google_transit.zip files
	hasNestedStructure := false
	var nestedFile *zip.File
	
	for _, file := range reader.File {
		if strings.HasSuffix(file.Name, "/google_transit.zip") {
			// For metro source, use folder 2 (Metropolitan Train)
			// TODO: Make this configurable based on source mapping
			if strings.HasPrefix(file.Name, "2/") {
				hasNestedStructure = true
				nestedFile = file
				break
			}
		}
	}
	
	if hasNestedStructure && nestedFile != nil {
		p.logger.Info("Detected Victorian nested GTFS structure, parsing nested file", "file", nestedFile.Name)
		return p.parseNestedGTFS(ctx, nestedFile, callbacks)
	}
	
	// Standard GTFS parsing
	return p.parseStandardGTFS(ctx, &reader.Reader, callbacks)
}

func (p *Parser) parseNestedGTFS(ctx context.Context, zipFile *zip.File, callbacks ParseCallbacks) error {
	// Open the nested zip file
	rc, err := zipFile.Open()
	if err != nil {
		return fmt.Errorf("opening nested zip: %w", err)
	}
	defer rc.Close()
	
	// Read the entire nested zip into memory
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("reading nested zip: %w", err)
	}
	
	// Create a reader for the nested zip
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("creating zip reader: %w", err)
	}
	
	// Parse the nested GTFS files
	return p.parseStandardGTFS(ctx, reader, callbacks)
}

func (p *Parser) parseStandardGTFS(ctx context.Context, reader *zip.Reader, callbacks ParseCallbacks) error {
	// Define parsing order for referential integrity
	parseOrder := []string{
		"agency.txt",
		"levels.txt",
		"stops.txt",
		"routes.txt",
		"calendar.txt",
		"calendar_dates.txt",
		"shapes.txt",
		"trips.txt",
		"stop_times.txt",
		"pathways.txt",
		"transfers.txt",
	}
	
	// Create a map for quick file lookup
	fileMap := make(map[string]*zip.File)
	for _, file := range reader.File {
		fileMap[file.Name] = file
	}
	
	// Parse files in order
	for _, fileName := range parseOrder {
		if file, exists := fileMap[fileName]; exists {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if err := p.parseFile(file, callbacks); err != nil {
					return fmt.Errorf("parsing %s: %w", fileName, err)
				}
			}
		} else {
			p.logger.Debug("File not found in archive", "file", fileName)
		}
	}
	
	p.logger.Info("GTFS parsing completed successfully")
	return nil
}

func (p *Parser) parseFile(file *zip.File, callbacks ParseCallbacks) error {
	p.logger.Debug("Parsing file", "name", file.Name, "size", file.UncompressedSize64)
	
	rc, err := file.Open()
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer rc.Close()
	
	reader := csv.NewReader(rc)
	reader.FieldsPerRecord = -1 // Variable number of fields
	reader.TrimLeadingSpace = true
	
	// Read header
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("reading header: %w", err)
	}
	
	// Create header index map
	headerMap := make(map[string]int)
	for i, h := range header {
		headerMap[strings.TrimSpace(h)] = i
	}
	
	// Parse records
	count := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading record: %w", err)
		}
		
		switch file.Name {
		case "agency.txt":
			if callbacks.OnAgency != nil {
				agency := p.parseAgency(record, headerMap)
				if err := callbacks.OnAgency(agency); err != nil {
					return err
				}
			}
		case "stops.txt":
			if callbacks.OnStop != nil {
				stop := p.parseStop(record, headerMap)
				if err := callbacks.OnStop(stop); err != nil {
					return err
				}
			}
		case "routes.txt":
			if callbacks.OnRoute != nil {
				route := p.parseRoute(record, headerMap)
				if err := callbacks.OnRoute(route); err != nil {
					return err
				}
			}
		case "trips.txt":
			if callbacks.OnTrip != nil {
				trip := p.parseTrip(record, headerMap)
				if err := callbacks.OnTrip(trip); err != nil {
					return err
				}
			}
		case "stop_times.txt":
			if callbacks.OnStopTime != nil {
				stopTime := p.parseStopTime(record, headerMap)
				if err := callbacks.OnStopTime(stopTime); err != nil {
					return err
				}
			}
		case "calendar.txt":
			if callbacks.OnCalendar != nil {
				calendar, err := p.parseCalendar(record, headerMap)
				if err != nil {
					p.logger.Warn("Failed to parse calendar record", "error", err)
					continue
				}
				if err := callbacks.OnCalendar(calendar); err != nil {
					return err
				}
			}
		case "calendar_dates.txt":
			if callbacks.OnCalendarDate != nil {
				calendarDate, err := p.parseCalendarDate(record, headerMap)
				if err != nil {
					p.logger.Warn("Failed to parse calendar_date record", "error", err)
					continue
				}
				if err := callbacks.OnCalendarDate(calendarDate); err != nil {
					return err
				}
			}
		case "shapes.txt":
			if callbacks.OnShape != nil {
				shape := p.parseShape(record, headerMap)
				if err := callbacks.OnShape(shape); err != nil {
					return err
				}
			}
		case "levels.txt":
			if callbacks.OnLevel != nil {
				level := p.parseLevel(record, headerMap)
				if err := callbacks.OnLevel(level); err != nil {
					return err
				}
			}
		case "pathways.txt":
			if callbacks.OnPathway != nil {
				pathway := p.parsePathway(record, headerMap)
				if err := callbacks.OnPathway(pathway); err != nil {
					return err
				}
			}
		case "transfers.txt":
			if callbacks.OnTransfer != nil {
				transfer := p.parseTransfer(record, headerMap)
				if err := callbacks.OnTransfer(transfer); err != nil {
					return err
				}
			}
		}
		
		count++
		if count%10000 == 0 {
			p.logger.Debug("Progress", "file", file.Name, "records", count)
		}
	}
	
	p.logger.Info("File parsed", "name", file.Name, "records", count)
	
	// Call file complete callback if provided
	if callbacks.OnFileComplete != nil {
		if err := callbacks.OnFileComplete(file.Name); err != nil {
			return fmt.Errorf("file complete callback: %w", err)
		}
	}
	
	return nil
}

// Helper functions to safely get values from CSV records
func (p *Parser) getString(record []string, headerMap map[string]int, field string) string {
	if idx, ok := headerMap[field]; ok && idx < len(record) {
		return strings.TrimSpace(record[idx])
	}
	return ""
}

func (p *Parser) getInt(record []string, headerMap map[string]int, field string, defaultVal int) int {
	str := p.getString(record, headerMap, field)
	if str == "" {
		return defaultVal
	}
	val, err := strconv.Atoi(str)
	if err != nil {
		return defaultVal
	}
	return val
}

func (p *Parser) getFloat(record []string, headerMap map[string]int, field string, defaultVal float64) float64 {
	str := p.getString(record, headerMap, field)
	if str == "" {
		return defaultVal
	}
	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return defaultVal
	}
	return val
}

// Parse individual record types
func (p *Parser) parseAgency(record []string, headerMap map[string]int) *models.Agency {
	return &models.Agency{
		AgencyID:       p.getString(record, headerMap, "agency_id"),
		AgencyName:     p.getString(record, headerMap, "agency_name"),
		AgencyURL:      p.getString(record, headerMap, "agency_url"),
		AgencyTimezone: p.getString(record, headerMap, "agency_timezone"),
		AgencyLang:     p.getString(record, headerMap, "agency_lang"),
		AgencyFareURL:  p.getString(record, headerMap, "agency_fare_url"),
	}
}

func (p *Parser) parseStop(record []string, headerMap map[string]int) *models.Stop {
	return &models.Stop{
		StopID:             p.getString(record, headerMap, "stop_id"),
		StopName:           p.getString(record, headerMap, "stop_name"),
		StopLat:            p.getFloat(record, headerMap, "stop_lat", 0),
		StopLon:            p.getFloat(record, headerMap, "stop_lon", 0),
		LocationType:       p.getInt(record, headerMap, "location_type", 0),
		ParentStation:      p.getString(record, headerMap, "parent_station"),
		WheelchairBoarding: p.getInt(record, headerMap, "wheelchair_boarding", 0),
		LevelID:            p.getString(record, headerMap, "level_id"),
	}
}

func (p *Parser) parseRoute(record []string, headerMap map[string]int) *models.Route {
	return &models.Route{
		RouteID:        p.getString(record, headerMap, "route_id"),
		AgencyID:       p.getString(record, headerMap, "agency_id"),
		RouteShortName: p.getString(record, headerMap, "route_short_name"),
		RouteLongName:  p.getString(record, headerMap, "route_long_name"),
		RouteType:      p.getInt(record, headerMap, "route_type", 0),
		RouteColor:     p.getString(record, headerMap, "route_color"),
		RouteTextColor: p.getString(record, headerMap, "route_text_color"),
	}
}

func (p *Parser) parseTrip(record []string, headerMap map[string]int) *models.Trip {
	return &models.Trip{
		TripID:               p.getString(record, headerMap, "trip_id"),
		RouteID:              p.getString(record, headerMap, "route_id"),
		ServiceID:            p.getString(record, headerMap, "service_id"),
		ShapeID:              p.getString(record, headerMap, "shape_id"),
		TripHeadsign:         p.getString(record, headerMap, "trip_headsign"),
		DirectionID:          p.getInt(record, headerMap, "direction_id", 0),
		BlockID:              p.getString(record, headerMap, "block_id"),
		WheelchairAccessible: p.getInt(record, headerMap, "wheelchair_accessible", 0),
	}
}

func (p *Parser) parseStopTime(record []string, headerMap map[string]int) *models.StopTime {
	return &models.StopTime{
		TripID:            p.getString(record, headerMap, "trip_id"),
		StopID:            p.getString(record, headerMap, "stop_id"),
		StopSequence:      p.getInt(record, headerMap, "stop_sequence", 0),
		ArrivalTime:       p.getString(record, headerMap, "arrival_time"),
		DepartureTime:     p.getString(record, headerMap, "departure_time"),
		StopHeadsign:      p.getString(record, headerMap, "stop_headsign"),
		PickupType:        p.getInt(record, headerMap, "pickup_type", 0),
		DropOffType:       p.getInt(record, headerMap, "drop_off_type", 0),
		ShapeDistTraveled: p.getFloat(record, headerMap, "shape_dist_traveled", 0),
	}
}

func (p *Parser) parseCalendar(record []string, headerMap map[string]int) (*models.Calendar, error) {
	startDate, err := time.Parse("20060102", p.getString(record, headerMap, "start_date"))
	if err != nil {
		return nil, fmt.Errorf("parsing start_date: %w", err)
	}
	
	endDate, err := time.Parse("20060102", p.getString(record, headerMap, "end_date"))
	if err != nil {
		return nil, fmt.Errorf("parsing end_date: %w", err)
	}
	
	return &models.Calendar{
		ServiceID: p.getString(record, headerMap, "service_id"),
		Monday:    p.getInt(record, headerMap, "monday", 0),
		Tuesday:   p.getInt(record, headerMap, "tuesday", 0),
		Wednesday: p.getInt(record, headerMap, "wednesday", 0),
		Thursday:  p.getInt(record, headerMap, "thursday", 0),
		Friday:    p.getInt(record, headerMap, "friday", 0),
		Saturday:  p.getInt(record, headerMap, "saturday", 0),
		Sunday:    p.getInt(record, headerMap, "sunday", 0),
		StartDate: startDate,
		EndDate:   endDate,
	}, nil
}

func (p *Parser) parseCalendarDate(record []string, headerMap map[string]int) (*models.CalendarDate, error) {
	date, err := time.Parse("20060102", p.getString(record, headerMap, "date"))
	if err != nil {
		return nil, fmt.Errorf("parsing date: %w", err)
	}
	
	return &models.CalendarDate{
		ServiceID:     p.getString(record, headerMap, "service_id"),
		Date:          date,
		ExceptionType: p.getInt(record, headerMap, "exception_type", 0),
	}, nil
}

func (p *Parser) parseShape(record []string, headerMap map[string]int) *models.Shape {
	return &models.Shape{
		ShapeID:           p.getString(record, headerMap, "shape_id"),
		ShapePtLat:        p.getFloat(record, headerMap, "shape_pt_lat", 0),
		ShapePtLon:        p.getFloat(record, headerMap, "shape_pt_lon", 0),
		ShapePtSequence:   p.getInt(record, headerMap, "shape_pt_sequence", 0),
		ShapeDistTraveled: p.getFloat(record, headerMap, "shape_dist_traveled", 0),
	}
}

func (p *Parser) parseLevel(record []string, headerMap map[string]int) *models.Level {
	return &models.Level{
		LevelID:    p.getString(record, headerMap, "level_id"),
		LevelIndex: p.getFloat(record, headerMap, "level_index", 0),
		LevelName:  p.getString(record, headerMap, "level_name"),
	}
}

func (p *Parser) parsePathway(record []string, headerMap map[string]int) *models.Pathway {
	return &models.Pathway{
		PathwayID:       p.getString(record, headerMap, "pathway_id"),
		FromStopID:      p.getString(record, headerMap, "from_stop_id"),
		ToStopID:        p.getString(record, headerMap, "to_stop_id"),
		PathwayMode:     p.getInt(record, headerMap, "pathway_mode", 0),
		IsBidirectional: p.getInt(record, headerMap, "is_bidirectional", 0),
		TraversalTime:   p.getInt(record, headerMap, "traversal_time", 0),
	}
}

func (p *Parser) parseTransfer(record []string, headerMap map[string]int) *models.Transfer {
	return &models.Transfer{
		FromStopID:      p.getString(record, headerMap, "from_stop_id"),
		ToStopID:        p.getString(record, headerMap, "to_stop_id"),
		FromRouteID:     p.getString(record, headerMap, "from_route_id"),
		ToRouteID:       p.getString(record, headerMap, "to_route_id"),
		FromTripID:      p.getString(record, headerMap, "from_trip_id"),
		ToTripID:        p.getString(record, headerMap, "to_trip_id"),
		TransferType:    p.getInt(record, headerMap, "transfer_type", 0),
		MinTransferTime: p.getInt(record, headerMap, "min_transfer_time", 0),
	}
}