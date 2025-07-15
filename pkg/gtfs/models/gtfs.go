package models

import (
	"time"
)

type Agency struct {
	AgencyID      string
	AgencyName    string
	AgencyURL     string
	AgencyTimezone string
	AgencyLang    string
	AgencyFareURL string
}

type Stop struct {
	StopID             string
	StopName           string
	StopLat            float64
	StopLon            float64
	LocationType       int
	ParentStation      string
	WheelchairBoarding int
	LevelID            string
}

type Route struct {
	RouteID        string
	AgencyID       string
	RouteShortName string
	RouteLongName  string
	RouteType      int
	RouteColor     string
	RouteTextColor string
}

type Trip struct {
	TripID               string
	RouteID              string
	ServiceID            string
	ShapeID              string
	TripHeadsign         string
	DirectionID          int
	BlockID              string
	WheelchairAccessible int
}

type StopTime struct {
	TripID             string
	StopID             string
	StopSequence       int
	ArrivalTime        string  // Format: HH:MM:SS
	DepartureTime      string  // Format: HH:MM:SS
	StopHeadsign       string
	PickupType         int
	DropOffType        int
	ShapeDistTraveled  float64
}

type Calendar struct {
	ServiceID  string
	Monday     int
	Tuesday    int
	Wednesday  int
	Thursday   int
	Friday     int
	Saturday   int
	Sunday     int
	StartDate  time.Time
	EndDate    time.Time
}

type CalendarDate struct {
	ServiceID     string
	Date          time.Time
	ExceptionType int
}

type Shape struct {
	ShapeID           string
	ShapePtLat        float64
	ShapePtLon        float64
	ShapePtSequence   int
	ShapeDistTraveled float64
}

type Level struct {
	LevelID    string
	LevelIndex float64
	LevelName  string
}

type Pathway struct {
	PathwayID       string
	FromStopID      string
	ToStopID        string
	PathwayMode     int
	IsBidirectional int
	TraversalTime   int
}

type Transfer struct {
	FromStopID      string
	ToStopID        string
	FromRouteID     string
	ToRouteID       string
	FromTripID      string
	ToTripID        string
	TransferType    int
	MinTransferTime int
}