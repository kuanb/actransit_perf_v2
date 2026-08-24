package main

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"time"

	"cloud.google.com/go/civil"
	"google.golang.org/api/iterator"
)

const (
	bunchingMethodologyVersion  = 2
	bunchingMinHeadwaySeconds   = 30
	bunchingMaxHeadwaySeconds   = 90 * 60
	bunchingMaxFrequencySeconds = 40 * 60
	bunchingMinCellHeadways     = 2
	bunchingMinCVHeadways       = 100
	bunchingMinCVCoveragePct    = 10.0
)

var bunchingProgressPoints = [...]int{10, 25, 40, 55, 70, 85}

type bunchingAggregation struct {
	CVWeightedSum                     float64 `json:"cv_weighted_sum"`
	CVWeight                          int64   `json:"cv_weight"`
	ObservedHeadwaySeconds            float64 `json:"observed_headway_seconds"`
	ObservedHeadwaySquaredSeconds     float64 `json:"observed_headway_squared_seconds"`
	ComparableHeadwaySeconds          float64 `json:"comparable_headway_seconds"`
	ComparableHeadwaySquaredSeconds   float64 `json:"comparable_headway_squared_seconds"`
	EvenSpacingWaitAreaSecondsSquared float64 `json:"even_spacing_wait_area_seconds_squared"`
	ScheduledHeadwaySeconds           float64 `json:"scheduled_headway_seconds"`
	ScheduledHeadwaySquaredSeconds    float64 `json:"scheduled_headway_squared_seconds"`
	AllScheduledHeadwaySeconds        float64 `json:"all_scheduled_headway_seconds"`
}

type bunchingMetrics struct {
	Status                   string              `json:"status"`
	HeadwayN                 int64               `json:"headway_n"`
	CellN                    int64               `json:"cell_n"`
	ComparisonN              int64               `json:"comparison_n"`
	BunchedHeadwayN          int64               `json:"bunched_headway_n"`
	LongGapN                 int64               `json:"long_gap_n"`
	ScheduledHeadwayN        int64               `json:"scheduled_headway_n"`
	AllScheduledHeadwayN     int64               `json:"all_scheduled_headway_n"`
	HeadwayCV                *float64            `json:"headway_cv,omitempty"`
	BunchedHeadwayPct        *float64            `json:"bunched_headway_pct,omitempty"`
	LongGapPct               *float64            `json:"long_gap_pct,omitempty"`
	MeanHeadwayMin           *float64            `json:"mean_headway_min,omitempty"`
	ExpectedWaitMin          *float64            `json:"expected_wait_min,omitempty"`
	ScheduledExpectedWaitMin *float64            `json:"scheduled_expected_wait_min,omitempty"`
	EvenSpacingWaitMin       *float64            `json:"even_spacing_wait_min,omitempty"`
	SpacingPenaltyMin        *float64            `json:"spacing_penalty_min,omitempty"`
	Aggregation              bunchingAggregation `json:"aggregation"`
}

type bunchingHourStats struct {
	Hour int `json:"hour"`
	bunchingMetrics
}

type bunchingProgressStats struct {
	ProgressPct   int      `json:"progress_pct"`
	Status        string   `json:"status"`
	HeadwayN      int64    `json:"headway_n"`
	HeadwayCV     *float64 `json:"headway_cv,omitempty"`
	CVWeightedSum float64  `json:"cv_weighted_sum"`
	CVWeight      int64    `json:"cv_weight"`
}

type bunchingStats struct {
	MethodologyVersion int                 `json:"methodology_version"`
	GeneratedAt        time.Time           `json:"generated_at"`
	DaysExpected       int                 `json:"days_expected"`
	DaysAvailable      int                 `json:"days_available"`
	MissingDays        int                 `json:"missing_days"`
	Eligibility        bunchingEligibility `json:"eligibility"`
	bunchingMetrics
	ByHour     []bunchingHourStats     `json:"by_hour,omitempty"`
	ByProgress []bunchingProgressStats `json:"by_progress,omitempty"`
}

type bunchingEligibility struct {
	Eligible            bool    `json:"eligible"`
	Reason              string  `json:"reason,omitempty"`
	CVHeadwayN          int64   `json:"cv_headway_n"`
	CVCoveragePct       float64 `json:"cv_coverage_pct"`
	MinimumCVHeadwayN   int64   `json:"minimum_cv_headway_n"`
	MinimumCoveragePct  float64 `json:"minimum_coverage_pct"`
	MaximumFrequencyMin float64 `json:"maximum_frequency_min"`
}

type bunchingAccumulator struct {
	headwayN             int64
	cellN                int64
	comparisonN          int64
	bunchedN             int64
	longN                int64
	scheduledHeadwayN    int64
	allScheduledHeadwayN int64
	aggregation          bunchingAggregation
}

func (a *bunchingAccumulator) add(other bunchingAccumulator) {
	a.headwayN += other.headwayN
	a.cellN += other.cellN
	a.comparisonN += other.comparisonN
	a.bunchedN += other.bunchedN
	a.longN += other.longN
	a.scheduledHeadwayN += other.scheduledHeadwayN
	a.allScheduledHeadwayN += other.allScheduledHeadwayN
	a.aggregation.CVWeightedSum += other.aggregation.CVWeightedSum
	a.aggregation.CVWeight += other.aggregation.CVWeight
	a.aggregation.ObservedHeadwaySeconds += other.aggregation.ObservedHeadwaySeconds
	a.aggregation.ObservedHeadwaySquaredSeconds += other.aggregation.ObservedHeadwaySquaredSeconds
	a.aggregation.ComparableHeadwaySeconds += other.aggregation.ComparableHeadwaySeconds
	a.aggregation.ComparableHeadwaySquaredSeconds += other.aggregation.ComparableHeadwaySquaredSeconds
	a.aggregation.EvenSpacingWaitAreaSecondsSquared += other.aggregation.EvenSpacingWaitAreaSecondsSquared
	a.aggregation.ScheduledHeadwaySeconds += other.aggregation.ScheduledHeadwaySeconds
	a.aggregation.ScheduledHeadwaySquaredSeconds += other.aggregation.ScheduledHeadwaySquaredSeconds
	a.aggregation.AllScheduledHeadwaySeconds += other.aggregation.AllScheduledHeadwaySeconds
}

func (a *bunchingAccumulator) addObservedCell(gaps []bunchingGap) {
	if len(gaps) == 0 {
		return
	}
	a.cellN++
	var sum, sumSquares float64
	for _, gap := range gaps {
		h := gap.Seconds
		a.headwayN++
		sum += h
		sumSquares += h * h
		if gap.ScheduledSeconds <= 0 {
			continue
		}
		a.comparisonN++
		if h < 0.5*gap.ScheduledSeconds {
			a.bunchedN++
		}
		if h > 1.5*gap.ScheduledSeconds {
			a.longN++
		}
	}
	a.aggregation.ObservedHeadwaySeconds += sum
	a.aggregation.ObservedHeadwaySquaredSeconds += sumSquares
	if len(gaps) < bunchingMinCellHeadways {
		return
	}
	n := float64(len(gaps))
	mean := sum / n
	variance := math.Max(0, sumSquares/n-mean*mean)
	if mean > 0 {
		cv := math.Sqrt(variance) / mean
		a.aggregation.CVWeightedSum += cv * n
		a.aggregation.CVWeight += int64(len(gaps))
		a.aggregation.ComparableHeadwaySeconds += sum
		a.aggregation.ComparableHeadwaySquaredSeconds += sumSquares
		a.aggregation.EvenSpacingWaitAreaSecondsSquared += sum * sum / (2 * n)
	}
}

func (a *bunchingAccumulator) addScheduledHeadway(seconds float64) {
	a.allScheduledHeadwayN++
	a.aggregation.AllScheduledHeadwaySeconds += seconds
	if seconds > bunchingMaxFrequencySeconds {
		return
	}
	a.scheduledHeadwayN++
	a.aggregation.ScheduledHeadwaySeconds += seconds
	a.aggregation.ScheduledHeadwaySquaredSeconds += seconds * seconds
}

func (a bunchingAccumulator) metrics(minCVHeadways int64, minCoveragePct float64) bunchingMetrics {
	m := bunchingMetrics{
		Status:               "insufficient_data",
		HeadwayN:             a.headwayN,
		CellN:                a.cellN,
		ComparisonN:          a.comparisonN,
		BunchedHeadwayN:      a.bunchedN,
		LongGapN:             a.longN,
		ScheduledHeadwayN:    a.scheduledHeadwayN,
		AllScheduledHeadwayN: a.allScheduledHeadwayN,
		Aggregation:          a.aggregation,
	}
	coveragePct := 0.0
	if a.headwayN > 0 {
		coveragePct = 100 * float64(a.aggregation.CVWeight) / float64(a.headwayN)
	}
	if a.scheduledHeadwayN == 0 && a.allScheduledHeadwayN > 0 {
		m.Status = "too_low_frequency"
	} else if a.aggregation.CVWeight >= minCVHeadways && coveragePct >= minCoveragePct {
		v := roundTo(a.aggregation.CVWeightedSum/float64(a.aggregation.CVWeight), 2)
		m.HeadwayCV = &v
		m.Status = "available"
	}
	if a.comparisonN > 0 {
		bunched := round1(100 * float64(a.bunchedN) / float64(a.comparisonN))
		long := round1(100 * float64(a.longN) / float64(a.comparisonN))
		m.BunchedHeadwayPct = &bunched
		m.LongGapPct = &long
	}
	if a.aggregation.CVWeight > 0 {
		mean := round1(a.aggregation.ComparableHeadwaySeconds / float64(a.aggregation.CVWeight) / 60)
		m.MeanHeadwayMin = &mean
	}
	if a.aggregation.ComparableHeadwaySeconds > 0 {
		wait := round1(a.aggregation.ComparableHeadwaySquaredSeconds / (2 * a.aggregation.ComparableHeadwaySeconds) / 60)
		m.ExpectedWaitMin = &wait
		even := round1(a.aggregation.EvenSpacingWaitAreaSecondsSquared / a.aggregation.ComparableHeadwaySeconds / 60)
		m.EvenSpacingWaitMin = &even
		penalty := round1(math.Max(0, wait-even))
		m.SpacingPenaltyMin = &penalty
	}
	if a.aggregation.ScheduledHeadwaySeconds > 0 {
		wait := round1(a.aggregation.ScheduledHeadwaySquaredSeconds / (2 * a.aggregation.ScheduledHeadwaySeconds) / 60)
		m.ScheduledExpectedWaitMin = &wait
	}
	return m
}

func roundTo(v float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(v*scale) / scale
}

type bunchingTripMeta struct {
	RouteID     string
	DirectionID string
}

type bunchingTripStopKey struct {
	TripID       string
	StopSequence int64
}

type bunchingScheduledArrival struct {
	TripID   string
	Arrival  time.Time
	Progress int
	StopID   string
	StopSeq  int64
}

type bunchingStopKey struct {
	RouteID     string
	DirectionID string
	StopID      string
}

type bunchingSchedule struct {
	TripMeta     map[string]bunchingTripMeta
	TripProgress map[bunchingTripStopKey]int
	ByStop       map[bunchingStopKey][]bunchingScheduledArrival
}

type bunchingStopTime struct {
	StopID   string
	Sequence int64
	Arrival  time.Time
}

func loadBunchingSchedule(zr *zip.Reader, serviceDate civil.Date, services map[string]struct{}) (*bunchingSchedule, error) {
	tripCSV, tripRC, tripHeaders, err := openZipCSV(zr, "trips.txt")
	if err != nil {
		return nil, err
	}
	tripIdx := headerIndex(tripHeaders)
	tripMeta := make(map[string]bunchingTripMeta)
	for {
		row, err := tripCSV.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			tripRC.Close()
			return nil, err
		}
		if _, ok := services[col(row, tripIdx, "service_id")]; !ok {
			continue
		}
		tripMeta[col(row, tripIdx, "trip_id")] = bunchingTripMeta{
			RouteID:     col(row, tripIdx, "route_id"),
			DirectionID: col(row, tripIdx, "direction_id"),
		}
	}
	tripRC.Close()

	stopCSV, stopRC, stopHeaders, err := openZipCSV(zr, "stop_times.txt")
	if err != nil {
		return nil, err
	}
	defer stopRC.Close()
	stopIdx := headerIndex(stopHeaders)
	byTrip := make(map[string][]bunchingStopTime)
	for {
		row, err := stopCSV.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		tripID := col(row, stopIdx, "trip_id")
		if _, ok := tripMeta[tripID]; !ok {
			continue
		}
		arrival := parseScheduledArrival(serviceDate, col(row, stopIdx, "arrival_time"))
		if arrival.IsZero() {
			continue
		}
		sequence, err := strconv.ParseInt(col(row, stopIdx, "stop_sequence"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("trip %s has invalid stop_sequence: %w", tripID, err)
		}
		byTrip[tripID] = append(byTrip[tripID], bunchingStopTime{
			StopID:   col(row, stopIdx, "stop_id"),
			Sequence: sequence,
			Arrival:  arrival,
		})
	}

	out := &bunchingSchedule{
		TripMeta:     tripMeta,
		TripProgress: make(map[bunchingTripStopKey]int),
		ByStop:       make(map[bunchingStopKey][]bunchingScheduledArrival),
	}
	for tripID, stops := range byTrip {
		sort.Slice(stops, func(i, j int) bool { return stops[i].Sequence < stops[j].Sequence })
		meta := tripMeta[tripID]
		for i, stop := range stops {
			progress := 0
			if len(stops) > 1 {
				progress = int(math.Round(100 * float64(i) / float64(len(stops)-1)))
			}
			progress = nearestBunchingProgress(progress)
			out.TripProgress[bunchingTripStopKey{TripID: tripID, StopSequence: stop.Sequence}] = progress
			key := bunchingStopKey{RouteID: meta.RouteID, DirectionID: meta.DirectionID, StopID: stop.StopID}
			out.ByStop[key] = append(out.ByStop[key], bunchingScheduledArrival{
				TripID: tripID, Arrival: stop.Arrival, Progress: progress, StopID: stop.StopID, StopSeq: stop.Sequence,
			})
		}
	}
	for key := range out.ByStop {
		sort.Slice(out.ByStop[key], func(i, j int) bool {
			return out.ByStop[key][i].Arrival.Before(out.ByStop[key][j].Arrival)
		})
	}
	return out, nil
}

func nearestBunchingProgress(progress int) int {
	best := bunchingProgressPoints[0]
	bestDistance := absInt(progress - best)
	for _, candidate := range bunchingProgressPoints[1:] {
		distance := absInt(progress - candidate)
		if distance < bestDistance {
			best = candidate
			bestDistance = distance
		}
	}
	return best
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

type bunchingObservation struct {
	RouteID          string    `bigquery:"route_id"`
	TripID           string    `bigquery:"trip_id"`
	StopID           string    `bigquery:"stop_id"`
	StopSequence     int64     `bigquery:"stop_sequence"`
	ScheduledArrival time.Time `bigquery:"scheduled_arrival"`
	ActualArrival    time.Time `bigquery:"actual_arrival"`
}

func queryBunchingObservations(ctx context.Context, serviceDate civil.Date) ([]bunchingObservation, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s
		SELECT route_id, trip_id, stop_id, stop_sequence, scheduled_arrival, actual_arrival
		FROM obs
		WHERE actual_arrival IS NOT NULL
		  AND scheduled_arrival IS NOT NULL
		  AND is_stale = FALSE
	`, dedupedDayObservationsCTE(serviceDate)))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	var out []bunchingObservation
	for {
		var row bunchingObservation
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

type bunchingCellKey struct {
	RouteID     string
	DirectionID string
	StopID      string
	Hour        int
}

type bunchingGap struct {
	Seconds          float64
	ScheduledSeconds float64
	Progress         int
}

func calculateDailyBunching(ctx context.Context, zr *zip.Reader, serviceDate civil.Date, services map[string]struct{}) (*bunchingStats, map[string]*bunchingStats, error) {
	schedule, err := loadBunchingSchedule(zr, serviceDate, services)
	if err != nil {
		return nil, nil, fmt.Errorf("load schedule: %w", err)
	}
	observations, err := queryBunchingObservations(ctx, serviceDate)
	if err != nil {
		return nil, nil, fmt.Errorf("query observations: %w", err)
	}
	system, routes := computeDailyBunching(observations, schedule)
	return system, routes, nil
}

func computeDailyBunching(observations []bunchingObservation, schedule *bunchingSchedule) (*bunchingStats, map[string]*bunchingStats) {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		loc = time.UTC
	}
	byStop := make(map[bunchingStopKey][]bunchingObservation)
	for _, observation := range observations {
		meta, ok := schedule.TripMeta[observation.TripID]
		if !ok {
			continue
		}
		key := bunchingStopKey{RouteID: observation.RouteID, DirectionID: meta.DirectionID, StopID: observation.StopID}
		byStop[key] = append(byStop[key], observation)
	}

	actualCells := make(map[bunchingCellKey][]bunchingGap)
	for stopKey, arrivals := range byStop {
		sort.Slice(arrivals, func(i, j int) bool { return arrivals[i].ActualArrival.Before(arrivals[j].ActualArrival) })
		for i := 1; i < len(arrivals); i++ {
			previous := arrivals[i-1]
			current := arrivals[i]
			seconds := current.ActualArrival.Sub(previous.ActualArrival).Seconds()
			if seconds < bunchingMinHeadwaySeconds || seconds > bunchingMaxHeadwaySeconds {
				continue
			}
			scheduledSeconds := scheduledHeadwayBefore(schedule.ByStop[stopKey], current.ScheduledArrival)
			if scheduledSeconds <= 0 || scheduledSeconds > bunchingMaxFrequencySeconds {
				continue
			}
			progress := schedule.TripProgress[bunchingTripStopKey{TripID: current.TripID, StopSequence: current.StopSequence}]
			key := bunchingCellKey{
				RouteID: stopKey.RouteID, DirectionID: stopKey.DirectionID, StopID: stopKey.StopID,
				Hour: previous.ActualArrival.In(loc).Hour(),
			}
			actualCells[key] = append(actualCells[key], bunchingGap{
				Seconds: seconds, ScheduledSeconds: scheduledSeconds, Progress: progress,
			})
		}
	}

	systemAcc := &bunchingAccumulator{}
	routeAcc := make(map[string]*bunchingAccumulator)
	systemHours := make(map[int]*bunchingAccumulator)
	routeHours := make(map[string]map[int]*bunchingAccumulator)
	systemProgress := make(map[int]*bunchingAccumulator)
	routeProgress := make(map[string]map[int]*bunchingAccumulator)
	getRouteAcc := func(routeID string) *bunchingAccumulator {
		if routeAcc[routeID] == nil {
			routeAcc[routeID] = &bunchingAccumulator{}
		}
		return routeAcc[routeID]
	}

	for key, gaps := range actualCells {
		cell := bunchingAccumulator{}
		cell.addObservedCell(gaps)
		progress := dominantProgress(gaps)
		systemAcc.add(cell)
		getRouteAcc(key.RouteID).add(cell)
		getAccumulator(systemHours, key.Hour).add(cell)
		getNestedAccumulator(routeHours, key.RouteID, key.Hour).add(cell)
		getAccumulator(systemProgress, progress).add(cell)
		getNestedAccumulator(routeProgress, key.RouteID, progress).add(cell)
	}

	for key, arrivals := range schedule.ByStop {
		getRouteAcc(key.RouteID)
		for i := 1; i < len(arrivals); i++ {
			seconds := arrivals[i].Arrival.Sub(arrivals[i-1].Arrival).Seconds()
			if seconds < bunchingMinHeadwaySeconds {
				continue
			}
			hour := arrivals[i-1].Arrival.In(loc).Hour()
			progress := arrivals[i].Progress
			systemAcc.addScheduledHeadway(seconds)
			getRouteAcc(key.RouteID).addScheduledHeadway(seconds)
			getAccumulator(systemHours, hour).addScheduledHeadway(seconds)
			getNestedAccumulator(routeHours, key.RouteID, hour).addScheduledHeadway(seconds)
			getAccumulator(systemProgress, progress).addScheduledHeadway(seconds)
			getNestedAccumulator(routeProgress, key.RouteID, progress).addScheduledHeadway(seconds)
		}
	}

	generatedAt := time.Now().UTC()
	system := finalizeBunchingStats(*systemAcc, systemHours, systemProgress, generatedAt, 1, 1)
	routes := make(map[string]*bunchingStats, len(routeAcc))
	for routeID, acc := range routeAcc {
		routes[routeID] = finalizeBunchingStats(*acc, routeHours[routeID], routeProgress[routeID], generatedAt, 1, 1)
		routes[routeID].ByHour = nil
	}
	return system, routes
}

func scheduledHeadwayBefore(arrivals []bunchingScheduledArrival, scheduled time.Time) float64 {
	idx := sort.Search(len(arrivals), func(i int) bool { return !arrivals[i].Arrival.Before(scheduled) })
	if idx >= len(arrivals) || !arrivals[idx].Arrival.Equal(scheduled) {
		return 0
	}
	for previous := idx - 1; previous >= 0; previous-- {
		seconds := scheduled.Sub(arrivals[previous].Arrival).Seconds()
		if seconds > 0 {
			if seconds <= bunchingMaxHeadwaySeconds {
				return seconds
			}
			return 0
		}
	}
	return 0
}

func dominantProgress(gaps []bunchingGap) int {
	counts := make(map[int]int)
	best := bunchingProgressPoints[0]
	for _, gap := range gaps {
		progress := gap.Progress
		if progress == 0 {
			progress = bunchingProgressPoints[0]
		}
		counts[progress]++
		if counts[progress] > counts[best] {
			best = progress
		}
	}
	return best
}

func getAccumulator(values map[int]*bunchingAccumulator, key int) *bunchingAccumulator {
	if values[key] == nil {
		values[key] = &bunchingAccumulator{}
	}
	return values[key]
}

func getNestedAccumulator(values map[string]map[int]*bunchingAccumulator, routeID string, key int) *bunchingAccumulator {
	if values[routeID] == nil {
		values[routeID] = make(map[int]*bunchingAccumulator)
	}
	return getAccumulator(values[routeID], key)
}

func finalizeBunchingStats(acc bunchingAccumulator, hours, progress map[int]*bunchingAccumulator, generatedAt time.Time, daysExpected, daysAvailable int) *bunchingStats {
	out := &bunchingStats{
		MethodologyVersion: bunchingMethodologyVersion,
		GeneratedAt:        generatedAt,
		DaysExpected:       daysExpected,
		DaysAvailable:      daysAvailable,
		MissingDays:        daysExpected - daysAvailable,
		Eligibility:        bunchingEligibilityFor(acc),
		bunchingMetrics:    acc.metrics(bunchingMinCVHeadways, bunchingMinCVCoveragePct),
	}
	if out.MissingDays > 0 && out.Status == "available" {
		out.Status = "partial"
	}
	for hour := 0; hour < 24; hour++ {
		if hours[hour] == nil {
			continue
		}
		out.ByHour = append(out.ByHour, bunchingHourStats{Hour: hour, bunchingMetrics: hours[hour].metrics(bunchingMinCellHeadways, 0)})
	}
	for _, point := range bunchingProgressPoints {
		if progress[point] == nil {
			continue
		}
		metrics := progress[point].metrics(bunchingMinCellHeadways, 0)
		out.ByProgress = append(out.ByProgress, bunchingProgressStats{
			ProgressPct:   point,
			Status:        metrics.Status,
			HeadwayN:      metrics.HeadwayN,
			HeadwayCV:     metrics.HeadwayCV,
			CVWeightedSum: metrics.Aggregation.CVWeightedSum,
			CVWeight:      metrics.Aggregation.CVWeight,
		})
	}
	return out
}

func bunchingEligibilityFor(acc bunchingAccumulator) bunchingEligibility {
	coveragePct := 0.0
	if acc.headwayN > 0 {
		coveragePct = 100 * float64(acc.aggregation.CVWeight) / float64(acc.headwayN)
	}
	out := bunchingEligibility{
		Eligible:            true,
		CVHeadwayN:          acc.aggregation.CVWeight,
		CVCoveragePct:       round1(coveragePct),
		MinimumCVHeadwayN:   bunchingMinCVHeadways,
		MinimumCoveragePct:  bunchingMinCVCoveragePct,
		MaximumFrequencyMin: bunchingMaxFrequencySeconds / 60,
	}
	if acc.scheduledHeadwayN == 0 && acc.allScheduledHeadwayN > 0 {
		out.Eligible = false
		out.Reason = "too_low_frequency"
	} else if acc.aggregation.CVWeight < bunchingMinCVHeadways {
		out.Eligible = false
		out.Reason = "not_enough_comparable_headways"
	} else if coveragePct < bunchingMinCVCoveragePct {
		out.Eligible = false
		out.Reason = "low_comparable_headway_coverage"
	}
	return out
}

func accumulatorFromMetrics(m bunchingMetrics) bunchingAccumulator {
	return bunchingAccumulator{
		headwayN:             m.HeadwayN,
		cellN:                m.CellN,
		comparisonN:          m.ComparisonN,
		bunchedN:             m.BunchedHeadwayN,
		longN:                m.LongGapN,
		scheduledHeadwayN:    m.ScheduledHeadwayN,
		allScheduledHeadwayN: m.AllScheduledHeadwayN,
		aggregation:          m.Aggregation,
	}
}

func aggregateBunchingStats(values []*bunchingStats, daysExpected int) *bunchingStats {
	var total bunchingAccumulator
	hours := make(map[int]*bunchingAccumulator)
	progress := make(map[int]*bunchingAccumulator)
	daysAvailable := 0
	for _, value := range values {
		if value == nil || value.MethodologyVersion != bunchingMethodologyVersion {
			continue
		}
		daysAvailable += value.DaysAvailable
		total.add(accumulatorFromMetrics(value.bunchingMetrics))
		for _, hour := range value.ByHour {
			getAccumulator(hours, hour.Hour).add(accumulatorFromMetrics(hour.bunchingMetrics))
		}
		for _, point := range value.ByProgress {
			getAccumulator(progress, point.ProgressPct).add(bunchingAccumulator{
				headwayN:    point.HeadwayN,
				aggregation: bunchingAggregation{CVWeightedSum: point.CVWeightedSum, CVWeight: point.CVWeight},
			})
		}
	}
	if daysAvailable == 0 {
		return nil
	}
	if daysAvailable > daysExpected {
		daysAvailable = daysExpected
	}
	return finalizeBunchingStats(total, hours, progress, time.Now().UTC(), daysExpected, daysAvailable)
}
