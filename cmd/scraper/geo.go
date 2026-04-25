package main

import "math"

const (
	degToRad        = math.Pi / 180
	earthRadiusM    = 6371000.0
	metersPerDegLat = 110540.0
	metersPerDegLon = 111320.0
)

func haversineMeters(lat1, lon1, lat2, lon2 float64) float64 {
	phi1 := lat1 * degToRad
	phi2 := lat2 * degToRad
	dPhi := (lat2 - lat1) * degToRad
	dLambda := (lon2 - lon1) * degToRad
	a := math.Sin(dPhi/2)*math.Sin(dPhi/2) + math.Cos(phi1)*math.Cos(phi2)*math.Sin(dLambda/2)*math.Sin(dLambda/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusM * c
}

func projectPointOntoSegment(px, py, ax, ay, bx, by float64) (t, distSq float64) {
	dx := bx - ax
	dy := by - ay
	if dx == 0 && dy == 0 {
		ddx := px - ax
		ddy := py - ay
		return 0, ddx*ddx + ddy*ddy
	}
	t = ((px-ax)*dx + (py-ay)*dy) / (dx*dx + dy*dy)
	if t < 0 {
		t = 0
	} else if t > 1 {
		t = 1
	}
	fx := ax + t*dx
	fy := ay + t*dy
	ddx := px - fx
	ddy := py - fy
	return t, ddx*ddx + ddy*ddy
}

// projectLatLonOntoShape projects a (lat,lon) point onto a polyline shape.
// shape entries are [lat, lon, cumulative_distance_meters].
// Returns distance along the shape (meters from start) and perpendicular
// distance from the shape (meters).
func projectLatLonOntoShape(stopLat, stopLon float64, shape [][3]float64) (distAlong, perpDist float64) {
	if len(shape) < 2 {
		return 0, 0
	}
	refLat := shape[0][0]
	refLon := shape[0][1]
	cosLat := math.Cos(refLat * degToRad)
	mPerDegLon := metersPerDegLon * cosLat

	toMeters := func(lat, lon float64) (x, y float64) {
		return (lon - refLon) * mPerDegLon, (lat - refLat) * metersPerDegLat
	}

	sx, sy := toMeters(stopLat, stopLon)

	bestDistAlong := 0.0
	bestPerpSq := math.Inf(1)

	for i := 0; i < len(shape)-1; i++ {
		ax, ay := toMeters(shape[i][0], shape[i][1])
		bx, by := toMeters(shape[i+1][0], shape[i+1][1])
		t, distSq := projectPointOntoSegment(sx, sy, ax, ay, bx, by)
		if distSq < bestPerpSq {
			bestPerpSq = distSq
			segLen := shape[i+1][2] - shape[i][2]
			bestDistAlong = shape[i][2] + t*segLen
		}
	}
	return bestDistAlong, math.Sqrt(bestPerpSq)
}
