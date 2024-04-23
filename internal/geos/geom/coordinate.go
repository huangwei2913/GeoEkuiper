package geom

import (
	"math"
)

type Coordinate struct {
	X float64
	Y float64
	Z float64
}

type CoordinateLessThen struct {
}

func (*CoordinateLessThen) operator(a, b Coordinate) bool {
	if a.compareTo(b) < 0 {
		return true
	}
	return false
}

func (f *Coordinate) Equals2D(b Coordinate) bool {
	if f.X != b.X {
		return false
	}
	if f.Y != b.Y {
		return false
	}
	return true
}

func (f *Coordinate) equals3D(b Coordinate) bool {
	if f.X == b.X && f.Y == b.Y && f.Z == b.Z {
		return true
	}
	return false
}

func (f *Coordinate) compareTo(b Coordinate) int {
	if f.X < b.X {
		return -1
	}
	if f.X > b.X {
		return 1
	}
	if f.Y < b.Y {
		return -1
	}
	if f.Y > b.Y {
		return 1
	}
	return 0
}

func (f *Coordinate) distance(p Coordinate) float64 {
	dx := f.X - p.X
	dy := f.Y - p.Y
	return math.Sqrt(dx*dx + dy*dy)
}

func (f *Coordinate) distanceSquared(p Coordinate) float64 {
	dx := f.X - p.X
	dy := f.Y - p.Y
	return dx*dx + dy*dy
}
