package geom

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
)

/**
 * \class Envelope
 *
 * \brief
 * An Envelope defines a rectangulare region of the 2D coordinate plane.
 *
 * It is often used to represent the bounding box of a Geometry,
 * e.g. the minimum and maximum x and y values of the Coordinates.
 *
 * Note that Envelopes support infinite or half-infinite regions, by using
 * the values of `Double_POSITIVE_INFINITY` and `Double_NEGATIVE_INFINITY`.
 *
 * When Envelope objects are created or initialized, the supplies extent
 * values are automatically sorted into the correct order.
 *
 */

type Envelope struct {
	/// the minimum x-coordinate
	minx float64

	/// the maximum x-coordinate
	maxx float64

	/// the minimum y-coordinate
	miny float64

	/// the maximum y-coordinate
	maxy float64
}

func NewEnvelope(x1, x2, y1, y2 float64) *Envelope {
	var minx, maxx, miny, maxy float64
	if x1 < x2 {
		minx = x1
		maxx = x2
	} else {
		minx = x2
		maxx = x1
	}
	if y1 < y2 {
		miny = y1
		maxy = y2
	} else {
		miny = y2
		maxy = y1
	}
	return &Envelope{minx: minx, maxx: maxx, miny: miny, maxy: maxy}
}

func NewEnvelopeFromCoordinates(p1, p2 Coordinate) *Envelope {
	var minx, maxx, miny, maxy float64
	x1 := p1.X
	x2 := p2.X
	y1 := p1.Y
	y2 := p2.Y

	if x1 < x2 {
		minx = x1
		maxx = x2
	} else {
		minx = x2
		maxx = x1
	}
	if y1 < y2 {
		miny = y1
		maxy = y2
	} else {
		miny = y2
		maxy = y1
	}
	return &Envelope{minx: minx, maxx: maxx, miny: miny, maxy: maxy}
}

func (p *Envelope) SetToNull() {
	p.maxx = math.NaN()
	p.maxy = math.NaN()
	p.minx = math.NaN()
	p.miny = math.NaN()
}

func (p *Envelope) IsNull() bool {
	if p.maxx == math.NaN() {
		return true
	}
	return false
}

func (e *Envelope) GetWidth() float64 {
	if e.IsNull() {
		return 0
	}
	return e.maxx - e.minx
}

func (e *Envelope) GetHeight() float64 {
	if e.IsNull() {
		return 0
	}
	return e.maxy - e.miny
}

func (e *Envelope) GetArea() float64 {
	return e.GetWidth() * e.GetHeight()
}

func (e *Envelope) GetMaxY() float64 {
	if e.IsNull() {
		panic("Envelope is null")
	}
	return e.maxy
}

func (e *Envelope) GetMaxX() float64 {
	if e.IsNull() {
		panic("Envelope is null")
	}
	return e.maxx
}

func (e *Envelope) GetMinY() float64 {
	if e.IsNull() {
		panic("Envelope is null")
	}
	return e.miny
}

func (e *Envelope) GetMinX() float64 {
	if e.IsNull() {
		panic("Envelope is null")
	}
	return e.minx
}

func (e *Envelope) GetDiameter() float64 {
	if e.IsNull() {
		return 0.0
	}
	w := e.GetWidth()
	h := e.GetHeight()
	return math.Sqrt(w*w + h*h)
}

func (e *Envelope) ExpandToInclude(x, y float64) {
	if e.IsNull() {
		e.minx = x
		e.maxx = x
		e.miny = y
		e.maxy = y
	} else {
		if x < e.minx {
			e.minx = x
		}
		if x > e.maxx {
			e.maxx = x
		}
		if y < e.miny {
			e.miny = y
		}
		if y > e.maxy {
			e.maxy = y
		}
	}
}

func (e *Envelope) ExpandToInclude1(other *Envelope) {
	if e.IsNull() {
		e.minx = other.minx
		e.maxx = other.maxx
		e.miny = other.miny
		e.maxy = other.maxy
	} else {
		if other.minx < e.minx {
			e.minx = other.minx
		}
		if other.maxx > e.maxx {
			e.maxx = other.maxx
		}
		if other.miny < e.miny {
			e.miny = other.miny
		}
		if other.maxy > e.maxy {
			e.maxy = other.maxy
		}
	}
}

func (e *Envelope) covers(other *Envelope) bool {
	return other.minx >= e.minx &&
		other.maxx <= e.maxx &&
		other.miny >= e.miny &&
		other.maxy <= e.maxy
}

func (e *Envelope) covers1(x, y float64) bool {
	return x >= e.minx && x <= e.maxx && y >= e.miny && y <= e.maxy
}

func (e *Envelope) equals(other *Envelope) bool {
	if e.IsNull() {
		return other.IsNull()
	}
	return other.minx == e.minx &&
		other.maxx == e.maxx &&
		other.miny == e.miny &&
		other.maxy == e.maxy
}

func (e *Envelope) isfinite() bool {
	return !math.IsInf(e.minx, 0) && !math.IsInf(e.maxx, 0) &&
		!math.IsInf(e.miny, 0) && !math.IsInf(e.maxy, 0) &&
		!math.IsNaN(e.minx) && !math.IsNaN(e.maxx) &&
		!math.IsNaN(e.miny) && !math.IsNaN(e.maxy)
}

func (e *Envelope) toString() string {
	var s strings.Builder
	s.WriteString(fmt.Sprintf("%v", e))
	return s.String()
}

func split(str string, delimiters string) []string {
	var tokens []string

	lastPos := 0
	pos := strings.IndexAny(str, delimiters)

	for pos != -1 || lastPos != -1 {
		if pos == -1 {
			tokens = append(tokens, str[lastPos:])
			break
		}

		tokens = append(tokens, str[lastPos:pos])

		lastPos = strings.IndexAny(str[pos:], delimiters)
		if lastPos == -1 {
			break
		}

		lastPos += pos
		pos = strings.IndexAny(str[lastPos:], delimiters)
	}

	return tokens
}

func centre(e Envelope) (Coordinate, bool) {
	var p_centre Coordinate

	if e.IsNull() {
		return p_centre, false
	}

	p_centre.X = (e.GetMinX() + e.GetMaxX()) / 2.0
	p_centre.Y = (e.GetMinY() + e.GetMaxY()) / 2.0

	return p_centre, true
}

func (e *Envelope) init(x1 float64, x2 float64, y1 float64, y2 float64) {
	if x1 < x2 {
		e.minx = x1
		e.maxx = x2
	} else {
		e.minx = x2
		e.maxx = x1
	}

	if y1 < y2 {
		e.miny = y1
		e.maxy = y2
	} else {
		e.miny = y2
		e.maxy = y1
	}
}

func (e *Envelope) intersection(env Envelope, result *Envelope) bool {
	if e.IsNull() || env.IsNull() || !e.Intersects(&env) {
		return false
	}

	intMinX := math.Max(e.minx, env.minx)
	intMinY := math.Max(e.miny, env.miny)
	intMaxX := math.Min(e.maxx, env.maxx)
	intMaxY := math.Min(e.maxy, env.maxy)

	result.init(intMinX, intMaxX, intMinY, intMaxY)
	return true
}

func (p *Envelope) Intersects(other *Envelope) bool {
	return p.minx <= other.maxx && p.maxx >= other.minx && p.miny <= other.maxy && p.maxy >= other.miny
}

func (p *Envelope) Disjoint(other *Envelope) bool {
	return !p.Intersects(other)
}

func (p *Envelope) DistanceSquared(env *Envelope) float64 {
	dx := math.Max(0.0,
		math.Max(p.maxx, env.maxx)-math.Min(p.minx, env.minx)-(p.maxx-p.minx)-
			(env.maxx-env.minx))
	dy := math.Max(0.0,
		math.Max(p.maxy, env.maxy)-math.Min(p.miny, env.miny)-(p.maxy-p.miny)-
			(env.maxy-env.miny))

	return dx*dx + dy*dy
}

func (p *Envelope) Translate(transX, transY float64) {
	if p.IsNull() {
		return
	}
	p.init(p.GetMinX()+transX, p.GetMaxX()+transX,
		p.GetMinY()+transY, p.GetMaxY()+transY)
}

func (p *Envelope) ExpandBy(deltaX, deltaY float64) {
	p.minx -= deltaX
	p.maxx += deltaX
	p.miny -= deltaY
	p.maxy += deltaY

	// check for envelope disappearing
	if p.minx > p.maxx || p.miny > p.maxy {
		p.SetToNull()
	}
}

func (a *Envelope) LessThan(b *Envelope) bool {
	/*
	 * Compares two envelopes using lexicographic ordering.
	 * The ordering comparison is based on the usual numerical
	 * comparison between the sequence of ordinates.
	 * Null envelopes are less than all non-null envelopes.
	 */
	if a.IsNull() {
		// null == null
		if b.IsNull() {
			return false
		} else {
			return true
		}
	}
	// notnull > null
	if b.IsNull() {
		return false
	}

	// compare based on numerical ordering of ordinates
	if a.GetMinX() < b.GetMinX() {
		return true
	}
	if a.GetMinX() > b.GetMinX() {
		return false
	}
	if a.GetMinY() < b.GetMinY() {
		return true
	}
	if a.GetMinY() > b.GetMinY() {
		return false
	}
	if a.GetMaxX() < b.GetMaxX() {
		return true
	}
	if a.GetMaxX() > b.GetMaxX() {
		return false
	}
	if a.GetMaxY() < b.GetMaxY() {
		return true
	}
	if a.GetMaxY() > b.GetMaxY() {
		return false
	}
	return false // == is not strictly <
}

func (p *Envelope) HashCode() uint64 {
	var hash = fnv.New64a()
	var result uint64 = 17
	minxBytes := make([]byte, 8)
	maxxBytes := make([]byte, 8)
	minyBytes := make([]byte, 8)
	maxyBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(minxBytes, math.Float64bits(float64(p.minx)))
	binary.LittleEndian.PutUint64(maxxBytes, math.Float64bits(float64(p.maxx)))
	binary.LittleEndian.PutUint64(minyBytes, math.Float64bits(float64(p.miny)))
	binary.LittleEndian.PutUint64(maxyBytes, math.Float64bits(float64(p.maxy)))
	hash.Write(minxBytes)
	result = 37*result + hash.Sum64()
	hash.Reset()
	hash.Write(maxxBytes)
	result = 37*result + hash.Sum64()
	hash.Reset()
	hash.Write(minyBytes)
	result = 37*result + hash.Sum64()
	hash.Reset()
	hash.Write(maxyBytes)
	result = 37*result + hash.Sum64()
	return result
}

func (e *Envelope) intersects(a Coordinate, b Coordinate) bool {
	envminx := math.Min(a.X, b.X)
	if !(e.maxx >= envminx) {
		return false
	}

	envmaxx := math.Max(a.X, b.X)
	if envmaxx < e.minx {
		return false
	}

	envminy := math.Min(a.Y, b.Y)
	if envminy > e.maxy {
		return false
	}

	envmaxy := math.Max(a.Y, b.Y)
	if envmaxy < e.miny {
		return false
	}

	return true
}
