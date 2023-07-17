package geom

type CoordinateSequence interface {
	Clone() CoordinateSequence

	GetAt(i int) Coordinate

	GetAtWithCoordinate(i int, c *Coordinate)

	GetSize() int

	SetAt(c Coordinate, pos int)

	ToVector(coords []Coordinate)

	IsEmpty() bool

	ToString() string

	SetPoints(v []Coordinate)

	Back() Coordinate

	Front() Coordinate

	HasRepeatedPoints() bool

	MinCoordinate() *Coordinate

	atLeastNCoordinatesOrNothing(n int,
		c CoordinateSequence) CoordinateSequence

	indexOf(coordinate *Coordinate,
		cl CoordinateSequence) int

	equals(cl1, cl2 CoordinateSequence) bool

	scroll(cl *CoordinateSequence, firstCoordinate *Coordinate)
}
