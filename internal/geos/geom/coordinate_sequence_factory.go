package geom

type coordinateSequence struct {
	coords []Coordinate
}

type CoordinateSequenceFactory interface {
	Create() *coordinateSequence
	CreateFromVector(*[]Coordinate, uint) *coordinateSequence
	CreateFromVectorRValue([]Coordinate, uint) *coordinateSequence
	CreateWithSize(uint, uint) *coordinateSequence
	CreateFromCoordSeq(*coordinateSequence) *coordinateSequence
}

type coordinateSequenceFactory struct{}

func (f *coordinateSequenceFactory) Create() *coordinateSequence {
	return &coordinateSequence{}
}

func (f *coordinateSequenceFactory) CreateFromVector(v *[]Coordinate, dimension uint) *coordinateSequence {
	return &coordinateSequence{
		coords: *v,
	}
}

func (f *coordinateSequenceFactory) CreateFromVectorRValue(v []Coordinate, dimension uint) *coordinateSequence {
	return &coordinateSequence{
		coords: v,
	}
}

func (f *coordinateSequenceFactory) CreateWithSize(size uint, dimension uint) *coordinateSequence {
	return &coordinateSequence{
		coords: make([]Coordinate, size),
	}
}

func (f *coordinateSequenceFactory) CreateFromCoordSeq(coordSeq *coordinateSequence) *coordinateSequence {
	return &coordinateSequence{
		coords: coordSeq.coords,
	}
}
