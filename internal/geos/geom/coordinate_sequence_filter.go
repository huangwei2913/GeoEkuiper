package geom

type CoordinateSequenceFilter interface {
	FilterRw(seq *CoordinateSequence, i uint)
	FilterRo(seq *CoordinateSequence, i uint)
	IsDone() bool
	IsGeometryChanged() bool
}

type coordinateSequenceFilter struct{}

func (f *coordinateSequenceFilter) FilterRw(seq *CoordinateSequence, i uint) {
	panic("not implemented")
}

func (f *coordinateSequenceFilter) FilterRo(seq *CoordinateSequence, i uint) {
	panic("not implemented")
}

func (f *coordinateSequenceFilter) IsDone() bool {
	panic("not implemented")
}

func (f *coordinateSequenceFilter) IsGeometryChanged() bool {
	panic("not implemented")
}
