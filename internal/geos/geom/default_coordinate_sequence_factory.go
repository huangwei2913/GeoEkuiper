package geom

type DefaultCoordinateSequenceFactory struct{}

// func (f *DefaultCoordinateSequenceFactory) Create() CoordinateSequence {
// 	return NewCoordinateArraySequence()
// }

// func (f *DefaultCoordinateSequenceFactory) CreateWithCoords(coords []Coordinate, dims int) CoordinateSequence {
// 	return NewCoordinateArraySequenceFromCoords(coords, dims)
// }

// func (f *DefaultCoordinateSequenceFactory) CreateWithSize(size int, dims int) CoordinateSequence {
// 	switch size {
// 	case 1:
// 		return NewFixedSizeCoordinateSequence(1, dims)
// 	case 2:
// 		return NewFixedSizeCoordinateSequence(2, dims)
// 	case 3:
// 		return NewFixedSizeCoordinateSequence(3, dims)
// 	case 4:
// 		return NewFixedSizeCoordinateSequence(4, dims)
// 	case 5:
// 		return NewFixedSizeCoordinateSequence(5, dims)
// 	default:
// 		return NewCoordinateArraySequenceWithSize(size, dims)
// 	}
// }

// func (f *DefaultCoordinateSequenceFactory) CreateWithCoordSeq(coordSeq CoordinateSequence) CoordinateSequence {
// 	cs := f.CreateWithSize(coordSeq.Size(), coordSeq.GetDimension())
// 	for i := 0; i < cs.Size(); i++ {
// 		cs.SetAt(coordSeq.GetAt(i), i)
// 	}
// 	return cs
// }
