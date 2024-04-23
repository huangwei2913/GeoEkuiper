package geom

type CoordinateList struct {
	coords []Coordinate
}

func NewCoordinateList(v []Coordinate) *CoordinateList {
	return &CoordinateList{
		coords: v,
	}
}

func (cl *CoordinateList) Size() int {
	return len(cl.coords)
}

func (cl *CoordinateList) Empty() bool {
	return len(cl.coords) == 0
}

func (cl *CoordinateList) Begin() *Coordinate {
	if len(cl.coords) == 0 {
		return nil
	}
	return &cl.coords[0]
}

func (cl *CoordinateList) End() *Coordinate {
	if len(cl.coords) == 0 {
		return nil
	}
	return &cl.coords[len(cl.coords)-1]
}

func (cl *CoordinateList) Insert(pos int, c Coordinate, allowRepeated bool) *Coordinate {
	if !allowRepeated && pos > 0 && cl.coords[pos-1].Equals2D(c) {
		return &cl.coords[pos-1]
	}
	cl.coords = append(cl.coords[:pos], append([]Coordinate{c}, cl.coords[pos:]...)...)
	return &cl.coords[pos]
}

func (cl *CoordinateList) Add(c Coordinate, allowRepeated bool) *Coordinate {
	return cl.Insert(len(cl.coords), c, allowRepeated)
}

func (cl *CoordinateList) Erase(pos int) {
	cl.coords = append(cl.coords[:pos], cl.coords[pos+1:]...)
}

func (cl *CoordinateList) EraseRange(start int, end int) {
	cl.coords = append(cl.coords[:start], cl.coords[end:]...)
}

func (cl *CoordinateList) ToCoordinateArray() []Coordinate {
	ret := make([]Coordinate, len(cl.coords))
	copy(ret, cl.coords)
	return ret
}

func (cl *CoordinateList) CloseRing() {
	if !cl.Empty() && !(*cl.Begin()).Equals2D(*cl.End()) {
		c := *(cl.Begin())
		cl.Add(c, false)
	}
}

func CloseRing(coords []Coordinate) {
	if len(coords) > 0 && !coords[0].Equals2D(coords[len(coords)-1]) {
		c := coords[0]
		coords = append(coords, c)
	}
}
