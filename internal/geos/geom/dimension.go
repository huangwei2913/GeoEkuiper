package geom

type Dimension int64

const (
	False Dimension = iota - 3 // 0
	True
	DONTCARE
	P
	L // 1
	A
)

func (s Dimension) toDimensionSymbol(dimensionValue int) rune {
	if dimensionValue == -3 {
		return 'F'
	}
	if dimensionValue == -2 {
		return 'T'
	}
	if dimensionValue == -1 {
		return '*'
	}

	if dimensionValue == 0 {
		return '0'
	}
	if dimensionValue == 1 {
		return '1'
	}
	if dimensionValue == 2 {
		return '2'
	}

	return '9'
}

func (s Dimension) toDimensionValue(dimensionSymbol rune) Dimension {
	if dimensionSymbol == 'F' || dimensionSymbol == 'f' {
		return False
	}

	if dimensionSymbol == 'T' || dimensionSymbol == 't' {
		return True
	}
	if dimensionSymbol == '*' {
		return DONTCARE
	}

	if dimensionSymbol == '0' {
		return P
	}

	if dimensionSymbol == '1' {
		return L
	}

	if dimensionSymbol == '2' {
		return A
	}
	return False
}
