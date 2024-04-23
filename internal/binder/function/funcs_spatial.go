package function

/*
#include <geos_c.h>
#include <stdlib.h>
#include <stdbool.h>
bool geosBoundariesIntersect(const char* wktA, const char* wktB) {
    // Initialize GEOS
    initGEOS(NULL, NULL);

    // Create a WKT reader
    GEOSWKTReader* reader = GEOSWKTReader_create();

    // Read WKT into geometry objects
    GEOSGeometry* geomA = GEOSWKTReader_read(reader, wktA);
    GEOSGeometry* geomB = GEOSWKTReader_read(reader, wktB);

    // Get the boundaries of the two geometries
    GEOSGeometry* boundaryA = GEOSBoundary(geomA);
    GEOSGeometry* boundaryB = GEOSBoundary(geomB);

    // Check if the boundaries intersect
    char result = GEOSIntersects(boundaryA, boundaryB);
    bool intersect = result != 0;

    // Clean up allocated objects
    GEOSWKTReader_destroy(reader);
    GEOSGeom_destroy(geomA);
    GEOSGeom_destroy(geomB);
    GEOSGeom_destroy(boundaryA);
    GEOSGeom_destroy(boundaryB);

    // Clean up the global context
    finishGEOS();

    return intersect;
}

bool geosDisjoint(const char* wktA, const char* wktB) {
    // Initialize GEOS
    initGEOS(NULL, NULL);

    // Create a WKT reader
    GEOSWKTReader* reader = GEOSWKTReader_create();

    // Read WKT into geometry objects
    GEOSGeometry* geomA = GEOSWKTReader_read(reader, wktA);
    GEOSGeometry* geomB = GEOSWKTReader_read(reader, wktB);

    // Check if the geometries are disjoint
    char result = GEOSDisjoint(geomA, geomB);
    bool disjoint = result != 0;

    // Clean up allocated objects
    GEOSWKTReader_destroy(reader);
    GEOSGeom_destroy(geomA);
    GEOSGeom_destroy(geomB);

    // Clean up the global context
    finishGEOS();

    return disjoint;
}

bool geosInteriorsIntersect(const char* wktA, const char* wktB) {
    // Initialize GEOS
    initGEOS(NULL, NULL);

    // Create a WKT reader
    GEOSWKTReader* reader = GEOSWKTReader_create();

    // Read WKT into geometry objects
    GEOSGeometry* geomA = GEOSWKTReader_read(reader, wktA);
    GEOSGeometry* geomB = GEOSWKTReader_read(reader, wktB);

    // Get the interiors of the two geometries
    GEOSGeometry* interiorA = GEOSSymDifference(geomA, GEOSBoundary(geomA));
    GEOSGeometry* interiorB = GEOSSymDifference(geomB, GEOSBoundary(geomB));

    // Check if the interiors intersect
    char result = GEOSIntersects(interiorA, interiorB);
    bool intersect = result != 0;

    // Clean up allocated objects
    GEOSWKTReader_destroy(reader);
    GEOSGeom_destroy(geomA);
    GEOSGeom_destroy(geomB);
    GEOSGeom_destroy(interiorA);
    GEOSGeom_destroy(interiorB);

    // Clean up the global context
    finishGEOS();

    return intersect;
}


bool geosLineIntersectsPolygonInterior(const char* lineWKT, const char* polyWKT) {
    // Initialize GEOS
    initGEOS(NULL, NULL);

    // Create a WKT reader
    GEOSWKTReader* reader = GEOSWKTReader_create();

    // Read WKT into geometry objects
    GEOSGeometry* line = GEOSWKTReader_read(reader, lineWKT);
    GEOSGeometry* poly = GEOSWKTReader_read(reader, polyWKT);

    // Get the interior of the polygon
    GEOSGeometry* interior = GEOSSymDifference(poly, GEOSBoundary(poly));

    // Check if the line intersects the interior of the polygon
    char result = GEOSIntersects(line, interior);
    bool intersect = result != 0;

    // Clean up allocated objects
    GEOSWKTReader_destroy(reader);
    GEOSGeom_destroy(line);
    GEOSGeom_destroy(poly);
    GEOSGeom_destroy(interior);

    // Clean up the global context
    finishGEOS();

    return intersect;
}



#cgo LDFLAGS: -L/usr/local/lib -lgeos_c
#cgo CFLAGS: -I/usr/local/include
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

var mu sync.Mutex

func geosLineIntersectsPolygonInterior(lineWKT, polyWKT string) bool {
	wktA := C.CString(lineWKT)
	wktB := C.CString(polyWKT)
	defer C.free(unsafe.Pointer(wktA))
	defer C.free(unsafe.Pointer(wktB))
	return bool(C.geosLineIntersectsPolygonInterior(wktA, wktB))
}

func interiorDisjoint(geomAWKT, geomBWKT string) bool {
	// Initialize GEOS
	C.initGEOS(nil, nil)

	// Create a WKT reader
	reader := C.GEOSWKTReader_create()

	// Read WKT into geometry objects
	wktA := C.CString(geomAWKT)
	wktB := C.CString(geomBWKT)
	geomA := C.GEOSWKTReader_read(reader, wktA)
	geomB := C.GEOSWKTReader_read(reader, wktB)

	// Get the exterior ring of geometry B
	exteriorRing := C.GEOSGetExteriorRing(geomB)

	// Check if the interior of geometry A intersects the exterior ring of geometry B
	result := C.GEOSIntersects(geomA, exteriorRing)
	disjoint := result == 0

	// Clean up allocated objects
	C.GEOSWKTReader_destroy(reader)
	C.GEOSGeom_destroy(geomA)
	C.GEOSGeom_destroy(geomB)
	C.free(unsafe.Pointer(wktA))
	C.free(unsafe.Pointer(wktB))

	// Clean up the global context
	C.finishGEOS()

	return disjoint
}

func geosBoundariesIntersect(geomAWKT, geomBWKT string) bool {
	wktA := C.CString(geomAWKT)
	wktB := C.CString(geomBWKT)
	defer C.free(unsafe.Pointer(wktA))
	defer C.free(unsafe.Pointer(wktB))
	return bool(C.geosBoundariesIntersect(wktA, wktB))
}

func geosDisjoint(geomAWKT, geomBWKT string) bool {
	wktA := C.CString(geomAWKT)
	wktB := C.CString(geomBWKT)
	defer C.free(unsafe.Pointer(wktA))
	defer C.free(unsafe.Pointer(wktB))
	return bool(C.geosDisjoint(wktA, wktB))
}

func geosInteriorsIntersect(geomAWKT, geomBWKT string) bool {
	wktA := C.CString(geomAWKT)
	wktB := C.CString(geomBWKT)
	defer C.free(unsafe.Pointer(wktA))
	defer C.free(unsafe.Pointer(wktB))
	return bool(C.geosInteriorsIntersect(wktA, wktB))
}

//This one is correct
func registerSpatialFunc() {
	builtins["st_distance"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			var mydistance float64
			zz1, ok1 := args[0].(string)
			if ok1 == false {
				return ok1, false
			}

			zz2, ok2 := args[1].(string)
			if ok2 == false {
				return ok2, false
			}

			C.initGEOS(nil, nil)

			// Create a WKT reader
			reader := C.GEOSWKTReader_create()

			// Read WKT into geometry objects
			wktA := C.CString(zz1)
			wktB := C.CString(zz2)
			geomA := C.GEOSWKTReader_read(reader, wktA)
			geomB := C.GEOSWKTReader_read(reader, wktB)

			// Calculate the distance between the geometries
			var distance C.double
			C.GEOSDistance(geomA, geomB, &distance)
			fmt.Printf("Distance: %f\n", distance)
			mydistance = float64(distance)
			// Clean up allocated objects
			C.GEOSWKTReader_destroy(reader)
			C.GEOSGeom_destroy(geomA)
			C.GEOSGeom_destroy(geomB)
			C.free(unsafe.Pointer(wktA))
			C.free(unsafe.Pointer(wktB))

			// Clean up the global context
			C.finishGEOS()

			return mydistance, true

		},
		val: ValidateTwoStrArg,
	}
	//We assume the big strings,
	builtins["st_intersects"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			zz1, ok1 := args[0].(string)
			if ok1 == false {
				return ok1, false
			}

			zz2, ok2 := args[1].(string)
			if ok2 == false {
				return ok2, false
			}

			C.initGEOS(nil, nil)

			// Create a WKT reader
			reader := C.GEOSWKTReader_create()

			// Read WKT into geometry objects
			wktA := C.CString(zz1)
			wktB := C.CString(zz2)
			geomA := C.GEOSWKTReader_read(reader, wktA)
			geomB := C.GEOSWKTReader_read(reader, wktB)

			// Check if the geometries intersect
			result := C.GEOSIntersects(geomA, geomB)

			// Clean up allocated objects
			C.GEOSWKTReader_destroy(reader)
			C.GEOSGeom_destroy(geomA)
			C.GEOSGeom_destroy(geomB)
			C.free(unsafe.Pointer(wktA))
			C.free(unsafe.Pointer(wktB))
			C.finishGEOS()
			fmt.Printf("Geometries intersect. GGGGGGGGGGGGGGGGGGGGG.........: %v\n", result)
			var b bool
			if result != 0 {
				b = true
			} else {
				b = false
			}
			return b, true

		},
		val: ValidateTwoStrArg,
	}

	builtins["st_within"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			zz1, ok1 := args[0].(string)
			if ok1 == false {
				return ok1, false
			}

			zz2, ok2 := args[1].(string)
			if ok2 == false {
				return ok2, false
			}

			return interiorDisjoint(zz1, zz2), true

		},
		val: ValidateTwoStrArg,
	}

	builtins["st_touches"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			zz1, ok1 := args[0].(string)
			if ok1 == false {
				return ok1, false
			}

			zz2, ok2 := args[1].(string)
			if ok2 == false {
				return ok2, false
			}

			return geosBoundariesIntersect(zz1, zz2), true

		},
		val: ValidateTwoStrArg,
	}

	builtins["st_distjoint"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			zz1, ok1 := args[0].(string)
			if ok1 == false {
				return ok1, false
			}

			zz2, ok2 := args[1].(string)
			if ok2 == false {
				return ok2, false
			}

			return geosDisjoint(zz1, zz2), true

		},
		val: ValidateTwoStrArg,
	}

	builtins["st_contains"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {

			zz1, ok1 := args[0].(string)
			if ok1 == false {
				return ok1, false
			}

			zz2, ok2 := args[1].(string)
			if ok2 == false {
				return ok2, false
			}
			mu.Lock()
			defer mu.Unlock()
			C.initGEOS(nil, nil)
			reader := C.GEOSWKTReader_create()
			wktA := C.CString(zz1)
			wktB := C.CString(zz2)
			geomA := C.GEOSWKTReader_read(reader, wktA)
			geomB := C.GEOSWKTReader_read(reader, wktB)
			result := C.GEOSContains(geomA, geomB)

			C.GEOSWKTReader_destroy(reader)
			C.GEOSGeom_destroy(geomA)
			C.GEOSGeom_destroy(geomB)
			C.free(unsafe.Pointer(wktA))
			C.free(unsafe.Pointer(wktB))
			C.finishGEOS()

			var b bool
			if result != 0 {
				b = true
			} else {
				b = false
			}
			return b, true

			fmt.Println("IUIUII........", zz1, zz2)
			return b, true

		},
		val: ValidateTwoStrArg,
	}

	builtins["st_crosses"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			zz1, ok1 := args[0].(string)
			if ok1 == false {
				return ok1, false
			}

			zz2, ok2 := args[1].(string)
			if ok2 == false {
				return ok2, false
			}

			return geosLineIntersectsPolygonInterior(zz1, zz2), true

		},
		val: ValidateTwoStrArg,
	}
}
