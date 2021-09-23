package orca

import (
	"sort"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/pkg/errors"
)

type SubGraphPool interface {

	// AcquireSubGraph acquires a new SubGraph and initializes it with a *copy* of the given edge set.
	AcquireSubGraph(srcEdgeSet EdgeSet) SubGraph
}

type SubGraph interface {
	EdgeSet() EdgeSet
}

type EdgeSet []uint64

// edgeIdx is a zero-based index into graph.edges[]
type edgeIdx uint32

type dagEdgeType int32

const (
	dagEdgeNil dagEdgeType = iota
	dagEdgeIn
	dagEdgeCo
	dagEdgeOut
)

type dagEdge struct {
	edgeType   dagEdgeType
	edgeColor  EdgeColor
	toVtxColor VtxColor
	toVtx      VtxLabel
}

// Fun fact: a dagVtx with one or more co-bound edges (dagEdgeCo) always has 1+ inbound edges (dagEdgeIn)
type dagVtx struct {
	VtxLabel VtxLabel
	VtxColor VtxColor
	edges    []dagEdge
	depth    int32  // not needed
}


func (edge dagEdge) FormCanonicalEdge(fromVtx VtxLabel) CanonicalEdge {
    if fromVtx < edge.toVtx {
        return CanonicalEdge {
            Va:    fromVtx,
            Vb:    edge.toVtx,
            Color: edge.edgeColor,
        }
    } else {
        return CanonicalEdge {
            Va:    edge.toVtx,
            Vb:    fromVtx,
            Color: edge.edgeColor,
        }
    }
}

type graph struct {
	IGraphBuilder,

	fatalErr     error
	colorDefs    map[VtxColor]VtxColorDef
	vtxIndex   map[VtxLabel]uint32       // maps a VtxLabel to an index into vtx[]
	vtx          []dagVtx                  // all the graph's vertices with dagEdgeOut edges
	edgesOut     []dagEdge                 // backing buf for vtx[].edges
	edgeMapCap   int                       // cap(edgeMap)
	edgeMap      map[CanonicalEdge]edgeIdx // index into edges[]
	edges        []CanonicalEdge           // referenced by edgeSet.edgeSet[]
	subGraphs    redblacktree.Tree         // maps EdgeSet => SubGraph in log N time
	subGraphPool SubGraphPool              // SubGraph (re)allocation pool
	edgeSetTmp   EdgeSet
}

func (G *graph) init(subGraphPool SubGraphPool) {
	G.colorDefs = make(map[VtxColor]VtxColorDef, 16)
	G.subGraphs = redblacktree.Tree{
		Comparator: func(a, b interface{}) int {
			a0 := a.(EdgeSet)
			b0 := b.(EdgeSet)
			return edgeSetComparator(a0, b0)
		},
	}
	G.subGraphPool = subGraphPool
}

func (G *graph) vtxForLabel(vi VtxLabel) dagVtx {
	return G.vtx[G.vtxIndex[vi]]
}

func (G *graph) SelfSubGraph() SubGraph {
	allEdges := make(EdgeSet, len(G.edgeSetTmp))

	// for i := range G.edgeSetTmp {
	// 	allEdges[i] = 0xFFFFFFFFFFFFFFFF
	// }

	subG, _ := G.FetchSubGraph(allEdges, nil)
	return subG
}

func (G *graph) FetchSubGraph(from EdgeSet, removeEdges []Edge) (subG SubGraph, err error) {
	copy(G.edgeSetTmp, from)

	{
		edgeBits := G.edgeSetTmp

		// We first construct the edgeSet we want so that we can use it to perform a lookup in our existing catalog G.subGraphs[]
		// For each edge to remove, turn off its corresponding edge bit
		for _, removeEdge := range removeEdges {
			edgeIdx, found := G.edgeMap[removeEdge.FormCanonicalEdge()]
			if !found {
				return nil, ErrEdgeNotFound
			}
			maskIdx := edgeIdx >> 6
			edgeBit := uint64(1) << (edgeIdx & 0x3F)
			if (edgeBits[maskIdx] & edgeBit) != 0 {
				return nil, ErrEdgeNotFound
			}
			edgeBits[maskIdx] ^= edgeBit
		}
	}

	// If we're here, all the requested edges were removed.
	// We now check to see if the sub graph already exists.  If so, use that, otherwise retain our new creation
	if existing, alreadyExists := G.subGraphs.Get(G.edgeSetTmp); alreadyExists {
		subG = existing.(SubGraph)
	} else {
		//fmt.Printf("G.subGraphs: %d\n", G.subGraphs.Size())
		subG = G.subGraphPool.AcquireSubGraph(G.edgeSetTmp)
		G.subGraphs.Put(subG.EdgeSet(), subG)
	}

	return
}

func edgeSetComparator(es1, es2 EdgeSet) int {
	// bytes.Compare()?

	for i, e1 := range es1 {
		e2 := es2[i]
		if e1 != e2 {
			if e1 < e2 {
				return -1
			} else {
				return 1
			}
		}
	}
	return 0
}

func (G *graph) IsEdgePresent(edgeSet EdgeSet, edge CanonicalEdge) bool {
	edgeIdx, found := G.edgeMap[edge]
	if !found {
		panic(errors.Errorf("failed to find edge %v", edge))
		//return false
	}
	maskIdx := edgeIdx >> 6
	edgeBit := uint64(1) << (edgeIdx & 0x3F)

	return (edgeSet[maskIdx] & edgeBit) == 0
}

// func (G *graph) Reclaim() {
// 	graphPool.Put(G)
// 	// TODO: reclaim edgeSets in G.subs
// }

func (G *graph) NumVerts() int {
	return len(G.vtx)
}

func (G *graph) NumEdges() int {
	return len(G.edges)
}

// func (G *graph) Init() {
// 	G.BeginGraph(0, 0)
// 	G.EndGraph()
// }

// func (G *graph) AddVtxColors(defs []VtxColorDef) error {

// 	for _, newDef := range defs {
// 		def, defExists := G.colorDefs[newDef.VtxColor]
// 		if defExists {
// 			return nil, errors.
// 		}
// 	}
// 	colorDefs
// }


func (G *graph) BuildGraph(Gin GraphIn) error {
    G.BeginGraph(32, 32)

	// Ensure below for loop runs until EOS is signaled
	v := Vtx{ Label: 1 }
	e := Edge{ Va: 1 }
    
    for v.Label != 0 || e.Va != 0 {
        select {
        case v = <-Gin.Vtx:
            if v.Label != 0 {
                G.AddVertex(v)
            }
        case e = <-Gin.Edges:
            if e.Va != 0 {
                G.AddEdge(e)
            }
        }
    }

    G.EndGraph()

    return G.Error()
}



func (G *graph) BeginGraph(numVtxHint, numEdgesHint int) {
	G.fatalErr = nil
	
	if G.vtxIndex == nil {
		G.vtxIndex = make(map[VtxLabel]uint32, max(numVtxHint, 16))
	} else {
		for k := range G.vtxIndex {
			delete(G.vtxIndex, k)
		}
	}
	G.vtx = G.vtx[:0]
	G.addVtxHint(numVtxHint)

	numEdgesHint = max(numEdgesHint, numVtxHint + 4)
	if cap(G.edges) < numEdgesHint {
		G.edges = make([]CanonicalEdge, numEdgesHint)
	}
	G.edges = G.edges[:0]

	// This gets populated on EndGraph()
	G.edgesOut = G.edgesOut[:0]

	// TODO: move SubGraphs back into G.subGraphPool
	G.subGraphs.Clear()

	if G.edgeMapCap < numEdgesHint {
		G.edgeMapCap = (numEdgesHint + 0xF) &^ 0xF
		G.edgeMap = make(map[CanonicalEdge]edgeIdx, G.edgeMapCap)
	} else {
		for k := range G.edgeMap {
			delete(G.edgeMap, k)
		}
	}
}

func (G *graph) AddVertex(v Vtx) {
	
    if v.Label < 1 {
        G.ThrowErr(errors.New("failed to add vertex: VtxLabel must be > 0"))
        return
    }
    if _, exists := G.vtxIndex[v.Label]; exists {
        G.ThrowErr(errors.Errorf("failed to add vertex: VtxLabel %d already added", v.Label))
        return
    }
    G.vtxIndex[v.Label] = uint32(len(G.vtx))
    G.vtx = append(G.vtx, dagVtx{
        VtxLabel: v.Label,
        VtxColor: v.Color,
    })
}

func (G *graph) AddVtx(newVtx []Vtx) {
	G.addVtxHint(len(newVtx))
	
	for _, vi := range newVtx {
        G.AddVertex(vi)
	}
}

func (G *graph) AddEdge(newEdge Edge) {
    e := newEdge.FormCanonicalEdge()

    _, found := G.edgeMap[e]
    if found {
        G.ThrowErr(errors.Errorf("failed to add edge: edge between vertex %v and %v with edge color %v already exists", e.Va, e.Vb, e.Color))
        return
    }

    G.edgeMap[e] = edgeIdx(len(G.edges))
    G.edges = append(G.edges, e)

	// Maintain G.edgeMapCap
	curEdgeCount := len(G.edges)
	if curEdgeCount > G.edgeMapCap {
		G.edgeMapCap = curEdgeCount
	}
}


func (G *graph) AddEdges(newEdges []Edge) {
	for _, ei := range newEdges {
        G.AddEdge(ei)
	}
}

func (G *graph) ThrowErr(err error) {
	if G.fatalErr == nil {
		G.fatalErr = err
	}
}

func (G *graph) Error() error {
	return G.fatalErr
}

func (G *graph) EndGraph() {
	if G.Error() != nil {
		return
	}

	// Setup G.edgeSetTmp
	{
		edgeSetSz := (len(G.edges) + 0x3F) >> 6
		if cap(G.edgeSetTmp) < edgeSetSz {
			G.edgeSetTmp = make(EdgeSet, edgeSetSz)
		}
		G.edgeSetTmp = G.edgeSetTmp[:edgeSetSz]
	}

	// Populate vtx.edgesOut[]
	{
		Ne := 2 * len(G.edges)

		// First, populate G.edgesOut[] from the edges we were given
		if cap(G.edgesOut) < Ne {
			G.edgesOut = make([]dagEdge, Ne)
		}
		G.edgesOut = G.edgesOut[:Ne]
		duoEdges := make([]Edge, Ne)
		ei := 0
		for _, edge := range G.edges {
			duoEdges[ei] = Edge(edge)
			edge.Va, edge.Vb = edge.Vb, edge.Va
			ei++
			duoEdges[ei] = Edge(edge)
			ei++
		}

		// Sort all edge halves by VtxLabel, allowing us to group them into sub slices for each vtx
		sort.Slice(duoEdges, func(i, j int) bool {
			ei := duoEdges[i]
			ej := duoEdges[j]
			if ei.Va != ej.Va {
				return ei.Va < ej.Va
			}
			return ei.Color < ej.Color
		})

		ei = 0
		for ei < Ne {			
			ei_start := ei
			curVa :=  duoEdges[ei].Va

			// Set G.edgesOut[] for corresponding edges in duoEdges[]
			for ; ei < Ne && curVa == duoEdges[ei].Va; ei++ {
				ej := duoEdges[ei]
				G.edgesOut[ei] = dagEdge{
					edgeType:   dagEdgeOut,
					edgeColor:  ej.Color,
					toVtx:      ej.Vb,
					toVtxColor: G.vtxForLabel(ej.Vb).VtxColor,
				}
			}
			
			idxVa, found := G.vtxIndex[curVa]
			if !found {
				G.ThrowErr(errors.Errorf("edge(s) reference VtxLabel=%v, but no such vertex is defined", curVa))
				return
			}
			
			// Update the edges out for the current "from" vtx label
			G.vtx[idxVa].edges = G.edgesOut[ei_start:ei]
		}

		// if ei < Ne {
		// 	G.ThrowErr(errors.Errorf("edge(s) to vertex %v not found", vtxEdges[ei].Va))
		// 	return
		// }
	}

}

func (G *graph) addVtxHint(numToAdd int) {
	if numToAdd <= 0 {
		return
	}

	// Expand X.vtx if we don't have enough (and preserve vtxNodes)
	Nv := len(G.vtx)
	sz := cap(G.vtx)
	needed := Nv + numToAdd
	if needed > sz {
		vtxNew := make([]dagVtx, Nv, max(needed, 8))
		copy(vtxNew, G.vtx)
		G.vtx = vtxNew
	}
}
