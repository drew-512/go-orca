package orca

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// If > 0, this is a client VtxColor
// If < 0, this identifies a "local" graph encoding (where equal IDs reflect identical sub-graph encoding from a given vertex)
//type ColorID int64



func dagEdgeCanonicCompare(a, b dagEdge) int {
	if d := int(a.edgeType) - int(b.edgeType); d != 0 {
		return d
	}
	if d := int(a.edgeColor) - int(b.edgeColor); d != 0 {
		return d
	}
	return int(a.toVtxColor) - int(b.toVtxColor)
}

func dagVtxCanonicCompare(a, b *dagVtx) int {
	if d := int(a.depth) - int(b.depth); d != 0 {
	    panic("split depth levels")
		//return d
	}
	// Note that we sort so that vertices w/ more edges occur before vertices with less edges.
	// This allows a left-to-right traversal to enumerate vtx with more edge first, which is helpful when finding a vtx with unique traits.
	if d := len(b.edges) - len(a.edges); d != 0 {
		return d
	}
	if d := int(a.VtxColor) - int(b.VtxColor); d != 0 {
		return d
	}
	for i, ai := range a.edges {
		if d := dagEdgeCanonicCompare(ai, b.edges[i]); d != 0 {
			return d
		}
	}
	return 0
}

func (vtx *dagVtx) canonizeEdgeOrder() {
	sort.Slice(vtx.edges, func(i, j int) bool {
		return dagEdgeCanonicCompare(vtx.edges[i], vtx.edges[j]) < 0
	})
}

type subGraph struct {

	// Corresponds to each successive depth level of this dag for a given root vertex.
	dagFromVtx map[VtxLabel]*dag

	//subCalls []dagInvocation
	edgeSet EdgeSet
	edgeBuf [8]uint64
}

func (subG *subGraph) EdgeSet() EdgeSet {
	return subG.edgeSet
}

func (ctx *encoderCtx) dagForRootVtx(subG *subGraph, rootVtx VtxLabel) *dag {
    target := subG.dagFromVtx[rootVtx]
    if target == nil {
        target = dagPool.Get().(*dag)
        ctx.resetDag(target, rootVtx)
        subG.dagFromVtx[rootVtx] = target
    }
    return target
}

func (ctx *encoderCtx) AcquireSubGraph(srcEdgeSet EdgeSet) SubGraph {
	subG := subGraphPool.New().(*subGraph)
	subG.edgeSet = append(subG.edgeSet[:0], srcEdgeSet...)
	return SubGraph(subG)
}

var subGraphPool = sync.Pool{
	New: func() interface{} {
		subG := &subGraph{
			dagFromVtx: make(map[VtxLabel]*dag),
		}
		subG.edgeSet = subG.edgeBuf[:0]
		return subG
	},
}




type dag struct {
	vtxRoot VtxLabel
		
//	canonicDepth    int  // depth up to which this dag is canonized.
	canonicComplete bool

	edgeBuf    [64]dagEdge
	edgePool   []dagEdge
	vtxIndex   map[VtxLabel]uint32 // maps a (visited) VtxLabel to an index into vtx[]
	vtx        []dagVtx            // fills up and is canonized in blocks (with each depth step)
	//blocks     []uint32            // Starting offset into encoding[] where a given depth *ends*
	depthPos   []uint32            // Index into vtx[] on where a given depth *ends*
	//encoding   []byte

	vtxBuf      [36]dagVtx
	depthIdxBuf [24]uint32
	//encBuf      [256]byte

	// curRow    []VtxLabel
	// nxtRow    []VtxLabel

	// curRow map[VtxLabel]*dagVtx
	// nxtRow map[VtxLabel]*dagVtx

	// []VtxColor

	//vtx []dagVtx

	// Contains the "visited" vtx set as the dag initally traverses downward generating each successive dagRow
	//vtxAtDepth  map[VtxLabel]uint32

	// Should always equal the number of vertices instantiated thus far in depths[]
	//vtxCount int

	// unbound  dagVtxList // New vtx formed by a single parent vtx
	// inbound  dagVtxList // New vtx formed by 2+ parent vtx
	// cobound  dagVtxList // Edges that only connect vtx in this depth level (no new vtx)

	// unbound    edgeTraces // New vtx formed by a single parent vtx
	// inbound    edgeTraces // New vtx formed by 2+ parent vtx
	//cobound    []edgeTrace // Edges that only connect vtx in this depth level

	//vtxBuf     [32]dagVtx
}

// func newDag() *dagDepth {
//     depth := dagPool.Get().(*dag)
//     if parent != nil {
//         depth.depth = parent.depth + 1
//         depth.parent = parent
//         parent.subs = append(parent.subs, depth)
//     }
// 	return depth
// }

// func (depth *dagDepth) Reclaim(reclaimSubs bool) {
// 	depth.unbound = depth.unbound[:0]
// 	depth.cobound = nil
// 	depth.inbound = nil
//     if reclaimSubs {
//     	for _, v := range depth.subs {
//     	    v.Reclaim(true)
//         }
//     }
// 	for k, _ := range depth.subs {
// 	    delete(depth.subs, k)
//     }
// 	dagDepthPool.Put(depth)
// }

var dagPool = sync.Pool{
	New: func() interface{} {
		dag := &dag{}
		//dag.encoding = dag.encBuf[:]
		dag.depthPos = dag.depthIdxBuf[:]
		dag.vtx = dag.vtxBuf[:]
		return dag
	},
}




func (ctx *encoderCtx) encodeCanonicBlock(dag *dag, vtx []dagVtx, out []byte) []byte {
	var buf [3*binary.MaxVarintLen64]byte

	// depth_L := uint32(0)
	// if depth > 0 {
	// 	depth_L = dag.depthPos[depth-1]
	// }
	// depth_R := dag.depthPos[depth]
	
	// Write vtx count
	n := binary.PutUvarint(buf[:], uint64(len(vtx)))
	out = append(out, buf[:n]...)

	// edgeCount := uint64(0)

	// // Count edges
	// for i := depth_L; i < depth_R; i++ {
	// 	for _, edge := range dag.vtx[i].edges {
	// 		if edge.edgeType == dagEdgeIn {
	// 			edgeCount += 2
	// 		} else if edge.edgeType == dagEdgeCo {
	// 			edgeCount += 1
	// 		}
	// 	}
	// }

	// Write each vtx color
	for _, vi := range vtx {
		n := binary.PutVarint(buf[:], int64(vi.VtxColor))
		out = append(out, buf[:n]...)
	}
	
	// Write edge count (divide by 2 prevents double counting)
	// edgeCount >>= 1
	// n = binary.PutUvarint(buf[:], edgeCount)
	// out = append(out, buf[:n]...)
	
	edgesWritten := uint64(0)

	// Write edges
	for _, vi := range vtx {
		fromIdx := uint64(dag.vtxIndex[vi.VtxLabel])
		fromLen := binary.PutUvarint(buf[:], fromIdx+1) // +1 for one-based indexing

		for _, edge := range vi.edges {
			n = fromLen + binary.PutVarint(buf[fromLen:], int64(edge.edgeColor))

			if edge.edgeType == dagEdgeIn || edge.edgeType == dagEdgeCo {
				toIdx := uint64(dag.vtxIndex[edge.toVtx])

				// Skip edges that go ahead or else we'll get a duplicate for each cobound edge
				if toIdx < fromIdx {
					n += binary.PutUvarint(buf[n:], toIdx+1) // +1 for one-based indexing
					out = append(out, buf[:n]...)
					edgesWritten++
				}
			}
		}
	}

	// if edgeCount != edgesWritten {
	// 	panic("edge count mismatch")
	// }

	return out
}



func (ctx *encoderCtx) resetDag(dag *dag, rootVtx VtxLabel) {
	dag.vtxRoot = rootVtx
	//dag.encoding = dag.encoding[:0]
	dag.depthPos = dag.depthPos[:0]
	dag.canonicComplete = false

	Nv := ctx.NumVerts()
	if dag.vtxIndex == nil {
		dag.vtxIndex = make(map[VtxLabel]uint32, Nv)
	} else {
		for k := range dag.vtxIndex {
			delete(dag.vtxIndex, k)
		}
	}

	dag.vtx = append(dag.vtx[:0], dagVtx{
		VtxLabel: rootVtx,
		VtxColor: ctx.vtxForLabel(rootVtx).VtxColor,
		depth:    0,
	})
	dag.vtxIndex[rootVtx] = 0

}

// // canonicDepth returns the depth up to which this dag is canonized.
// // e.g. 0 denotes that the root depth is complete (which is always the case)
// func (dag *dag) canonicDepth() int {
// 	return len(dag.depthPos) - 1
// }



// func (ctx *encoderCtx) ExportCanonicEncoding(subGraph *subGraph, rootVtx VtxLabel, out []byte) []byte {
//     dag := ctx.dagForRootVtx(subGraph, rootVtx)
    
// 	for !dag.canonicComplete {
// 		ctx.canonizeNextDepth(subGraph, dag)
// 	}
	
// 	// TODO: start at the bottom depths of the dag and trace upwards etc (encoding compression, etc)
// 	out = ctx.encodeCanonicBlock(dag, dag.vtx, out[:0])
// 	return out
// }


func (ctx *encoderCtx) ExportCanonic(subGraph *subGraph, rootVtx VtxLabel, Gout GraphOut) {
    dag := ctx.dagForRootVtx(subGraph, rootVtx)
    
	for !dag.canonicComplete {
		ctx.canonizeNextDepth(subGraph, dag)
	}

    for i, vi := range dag.vtx {
        canonicFrom := VtxLabel(i+1) 

        Gout.Vtx <- Vtx{
            Label: canonicFrom,
            Color: vi.VtxColor,
        }
 
		for _, edge := range vi.edges {
			if edge.edgeType == dagEdgeIn || edge.edgeType == dagEdgeCo {

                // Add one to vtx index for one-based indexing (i.e. VtxLabel convention)
				canonicTo := VtxLabel(dag.vtxIndex[edge.toVtx]+1)

				// Skip edges that go ahead or else we'll get a duplicate for each cobound edge.
				if canonicTo < canonicFrom {
                    Gout.Edges <- Edge{
                        Va: canonicTo,
                        Vb: canonicFrom,
                        Color: edge.edgeColor,
                    }
				}
			}
        }
    }
    Gout.Break()
}



// depth == 0 corresponds to the root/starting depth
func (ctx *encoderCtx) ExportCanonicBlock(subGraph *subGraph, rootVtx VtxLabel, depth int, out []byte) []byte {    
    dag := ctx.dagForRootVtx(subGraph, rootVtx)
	
    if dag.vtx[0].depth != 0 {
        fmt.Print()
    }

	// Canonize as needed
	for !dag.canonicComplete && depth >= len(dag.depthPos) {
		ctx.canonizeNextDepth(subGraph, dag)
	}
	
	if depth >= len(dag.depthPos) {
		return nil
	}
	
	{
		L := uint32(0)
		if depth > 0 {
			L = dag.depthPos[depth-1]
		}
		R := dag.depthPos[depth]
		out = ctx.encodeCanonicBlock(dag, dag.vtx[L:R], out[:0])
	}
	
	return out
}



// Pre: all previous depths have been constructed and canonized
func (ctx *encoderCtx) canonizeNextDepth(subGraph *subGraph, dag *dag) {
	curDepth := int32(len(dag.depthPos))
	
	// curDepth_L corresponds to the start of where we left off 
	curDepth_L := uint32(0)
	if curDepth > 0 {
		curDepth_L = dag.depthPos[curDepth-1]
	}
	// As we canonize each successive depth, vtx are added dag.vtx[], so the "end" off the current depth corresponds to the end of dag.vtx[]
	curDepth_R := uint32(len(dag.vtx))

	for v_from := curDepth_L; v_from < curDepth_R; v_from++ {
		from := dag.vtx[v_from]

        v := ctx.vtxForLabel(from.VtxLabel)
		for _, edge := range v.edges {

			// If the edge is not present in the edgesRemoved set, then emit the edge
			if ctx.IsEdgePresent(subGraph.edgeSet, edge.FormCanonicalEdge(from.VtxLabel)) {
			
				// Test for edges that came from the previous depth.
				// If an edge out of this vtx connects to another vtx in this row, this is a "cobound" edge.
                // TODO: handle self-connected edges.
				edgeType := dagEdgeIn
				v_to, witnessed := dag.vtxIndex[edge.toVtx]
				if witnessed {
					// We only care about cobound edges and edges "out" (downward) to the next depth.
					// If the end connects back to the previous depth, then this edge has already been traversed and should be ignored.
					// This is the essential "forward progress" in dynamic programming we need to assure a deterministic (finite) outcome.
					if v_to < curDepth_L {
						continue
					}
					if v_to < curDepth_R {
						edgeType = dagEdgeCo
					}
				} else {
					v_to = uint32(len(dag.vtx))
					dag.vtx = append(dag.vtx, dagVtx{
						VtxLabel: edge.toVtx,
						VtxColor: edge.toVtxColor,
						depth:    curDepth + 1, // not needed; can be removed
					})
					dag.vtxIndex[edge.toVtx] = v_to
				}

				dag.vtx[v_to].edges = append(dag.vtx[v_to].edges, dagEdge{
					edgeType:   edgeType,
					edgeColor:  edge.edgeColor,
					toVtx:      from.VtxLabel,
					toVtxColor: from.VtxColor,
				})

                // Add a dagEdgeOut to the "from" vtx
				if edgeType == dagEdgeIn {
				    edge.edgeType = dagEdgeOut
					dag.vtx[v_from].edges = append(dag.vtx[v_from].edges, edge)
				}
			}
		}
	}

	ctx.canonizeVtxOrder(subGraph, dag, curDepth_L, curDepth_R)

    // Finally, with this depth now in canonic order, we reorder each edge by (canonic) label
    // See README notes for why this approach is inadequate and flowed.
    for vi := curDepth_L; vi < curDepth_R; vi++ {
        edges := dag.vtx[vi].edges
        sort.Slice(edges, func (i, j int) bool {
            if d := dagEdgeCanonicCompare(edges[i], edges[j]); d != 0 {
                return d < 0
            }
            return dag.vtxIndex[edges[i].toVtx] < dag.vtxIndex[edges[j].toVtx]
        })
    }

	// Set the start of the next row to the end of the canonic row (this is where new dagVtx were added)
	dag.depthPos = append(dag.depthPos, curDepth_R)
	if curDepth_R == uint32(len(dag.vtx)) {
		dag.canonicComplete = true
	}
}

type vtxToRank struct {
	subGraph *subGraph
	vtx      dagVtx
	rank     int // -1 if not yet known
	block    []byte
}



func canonicSort(vtx []dagVtx) {
	Nv := len(vtx)

	// Canonic sort each vertex's edges
	for i := 0; i < Nv; i++ {
		vtx[i].canonizeEdgeOrder()
	}
	// Canonic sort each vertex (based on local traits not requiring recursion, e.g. degree)
	sort.Slice(vtx, func(i, j int) bool {
		return dagVtxCanonicCompare(&vtx[i], &vtx[j]) < 0
	})
}



func (ctx *encoderCtx) canonizeVtxOrder(subGraph *subGraph, dag *dag, curDepth_L, curDepth_R uint32) {

	// Bail if there's no work to do
	if curDepth_R - curDepth_L <= 1 {
		return
	}
	
	{
		vtx := dag.vtx[curDepth_L:curDepth_R]
		canonicSort(vtx)
		
		var rankBuf [8]vtxToRank
		toRank := rankBuf[:0]
	
		rankAt := -1
	
		Nv := len(vtx)
		for i := 1; i < Nv; i++ {
	
			// Look for vtx that are equal, meaning that we need to rank recursively to canonize their order.
			// This means we recurse on 2+ vtx that don't have enough differentiating traits.
			if dagVtxCanonicCompare(&vtx[i-1], &vtx[i]) == 0 {
				if rankAt < 0 {
					rankAt = i - 1
				}
				
			// When we encounter unequal values, rank the current run (if present)
			} else if rankAt >= 0 {
				numToRank := i - rankAt
				if cap(toRank) < numToRank {
					toRank = make([]vtxToRank, numToRank)
				} else {
					toRank = toRank[:numToRank]
				}
	
				for j := rankAt; j < i; j++ {
					toRank[j-rankAt] = vtxToRank{
						subGraph: ctx.fetchSubGraphForVtx(subGraph, &vtx[j]),
						vtx:      vtx[j],
						rank:     -1, // can be removed 
						block:    nil,
					}
				}
	
				// using ranking to output final canonic vtx order
				{
					ctx.rankVtx(toRank)
					
					// Copy the sorted vtx back (in ranked sorted order)
					for j := rankAt; j < i; j++ {
						vtx[j] = toRank[j-rankAt].vtx
					}
				}
	
				// Resume looking for vtx runs to rank
				rankAt = -1
			}
		}
	}
	
	// Now that the current row depth has been reordered, update the vtx lookup map
	for i := curDepth_L; i < curDepth_R; i++ {
		dag.vtxIndex[dag.vtx[i].VtxLabel] = i
	}
}

func (ctx *encoderCtx) rankVtx(vtxToRank []vtxToRank) {
	{
		L := 0
		R := len(vtxToRank)-1
		
		// TODO: make better?
		var encScrap [256]byte
		encBuf := encScrap[:0]

		for rankDepth := 0; L < R; rankDepth++ {
		
			// "Zoom in" into what still needs to be ranked (i.e. vtx that are still "equal" and not yet have a terminated dag
			toRank := vtxToRank[L:R+1]

			// Recurse and pull out the encoding for each vtx to rank at the current rank depth
			{
				encBuf = encBuf[:0]
				for i, vi := range toRank {
					block := ctx.ExportCanonicBlock(vi.subGraph, vi.vtx.VtxLabel, rankDepth, encBuf)
					toRank[i].block = block
					encBuf = block[len(block):]
				}
			}

			// Now that we have all the encodings in hand, rank them and continue as needed.
			// This could probably be cleverly merged with the below to reduce the number of compares
			sort.Slice(toRank, func(i, j int) bool {
				return bytes.Compare(toRank[i].block, toRank[j].block) < 0
			})
			// Check the beginning and end for vtx that are now rankable.  Take vtx that have been sorted out of the running.
			// An encoding that has length 0 signal the encoding is complete (and first in LSM)
			for L < R {
				if len(vtxToRank[L].block) > 0 {
					diff := bytes.Compare(vtxToRank[L].block, vtxToRank[L+1].block)
					if diff == 0 {
						break
					}
				}
				vtxToRank[L].rank = L
				L++
			}

// vtx that are still "equal" must fallback to be ranked by the labels of the parent depth 

			for L+1 < R {
				{
					diff := bytes.Compare(vtxToRank[R-1].block, vtxToRank[R].block)
					if diff == 0 {
						break
					}
				}
				vtxToRank[R].rank = R
				R--
			}
		}
	}
}

func (ctx *encoderCtx) fetchSubGraphForVtx(forSubGraph *subGraph, dagVtx *dagVtx) *subGraph {
	var edgesBuf [16]Edge
	removeEdges := edgesBuf[:0]

	for _, edge := range dagVtx.edges {
	    if edge.edgeType == dagEdgeIn {
    		removeEdges = append(removeEdges, Edge{
    			Va:    dagVtx.VtxLabel,
    			Vb:    edge.toVtx,
    			Color: edge.edgeColor,
    		})
        }
	}

	subG, err := ctx.FetchSubGraph(forSubGraph.edgeSet, removeEdges)
	if err != nil {
		panic(err)
	}

	return subG.(*subGraph)
}




var (
	ErrStackIterationLimit = errors.New("iteration limit reached (infinity assumed)")
	ErrEdgeNotFound        = errors.New("failed to find edge to remove")
)



func (ctx *encoderCtx) Reclaim() {
	// TODO
}
