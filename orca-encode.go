package orca

import (
	"bytes"
	"sort"

	"github.com/emirpasic/gods/trees/redblacktree"
)

type CanonizeOpts struct {

}

// func (ctx *encoderCtx) Canonize(vtx chan Vtx, edges <-chan Edge) (<-chan VtxColor, <-chan Edge) {

// }

        


type encoderCtx struct {
	graph

	Opts           CanonizerOpts
	encodingLookup redblacktree.Tree // maps []byte (a subgraph encoding) => encodingID
}



func newEncoder(opts CanonizerOpts) *encoderCtx {
    ctx := &encoderCtx{
        Opts: opts,
    }
    ctx.init(SubGraphPool(ctx))
    return ctx
}



// EncodingID names an encoding in a local lookup, referring to 1 or more vertices and 0 or more edges that connect those vertices.
//
// By convention, positive EncodingIDs map to VtxColor and negative EncodingIDs map to a encoder/decoder dict lookup ID. 
type EncodingID int

// NilEncoding denotes an unassigned or otherwise absent EncodingID.
const NilEncoding = EncodingID(0)


func (ctx *encoderCtx) lookupEncoding(Genc GraphEncoding, autoCreate bool) EncodingID {
    val, found := ctx.encodingLookup.Get(Genc)
    var encID EncodingID
    if found {
        encID = val.(EncodingID)
    } else if autoCreate {
        encID = EncodingID(ctx.encodingLookup.Size()+1)
        ctx.encodingLookup.Put(Genc, encID)
    }
    return encID
}


func (ctx *encoderCtx) resetCtx() {
    
    // Reset the encoding dict lookup 
    ctx.encodingLookup = redblacktree.Tree{
        Comparator: func(a, b interface{}) int {
            a0 := a.(GraphEncoding)
            b0 := b.(GraphEncoding)
            return bytes.Compare(a0, b0)
        },
    }
    
}

type vtxRange struct {
    L, Len int
}




// BuildCanonicEncoding appends the canonical encoding to given buffer.
//func (ctx *encoderCtx) BuildCanonicEncoding(in []byte) (out []byte, err error) {
func (ctx *encoderCtx) Canonize(Gout GraphOut) {
        
    if ctx.Error() != nil {
        return
    }

    ctx.resetCtx()

    subG := ctx.SelfSubGraph().(*subGraph)
    
    // First, do a surface canonic sort and see we can we canonically identify.
    // Vtx are sorted such that higher degree vtx appear
    Nv := ctx.NumVerts()
    vtx := ctx.vtx
    
    canonicSort(ctx.vtx)
    for i := range vtx {
        ctx.vtxIndex[vtx[i].VtxLabel] = uint32(i)
    }
    
    rankSpan := vtxRange{0, Nv}
    runLen := 1
    
    // Find the smallest run of vertices that are "equal"
    for i := 1; i <= Nv; i++ {
    
        // Make the end induce a step change
        diff := -1
        if i < Nv {
            diff = dagVtxCanonicCompare(&vtx[i-1], &vtx[i])
            if diff > 0 {
                panic("bad sort")
            }
        }
        if diff == 0 {
            runLen++
        } else {
            if runLen < rankSpan.Len {
                rankSpan.L = i - runLen
                rankSpan.Len = runLen
            }
            // We can't get any better than a run length of 1, plus we want to choose a vtx w/ the largest number of edges possible
            if runLen == 1 {
                break
            }
            runLen = 1
        }
    }
    
    toRank := make([]vtxToRank, rankSpan.Len)
    for i := 0; i < rankSpan.Len; i++ {
        toRank[i] = vtxToRank{
            vtx: vtx[rankSpan.L+i],
        }
    }
        
    canonicRoot := ctx.findCanonicRoot(toRank)
    
    ctx.ExportCanonic(subG, canonicRoot, Gout)

}


func (ctx *encoderCtx) findCanonicRoot(toRank []vtxToRank) VtxLabel {

    subG := ctx.SelfSubGraph().(*subGraph)
    L := 0
    R := len(toRank)-1

	// TODO: make better?
	var encScrap [256]byte
	encBuf := encScrap[:0]
		
    for rankDepth := 0; L < R; rankDepth++ {

        {
            encBuf = encBuf[:0]
            for i := L; i <= R; i++ {
                block := ctx.ExportCanonicBlock(subG, toRank[i].vtx.VtxLabel, rankDepth, encBuf)
                toRank[i].block = block
                encBuf = block[len(block):]
            }
        }
        
        // LSM sort all the blocks we just got from each vtx (for the given depth)
        sort.Slice(toRank[L:R+1], func(i, j int) bool {
            return bytes.Compare(toRank[i].block, toRank[j].block) < 0
        })
        
        // (1) If one (or more) of the encodings terminate (i.e. complete), then we have our winner.
        // If 2+ terminate at the same step, they are canonic (i.e. "label synonyms"), so they are one and the same, brah.
        if len(toRank[L].block) == 0 {
            return toRank[L].vtx.VtxLabel
        }
        
        // (2) Otherwise, if one encoding differentiates itself, then we have our winner (and we eliminate the others)
        // This means we eliminate the most encodings possible (by eliminating all but the smallest still-equal run). 
        // By favoring (equal length) runs that occur first, we are selecting vtx have a the highest degree  possible
        runLen := 1
		nextRank := vtxRange{L, R-L+1}
        for i := L; i <= R; i++ {
            diff := -1
            if i < R {
                diff = bytes.Compare(toRank[i].block, toRank[i+1].block)
            }
            if diff == 0 {
                runLen++
            } else {
                if runLen < nextRank.Len {
                    nextRank.L = i + 1 - runLen
                    nextRank.Len = runLen
                }
                // We can't get any better than a run length of 1, plus we want to choose a vtx w/ the largest number of edges possible
                if runLen == 1 {
                    break
                }
                runLen = 1
            }
        }
        
        L = nextRank.L
        R = nextRank.L + nextRank.Len - 1
    }
    
    return toRank[L].vtx.VtxLabel

}

// func (ctx *encoderCtx) BuildGraphFromEncoding(Gsrc GraphEncoding, Xdst IGraphBuilder) error {
//     return nil
// }

// // Running time: O(X1.NumVerts()^4) + O(X2.NumVerts()^4)
// func (ctx *encoderCtx) IsEquivalent(G1, G2 IGraphExporter) (bool, error) {
//     var buf [2048]byte
    
//     // TODO: put in go routines for parallelization
//     _, G1enc, err := ctx.BuildCanonicEncoding(G1, buf[:0])
//     if err != nil {
//         return false, errors.Wrap(err, "failed to encode graph X1")
//     }
//     _, G2enc, err := ctx.BuildCanonicEncoding(G2, G1enc[len(G1enc):])
//     if err != nil {
//         return false, errors.Wrap(err, "failed to encode graph X2")
//     }
    
//     return bytes.Equal(G1enc, G2enc), nil
// }






// func (ctx *encoderCtx) boostrap() *subGraph {
    
//     subG := ctx.SelfSubGraph().(*subGraph)
    
//     Nv := VtxLabel(ctx.NumVerts())
//     if subG.dagFromVtx == nil {
//         subG.dagFromVtx = make(map[VtxLabel]*dag, Nv)
//     }

//     // if cap(ctx.dagFromRoot) < Nv {
//     // } else {
//     //     ctx.dagFromRoot = ctx.dagFromRoot[:Nv]
//     // }f

//     // Construct each root dag (rooted at each vtx)
//     for vi := VtxLabel(1); vi <= Nv; vi++ {
//         dag := dagPool.Get().(*dag)
//         ctx.resetDag(dag, vi)
//         subG.dagFromVtx[vi] = dag
//     }
    
//     return subG
// }


// func (ctx *encoderCtx) encode() {
//     ctx.boostrap()

//     switch ctx.encodeMode {
//     case FindSmallest:
//     case EncodeFastest:
//     }
// }
