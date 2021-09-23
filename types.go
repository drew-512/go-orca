package orca

import (
	"fmt"
	"strings"
)


    

type CanonizerOpts struct {
    SubGraphLimit int64
    SoftInfinity  bool
}


var DefaultCanonizerOpts = CanonizerOpts{
    SubGraphLimit: 3*1000*1000*1000,
}


func NewCanonizer(opts CanonizerOpts) IGraphCanonizer {
    return newEncoder(opts)
}

// func NewEncoder(opts CanonizerOpts) IGraphEncoder {
//     return newEncoder(opts)
// }

// func NewDecoder() IGraphDecoder {
//     ctx := &decoderCtx{}
//     return ctx
// }


// VtxLabel identifies a particular vertex when using IGraphBuilder.
//
// If Graph G has Nv vertices, then valid VtxLabel values are [1..Nv] inclusive
// A VtxLabel value of 0 is considered nil/invalid.
type VtxLabel uint32

// VtxColor is a client-chosen value that expressed an immutable vertex flavor/class.
// A VtxColor assignment is client-defined and must be >= 0 (negative values are reserved internally).
type VtxColor int64

// // NilVtxColor denotes an unassigned/nil vertex color
// const NilVtxColor = VtxColor(0)

type EdgeColor int64


type GraphEncoding []byte




type Vtx struct {
    Color VtxColor
    Label VtxLabel
}

// Edge specifies an edge between to vertices.  
type Edge struct {
    Va    VtxLabel
    Vb    VtxLabel
    Color EdgeColor
}

// CanonicalEdge assumes/requires that Va < Vb
type CanonicalEdge Edge

func (e Edge) Less(o Edge) bool {
    Ei := (uint64(e.Va) << 32) | uint64(e.Vb)
    Ej := (uint64(o.Va) << 32) | uint64(o.Vb)
    if Ei != Ej {
        return Ei < Ej
    }
    return e.Color < o.Color   
}

func (e Edge) FormCanonicalEdge() CanonicalEdge {
    if e.Va < e.Vb {
        return CanonicalEdge(e)
    } else {
        return CanonicalEdge {
            Va:    e.Vb,
            Vb:    e.Va,
            Color: e.Color,
        } 
    }
}

// type EdgeList []Edge
// func (E EdgeList) Len() int           { return len(E) }
// func (E EdgeList) Less(i, j int) bool {
//     Ei := (uint64(E[i].Va) << 32) | uint64(E[i].Vb)
//     Ej := (uint64(E[j].Va) << 32) | uint64(E[j].Vb)
//     if Ei != Ej {
//         return Ei < Ej
//     }
//     return E[i].Color < E[j].Color     
// }
// func (E EdgeList) Swap(i, j int)      { E[i], E[j] = E[j], E[i] }



type VtxColorDef struct {
    NameAscii string
    NameUTF8  string
    Aliases   []string
    Desc      string
    VtxColor  int64 // Always > 0 and unique amongst other VtxColorDefs.
}
    
// type IGraphBuilder interface {
//     NumVerts() int
//     NumEdges() int
    
//     // Adds the given entries 
//     //AddVtxColorDefs(defs []VtxColorDef) ([]VtxColor, error)
    
//     // BeginGraph starts a new graph to be built, completely resetting this IGraphBuilder's state.
//     // If the number of vertices and/or edges is easily known, 0 can be passed.
//     BeginGraph(numVtxHint, numEdgesHint int)
    
//     EndGraph()

//     AddVtx(newVtx []Vtx)
//     AddEdges(newEdges []Edge)
    
//     //ExportGraph(Gdst IGraphBuilder)
    
//     ExportGraph() (chan VtxColor, chan Edge)
    
//     Error() error 
    
// }

func NewGraphIO() (GraphIn, GraphOut) {
    vtx := make(chan Vtx)
    edg := make(chan Edge)

    in := GraphIn{
        Vtx:   vtx,
        Edges: edg,
    }
    out := GraphOut{
        Vtx:   vtx,
        Edges: edg,
    }
    return in, out
}

type GraphIn struct {
    Vtx   <-chan Vtx
    Edges <-chan Edge
}

func (Gin GraphIn) Consume(handler func (v Vtx, e Edge)) {
	v := Vtx{ Label: 1 }
	e := Edge{ Va: 1 }

    for v.Label != 0 || e.Va != 0 {
        select {
        case v = <-Gin.Vtx:
            if v.Label != 0 {
                handler(v, Edge{})
            }
        case e = <-Gin.Edges:
            if e.Va != 0 {
                handler(Vtx{}, e)
            }
        }
    }
}




func (Gin GraphIn) String() string {
    buf := &strings.Builder{}

	Gin.Consume(func (v Vtx, e Edge) {
        if v.Label != 0 {
            fmt.Fprintf(buf, "v%d: %d  ", v.Label, v.Color)
        } else {
            fmt.Fprintf(buf, "%d-(%d)-%d, ", e.Va, e.Color, e.Vb)
        }
    })
    
    return buf.String()
}



type GraphOut struct {
    Vtx   chan<- Vtx
    Edges chan<- Edge
}

func (Gout *GraphOut) Break() {
    Gout.Edges <- Edge{}
    Gout.Vtx <- Vtx{}
}

type IGraphCanonizer interface {

    BuildGraph(Gin GraphIn) error
    
    Canonize(Gout GraphOut)

}



// // IGraphEncoder performs canonical encoding of any general graph.
// // 
// // This interface should be used as a context in that if you want parallelization, 
// //    then make an IGraphEncoder instance via NewEncoder*() for each goroutine context.
// type IGraphEncoder interface {
//     IGraphBuilder
    
//     // BuildCanonicEncoding appends a GraphEncoding to io[] such as to retain the *structure* of the graph but *not* the labeling.
//     // This means that any graph buildable via IGraphBuilder can be canonically encoded and therefore used to compare with other graphs.
//     //BuildCanonicEncoding(io []byte) (out []byte, err error)

//     Canonize(vtx chan Vtx, edges chan Edge)
    


// }



// type IGraphDecoder interface {

//     InflateEncoding(Genc GraphEncoding, Gdst IGraphBuilder) error

// }




