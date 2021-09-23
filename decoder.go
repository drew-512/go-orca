package orca

import (
	"bytes"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

/* Decoder commands:

// BeginGraphDef  <subGraphID>
// EndGraphDef
// AddSubGraphID  <subGraphID>
// AddEdges       <CanonicalEdgeList>
*/

// GeohashBase32Alphabet is the alphabet used for Base32Encoding
const GeohashBase32Alphabet = "0123456789bcdefghjkmnpqrstuvwxyz"

// Base32Encoding is used to encode/decode binary buffer to/from base 32
var Base32Encoding = base32.NewEncoding(GeohashBase32Alphabet).WithPadding(base32.NoPadding)



type decoderCmdID byte
const (
    CmdNil decoderCmdID = iota
    CmdNextGraphDef // Begins a fresh graph defined by the accompanying (or implicit) EncodingID.  This cmd also completes (stores) the currently active def.
    CmdInflate      // [1..Nv]GraphDefID, [1..Ne]Edge    
)

type decoderCmd struct {
    cmdID   decoderCmdID
    count   int64
    graphID int64 
}


type graphCanvas struct {
    vtx   []Vtx
    edges []Edge
    
    vtxBuf   [16]Vtx
    edgesBuf [32]Edge
}


type graphDefID int64
type decoderCtx struct {
    defs       map[EncodingID]*graphCanvas
    stream     *bytes.Reader
    curGraphID EncodingID
    curGraph   *graphCanvas
    fatalErr   error
}

var (
    ErrBadEncoding = errors.New("bad or corrupt encoding")
)


func (ctx *decoderCtx) inflateVtx() {
    count, stop := ctx.readUint("encoding count"); 
    if stop {
        return
    }
    Nv := int(count)
    
    for i := 0; i < Nv; i++ {
        var colorID int64
        if colorID, stop = ctx.readInt("color ID"); stop {
            return
        }
        
        switch {
            
            case colorID == 0:
                ctx.throwErr("nil color ID")
                return
        
            case colorID > 0:
                ctx.curGraph.vtx = append(ctx.curGraph.vtx, Vtx{
                    Color: VtxColor(colorID),
                    Label: VtxLabel(len(ctx.curGraph.vtx) + 1),
                })
            
            default:
                ctx.inflateEncoding(EncodingID(colorID))
                
        }   
    }

}


func (ctx *decoderCtx) inflateEncoding(encID EncodingID) {
    def, found := ctx.defs[encID]
    if !found {
        ctx.throwErr(fmt.Sprintf("encoding not found (ID=%d)", encID))
        return
    }
    
    labelOffset := VtxLabel(len(ctx.curGraph.vtx))
    Ne := len(ctx.curGraph.edges)
    
    ctx.curGraph.vtx = append(ctx.curGraph.vtx, def.vtx...)
    ctx.curGraph.edges = append(ctx.curGraph.edges, def.edges...)
    
    edgesAdded := ctx.curGraph.edges[Ne:]
    for ei := 0; ei < len(edgesAdded); ei++ {
        edgesAdded[ei].Va += labelOffset
        edgesAdded[ei].Vb += labelOffset
    }
}

func (ctx *decoderCtx) readEdges() {
    Ne, stop := ctx.readUint("edge count")
    if stop {
        return
    }
    
    var (
        Va, Vb uint64
        color int64
    )
    for i := uint64(0); i < Ne; i++ {
        if color, stop = ctx.readInt("EdgeColor"); stop {
            return
        }
        if Va, stop = ctx.readUint("Edge.Va"); stop {
            return
        }
        if Vb, stop = ctx.readUint("Edge.Vb"); stop {
            return
        }
        ctx.curGraph.edges = append(ctx.curGraph.edges, Edge{
            Va:    VtxLabel(Va),
            Vb:    VtxLabel(Vb),
            Color: EdgeColor(color),
        })
    }
}


func (ctx *decoderCtx) throwErr(msg string) {
    if ctx.fatalErr == nil {
        offset := ctx.stream.Size() - int64(ctx.stream.Len())
        ctx.fatalErr = errors.Wrapf(ErrBadEncoding, "%s at offset %v", msg, offset)
    }
}

func (ctx *decoderCtx) readInt(intDesc string) (val int64, stop bool) {
    if ctx.fatalErr != nil {
        return 0, true
    }
    N, err := binary.ReadVarint(ctx.stream)
    if err != nil {
        offset := ctx.stream.Size() - int64(ctx.stream.Len())
        ctx.fatalErr = errors.Wrapf(ErrBadEncoding, "error reading %s at offset %v", intDesc, offset)
        return 0, true
    }
    return N, false
}

func (ctx *decoderCtx) readUint(intDesc string) (val uint64, stop bool) {
    if ctx.fatalErr != nil {
        return 0, true
    }
    N, err := binary.ReadUvarint(ctx.stream)
    if err != nil {
        offset := ctx.stream.Size() - int64(ctx.stream.Len())
        ctx.fatalErr = errors.Wrapf(ErrBadEncoding, "error reading %s at offset %v", intDesc, offset)
        return 0, true
    }
    return N, false
}

var graphCanvasPool = sync.Pool {
    New: func () interface{} {
        G := &graphCanvas{}
        G.vtx = G.vtxBuf[:0]
        G.edges = G.edgesBuf[:0]
        return G
    },
}

func (ctx *decoderCtx) resetCurGraph() {
    G := ctx.curGraph
    if G == nil {
        G = graphCanvasPool.Get().(*graphCanvas)
        ctx.curGraph = G
    }
    G.vtx = G.vtx[:0]
    G.edges = G.edges[:0]
}



func (ctx *decoderCtx) InflateEncoding(Genc GraphEncoding) error {
    ctx.stream = bytes.NewReader(Genc[0:])
    
    for ctx.fatalErr == nil {
        cmd, stop := ctx.readUint("decoder command")
        if stop {
            break
        }
        switch decoderCmdID(cmd) {
        
        case CmdNextGraphDef:
        
            // Store the current graph under a newly issued encoding ID (if present)
            if len(ctx.curGraph.vtx) > 0 {
                newID := EncodingID(len(ctx.defs)+1)
                ctx.defs[newID] = ctx.curGraph
                ctx.curGraph = nil
            }
            ctx.resetCurGraph()
            
        case CmdInflate:
            ctx.inflateVtx()
            ctx.readEdges()   
        }
    }
    
    ctx.stream = nil
    return nil
}


func printBlock(blk []byte) string {
    in := bytes.NewReader(blk)
    
    out := strings.Builder{}

    Nv, _ := binary.ReadUvarint(in)
    for vi := uint64(1); vi <= Nv; vi++ {
        color, _ := binary.ReadVarint(in)
        fmt.Fprintf(&out, "v%d: %d  ", vi, color)
    }
    
    Ne, _ := binary.ReadUvarint(in)
    for ei := uint64(0); ei < Ne; ei++ {
        from, _ := binary.ReadUvarint(in)
        color, _ := binary.ReadVarint(in)
        to, _ := binary.ReadUvarint(in)
        
        fmt.Fprintf(&out, "%d~%d~%d, ", from, color, to)
    }

    return out.String()
}