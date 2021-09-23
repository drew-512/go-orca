package orca

import (
	"fmt"
	"testing"
)

// func exportTetra(G IGraphBuilder) {
//     G.BeginGraph(4, 6)
//     G.AddVtx([]Vtx{
//         {
//             Color: 10,
//             Label: 4,
//         },{
//             Color: 10,
//             Label: 2,
//         },{
//             Color: 10,
//             Label: 1,
//         },{
//             Color: 10,
//             Label: 3,
//         },
//     })
//     G.AddEdges([]Edge{
//         {
//             Va: 4,
//             Vb: 1,
//             Color: 20,
//         },{
//             Va: 4,
//             Vb: 2,
//             Color: 20,
//         },{
//             Va: 4,
//             Vb: 3,
//             Color: 20,
//         },{
//             Va: 2,
//             Vb: 1,
//             Color: 19,
//         },{
//             Va: 1,
//             Vb: 3,
//             Color: 20,
//         },{
//             Va: 3,
//             Vb: 2,
//             Color: 20,
//         },
//     })
//     G.EndGraph()
// }


func genK8(Gout GraphOut) {

    for vi := VtxLabel(1); vi <= 8; vi++ {
        color := VtxColor(3)
        if vi == 1 || vi == 5 {
            color = VtxColor(11)
        }
        if vi == 3 || vi == 7 {
            color = VtxColor(1)
        }

        Gout.Vtx <- Vtx{
            Label: vi,
            Color: color,
        }
    }

    edges := []Edge{
        {1, 2, 20},
        {2, 3, 20},
        {3, 4, 20},
        {4, 5, 20},
        {5, 6, 20},
        {6, 7, 20},
        {7, 8, 20},
        {8, 1, 20},
        
        {2, 8, 20},
        {4, 6, 20},
    }
    for _, ei := range edges {
        Gout.Edges <- ei
    }

    Gout.Break()

}


func genHiggs(Gout GraphOut) {

    for vi := VtxLabel(1); vi <= 8; vi++ {
        Gout.Vtx <- Vtx{
            Label: vi,
        }
    }

    edges := []Edge{
        {1, 2, 20},
        {2, 3, 20},
        {3, 4, 20},
        {4, 1, 20},
        
        {5, 6, 19},
        {6, 7, 20},
        {7, 8, 20},
        {8, 5, 20},
        
        {5, 1, 20},
        {6, 2, 20},
        {7, 3, 20},
        {8, 4, 20},
    }
    for _, ei := range edges {
        Gout.Edges <- ei
    }

    Gout.Break()
}

// func exportNxN(G IGraphBuilder, N int) {
//     G.BeginGraph(0, 0)

//     for i := 0; i < N; i++ {
//         G.AddVtx([]Vtx{
//             {Label: VtxLabel(i+1)},
//         })
//     }
    
//     for i := VtxLabel(1); i <= VtxLabel(N); i++ {
//         for j := VtxLabel(1); j <= VtxLabel(N); j++ {
//             if i != j {
//                 G.AddEdges([]Edge{
//                     {i, j, 20},
//                 })
//             }
//         }
//     }
    
// }


func TestHello(t *testing.T) {

    ctx := NewCanonizer(DefaultCanonizerOpts)
    //exportTetra(encoder)
    
    Gin, Gout := NewGraphIO()
    go genK8(Gout)

    err := ctx.BuildGraph(Gin)
    if err != nil {
        t.Error(err)
    }

    go ctx.Canonize(Gout)

    fmt.Println(Gin.String())
    
    // encoding, err := ctx.BuildCanonicEncoding(nil)
    // if err != nil {
    //     t.Error(err)
    // }
    
    // //decoder := NewDecoder()
    
    // fmt.Println(Base32Encoding.EncodeToString(encoding))
    //decoder.InflateEncoding(encoding, nil)
    //encoding

}



func testPrism(numFace1Verts, numFace2Verts int) {


    
}


// func test