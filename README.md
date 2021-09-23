# ORCA
```
O'Meara
Recursive
Canonicalization
Algorithm
```

## Abstract

"ORCA" is algorithm that canonicalizes (uniquely encodes) any graph having up to 2<sup>31</sup> client-assigned vertex colors and edge colors.  This is critical in chemistry and theoretical physics where input graphs (e.g. molecules and particles) must be rewritten in a consistent (deterministic) way such that they can be compared to other graphs for equivalence.  The ability to compare one encoding with others also means that encodings can be lexicographically (predictably) stored in a catalog.

## Background

"H<sub>2</sub>O", "HOH", "OH<sub>2</sub>", and "(2(H-))O", all are encodings of a water molecule, but a cataloging system that stores data indexed by a particualar molecule must commit to a single canonicalization schema so that molecules can be predictably stored and retrieved, regardless of which label-permutation one happens to have in-hand.

[Graph Isomorphism](https://en.wikipedia.org/wiki/Graph_isomorphism) is a deeply researched area as it also closely related to data mining, graph-based cryptography, and information and network meta-structure analysis.  Graphs with even just hundreds of nodes containing high orders of internal symmetry can quickly overwhelm many graph analysis algorithms as they lack self meta analysis to detect high internal symmetry conditions, resulting in an exponential expansion of state analysis and consume all available resources (or otherwise unable to run to completion).  

Several highly capable and high performance graph canonicalization algorithms exist, however many do not offer edge color support, meaning that all edges are the same and homogenous.  In chemistry, for example, there are several types of bonds and in theoretical physics there there are several kinds of edge colors in a graph representing a particle.  These algorithms also typically reside in C++ or Python and are often some combination of not easily harnessed, not performant, or have a non-trivial learning curve.

## Running Time

Given: graph G with vertices V and edges E, ORCA runs in pseudo-polynomial time: O(EVG) where G is the number of internal (interior) subgraph symmetries. So for a highly asymmetric graph, G is in the single digits, while for, say [E<sub>8</sub>](https://en.wikipedia.org/wiki/E8_(mathematics)), there are many more interior internal symmetries. Of course, this rhymes with the behavior of Scott[1] as this is a natural reflection of a graph's structure. The algorithm is also a "demand-pull" (i.e. "lazy" evaluation) so it can evaluate the DAG that emerges in ways that (deterministically) avoid the harder sub-problems (versus having to evaluate all sub-problems befor2e a canonicalization can be produced).

## Known Issue: Ambiguous Leaf Order

Consider the two equivalent graphs written as a DAG from vertex 1 (differences in bold).   Essentially, the difference is that the labels for 5 and 6 and swapped.
- 1-2 1-3 2-3 1-4 **3-5** 4-5 **2-6** 4-6 5-6
- 1-2 1-3 2-3 1-4 **2-5** 4-5 **3-6** 4-6 5-6

```
     1
  /  |  \
 2 - 3   4
   \   X |
     5 - 6
 ```

 ORCA cannot currently reconcile these two since the order of 2 & 3 affect how the canonical order of edges for 5 & 6 appear.  This is to say the order of 2 & 3 and 5 & 6 form a bistable state, so something more is needed for ORCA to "know" which is the canonical labeling.  This means if ORCA is given these two graphs, it will currently not recognize them as equivalent.


 ## Forward

 To address the above, a "gravity sort" is proposed where `dagVtx` that are canonically equal are allowed to be pulled towards vertices they are connected to.  In effect, vertices connected together to gravitate towards each other while edges disentangle.  As the system moves (iterates) towards steady state, symmetries "stack" on top of each other, allowing them to be detected and compacted.  _Such an algorithm appears to complete in polynomial time since sub graph traversal is never needed._

### Works Cited

[1] Nicolas Bloyet, Pierre-François Marteau, Emmanuel Frenod, [**Scott: A method for representing graphs as rooted trees for graph canonization**](https://hal.archives-ouvertes.fr/hal-02314658), *International Conference on Complex Networks and Their Applications*, pp. 578-590, 2019.  Website: [theplatypus.github.io/scott/](https://theplatypus.github.io/scott/)

