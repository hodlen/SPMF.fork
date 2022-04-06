package ca.pfv.spmf.algorithms.graph_mining.tkg;

import java.util.*;
import java.util.stream.Collectors;

/* This file is copyright (c) 2022 by Shaul Zevin
 *
 * This file is part of the SPMF DATA MINING SOFTWARE
 * (http://www.philippe-fournier-viger.com/spmf).
 *
 * SPMF is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with
 * SPMF. If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * This is an implementation of early termination failure handling, used by the CGSPAN algorithm
 * <br/><br/>
 * The cgspan algorithm is described in : <br/>
 * <br/>
 * <p>
 * cgSpan: Closed Graph-Based Substructure Pattern Mining, by Zevin Shaul, Sheikh Naaz
 * IEEE BigData 2021 7th Special Session on Intelligent Data Mining
 * <p>
 * <p>
 * <br/><br/>
 * <p>
 * The handler analyzes frequent subgraph for causing early termination failure.
 * If the subgraph or it's extension will be used for early termination, some closed graphs may not be discovered.
 * The handler examines 5 known early termination failure cases.
 * If one of the cases is discovered, the subgraph DFS code is added to a trie maintained by the handler.
 * <br/><br/>
 * The handler detects early termination failure by searching the terminating DFS code in the trie.
 * <br/>
 *
 * @author Shaul Zevin
 * @see AlgoCGSPAN
 */
public class EarlyTerminationFailureHandler {

    /**
     * database graphs
     */
    private List<DatabaseGraph> graphDB;
    /**
     * the minimum support represented as a count (number of subgraph occurrences)
     */
    private int minSup;
    /**
     * DFS codes trie
     */
    private Trie trie;

    public EarlyTerminationFailureHandler(List<DatabaseGraph> graphDB, int minSup) {
        this.graphDB = graphDB;
        this.minSup = minSup;
        trie = new Trie();
    }

    /**
     * analyzes 5 early termination failure cases. DFS code is added to trie if one of 5 cases is discovered.
     *
     * @param dfsCode    analyzed DFS code
     * @param projected  analyzed projections of the DFS code into graphs database
     * @param extensions DFS code extension from the rightmost path.
     */
    public void analyze(DFSCode dfsCode, Projected projected, Map<ExtendedEdge, Projected> extensions) {
        List<Integer> rightMostPathEdges = rightMostPathEdges(dfsCode);
        Map<ExtendedEdge, Projected> forwardExtensions = extractForwardExtensions(dfsCode, extensions);

        if (analyzeCase1(dfsCode, projected, rightMostPathEdges, forwardExtensions)) {
            return;
        }

        if (analyzeCase2(dfsCode, projected, rightMostPathEdges, forwardExtensions)) {
            return;
        }

        if (analyzeCase3(dfsCode, projected, rightMostPathEdges, forwardExtensions)) {
            return;
        }

        if (analyzeCase4(dfsCode, projected, rightMostPathEdges)) {
            return;
        }

        if (analyzeCase5(dfsCode, projected)) {
            return;
        }
    }

    /**
     *
     * Checks if by breaking some edges not on the rightmost path
     * either a new frequent forward extension from the rightmost vertex can be created
     * or more projections can be added to a forward extension from the rightmost vertex and this extension is frequent
     * Adds DFS code to trie if check evaluates to true
     * @param dfsCode examined DFS code
     * @param projected examined DFS code projections
     * @param rightMostPathEdges edges on the rightmost path
     * @param forwardExtensions forward extensions
     * @return true if the check evaluates to true, false otherwise
     */
    private boolean analyzeCase1(DFSCode dfsCode, Projected projected, List<Integer> rightMostPathEdges, Map<ExtendedEdge, Projected> forwardExtensions) {
        int length = projected.getProjected().get(0).getLength();

        if (dfsCode.getAt(length - 1).v2 > dfsCode.getAt(length - 1).v1) {
            Map<ElbVlbKey, Set<Integer>> elbVlbGids = new HashMap<ElbVlbKey, Set<Integer>>();
            for (PDFS pdfs : projected.getProjected()) {
                int edgeIndex = rightMostPathEdges.get(0);
                // rightmost vertex
                int fromVertexId = pdfs.getDFSedgeAtToVertex(edgeIndex);
                DatabaseGraph g = graphDB.get(pdfs.getEdgeEnumeration().getGid());
                Vertex fromVertex = g.vMap.get(fromVertexId);
                for (Edge edge : fromVertex.getEdgeList()) {
                    if (pdfs.hasEdge(edge)) {
                        continue;
                    }

                    int toVertexId = (edge.v1 == fromVertexId ? edge.v2 : edge.v1);

                    if (pdfs.hasVertex(toVertexId, rightMostPathEdges)) {
                        continue;
                    }

                    if (pdfs.hasVertex(toVertexId)) {
                        // edge from a rightmost vertex to one of the vertices not on the rightmost path found
                        ElbVlbKey elbVlbKey = new ElbVlbKey(edge.getEdgeLabel(), g.getVLabel(toVertexId));

                        // break the edge(s) which have 'to' vertex of the found edge
                        if (!elbVlbGids.containsKey(elbVlbKey)) {
                            elbVlbGids.put(elbVlbKey, new HashSet<Integer>());
                        }
                        elbVlbGids.get(elbVlbKey).add(g.getId());
                        // check if a new forward edge is frequent
                        if (elbVlbGids.get(elbVlbKey).size() >= minSup) {
                            trie.insert(dfsCode.getEeL());
                            return true;
                        }
                    }
                }
            }

            for (ElbVlbKey elbVlbKey : elbVlbGids.keySet()) {
                for (ExtendedEdge extendedEdge : forwardExtensions.keySet()) {
                    if (extendedEdge.v1 == dfsCode.getRightMost()
                            && extendedEdge.edgeLabel == elbVlbKey.elb && extendedEdge.vLabel2 == elbVlbKey.vlb) {
                        // add projections to the original forward extensions
                        for (PDFS pdfs : forwardExtensions.get(extendedEdge).getProjected()) {
                            elbVlbGids.get(elbVlbKey).add(pdfs.getEdgeEnumeration().getGid());
                        }

                        // check if forward extension is frequent
                        if (elbVlbGids.get(elbVlbKey).size() >= minSup) {
                            trie.insert(dfsCode.getEeL());
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }

    /**
     *
     * Checks if by breaking some edges not on the rightmost path
     * either a new frequent forward extension not from the rightmost vertex can be created
     * or more projections can be added to a forward extension not from the rightmost vertex and this extension is frequent
     * Adds DFS code to trie if check evaluates to true
     * @param dfsCode examined DFS code
     * @param projected examined DFS code projections
     * @param rightMostPathEdges edges on the rightmost path
     * @param forwardExtensions forward extensions
     * @return true if the check evaluates to true, false otherwise
     */
    private boolean analyzeCase2(DFSCode dfsCode, Projected projected, List<Integer> rightMostPathEdges, Map<ExtendedEdge, Projected> forwardExtensions) {
        int length = projected.getProjected().get(0).getLength();

        if (dfsCode.getAt(length - 1).v2 > dfsCode.getAt(length - 1).v1) {
            Map<VertexElbVlbKey, Set<Integer>> vertexElbVlbGids = new HashMap<VertexElbVlbKey, Set<Integer>>();

            // ?? check if it should be enough to consider only the last rightmost path edge
            for (int i = 0; i < rightMostPathEdges.size(); i++) {
                ExtendedEdge extendedEdge = dfsCode.getAt(rightMostPathEdges.get(i));
                int vertexStart = extendedEdge.v1;
                int vertexEnd = extendedEdge.v2;
                // select only rightmost path edges such that not rightmost path edge starts from the edge v1 (vertexStart)
                // ?? check if considering multiple forks together is better
                if (vertexEnd == vertexStart + 1) {
                    continue;
                }

                // check all rightmost path edges before the edge
                for (int j = i + 1; j < rightMostPathEdges.size(); j++) {
                    int vertexFrom = dfsCode.getAt(rightMostPathEdges.get(j)).v1;
                    for (PDFS pdfs : projected.getProjected()) {
                        DatabaseGraph g = graphDB.get(pdfs.getEdgeEnumeration().getGid());
                        Map<Integer, Integer> dfs2g = pdfs.subgraphIsomorphism(dfsCode);
                        Map<Integer, Integer> g2dfs =
                                dfs2g.entrySet()
                                        .stream()
                                        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

                        // collect all vertices between vertexStart + 1 and vertexEnd not inclusive
                        Set<Integer> gNotRmpathVertices = new HashSet<Integer>();
                        for (int v = vertexStart + 1; v < vertexEnd; v++) {
                            gNotRmpathVertices.add(dfs2g.get(v));
                        }

                        for (Edge edge : g.vMap.get(dfs2g.get(vertexFrom)).getEdgeList()) {
                            if (pdfs.hasEdge(edge)) {
                                continue;
                            }

                            int vertexTo = (edge.v1 == dfs2g.get(vertexFrom) ? edge.v2 : edge.v1);

                            if (!gNotRmpathVertices.contains(vertexTo)) {
                                continue;
                            }

                            // edge from a rightmost vertex to one of gNotRmpathVertices found
                            VertexElbVlbKey vertexElbVlbKey = new VertexElbVlbKey(vertexFrom, edge.getEdgeLabel(), g.vMap.get(vertexTo).getLabel());

                            // break the edge(s) which have 'to' vertex of the found edge
                            if (!vertexElbVlbGids.containsKey(vertexElbVlbKey)) {
                                vertexElbVlbGids.put(vertexElbVlbKey, new HashSet<Integer>());
                            }
                            vertexElbVlbGids.get(vertexElbVlbKey).add(g.getId());
                            // check if a new forward edge is frequent
                            if (vertexElbVlbGids.get(vertexElbVlbKey).size() >= minSup) {
                                trie.insert(dfsCode.getEeL());
                                return true;
                            }
                        }
                    }
                }
            }

            for (VertexElbVlbKey vertexElbVlbKey : vertexElbVlbGids.keySet()) {
                for (ExtendedEdge extendedEdge : forwardExtensions.keySet()) {
                    if (extendedEdge.v1 == vertexElbVlbKey.getVertex()
                            && extendedEdge.edgeLabel == vertexElbVlbKey.elb && extendedEdge.vLabel2 == vertexElbVlbKey.vlb) {
                        // add projections to the original forward extensions
                        for (PDFS pdfs : forwardExtensions.get(extendedEdge).getProjected()) {
                            vertexElbVlbGids.get(vertexElbVlbKey).add(pdfs.getEdgeEnumeration().getGid());
                        }

                        // check if forward extension is frequent
                        if (vertexElbVlbGids.get(vertexElbVlbKey).size() >= minSup) {
                            trie.insert(dfsCode.getEeL());
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }

    /**
     *
     * Checks if by breaking first rightmost path edge
     * either a new frequent forward extension from the rightmost vertex can be created
     * or more projections can be added to a forward extension from the rightmost vertex and this extension is frequent
     * Adds DFS code to trie if check evaluates to true
     * @param dfsCode examined DFS code
     * @param projected examined DFS code projections
     * @param rightMostPathEdges edges on the rightmost path
     * @param forwardExtensions forward extensions
     * @return true if the check evaluates to true, false otherwise
     */
    private boolean analyzeCase3(DFSCode dfsCode, Projected projected, List<Integer> rightMostPathEdges, Map<ExtendedEdge, Projected> forwardExtensions) {
        int length = projected.getProjected().get(0).getLength();

        // second rightmost path edge should have same vertices and edge labels as the first rightmost path edge
        // ?? check if longer rightmost path prefixes which are followed by identically labeled edges should be considered
        if ((dfsCode.getAt(length - 1).v2 > dfsCode.getAt(length - 1).v1)
                && (rightMostPathEdges.size() > 2)
                && (dfsCode.getAt(rightMostPathEdges.get(rightMostPathEdges.size() - 1)).edgeLabel ==
                dfsCode.getAt(rightMostPathEdges.get(rightMostPathEdges.size() - 2)).edgeLabel)
                && (dfsCode.getAt(rightMostPathEdges.get(rightMostPathEdges.size() - 1)).vLabel2 ==
                dfsCode.getAt(rightMostPathEdges.get(rightMostPathEdges.size() - 2)).vLabel2)) {
            Map<ElbVlbKey, Set<Integer>> elbVlbGids = new HashMap<ElbVlbKey, Set<Integer>>();
            for (PDFS pdfs : projected.getProjected()) {
                DatabaseGraph g = graphDB.get(pdfs.getEdgeEnumeration().getGid());
                // first vertex in rightmost path
                int firstRmpathEdgeIndex = rightMostPathEdges.get(rightMostPathEdges.size() - 1);
                int gRmpathFirstVertex = pdfs.getDFSedgeAtFromVertex(firstRmpathEdgeIndex);
                int edgeIndex = rightMostPathEdges.get(0);
                // rightmost vertex
                int fromVertexId = pdfs.getDFSedgeAtToVertex(edgeIndex);
                Vertex fromVertex = g.vMap.get(fromVertexId);
                for (Edge edge : fromVertex.getEdgeList()) {
                    if (pdfs.hasEdge(edge)) {
                        continue;
                    }

                    int vertexTo = (edge.v1 == fromVertexId ? edge.v2 : edge.v1);
                    if (vertexTo != gRmpathFirstVertex) {
                        continue;
                    }

                    // edge from rightmost vertex to the first rightmost path vertex found
                    ElbVlbKey elbVlbKey = new ElbVlbKey(edge.getEdgeLabel(), g.getVLabel(vertexTo));

                    // break the first rightmost path edge
                    if (!elbVlbGids.containsKey(elbVlbKey)) {
                        elbVlbGids.put(elbVlbKey, new HashSet<Integer>());
                    }
                    elbVlbGids.get(elbVlbKey).add(g.getId());
                    // check if a new forward edge is frequent
                    if (elbVlbGids.get(elbVlbKey).size() >= minSup) {
                        trie.insert(dfsCode.getEeL());
                        return true;
                    }
                }
            }

            for (ElbVlbKey elbVlbKey : elbVlbGids.keySet()) {
                for (ExtendedEdge extendedEdge : forwardExtensions.keySet()) {
                    if (extendedEdge.v1 == dfsCode.getRightMost()
                            && extendedEdge.edgeLabel == elbVlbKey.elb && extendedEdge.vLabel2 == elbVlbKey.vlb) {
                        // add projections to the original forward extensions
                        for (PDFS pdfs : forwardExtensions.get(extendedEdge).getProjected()) {
                            elbVlbGids.get(elbVlbKey).add(pdfs.getEdgeEnumeration().getGid());
                        }

                        // check if forward extension is frequent
                        if (elbVlbGids.get(elbVlbKey).size() >= minSup) {
                            trie.insert(dfsCode.getEeL());
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }

    /**
     *
     * Checks if by breaking forward rightmost path edge
     * more projections can be added to the next forward edge on the rightmost path.
     * The condition is to have backward edge as the last DFS code edge.
     * Another rightmost path is built in an 'opposite' direction starting with a backward edge
     * and breaking forward edge in the original rightmost path.
     * Adds DFS code to trie if check evaluates to true
     * @param dfsCode examined DFS code
     * @param projected examined DFS code projections
     * @param rightMostPathEdges edges on the rightmost path
     * @return true if the check evaluates to true, false otherwise
     */
    private boolean analyzeCase4(DFSCode dfsCode, Projected projected, List<Integer> rightMostPathEdges) {
        int length = projected.getProjected().get(0).getLength();

        if (dfsCode.getAt(length - 1).v2 < dfsCode.getAt(length - 1).v1) {
            Integer rmpathLoop = null;

            // backward edge
            ExtendedEdge lastDfsEdge = dfsCode.getAt(dfsCode.size() - 1);
            for (int i = 0; i < rightMostPathEdges.size(); i++) {
                ExtendedEdge rightMostPathEdge = dfsCode.getAt(rightMostPathEdges.get(i));
                if (rightMostPathEdge.v1 != lastDfsEdge.v2) {
                    continue;
                }

                // last forward edge to be broken
                rmpathLoop = rightMostPathEdges.get(i);
                break;
            }

            // construct rightmost path in the 'opposite' direction by breaking one forward edge each time
            for (int rmpathEdgeIndex : rightMostPathEdges) {
                // construct only edges before the last forward edge to be broken
                if (rmpathEdgeIndex < rmpathLoop) {
                    break;
                }

                // last constructed rightmost path forward edge label
                int elb = dfsCode.getAt(rmpathEdgeIndex).getEdgeLabel();
                // last constructed rightmost path forward edge 'to' vertex label
                int vlb = dfsCode.getAt(rmpathEdgeIndex).getvLabel1();

                // search for a vacant edge that has 'from' vertex same as last constructed rightmost path forward edge 'from' vertex
                // and same edge label as last constructed rightmost path forward edge label
                // and 'to' vertex label same as last constructed rightmost path forward edge 'to' vertex label
                for (PDFS pdfs : projected.getProjected()) {
                    DatabaseGraph g = graphDB.get(pdfs.getEdgeEnumeration().getGid());
                    int vertexId = pdfs.getDFSedgeAtToVertex(rmpathEdgeIndex);
                    Vertex vertex = g.vMap.get(vertexId);
                    for (Edge edge : vertex.getEdgeList()) {
                        if (edge.getEdgeLabel() != elb) {
                            continue;
                        }

                        int vertexTo = (edge.v1 == vertexId ? edge.v2 : edge.v1);
                        if (g.vMap.get(vertexTo).getLabel() != vlb) {
                            continue;
                        }

                        if (pdfs.hasEdge(edge)) {
                            continue;
                        }

                        if (pdfs.hasVertex(vertexTo)) {
                            continue;
                        }

                        // another projection was found
                        trie.insert(dfsCode.getEeL());
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     *
     * Checks if by breaking forward not necessarily rightmost path edge
     * more projections can be added to the next forward edge.
     * DFS code prefixes having backward edge as the last DFS code edge are built.
     * Another rightmost path is built in an 'opposite' direction starting with a backward edge
     * and breaking forward edge in the DFS code prefix original rightmost path.
     * Adds DFS code to trie if check evaluates to true
     * @param dfsCode examined DFS code
     * @param projected examined DFS code projections
     * @return true if the check evaluates to true, false otherwise
     */
    private boolean analyzeCase5(DFSCode dfsCode, Projected projected) {
        // build dfs code prefixes
        List<DFSCode> dfsCodes = buildDFSCodesPrefixes(dfsCode);

        for (int i = 1; i < dfsCodes.size(); i++) {
            DFSCode testedDfsCode = dfsCodes.get(i);
            // check only codes for which the last DFS code edge is backward
            if (testedDfsCode.getAt(testedDfsCode.size() - 1).v2 < testedDfsCode.getAt(testedDfsCode.size() - 1).v1) {
                List<Integer> testedRightMostPathEdges = rightMostPathEdges(testedDfsCode);
                Integer rmpathLoop = null;
                Integer rmpathLoopIndex = null;
                for (int j = 0; j < testedRightMostPathEdges.size(); j++) {
                    if (testedDfsCode.getAt(testedRightMostPathEdges.get(j)).v1 != testedDfsCode.getAt(testedDfsCode.size() - 1).v2) {
                        continue;
                    } else {
                        // last forward edge to be broken
                        rmpathLoop = testedRightMostPathEdges.get(j);
                        rmpathLoopIndex = j;
                        break;
                    }
                }

                // this condition is necessary for a new projection creation later
                Integer rmpathBeforeLoopIndex = rmpathLoopIndex + 1;
                if (rmpathBeforeLoopIndex < testedRightMostPathEdges.size()) {
                    int rmpathBeforeLoopIndexVlb = testedDfsCode.getAt(testedRightMostPathEdges.get(rmpathBeforeLoopIndex)).vLabel1;
                    int rmpath0Vlb = testedDfsCode.getAt(testedRightMostPathEdges.get(testedRightMostPathEdges.size() - 1)).vLabel1;

                    if (rmpath0Vlb != testedDfsCode.getAt(testedRightMostPathEdges.get(rmpathBeforeLoopIndex)).vLabel2 ||
                            testedDfsCode.getAt(testedRightMostPathEdges.get(testedRightMostPathEdges.size() - 1)).edgeLabel !=
                                    testedDfsCode.getAt(testedRightMostPathEdges.get(rmpathBeforeLoopIndex)).edgeLabel ||
                            testedDfsCode.getAt(testedRightMostPathEdges.get(testedRightMostPathEdges.size() - 1)).vLabel2 !=
                                    rmpathBeforeLoopIndexVlb) {
                        continue;
                    }
                }

                // build projections of the DFS code prefix
                List<PDFS> dfsCodeProjected = prefixProjections(projected, dfsCode.size() - testedDfsCode.size());

                // construct rightmost path in the 'opposite' direction by breaking one forward edge each time
                for (int rmpathEdgeIndex : testedRightMostPathEdges) {
                    // construct only edges before the last broken forward edge
                    if (rmpathEdgeIndex < rmpathLoop) {
                        break;
                    }

                    // last constructed rightmost path forward edge label
                    int elb = testedDfsCode.getAt(rmpathEdgeIndex).getEdgeLabel();
                    // last constructed rightmost path forward edge 'to' vertex label
                    int vlb = testedDfsCode.getAt(rmpathEdgeIndex).vLabel1;

                    // search for a vacant edge that has 'from' vertex same as last constructed rightmost path forward edge 'from' vertex
                    // and same edge label as last constructed rightmost path forward edge label
                    // and 'to' vertex label same as last constructed rightmost path forward edge 'to' vertex label
                    for (PDFS pdfs : dfsCodeProjected) {
                        DatabaseGraph g = graphDB.get(pdfs.getEdgeEnumeration().getGid());
                        int vertexId = pdfs.getDFSedgeAtToVertex(rmpathEdgeIndex);
                        Vertex vertex = g.vMap.get(vertexId);

                        for (Edge edge : vertex.getEdgeList()) {
                            if (edge.getEdgeLabel() != elb) {
                                continue;
                            }

                            int vertexTo = (edge.v1 == vertexId ? edge.v2 : edge.v1);
                            if (g.vMap.get(vertexTo).getLabel() != vlb) {
                                continue;
                            }

                            if (pdfs.hasEdge(edge)) {
                                continue;
                            }

                            if (pdfs.hasVertex(vertexTo)) {
                                continue;
                            }

                            // another projection was found
                            trie.insert(dfsCode.getEeL());
                            return true;
                        }
                    }
                }

            }
        }

        return false;
    }


    private Map<ExtendedEdge, Projected> extractForwardExtensions(DFSCode dfsCode, Map<ExtendedEdge, Projected> extensions) {
        Map<ExtendedEdge, Projected> forwardExtensions = new HashMap<ExtendedEdge, Projected>();
        int minVlb = dfsCode.getEeL().get(0).vLabel1;
        int maxVertex = -1;
        for (ExtendedEdge extendedEdge : dfsCode.getEeL()) {
            if (extendedEdge.v1 > maxVertex) {
                maxVertex = extendedEdge.v1;
            }

            if (extendedEdge.v2 > maxVertex) {
                maxVertex = extendedEdge.v2;
            }
        }

        for (ExtendedEdge extendedEdge : extensions.keySet()) {
            if (extendedEdge.v1 > extendedEdge.v2) {
                continue;
            }

            // pure forward
            if (extendedEdge.v1 == maxVertex && extendedEdge.v2 > maxVertex) {
                if (extendedEdge.vLabel2 < minVlb) {
                    continue;
                } else {
                    forwardExtensions.put(extendedEdge, extensions.get(extendedEdge));
                    continue;
                }
            }

            boolean add = true;
            for (ExtendedEdge dfsEdge : dfsCode.getEeL()) {
                if (dfsEdge.v1 > dfsEdge.v2) {
                    continue;
                }
                if (dfsEdge.v1 == extendedEdge.v1) {
                    if (extendedEdge.vLabel2 < minVlb) {
                        add = false;
                        break;
                    }
                    if (extendedEdge.edgeLabel < dfsEdge.edgeLabel) {
                        add = false;
                        break;
                    }

                    if (extendedEdge.edgeLabel == dfsEdge.edgeLabel && extendedEdge.vLabel2 < dfsEdge.vLabel2) {
                        add = false;
                        break;
                    }
                }
            }

            if (add) {
                forwardExtensions.put(extendedEdge, extensions.get(extendedEdge));
            }
        }

        return forwardExtensions;
    }

    /**
     *
     * @param projected DFS code projections
     * @param index DFS code prefix length
     * @return projections of the DFS code prefix
     */
    private List<PDFS> prefixProjections(Projected projected, int index) {
        List<PDFS> pdfsTails = new LinkedList<PDFS>();

        for (PDFS pdfs : projected.getProjected()) {
            for (int i = 0; i < index; i++) {
                pdfs = pdfs.getPrevious();
            }
            boolean alreadyExist = false;
            for (PDFS pdfsTail : pdfsTails) {
                PDFS pdfsi = pdfs;
                PDFS pdfsj = pdfsTail;
                if (!pdfsi.getEdgeEnumeration().equals(pdfsj.getEdgeEnumeration())) {
                    break;
                }
                pdfsi = pdfsi.getPrevious();
                pdfsj = pdfsj.getPrevious();

                if (pdfsi == null) {
                    alreadyExist = true;
                    break;
                }
            }

            if (!alreadyExist) {
                pdfsTails.add(pdfs);
            }
        }

        return pdfsTails;
    }

    private List<Integer> rightMostPathEdges(DFSCode dfsCode) {
        List<Integer> rightMostPathEdges = new ArrayList<Integer>();
        List<ExtendedEdge> eel = dfsCode.getEeL();
        dfsCode.getRightMostPath();
        Iterable<Integer> rightMostPathVertices = dfsCode.getRightMostPath();

        Iterator<Integer> rightMostPathVerticesIterator = rightMostPathVertices.iterator();
        int from = rightMostPathVerticesIterator.next();
        int to = rightMostPathVerticesIterator.next();

        int i = 0;
        for (ExtendedEdge extendedEdge : eel) {
            if (extendedEdge.v1 == from && extendedEdge.v2 == to) {
                rightMostPathEdges.add(i);
                from = to;
                to = (rightMostPathVerticesIterator.hasNext() ? rightMostPathVerticesIterator.next() : -1);
            }

            i++;
        }

        Collections.reverse(rightMostPathEdges);

        return rightMostPathEdges;
    }

    /**
     *
     * @param dfsCode examined DFS code
     * @return all prefixes of the DFS code such that next edge after the prefix is forward edge not from a rightmost vertex of the prefix
     */
    private List<DFSCode> buildDFSCodesPrefixes(DFSCode dfsCode) {
        List<DFSCode> dfsCodes = new LinkedList<DFSCode>();
        dfsCodes.add(dfsCode);

        List<ExtendedEdge> extendedEdges = dfsCode.getEeL();
        Integer oldFrom = null;
        for (int i = extendedEdges.size() - 1; i >= 0; i--) {
            ExtendedEdge extendedEdge = extendedEdges.get(i);
            int from = extendedEdge.v1;
            int to = extendedEdge.v2;
            if (from < to && (oldFrom == null || to == oldFrom)) {
                oldFrom = from;
                continue;
            } else {
                if (from > to && (oldFrom == null || from == oldFrom)) {
                    continue;
                } else {
                    oldFrom = from;
                    DFSCode dfsCodeNew = new DFSCode();
                    for (int j = 0; j <= i; j++) {
                        dfsCodeNew.add(extendedEdges.get(j));
                    }
                    dfsCodes.add(dfsCodeNew);
                }
            }
        }

        return dfsCodes;
    }

    private class ElbVlbKey {
        private int elb;
        private int vlb;

        public ElbVlbKey(int elb, int vlb) {
            this.elb = elb;
            this.vlb = vlb;
        }

        public int getElb() {
            return elb;
        }

        public void setElb(int elb) {
            this.elb = elb;
        }

        public int getVlb() {
            return vlb;
        }

        public void setVlb(int vlb) {
            this.vlb = vlb;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ElbVlbKey elbVlbKey = (ElbVlbKey) o;
            return elb == elbVlbKey.elb && vlb == elbVlbKey.vlb;
        }

        @Override
        public int hashCode() {
            return Objects.hash(elb, vlb);
        }
    }

    private class VertexElbVlbKey {
        private int vertex;
        private int elb;
        private int vlb;

        public VertexElbVlbKey(int vertex, int elb, int vlb) {
            this.vertex = vertex;
            this.elb = elb;
            this.vlb = vlb;
        }

        public int getElb() {
            return elb;
        }

        public void setElb(int elb) {
            this.elb = elb;
        }

        public int getVlb() {
            return vlb;
        }

        public void setVlb(int vlb) {
            this.vlb = vlb;
        }

        public int getVertex() {
            return vertex;
        }

        public void setVertex(int vertex) {
            this.vertex = vertex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VertexElbVlbKey that = (VertexElbVlbKey) o;
            return vertex == that.vertex && elb == that.elb && vlb == that.vlb;
        }

        @Override
        public int hashCode() {
            return Objects.hash(vertex, elb, vlb);
        }
    }

    /**
     *
     * @param extendedEdges DFS code edges
     * @return true if DFS code found in the trie, false otherwise
     */
    public boolean detect(List<ExtendedEdge> extendedEdges) {
        boolean detected = trie.search(extendedEdges);
        return detected;
    }

    /**
     * DFS codes trie node
     */
    private class TrieNode {
        private Map<ExtendedEdge, TrieNode> children;

        public TrieNode() {
            children = new HashMap<ExtendedEdge, TrieNode>();
        }
    }

    /**
     * DFS codes trie
     */
    class Trie {
        private TrieNode root;

        public Trie() {
            root = new TrieNode();
        }

        /**
         * adds DFS code edges to the trie
         * @param extendedEdges DFS code edges
         */
        public void insert(List<ExtendedEdge> extendedEdges) {
            TrieNode node = root;

            for (ExtendedEdge extendedEdge : extendedEdges) {
                if (node.children.containsKey(extendedEdge)) {
                    node = node.children.get(extendedEdge);
                } else {
                    TrieNode child = new TrieNode();
                    node.children.put(extendedEdge, child);
                    node = child;
                }
            }
        }

        /**
         * searches path created by DFS code edges in the trie
         * @param extendedEdges DFS code edges
         * @return true if path is present in the trie, false otherwise
         */
        public boolean search(List<ExtendedEdge> extendedEdges) {
            TrieNode node = root;

            for (ExtendedEdge extendedEdge : extendedEdges) {
                if (node.children.containsKey(extendedEdge)) {
                    node = node.children.get(extendedEdge);
                } else {
                    return false;
                }
            }

            return true;
        }
    }
}
