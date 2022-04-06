package ca.pfv.spmf.algorithms.graph_mining.tkg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * This is an implementation of a DFS code projection into a database graph, used by the CGSPAN algorithm.
 * The projection is implemented as a linked list of database graph edges.
 *  <br/><br/>
 *
 * The cgspan algorithm is described in : <br/>
 * <br/>
 * <p>
 * cgSpan: Closed Graph-Based Substructure Pattern Mining, by Zevin Shaul, Sheikh Naaz
 * IEEE BigData 2021 7th Special Session on Intelligent Data Mining
 * <p>
 *
 * <br/>
 *
 * The CGspan algorithm finds all the closed subgraphs and their support in a
 * graph provided by the user.
 * <br/><br/>
 *
 * This implementation saves the result to a file
 *
 * @see AlgoCGSPAN
 * @author Shaul Zevin
 */

public class PDFS {
    /**
     * projection of the DFS code edge into database graph edge
     */
    private EdgeEnumeration edgeEnumeration;
    /**
     * direction of the edge as used in the DFS code
     */
    private boolean isReversed;
    /**
     * projection of the previous edge in the DFS code into the database graph edge
     */
    private PDFS previous;
    /**
     * length of the projection
     */
    private int length;
    /**
     * cache of projection edges used to speed up edges search
     */
    private Map<Edge, Boolean> edgesCache = new HashMap<Edge, Boolean>();

    public PDFS(EdgeEnumeration edgeEnumeration, boolean isReversed, PDFS previous) {
        this.edgeEnumeration = edgeEnumeration;
        this.isReversed = isReversed;
        // append previous projection
        this.previous = previous;
        if (previous == null) {
            length = 1;
        }
        else {
            length = previous.getLength() + 1;
        }
    }

    public EdgeEnumeration getEdgeEnumeration() {
        return edgeEnumeration;
    }

    public void setEdgeEnumeration(EdgeEnumeration edgeEnumeration) {
        this.edgeEnumeration = edgeEnumeration;
    }

    public PDFS getPrevious() {
        return previous;
    }

    public void setPrevious(PDFS previous) {
        this.previous = previous;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public boolean isReversed() {
        return isReversed;
    }

    public void setReversed(boolean reversed) {
        isReversed = reversed;
    }

    /**
     * builds isomorphism between DFS code vertices and vertices in the database graph
     * @param c DFS code
     * @return isomorphism that maps each vertex in DFS code to it's matching vertex in the projection
     */
    public Map<Integer, Integer> subgraphIsomorphism(DFSCode c) {
        Map<Integer, Integer> isomorphism = new HashMap<Integer, Integer>();
        List<ExtendedEdge> dfsEdges = c.getEeL();
        PDFS current = this;
        for (int i = dfsEdges.size() - 1; i >= 0; i--) {
            ExtendedEdge dfsEdge = dfsEdges.get(i);
            Edge projectedEdge = current.getEdgeEnumeration().getEdge();
            if (current.isReversed()) {
                isomorphism.put(dfsEdge.v1, projectedEdge.v2);
                isomorphism.put(dfsEdge.v2, projectedEdge.v1);
            }
            else {
                isomorphism.put(dfsEdge.v1, projectedEdge.v1);
                isomorphism.put(dfsEdge.v2, projectedEdge.v2);
            }

            current = current.getPrevious();
        }

        return isomorphism;
    }

    /**
     *
     * @param index edge index in the projection
     * @return 'to' vertex of the edge specified by index
     */
    public int getDFSedgeAtToVertex(int index) {
        int pdfsIndex = length - 1 - index;
        PDFS pdfsAt = this;
        for (int i = 0; i < pdfsIndex; i++) {
            pdfsAt = pdfsAt.getPrevious();
        }

        return (pdfsAt.isReversed() ? pdfsAt.getEdgeEnumeration().getEdge().v1 : pdfsAt.getEdgeEnumeration().getEdge().v2);
    }

    /**
     *
     * @param index edge index in the projection
     * @return 'from' vertex of the edge specified by index
     */
    public int getDFSedgeAtFromVertex(int index) {
        int pdfsIndex = length - 1 - index;
        PDFS pdfsAt = this;
        for (int i = 0; i < pdfsIndex; i++) {
            pdfsAt = pdfsAt.getPrevious();
        }

        return (pdfsAt.isReversed() ? pdfsAt.getEdgeEnumeration().getEdge().v2 : pdfsAt.getEdgeEnumeration().getEdge().v1);
    }


    /**
     *
     * @param edge searched edge
     * @return true if projection includes the edge, false otherwise
     * uses edges cache for the search
     * if hit serves result from the cache
     * if miss updates cache with search result
     */
    public boolean hasEdge(Edge edge) {
        PDFS pdfsAt = this;
        while (pdfsAt != null) {
            if (pdfsAt.edgesCache.containsKey(edge)) {
                edgesCache.put(edge, pdfsAt.edgesCache.get(edge));
                return pdfsAt.edgesCache.get(edge);
            }
            if (pdfsAt.getEdgeEnumeration().getEdge().equals(edge)) {
                edgesCache.put(edge, true);
                return true;
            }

            pdfsAt = pdfsAt.getPrevious();
        }

        edgesCache.put(edge, false);
        return false;
    }

    /**
     *
     * @param vertexId searched vertex
     * @param edgesIndexes indexes of edges in projection to use for the search
     * @return true if vertex belongs to one of the edges specified by indexes, false otherwise
     */
    public boolean hasVertex(int vertexId, List<Integer> edgesIndexes) {
        PDFS pdfsAt = this;
        int index = length - 1;
        int edgesIndexesIndex = 0;
        while (pdfsAt != null) {
            if (index != edgesIndexes.get(edgesIndexesIndex)) {
                index--;
                pdfsAt = pdfsAt.getPrevious();
                continue;
            }

            if (pdfsAt.getEdgeEnumeration().getEdge().v1 == vertexId || pdfsAt.getEdgeEnumeration().getEdge().v2 == vertexId) {
                return true;
            }

            index--;
            edgesIndexesIndex++;
            if (edgesIndexesIndex == edgesIndexes.size()) {
                return false;
            }
            pdfsAt = pdfsAt.getPrevious();
        }

        return false;
    }

    /**
     *
     * @param vertexId searched vertex
     * @return true if vertex belongs to one of the edges in projection, false otherwise
     */
    public boolean hasVertex(int vertexId) {
        PDFS pdfsAt = this;
        while (pdfsAt != null) {
            if (pdfsAt.getEdgeEnumeration().getEdge().v1 == vertexId || pdfsAt.getEdgeEnumeration().getEdge().v2 == vertexId) {
                return true;
            }

            pdfsAt = pdfsAt.getPrevious();
        }

        return false;
    }
}
