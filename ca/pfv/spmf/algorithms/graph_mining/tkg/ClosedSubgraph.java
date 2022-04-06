package ca.pfv.spmf.algorithms.graph_mining.tkg;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * This is a closed subgraph. It stores the (1) DFS code of the subgraph,
 * (2) the support of the subgraph, (3) the set of graphs where the
 * subgraph appears, (4) list of closed subgraph projections into database graphs,
 * (5) list of closed subgraph projections into database graphs mapped by database graph id
 * and (6) id of the database graph with minimum number of projections.
 *
 * @see AlgoCGSPAN
 * @author Shaul Zevin
 */
public class ClosedSubgraph extends FrequentSubgraph {
	
    /** list of closed subgraph projections into database graphs */
    private Projected projected;
    
    /** list of closed subgraph projections into database graphs mapped by database graph id */
    private Map<Integer, List<List<EdgeEnumeration>>> projectionsInDatabaseGraph;
    
    /** id of the database graph with minimum number of projections */	
    private Integer exampleGid;


    /**
     * Constructor
     *
     * @param dfsCode        a dfs code
     * @param setOfGraphsIDs the ids of graphs where the subgraph appears
     * @param support        the support of the subgraph
     */
    public ClosedSubgraph(DFSCode dfsCode, Set<Integer> setOfGraphsIDs, int support, Projected projected) {
        super(dfsCode, setOfGraphsIDs, support);
        this.projected = projected;
        this.projectionsInDatabaseGraph = projected.buildProjectionsInDatabaseGraph();
        // find gid with minimal number of occurrences
        exampleGid = null;
        Integer numOccurrences = null;
        for (int gid : projectionsInDatabaseGraph.keySet()) {
            if (exampleGid == null) {
                exampleGid = gid;
                numOccurrences = projectionsInDatabaseGraph.get(gid).size();
                continue;
            }
            if (projectionsInDatabaseGraph.get(gid).size() < numOccurrences) {
                exampleGid = gid;
                numOccurrences = projectionsInDatabaseGraph.get(gid).size();
            }
        }
    }

    /**
     * builds set of projected edges enumerations for each edge in the closed subgraph
     * @return list of sets of projected edges enumerations
     */
    public List<Set<EdgeEnumeration>> getKeys() {
        return projected.buildKeys();
    }

    /**
     * checks if frequent subgraph with projections has equivalent occurrence with this closed subgraph
     * @param otherSetOfGraphsIDs ids of database graphs where other subgraph has projection
     * @param otherSupport support of other subgraph
     * @param otherProjected projections of the other subgraph
     * @return isomorphism of frequent subgraph into this closed subgraph if equivalent occurrence exists, null otherwise
     */
    public Map<Integer, Integer> checkEquivalentOccurrence(Set<Integer> otherSetOfGraphsIDs, int otherSupport, Projected otherProjected) {
        if (otherSupport > support) {
            return null;
        }
        if (!setOfGraphsIDs.equals(otherSetOfGraphsIDs)) {
            return null;
        }

        if (otherProjected.getProjected().size() > projected.getProjected().size()) {
            return null;
        }

        Map<Integer, List<List<EdgeEnumeration>>> otherProjectionsInDatabaseGraph = otherProjected.buildProjectionsInDatabaseGraph();

        // find all possible isomorphisms of projections of other subgraph into projections of this closed graph in database graph with id exampleGid
        List<Map<Integer, Integer>> possibleIsomorphisms = findPossibleIsomorphisms(otherProjectionsInDatabaseGraph.get(exampleGid));

        Map<Integer, Integer> isomorphism = null;
        boolean isomorphismFound = false;

        // check if one possible isomorphism is valid for projections in the rest of database graphs
        for (Map<Integer, Integer> possibleIsomorphism: possibleIsomorphisms) {
            for (int gid:  otherProjectionsInDatabaseGraph.keySet()) {
                for (List<EdgeEnumeration> otherProjectedEdges: otherProjectionsInDatabaseGraph.get(gid)) {
                    isomorphismFound = true;
                    for (List<EdgeEnumeration> myProjectedEdges: projectionsInDatabaseGraph.get(gid)) {
                        isomorphismFound = true;
                        for (int otherIndex: possibleIsomorphism.keySet()) {
                            int myIndex = possibleIsomorphism.get(otherIndex);
                            // check if edges match
                            if (otherProjectedEdges.get(otherIndex) != myProjectedEdges.get(myIndex)) {
                                isomorphismFound = false;
                                break;
                            }
                        }

                        if (isomorphismFound) {
                            break;
                        }
                    }

                    if (!isomorphismFound) {
                        break;
                    }
                }

                if (!isomorphismFound) {
                    break;
                }
            }

            if (isomorphismFound) {
                isomorphism = possibleIsomorphism;
                break;
            }
        }

        return isomorphism;
    }

    /**
     * finds all possible isomorphisms from projections passed in the parameter into this closed subgraph projections in database graph with id exampleGid
     * @param edgesProjectionList projections to be checked for isomorphism(s)
     * @return list of isomorphisms
     */
    private List<Map<Integer, Integer>> findPossibleIsomorphisms(List<List<EdgeEnumeration>> edgesProjectionList) {
        List<Map<Integer, Integer>> isomorphisms = new LinkedList<Map<Integer, Integer>>();

        List<EdgeEnumeration> otherProjectedEdges = edgesProjectionList.get(0);
        for (List<EdgeEnumeration> myProjectedEdges : projectionsInDatabaseGraph.get(exampleGid)) {
            Map<Integer, Integer> isomorphism = new HashMap<Integer, Integer>();
            for (int i = 0; i < otherProjectedEdges.size(); i++) {
                EdgeEnumeration otherEdge = otherProjectedEdges.get(i);
                for (int j = 0; j < myProjectedEdges.size(); j++) {
                    EdgeEnumeration myEdge = myProjectedEdges.get(j);
                    // if edges match, update isomorphism
                    if (otherEdge == myEdge) {
                        isomorphism.put(i, j);
                        break;
                    }
                }

                // all edges were matched
                if (isomorphism.size() == otherProjectedEdges.size()) {
                    isomorphisms.add(isomorphism);
                }
            }
        }

        return isomorphisms;
    }

    public Projected getProjected() {
        return projected;
    }
}
