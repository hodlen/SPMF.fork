package ca.pfv.spmf.algorithms.graph_mining.tkg;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import ca.pfv.spmf.tools.MemoryLogger;

/**
 * This file is copyright (c) 2022 by Shaul Zevin
 * <p>
 * This is an implementation of the CGSPAN algorithm <br/>
 * <br/>
 * <p>
 * The cgspan algorithm is described in : <br/>
 * <br/>
 * <p>
 * cgSpan: Closed Graph-Based Substructure Pattern Mining, by Zevin Shaul, Sheikh Naaz
 * IEEE BigData 2021 7th Special Session on Intelligent Data Mining
 * <br/>
 * <br/>
 * <p>
 * The cgspan algorithm finds all the closed subgraphs and their support in a
 * graph provided by the user. <br/>
 * <br/>
 * <p>
 * This implementation saves the result to a file
 *
 * @author Shaul Zevin
 */
public class AlgoCGSPAN {

    /**
     * the minimum support represented as a count (number of subgraph occurrences)
     */
    private int minSup;

    /**
     * The list of closed subgraphs found by the last execution
     */
    private List<ClosedSubgraph> closedSubgraphs;

    /**
     * runtime of the most recent execution
     */
    private long runtime = 0;

    /**
     * runtime of the most recent execution
     */
    private double maxmemory = 0;

    /**
     * pattern count of the most recent execution
     */
    private int patternCount = 0;

    /**
     * number of graph in the input database
     */
    private int graphCount = 0;

    /**
     * frequent vertex labels
     */
    List<Integer> frequentVertexLabels;

    /**
     * if true, debug mode is activated
     */
    private boolean DEBUG_MODE = false;

    /**
     * eliminate infrequent labels from graphs
     */
    private static final boolean ELIMINATE_INFREQUENT_VERTICES = true;  // strategy in Gspan paper

    /**
     * eliminate infrequent vertex pairs from graphs
     */
    private static final boolean ELIMINATE_INFREQUENT_VERTEX_PAIRS = true;

    /**
     * eliminate infrequent labels from graphs
     */
    private static final boolean ELIMINATE_INFREQUENT_EDGE_LABELS = true;  // strategy in Gspan paper

    /**
     * apply edge count pruning strategy
     */
    private static final boolean EDGE_COUNT_PRUNING = true;

    /**
     * skip strategy
     */
    private static final boolean SKIP_STRATEGY = false;

    /**
     * infrequent edges removed
     */
    int infrequentVertexPairsRemoved;

    /**
     * infrequent edges removed
     */
    int infrequentVerticesRemovedCount;

    /**
     * remove infrequent edge labels
     */
    int edgeRemovedByLabel;

    /**
     * remove infrequent edge labels
     */
    int eliminatedWithMaxSize;

    /**
     * empty graph removed count
     */
    int emptyGraphsRemoved;

    /**
     * empty graph removed by edge count pruning
     */
    int pruneByEdgeCountCount;

    /**
     * skip strategy count
     */
    int skipStrategyCount;

    /**
     * maximum number of edges in each closed subgraph
     */
    int maxNumberOfEdges = Integer.MAX_VALUE;

    /**
     * Output the ids of graph containing each closed subgraph
     */
    boolean outputGraphIds = true;

    /**
     * activates early termination failure analysis and detection
     */
    boolean detectEarlyTerminationFailure = true;

    /**
     * counts number of times early termination was applied
     */
    int earlyTerminationAppliedCount;

    /**
     * counts number of times early termination failure was detected
     */
    int earlyTerminationFailureDetectedCount;

    /**
     * discovered closed subgraphs hash table
     */
    Map<Set<EdgeEnumeration>, List<ClosedSubgraph>> closedSubgraphsHashTable = new HashMap<Set<EdgeEnumeration>, List<ClosedSubgraph>>();

    /**
     * counts of vertices labels in database graphs, used only for closed one vertex subgraphs discovery and output
     */
    private HashMap<Integer, Integer> labelCountM;
    /**
     * counts of vertices labels for each graph in database graphs, used only for closed one vertex subgraphs output
     */
    Map<Integer, Map<Integer, Integer>> labelInGraphCountM;

    /**
     * Run the GSpan algorithm
     *
     * @param inPath               the input file
     * @param outPath              the output file
     * @param minSupport           a minimum support value (a percentage represented
     *                             by a value between 0 and 1)
     * @param outputSingleVertices if true, closed subgraphs containing a single
     *                             vertex will be output
     * @param outputDotFile        if true, a graphviz DOT file will be generated to
     *                             visualize the patterns
     * @param maxNumberOfEdges     an integer indicating a maximum number of edges
     *                             for each frequent subgraph
     * @param outputGraphIds       Output the ids of graph containing each frequent
     *                             subgraph
     * @throws IOException            if error while writing to file
     * @throws ClassNotFoundException
     */
    public void runAlgorithm(String inPath, String outPath, double minSupport, boolean outputSingleVertices,
                             boolean outputDotFile, int maxNumberOfEdges, boolean outputGraphIds)
            throws IOException, ClassNotFoundException {

        // if maximum size is 0
        if (maxNumberOfEdges <= 0) {
            return;
        }

        /**
         * the minimum support threshold as a precentage represented by a value between
         * 0 and 1
         */
        double minFrequency = minSupport;

        // Save the maximum number of edges
        this.maxNumberOfEdges = maxNumberOfEdges;

        // Save parameter
        this.outputGraphIds = outputGraphIds;

        // initialize variables for statistics
        infrequentVertexPairsRemoved = 0;
        infrequentVerticesRemovedCount = 0;
        edgeRemovedByLabel = 0;
        eliminatedWithMaxSize = 0;
        emptyGraphsRemoved = 0;
        pruneByEdgeCountCount = 0;
        earlyTerminationAppliedCount = 0;
        earlyTerminationFailureDetectedCount = 0;

        // initialize structure to store results
        closedSubgraphs = new ArrayList<ClosedSubgraph>();

        // Initialize the tool to check memory usage
        MemoryLogger.getInstance().reset();

        // reset the number of patterns found
        patternCount = 0;

        // Record the start time
        Long t1 = System.currentTimeMillis();

        // read graphs
        List<DatabaseGraph> graphDB = readGraphs(inPath);

        // Calculate the minimum support as a number of graphs
        minSup = (int) Math.ceil(minFrequency * graphDB.size());

        // mining
        cgSpan(graphDB, outputSingleVertices);

        // check the memory usage
        MemoryLogger.getInstance().checkMemory();

        // output
        writeResultToFile(outPath);

        Long t2 = System.currentTimeMillis();

        runtime = (t2 - t1) / 1000;

        maxmemory = MemoryLogger.getInstance().getMaxMemory();

        patternCount = closedSubgraphs.size();

        if (outputDotFile) {
            outputDotFile(outPath);
        }
    }

    /**
     * Output the DOT files to a given file path
     *
     * @param outputPath the output file path
     * @throws IOException if some exception when reading/writing the files
     */
    private static void outputDotFile(String outputPath) throws IOException {
        String dirName = outputPath + "_dotfile";
        File dir = new File(dirName);
        if (!dir.exists())
            dir.mkdir();
        VizGraph.visulizeFromFile(outputPath, dirName);
    }

    /**
     * Write the result to an output file
     *
     * @param outputPath an output file path
     **/
    private void writeResultToFile(String outputPath) throws IOException {
        // Create the output file
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outputPath)));

        closedSubgraphs.sort((ClosedSubgraph z1, ClosedSubgraph z2) -> (Integer.compare(z1.support, z2.support)));

        // For each frequent subgraph
        int i = 0;
        for (ClosedSubgraph subgraph : closedSubgraphs) {
            StringBuilder sb = new StringBuilder();

            DFSCode dfsCode = subgraph.dfsCode;
            if (dfsCode.size() == 1) {
                ExtendedEdge ee = dfsCode.getEeL().get(0);
                if (ee.getEdgeLabel() == -1) {
                    sb.append("t # ").append(i).append(" * ").append(subgraph.support);
                    if (DEBUG_MODE) {
                        sb.append(" * ").append(labelCountM.get(ee.getvLabel1()));
                    }
                    sb.append(System.lineSeparator());
                    sb.append("v 0 ").append(ee.getvLabel1()).append(System.lineSeparator());
                } else {
                    sb.append("t # ").append(i).append(" * ").append(subgraph.support);
                    if (DEBUG_MODE) {
                        sb.append(" * ").append(subgraph.getProjected().getProjected().size());
                    }
                    sb.append(System.lineSeparator());
                    sb.append("v 0 ").append(ee.getvLabel1()).append(System.lineSeparator());
                    sb.append("v 1 ").append(ee.getvLabel2()).append(System.lineSeparator());
                    sb.append("e 0 1 ").append(ee.getEdgeLabel()).append(System.lineSeparator());
                }
            } else {
                sb.append("t # ").append(i).append(" * ").append(subgraph.support);
                if (DEBUG_MODE) {
                    sb.append(" * ").append(subgraph.getProjected().getProjected().size());
                }
                sb.append(System.lineSeparator());
                List<Integer> vLabels = dfsCode.getAllVLabels();
                for (int j = 0; j < vLabels.size(); j++) {
                    sb.append("v ").append(j).append(" ").append(vLabels.get(j)).append(System.lineSeparator());
                }
                for (ExtendedEdge ee : dfsCode.getEeL()) {
                    int startV = ee.getV1();
                    int endV = ee.getV2();
                    int eL = ee.edgeLabel;
                    sb.append("e ").append(startV).append(" ").append(endV).append(" ").append(eL)
                            .append(System.lineSeparator());
                }
            }
            // If the user choose to output the graph ids where the frequent subgraph
            // appears
            // We output it
            if (outputGraphIds) {
                if (dfsCode.size() > 1 || (dfsCode.size() == 1 && dfsCode.getEeL().get(0).getEdgeLabel() != -1)) {
                    List<Integer> projectionsGraphIds = subgraph.getProjected().projectionsGraphIds();
                    Map<Integer, Long> projectionsCounts = projectionsGraphIds.stream()
                            .collect(Collectors.groupingBy(p -> p, Collectors.counting()));
                    List<Integer> sortedGraphIds = new ArrayList(subgraph.setOfGraphsIDs);
                    sortedGraphIds.sort(Integer::compare);
                    sb.append("x");
                    for (int id : sortedGraphIds) {
                        sb.append(" ").append(id);
                        if (DEBUG_MODE) {
                            sb.append('x').append(projectionsCounts.get(id));
                        }
                    }
                }
                else {
                    List<Integer> sortedGraphIds = new ArrayList(labelInGraphCountM.keySet());
                    sortedGraphIds.sort(Integer::compare);
                    sb.append("x");
                    for (int id : sortedGraphIds) {
                        Integer count = labelInGraphCountM.get(id).get(dfsCode.getEeL().get(0).vLabel1);
                        if (count != null && count > 0) {
                            sb.append(" ").append(id);
                            if (DEBUG_MODE) {
                                sb.append('x').append(count);
                            }
                        }
                    }
                }
            }
            sb.append(System.lineSeparator()).append(System.lineSeparator());

            bw.write(sb.toString());

            i++;
        }
        bw.close();
    }

    /**
     * Read graph from the input file
     *
     * @param path the input file
     * @return a list of input graph from the input graph database
     * @throws IOException if error reading or writing to file
     */
    private List<DatabaseGraph> readGraphs(String path) throws IOException {
        if (DEBUG_MODE) {
            System.out.println("start reading graphs...");
        }
        BufferedReader br = new BufferedReader(new FileReader(new File(path)));
        List<DatabaseGraph> graphDatabase = new ArrayList<>();

        String line = br.readLine();
        Boolean hasNextGraph = (line != null) && line.startsWith("t");

        // For each graph of the graph database
        while (hasNextGraph) {
            hasNextGraph = false;
            int gId = Integer.parseInt(line.split(" ")[2]);
            Map<Integer, Vertex> vMap = new HashMap<>();
            while ((line = br.readLine()) != null && !line.startsWith("t")) {

                String[] items = line.split(" ");

                if (line.startsWith("v")) {
                    // If it is a vertex
                    int vId = Integer.parseInt(items[1]);
                    int vLabel = Integer.parseInt(items[2]);
                    vMap.put(vId, new Vertex(vId, vLabel));
                } else if (line.startsWith("e")) {
                    // If it is an edge
                    int v1 = Integer.parseInt(items[1]);
                    int v2 = Integer.parseInt(items[2]);
                    int eLabel = Integer.parseInt(items[3]);
                    Edge e = new Edge(v1, v2, eLabel);
//                    System.out.println(v1 + " " + v2 + " " + vMap.get(v1).id + " " + vMap.get(v2).id);
                    vMap.get(v1).addEdge(e);
                    vMap.get(v2).addEdge(e);
                }
            }
            graphDatabase.add(new DatabaseGraph(gId, vMap));
            if (line != null) {
                hasNextGraph = true;
            }
        }

        br.close();

        if (DEBUG_MODE) {
            System.out.println("read successfully, totally " + graphDatabase.size() + " graphs");
        }
        graphCount = graphDatabase.size();
        return graphDatabase;
    }

    /**
     * Find all isomorphisms between graph described by c and graph g each
     * isomorphism is represented by a map
     *
     * @param c a dfs code representing a subgraph
     * @param g a graph
     * @return the list of all isomorphisms
     */
    private List<Map<Integer, Integer>> subgraphIsomorphisms(DFSCode c, Graph g) {

        List<Map<Integer, Integer>> isoms = new ArrayList<>();

        // initial isomorphisms by finding all vertices with same label as vertex 0 in C
        int startLabel = c.getEeL().get(0).getvLabel1(); // only non-empty DFSCode will be real parameter
        for (int vID : g.findAllWithLabel(startLabel)) {
            Map<Integer, Integer> map = new HashMap<>();
            map.put(0, vID);
            isoms.add(map);
        }

        // each extended edge will update partial isomorphisms
        // for forward edge, each isomorphism will be either extended or discarded
        // for backward edge, each isomorphism will be either unchanged or discarded
        for (ExtendedEdge ee : c.getEeL()) {
            int v1 = ee.getV1();
            int v2 = ee.getV2();
            int v2Label = ee.getvLabel2();
            int eLabel = ee.getEdgeLabel();

            List<Map<Integer, Integer>> updateIsoms = new ArrayList<>();
            // For each isomorphism
            for (Map<Integer, Integer> iso : isoms) {

                // Get the vertex corresponding to v1 in the current edge
                int mappedV1 = iso.get(v1);

                // If it is a forward edge extension
                if (v1 < v2) {
                    Collection<Integer> mappedVertices = iso.values();

                    // For each neighbor of the vertex corresponding to V1
                    for (Vertex mappedV2 : g.getAllNeighbors(mappedV1)) {

                        // If the neighbor has the same label as V2 and is not already mapped and the
                        // edge label is
                        // the same as that between v1 and v2.
                        if (v2Label == mappedV2.getLabel() && (!mappedVertices.contains(mappedV2.getId()))
                                && eLabel == g.getEdgeLabel(mappedV1, mappedV2.getId())) {

                            // TODO: PHILIPPE: getEdgeLabel() in the above line could be precalculated in
                            // Graph.java ...

                            // because there may exist multiple extensions, need to copy original partial
                            // isomorphism
                            HashMap<Integer, Integer> tempM = new HashMap<>(iso.size() + 1);
                            tempM.putAll(iso);
                            tempM.put(v2, mappedV2.getId());

                            updateIsoms.add(tempM);
                        }
                    }
                } else {
                    // If it is a backward edge extension
                    // v2 has been visited, only require mappedV1 and mappedV2 are connected in g
                    int mappedV2 = iso.get(v2);
                    if (g.isNeighboring(mappedV1, mappedV2) && eLabel == g.getEdgeLabel(mappedV1, mappedV2)) {
                        updateIsoms.add(iso);
                    }
                }
            }
            isoms = updateIsoms;
        }

        // Return the isomorphisms
        return isoms;
    }

    private Map<ExtendedEdge, Set<Integer>> rightMostPathExtensionsFromSingle(DFSCode c, Graph g) {
        int gid = g.getId();

        // Map of extended edges to graph ids
        Map<ExtendedEdge, Set<Integer>> extensions = new HashMap<>();

        if (c.isEmpty()) {
            // IF WE HAVE AN EMPTY SUBGRAPH THAT WE WANT TO EXTEND

            // find all distinct label tuples
            for (Vertex vertex : g.vertices) {
                for (Edge e : vertex.getEdgeList()) {
                    int v1L = g.getVLabel(e.v1);
                    int v2L = g.getVLabel(e.v2);
                    ExtendedEdge ee1;
                    if (v1L < v2L) {
                        ee1 = new ExtendedEdge(0, 1, v1L, v2L, e.getEdgeLabel());
                    } else {
                        ee1 = new ExtendedEdge(0, 1, v2L, v1L, e.getEdgeLabel());
                    }

                    // Update the set of graph ids for this pattern
                    Set<Integer> setOfGraphIDs = extensions.get(ee1);
                    if (setOfGraphIDs == null) {
                        setOfGraphIDs = new HashSet<>();
                        extensions.put(ee1, setOfGraphIDs);
                    }
                    setOfGraphIDs.add(gid);
                }
            }
        } else {
            // IF WE WANT TO EXTEND A SUBGRAPH
            int rightMost = c.getRightMost();

            // Find all isomorphisms of the DFS code "c" in graph "g"
            List<Map<Integer, Integer>> isoms = subgraphIsomorphisms(c, g);

            // For each isomorphism
            for (Map<Integer, Integer> isom : isoms) {

                // backward extensions from rightmost child
                Map<Integer, Integer> invertedISOM = new HashMap<>();
                for (Entry<Integer, Integer> entry : isom.entrySet()) {
                    invertedISOM.put(entry.getValue(), entry.getKey());
                }
                int mappedRM = isom.get(rightMost);
                int mappedRMlabel = g.getVLabel(mappedRM);
                for (Vertex x : g.getAllNeighbors(mappedRM)) {
                    Integer invertedX = invertedISOM.get(x.getId());
                    if (invertedX != null && c.onRightMostPath(invertedX) && c.notPreOfRM(invertedX)
                            && !c.containEdge(rightMost, invertedX)) {
                        // rightmost and invertedX both have correspondings in g, so label of vertices
                        // and edge all
                        // can be found by correspondings
                        ExtendedEdge ee = new ExtendedEdge(rightMost, invertedX, mappedRMlabel, x.getLabel(),
                                g.getEdgeLabel(mappedRM, x.getId()));
                        if (extensions.get(ee) == null)
                            extensions.put(ee, new HashSet<>());
                        extensions.get(ee).add(g.getId());
                    }
                }
                // forward extensions from nodes on rightmost path
                Collection<Integer> mappedVertices = isom.values();
                for (int v : c.getRightMostPath()) {
                    int mappedV = isom.get(v);
                    int mappedVlabel = g.getVLabel(mappedV);
                    for (Vertex x : g.getAllNeighbors(mappedV)) {
                        if (!mappedVertices.contains(x.getId())) {
                            ExtendedEdge ee = new ExtendedEdge(v, rightMost + 1, mappedVlabel, x.getLabel(),
                                    g.getEdgeLabel(mappedV, x.getId()));
                            if (extensions.get(ee) == null)
                                extensions.put(ee, new HashSet<>());
                            extensions.get(ee).add(g.getId());
                        }
                    }
                }
            }
        }
        return extensions;
    }

    private Map<ExtendedEdge, Projected> rightMostPathExtensions(DFSCode c, List<DatabaseGraph> graphDatabase,
                                                                 Projected projected) {

        Set<Integer> graphIds = projected.getGraphIds();

        Map<ExtendedEdge, Projected> extensions = new HashMap<>();

        // if the DFS code is empty (WE START FROM AN EMPTY GRAPH)
        if (c.isEmpty()) {

            // For each graph
//            int highestSupport = 0;
//        	int remaininggraphCount = graphIds.size();
            for (Integer graphId : graphIds) {
                DatabaseGraph g = graphDatabase.get(graphId);

                if (EDGE_COUNT_PRUNING && c.size() >= g.getEdgeCount()) {
                    pruneByEdgeCountCount++;
                    continue;
                }

                // find all distinct label tuples
                for (Vertex vertex : g.vertices) {
                    for (Edge e : vertex.getEdgeList()) {
                        int v1L = g.getVLabel(e.v1);
                        int v2L = g.getVLabel(e.v2);

                        // if vertices have different labels, use the edge only once
                        if (v1L != v2L && vertex.getId() != e.v1) {
                            continue;
                        }

                        ExtendedEdge ee1;
                        if (v1L < v2L) {
                            ee1 = new ExtendedEdge(0, 1, v1L, v2L, e.getEdgeLabel());
                        } else {
                            ee1 = new ExtendedEdge(0, 1, v2L, v1L, e.getEdgeLabel());
                        }

                        // Update the set of graph ids for this pattern
                        Projected extensionProjected = extensions.get(ee1);
                        if (extensionProjected == null) {
                            extensionProjected = new Projected();
                            extensionProjected.setGraphIds(new HashSet<>());
                            extensions.put(ee1, extensionProjected);
                        }

                        EdgeEnumeration edgeEnumeration = g.getEdgeEnumeration(e);
                        boolean isReversed = (v1L < v2L ? false : (v2L < v1L ? true : vertex.getId() != e.v1));
                        PDFS pdfs = new PDFS(edgeEnumeration, isReversed, null);
                        extensionProjected.addProjection(pdfs);
                        extensionProjected.getGraphIds().add(graphId);
                    }
                }
//            	remaininggraphCount--;
//            	if(SKIP_STRATEGY && (highestSupport + remaininggraphCount  < minSup)){
////            		System.out.println("BREAK");
//            		skipStrategyCount++;
//            		break;
//            	}
            }
        } else {
            // IF THE DFS CODE IS NOT EMPTY (WE WANT TO EXTEND SOME EXISTING GRAPH)
            int remaininggraphCount = graphIds.size();
            int highestSupport = 0;
            int rightMost = c.getRightMost();
            // For each graph
            for (Integer graphId : graphIds) {
                DatabaseGraph g = graphDatabase.get(graphId);

                if (EDGE_COUNT_PRUNING && c.size() >= g.getEdgeCount()) {
                    pruneByEdgeCountCount++;
                    continue;
                }

                for (PDFS pdfs : projected.getProjected()) {
                    if (pdfs.getEdgeEnumeration().getGid() != g.getId()) {
                        continue;
                    }

                    Map<Integer, Integer> isom = pdfs.subgraphIsomorphism(c);

                    // backward extensions from rightmost child
                    Map<Integer, Integer> invertedISOM = new HashMap<>();
                    for (Entry<Integer, Integer> entry : isom.entrySet()) {
                        invertedISOM.put(entry.getValue(), entry.getKey());
                    }
                    int mappedRM = isom.get(rightMost);
                    int mappedRMlabel = g.getVLabel(mappedRM);
                    for (Vertex x : g.getAllNeighbors(mappedRM)) {
                        Integer invertedX = invertedISOM.get(x.getId());
                        if (invertedX != null && c.onRightMostPath(invertedX) && c.notPreOfRM(invertedX)
                                && !c.containEdge(rightMost, invertedX)) {
                            // rightmost and invertedX both have correspondings in g, so label of vertices
                            // and edge all
                            // can be found by correspondings
                            ExtendedEdge ee = new ExtendedEdge(rightMost, invertedX, mappedRMlabel, x.getLabel(),
                                    g.getEdgeLabel(mappedRM, x.getId()));

                            Projected extensionProjected = extensions.get(ee);
                            if (extensionProjected == null) {
                                extensionProjected = new Projected();
                                extensionProjected.setGraphIds(new HashSet<>());
                                extensions.put(ee, extensionProjected);
                            }

                            Edge e = g.getEdge(mappedRM, x.getId());
                            EdgeEnumeration edgeEnumeration = g.getEdgeEnumeration(e);
                            boolean isReversed = e.v1 != mappedRM;
                            PDFS extensionPdfs = new PDFS(edgeEnumeration, isReversed, pdfs);
                            extensionProjected.addProjection(extensionPdfs);
                            extensionProjected.getGraphIds().add(graphId);
                        }
                    }
                    // forward extensions from nodes on rightmost path
                    Collection<Integer> mappedVertices = isom.values();
                    for (int v : c.getRightMostPath()) {
                        int mappedV = isom.get(v);
                        int mappedVlabel = g.getVLabel(mappedV);
                        for (Vertex x : g.getAllNeighbors(mappedV)) {
                            if (!mappedVertices.contains(x.getId())) {
                                ExtendedEdge ee = new ExtendedEdge(v, rightMost + 1, mappedVlabel, x.getLabel(),
                                        g.getEdgeLabel(mappedV, x.getId()));


                                Projected extensionProjected = extensions.get(ee);
                                if (extensionProjected == null) {
                                    extensionProjected = new Projected();
                                    extensionProjected.setGraphIds(new HashSet<>());
                                    extensions.put(ee, extensionProjected);
                                }

                                Edge e = g.getEdge(mappedV, x.getId());
                                EdgeEnumeration edgeEnumeration = g.getEdgeEnumeration(e);
                                boolean isReversed = e.v1 != mappedV;
                                PDFS extensionPdfs = new PDFS(edgeEnumeration, isReversed, pdfs);
                                extensionProjected.addProjection(extensionPdfs);
                                extensionProjected.getGraphIds().add(graphId);

                                if (extensionProjected.getGraphIds().size() > highestSupport) {
                                    highestSupport = extensionProjected.getGraphIds().size();
                                }
                            }
                        }
                    }
                }

//                if (SKIP_STRATEGY && (highestSupport + remaininggraphCount < minSup)) {
//            		System.out.println("BREAK2");
//                    skipStrategyCount++;
//                    extensions = null;
//                    break;
//                }
                remaininggraphCount--;
            }
        }
        return extensions;
    }

    /**
     * Finds the set of projected edges enumerations for each edge in a closed subgraph and adds (set, subgraph) entries to the hash table.
     *
     * @param closedSubgraph closed subgraph to be added to hash table
     */
    private void updateClosedSubgraphsHashTable(ClosedSubgraph closedSubgraph) {
        Projected projected = closedSubgraph.getProjected();

        List<Set<EdgeEnumeration>> keys = projected.buildKeys();

        for (Set<EdgeEnumeration> key : keys) {
            if (!closedSubgraphsHashTable.containsKey(key)) {
                closedSubgraphsHashTable.put(key, new LinkedList<ClosedSubgraph>());
            }

            closedSubgraphsHashTable.get(key).add(closedSubgraph);
        }
    }

    /**
     * checks if further expansion of the DFS tree should be terminated
     *
     * @param setOfGraphsIDs                 ids of database graphs with current DFS code projection
     * @param projected                      edges of each projection of the current DFS code into database graph
     * @param earlyTerminationFailureHandler early termination failure detector
     * @return early termination and early termination failure indicators
     */
    private EarlyTerminationResult earlyTermination(Set<Integer> setOfGraphsIDs, Projected projected, EarlyTerminationFailureHandler earlyTerminationFailureHandler) {
        // set of edges enumerations of the projection of the last edge in the DFS code
        Set<EdgeEnumeration> key = projected.lastEdgeKey();

        // checks if exist close subgraph(s) with one of the edges projected into the same set of edges enumerations
        if (!closedSubgraphsHashTable.containsKey(key)) {
            return new EarlyTerminationResult(false, false);
        }

        boolean earlyTermination = false;
        for (ClosedSubgraph closedSubgraph : closedSubgraphsHashTable.get(key)) {
            // checks equivalent occurrence of the DFS code with a previously discovered closed subgraph
            Map<Integer, Integer> isomorphism = closedSubgraph.checkEquivalentOccurrence(setOfGraphsIDs, setOfGraphsIDs.size(), projected);
            if (isomorphism != null) {
                earlyTermination = true;
                if (detectEarlyTerminationFailure) {
                    // checks if early termination should not be applied because of early termination failure conditions
                    boolean earlyTerminationFailure = checkEarlyTerminationFailure(closedSubgraph, isomorphism, earlyTerminationFailureHandler);
                    if (earlyTerminationFailure) {
                        return new EarlyTerminationResult(false, true);
                    }
                }
            }
        }

        return new EarlyTerminationResult(earlyTermination, false);
    }

    /**
     * checks if early termination should not be applied
     *
     * @param closedSubgraph                 previously discovered closed subgraph with equivalent occurrence from the current DFS code
     * @param isomorphism                    isomorphism of the current DFS code into closed subgraph
     * @param earlyTerminationFailureHandler early termination failure detector
     * @return true if early termination should not be applied, false otherwise
     */
    private boolean checkEarlyTerminationFailure(ClosedSubgraph closedSubgraph, Map<Integer, Integer> isomorphism, EarlyTerminationFailureHandler earlyTerminationFailureHandler) {
        // finds maximum edge index in the closed subgraph DFS code injected by the isomorphism
        int maxDfsIndex = 0;
        for (int dfsIndex : isomorphism.values()) {
            if (dfsIndex > maxDfsIndex) {
                maxDfsIndex = dfsIndex;
            }
        }

        // extract prefix of the closed subgraph DFS code
        List<ExtendedEdge> extendedEdges = closedSubgraph.dfsCode.getEeL().subList(0, maxDfsIndex + 1);
        // search DFS code prefix in early termination failure DFS codes trie
        boolean detected = earlyTerminationFailureHandler.detect(extendedEdges);
        return detected;
    }

    /**
     * Initial call of the depth-first search
     *
     * @param graphDB              a graph database
     * @param outputClosedVertices if true, include frequent subgraph with a
     *                             single vertex in the output
     * @throws IOException            exception if error writing/reading to file
     * @throws ClassNotFoundException if error casting a class
     */
    private void cgSpan(List<DatabaseGraph> graphDB, boolean outputClosedVertices) throws IOException, ClassNotFoundException {

        // If the user wants single vertex graph, we will output them
        if (outputClosedVertices || ELIMINATE_INFREQUENT_VERTICES) {
            findAllOnlyOneVertex(graphDB);
        }

        for (DatabaseGraph g : graphDB) {
            g.precalculateVertexList();
        }

        if (ELIMINATE_INFREQUENT_VERTEX_PAIRS || ELIMINATE_INFREQUENT_EDGE_LABELS) {
            removeInfrequentVertexPairs(graphDB);
        }

        if (DEBUG_MODE) {
            System.out.println("Precalculating information...");
        }

        // Create a set with all the graph ids
        Set<Integer> graphIds = new HashSet<Integer>();
        for (int i = 0; i < graphDB.size(); i++) {
            DatabaseGraph g = graphDB.get(i);

            if (g.vertices == null || g.vertices.length != 0) {
                // If we deleted some vertices, we recalculate again the vertex list
                if (infrequentVerticesRemovedCount > 0) {
                    g.precalculateVertexList();
                }

                graphIds.add(i);

                // Precalculate the list of neighbors of each vertex
                g.precalculateVertexNeighbors();

                // Precalculate the list of vertices having each label
                g.precalculateLabelsToVertices();

                g.buildEdgeEnumeration();
            } else {
                if (DEBUG_MODE) {
                    System.out.println("EMPTY GRAPHS REMOVED");
                }
                emptyGraphsRemoved++;
            }
        }

        if (outputClosedVertices) {
            Projected projected = new Projected();
            projected.setGraphIds(graphIds);
            outputClosedOneVertex(graphDB, projected);
        }

        if (frequentVertexLabels.size() != 0) {
            if (DEBUG_MODE) {
                System.out.println("Starting depth-first search...");
            }

            Projected projected = new Projected();
            projected.setGraphIds(graphIds);
            EarlyTerminationFailureHandler earlyTerminationFailureHandler = new EarlyTerminationFailureHandler(graphDB, minSup);
            // Start the depth-first search
            cgSpanDFS(new DFSCode(), graphDB, graphIds, projected, earlyTerminationFailureHandler);
        }
    }

    /**
     * Pair
     */
    class Pair {
        /**
         * a value
         */
        int x;
        /**
         * another value
         */
        int y;

        Pair(int x, int y) {
            if (x < y) {
                this.x = x;
                this.y = y;
            } else {
                this.x = y;
                this.y = x;
            }
        }

        @Override
        public boolean equals(Object obj) {
            Pair other = (Pair) obj;
            return other.x == this.x && other.y == this.y;
        }

        @Override
        public int hashCode() {
            return x + 100 * y;
        }
    }

    /**
     * Create the pruning matrix
     */
    private void removeInfrequentVertexPairs(List<DatabaseGraph> graphDB) {

        Set<Pair> alreadySeenPair;
        SparseTriangularMatrix matrix;
        if (ELIMINATE_INFREQUENT_EDGE_LABELS) {
            if (DEBUG_MODE) {
                System.out.println("Calculating the pruning matrix...");
            }
            matrix = new SparseTriangularMatrix();
            alreadySeenPair = new HashSet<Pair>();
        }

        Set<Integer> alreadySeenEdgeLabel;
        Map<Integer, Integer> mapEdgeLabelToSupport;
        if (ELIMINATE_INFREQUENT_EDGE_LABELS) {
            mapEdgeLabelToSupport = new HashMap<Integer, Integer>();
            alreadySeenEdgeLabel = new HashSet<Integer>();
        }

        // CALCULATE THE SUPPORT OF EACH ENTRY
        for (DatabaseGraph g : graphDB) {
            Vertex[] vertices = g.getAllVertices();

            for (int i = 0; i < vertices.length; i++) {
                Vertex v1 = vertices[i];
                int labelV1 = v1.getLabel();

                for (Edge edge : v1.getEdgeList()) {
                    int v2 = edge.another(v1.getId());
                    int labelV2 = g.getVLabel(v2);

                    if (ELIMINATE_INFREQUENT_EDGE_LABELS) {
                        // Update vertex pair count
                        Pair pair = new Pair(labelV1, labelV2);
                        boolean seen = alreadySeenPair.contains(pair);
                        if (!seen) {
                            matrix.incrementCount(labelV1, labelV2);
                            alreadySeenPair.add(pair);
                        }
                    }

                    if (ELIMINATE_INFREQUENT_EDGE_LABELS) {
                        // Update edge label count
                        int edgeLabel = edge.getEdgeLabel();
                        if (!alreadySeenEdgeLabel.contains(edgeLabel)) {
                            alreadySeenEdgeLabel.add(edgeLabel);

                            Integer edgeSupport = mapEdgeLabelToSupport.get(edgeLabel);
                            if (edgeSupport == null) {
                                mapEdgeLabelToSupport.put(edgeLabel, 1);
                            } else {
                                mapEdgeLabelToSupport.put(edgeLabel, edgeSupport + 1);
                            }
                        }
                    }
                }
            }
            if (ELIMINATE_INFREQUENT_VERTEX_PAIRS) {
                alreadySeenPair.clear();
            }
            if (ELIMINATE_INFREQUENT_EDGE_LABELS) {
                alreadySeenEdgeLabel.clear();
            }
        }

        alreadySeenPair = null;

        // REMOVE INFREQUENT ENTRIES FROM THE MATRIX
        if (ELIMINATE_INFREQUENT_VERTEX_PAIRS) {
            if (DEBUG_MODE) {
                System.out.println("Removing infrequent pairs...  minsup = " + minSup);
            }
            matrix.removeInfrequentEntriesFromMatrix(minSup);
        }

        // REMOVE INFREQUENT EDGES
        if (ELIMINATE_INFREQUENT_VERTEX_PAIRS || ELIMINATE_INFREQUENT_EDGE_LABELS) {
            // CALCULATE THE SUPPORT OF EACH ENTRY
            for (DatabaseGraph g : graphDB) {
                Vertex[] vertices = g.getAllVertices();

                for (int i = 0; i < vertices.length; i++) {
                    Vertex v1 = vertices[i];
                    int labelV1 = v1.getLabel();

                    Iterator<Edge> iter = v1.getEdgeList().iterator();
                    while (iter.hasNext()) {
                        Edge edge = (Edge) iter.next();
                        int v2 = edge.another(v1.getId());
                        int labelV2 = g.getVLabel(v2);

                        int count = matrix.getSupportForItems(labelV1, labelV2);
                        if (ELIMINATE_INFREQUENT_VERTEX_PAIRS && count < minSup) {
                            iter.remove();

                            infrequentVertexPairsRemoved++;
                        } else if (ELIMINATE_INFREQUENT_EDGE_LABELS
                                && mapEdgeLabelToSupport.get(edge.getEdgeLabel()) < minSup) {
                            iter.remove();
                            edgeRemovedByLabel++;
                        }
                    }

                }
            }
        }
    }

    /**
     * compares two edges by the lexicographical order
     */
    public class ExtendedEdgeLexicographicalComparator implements Comparator<ExtendedEdge> {
        @Override
        public int compare(ExtendedEdge ee1, ExtendedEdge ee2) {
            if (ee1.equals(ee2)) {
                return 0;
            }

            if (ee1.smallerThanOriginal(ee2)) {
                return -1;
            } else {
                return 1;
            }
        }
    }

    /**
     * Recursive method to perform the depth-first search
     *
     * @param c                              the current DFS code
     * @param graphDB                        the graph database
     * @param graphIds                       the ids of graph where the graph "c" appears
     * @param projected                      projections of the current DFS code into database graphs
     * @param earlyTerminationFailureHandler early termination failure detector
     * @throws IOException            exception if error writing/reading to file
     * @throws ClassNotFoundException if error casting a class
     */
    private void cgSpanDFS(DFSCode c, List<DatabaseGraph> graphDB, Set<Integer> graphIds, Projected projected, EarlyTerminationFailureHandler earlyTerminationFailureHandler)
            throws IOException, ClassNotFoundException {
        // If we have reached the maximum size, we do not need to extend this graph
        if (c.size() == maxNumberOfEdges - 1) {
            return;
        }

        // check if DFS tree should not be further extended
        EarlyTerminationResult earlyTerminationResult = earlyTermination(graphIds, projected, earlyTerminationFailureHandler);
        if (earlyTerminationResult.isEarlyTerminationFailure()) {
            earlyTerminationFailureDetectedCount++;
        }
        if (earlyTerminationResult.isEarlyTermination()) {
            // terminate further DFS tree extension
            earlyTerminationAppliedCount++;
            return;
        }

        // Find all the extensions of this graph, with their support values
        // They are stored in a map where the key is an extended edge, and the value is
        // the
        // is the list of graph ids where this edge extends the current subgraph c.
        Map<ExtendedEdge, Projected> extensions = rightMostPathExtensions(c, graphDB, projected);

        // For each extension
        if (extensions != null) {
            // sort extension by lexicographical order
            List<ExtendedEdge> orderedExtensions = new ArrayList<ExtendedEdge>(extensions.keySet());
            Collections.sort(orderedExtensions, new ExtendedEdgeLexicographicalComparator());

            for (ExtendedEdge extension : orderedExtensions) {
                Projected newProjected = extensions.get(extension);
                // Get the support
                Set<Integer> newGraphIDs = newProjected.getGraphIds();
                int sup = newGraphIDs.size();

                // if the support is enough
                if (sup >= minSup) {

                    // Create the new DFS code of this graph
                    DFSCode newC = c.copy();
                    newC.add(extension);

                    // if the resulting graph is canonical (it means that the graph is non
                    // redundant)
                    if (isCanonical(newC)) {
                        // Try to extend this graph to generate larger frequent subgraphs
                        cgSpanDFS(newC, graphDB, newGraphIDs, newProjected, earlyTerminationFailureHandler);
                    }
                }
            }
        }

        if (c.size() > 0) {
            // analyze current DFS code for early termination failure conditions
            if (detectEarlyTerminationFailure) {
                earlyTerminationFailureHandler.analyze(c, projected, extensions);
            }

            // if early termination failure was detected, the current DFS code has equivalent occurrence with some closed subgraph
            // and therefore is not a closed graph
            if (earlyTerminationResult.isEarlyTerminationFailure) {
                return;
            }

            // check if one of the extensions has equivalent occurrence with current DFS code
            boolean hasEquivalentOccurrence = false;
            if (extensions != null) {
                for (Projected extendedProjected : extensions.values()) {
                    if (projected.hasEquivalentOccurrence(extendedProjected)) {
                        hasEquivalentOccurrence = true;
                        break;
                    }
                }
            }

            if (!hasEquivalentOccurrence) {
                // Save the graph
                ClosedSubgraph subgraph = new ClosedSubgraph(c, graphIds, graphIds.size(), projected);
                closedSubgraphs.add(subgraph);
                updateClosedSubgraphsHashTable(subgraph);
            }
        }

        // check the memory usage
        MemoryLogger.getInstance().checkMemory();
    }

    /**
     * Check if a DFS code is canonical
     *
     * @param c a DFS code
     * @return true if it is canonical, and otherwise, false.
     */
    private boolean isCanonical(DFSCode c) {
        DFSCode canC = new DFSCode();
        for (int i = 0; i < c.size(); i++) {
            Map<ExtendedEdge, Set<Integer>> extensions = rightMostPathExtensionsFromSingle(canC, new Graph(c));
            ExtendedEdge minEE = null;
            for (ExtendedEdge ee : extensions.keySet()) {
                if (ee.smallerThanOriginal(minEE))
                    minEE = ee;
            }

            if (minEE.smallerThanOriginal(c.getAt(i)))
                return false;
            canC.add(minEE);
        }
        return true;
    }

    /**
     * This method finds all frequent vertex labels from a graph database.
     *
     * @param graphDB                a graph database
     */
    private void findAllOnlyOneVertex(List<DatabaseGraph> graphDB) {

        frequentVertexLabels = new ArrayList<Integer>();

        // Create a map (key = vertex label, value = graph ids)
        // to count the support of each vertex
        Map<Integer, Set<Integer>> labelM = new HashMap<>();

        // For each graph
        for (DatabaseGraph g : graphDB) {
            // For each vertex
            for (Vertex v : g.getNonPrecalculatedAllVertices()) {

                // if it has some edges
                if (!v.getEdgeList().isEmpty()) {

                    // Get the vertex label
                    Integer vLabel = v.getLabel();

                    // Store the graph id in the map entry for this label
                    // if it is not there already
                    Set<Integer> set = labelM.get(vLabel);
                    if (set == null) {
                        set = new HashSet<>();
                        labelM.put(vLabel, set);
                    }
                    set.add(g.getId());
                }
            }
        }

        // For each vertex label
        for (Entry<Integer, Set<Integer>> entry : labelM.entrySet()) {
            int label = entry.getKey();

            // if it is a frequent vertex, then record that as a frequent subgraph
            Set<Integer> tempSupG = entry.getValue();
            int sup = tempSupG.size();
            if (sup >= minSup) {
                frequentVertexLabels.add(label);
            } else if (ELIMINATE_INFREQUENT_VERTICES) {
                // for each graph
                for (Integer graphid : tempSupG) {
                    Graph g = graphDB.get(graphid);

                    g.removeInfrequentLabel(label);
                    infrequentVerticesRemovedCount++;
                }
            }
        }
    }

    /**
     * This method outputs vertices that do not have equivalent occurrence with any closed subgraph.
     *
     * @param graphDB                a graph database
     * @param projected              empty projections
     */
    private void outputClosedOneVertex(List<DatabaseGraph> graphDB, Projected projected) {
        // Create a map (key = vertex label, value = graph ids)
        // to count the support of each vertex
        Map<Integer, Set<Integer>> labelM = new HashMap<>();
        // count vertices with label in graphs database
        labelCountM = new HashMap<Integer, Integer>();
        // count vertices with label for each graph in graphs database
        labelInGraphCountM = new HashMap<Integer, Map<Integer, Integer>>();

        Set<Integer> gids = projected.getGraphIds();

        // For each graph
        for (DatabaseGraph g : graphDB) {
            if (!gids.contains(g.getId())) {
                continue;
            }
            // For each vertex
            for (Vertex v : g.getAllVertices()) {

                // if it has some edges
                if (!v.getEdgeList().isEmpty()) {

                    // Get the vertex label
                    Integer vLabel = v.getLabel();

                    // Store the graph id in the map entry for this label
                    // if it is not there already
                    Set<Integer> set = labelM.get(vLabel);
                    if (set == null) {
                        set = new HashSet<>();
                        labelM.put(vLabel, set);
                    }
                    set.add(g.getId());

                    // increase label count
                    if (!labelCountM.containsKey(vLabel)) {
                        labelCountM.put(vLabel, 0);
                    }
                    labelCountM.put(vLabel, labelCountM.get(vLabel) + 1);

                    if (!labelInGraphCountM.containsKey(g.getId())) {
                        labelInGraphCountM.put(g.getId(), new HashMap<Integer, Integer>());
                    }

                    if (!labelInGraphCountM.get(g.getId()).containsKey(vLabel)) {
                        labelInGraphCountM.get(g.getId()).put(vLabel, 0);
                    }

                    labelInGraphCountM.get(g.getId()).put(vLabel, labelInGraphCountM.get(g.getId()).get(vLabel) + 1);
                }
            }
        }

        Map<ExtendedEdge, Projected> extensions = rightMostPathExtensions(new DFSCode(), graphDB, projected);

        // For each vertex label
        for (Entry<Integer, Set<Integer>> entry : labelM.entrySet()) {
            int label = entry.getKey();

            // if it is a frequent vertex, then record that as a frequent subgraph
            Set<Integer> tempSupG = entry.getValue();
            int sup = tempSupG.size();
            if (sup >= minSup) {
                boolean output = true;

                int labelCount = labelCountM.get(label);

                for (ExtendedEdge extendedEdge : extensions.keySet()) {
                    if (extendedEdge.vLabel1 != label && extendedEdge.vLabel2 != label) {
                        continue;
                    }
                    Projected extensionProjected = extensions.get(extendedEdge);
                    int labelCountInProjections = extensionProjected.verticesWithLabelCount(label, graphDB);
                    if (labelCountInProjections == labelCount) {
                        output = false;
                        break;
                    }
                }

                if (output) {
                    DFSCode tempD = new DFSCode();
                    tempD.add(new ExtendedEdge(0, 0, label, label, -1));

                    closedSubgraphs.add(new ClosedSubgraph(tempD, tempSupG, sup, new Projected()));
                }
            }
        }
    }

    public boolean isDetectEarlyTerminationFailure() {
        return detectEarlyTerminationFailure;
    }

    public void setDetectEarlyTerminationFailure(boolean detectEarlyTerminationFailure) {
        this.detectEarlyTerminationFailure = detectEarlyTerminationFailure;
    }

    /**
     * Print statistics about the algorithm execution to System.out.
     */
    public void printStats() {
        System.out.println("=============  CGSPAN v2.53 - STATS =============");
        System.out.println(" Number of graph in the input database: " + graphCount);
        System.out.println(" Frequent subgraph count : " + patternCount);
        System.out.println(" Total time ~ " + runtime + " s");
        System.out.println(" Minsup : " + minSup + " graphs");
        System.out.println(" Maximum memory usage : " + maxmemory + " mb");

        if (DEBUG_MODE) {
            if (ELIMINATE_INFREQUENT_VERTEX_PAIRS || ELIMINATE_INFREQUENT_VERTICES) {
                System.out.println("  -------------------");
            }
            if (ELIMINATE_INFREQUENT_VERTICES) {
                System.out.println("  Number of infrequent vertices pruned : " + infrequentVerticesRemovedCount);
                System.out.println("  Empty graphs removed : " + emptyGraphsRemoved);
            }
            if (ELIMINATE_INFREQUENT_VERTEX_PAIRS) {
                System.out.println("  Number of infrequent vertex pairs pruned : " + infrequentVertexPairsRemoved);
            }
            if (ELIMINATE_INFREQUENT_EDGE_LABELS) {
                System.out.println("  Number of infrequent edge labels pruned : " + edgeRemovedByLabel);
            }
            if (EDGE_COUNT_PRUNING) {
                System.out.println("  Extensions skipped (edge count pruning) : " + pruneByEdgeCountCount);
            }
            if (SKIP_STRATEGY) {
                System.out.println("  Skip strategy count : " + skipStrategyCount);
            }
            System.out.println("early termination was applied " + earlyTerminationAppliedCount + " times");
            System.out.println("early termination failure was detected " + earlyTerminationFailureDetectedCount + " times");
        }
        System.out.println("===================================================");
    }
    
    /**
     * Set the debug mode to true or false
     * @param value true or false
     */
    public void setDebugMode(boolean value) {
    	DEBUG_MODE = value;
    }

    /**
     * used to return early termination and early termination failure indicators
     */
    private class EarlyTerminationResult {
        private boolean earlyTermination;
        private boolean isEarlyTerminationFailure;

        public EarlyTerminationResult(boolean earlyTermination, boolean isEarlyTerminationFailure) {
            this.earlyTermination = earlyTermination;
            this.isEarlyTerminationFailure = isEarlyTerminationFailure;
        }

        public boolean isEarlyTermination() {
            return earlyTermination;
        }

        public boolean isEarlyTerminationFailure() {
            return isEarlyTerminationFailure;
        }
    }
}
