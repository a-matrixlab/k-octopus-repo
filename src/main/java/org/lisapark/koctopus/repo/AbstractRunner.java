/* 
 * Copyright (C) 2019 Lisa Park, Inc. (www.lisa-park.net)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.lisapark.koctopus.repo;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.lisapark.koctopus.core.graph.Edge;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.graph.Graph;
import org.lisapark.koctopus.core.graph.api.GraphVocabulary;

/**
 *
 * @author alexmylnikov
 * @param <V>
 */
public abstract class AbstractRunner<V> {   
    
    /**
     * @return the standardOut
     */
    public PrintStream getStandardOut() {
        return standardOut;
    }

    /**
     * @param standardOut the standardOut to set
     */
    public void setStandardOut(PrintStream standardOut) {
        this.standardOut = standardOut;
    }

    /**
     * @return the standardError
     */
    public PrintStream getStandardError() {
        return standardError;
    }

    /**
     * @param standardError the standardError to set
     */
    public void setStandardError(PrintStream standardError) {
        this.standardError = standardError;
    }
    
    private PrintStream standardOut = System.out;
    private PrintStream standardError = System.err;

    // In case of Db meta data Graph nodeResults map serves as a temporary storage for PK of the corresponding Table.
    // The initial capacity can be set to the Graph node collection size. However this map will hold only PK for
    // "white", "grey" and "red" nodes. Black nodes can be removed from the map.
    // This map should be persisted in order to restore Subsetting process from the failure.
    // Keys in this map are nodeWhite's ids, values are lists of PK's for the each nodeWhite in the subsetting use case.
    //
    // In the case of a different type of graphs use approapriet data type to hold nodeWhite's result collection.
    //
    // In case of very large result sets nodeResults map can hold references to the external data store.
    //   
//    Map<String, Object> env;
    Map<String, Gnode> nodeMap;
    Map<String, V> nodeStatus;
    Map<String, ConcurrentLinkedQueue<Gnode>> colorBuckets;
    Map<String, Set<Edge>> forwardRels;
    Map<String, Set<Edge>> backwardRels;

    String delim = ":";

    private Graph graph;
    
    private RepoCache koCache;


    /**
     * This is the place where the real data processing happened.Processing is
     * changing state of the node: - BACK_LOG, if node needs some additional
     * work to be completed; - COMPLETE, if node processing is done and
     * complete.
     *
     * @param node
     * @param forward
     * @return
     */
    public abstract String processNode(Gnode node, boolean forward);

    public AbstractRunner() {
    }

    public AbstractRunner(Graph graph) {
        this.graph = graph;
        init();
    }

    public AbstractRunner(String json) {
        this.graph = new Graph().fromJson(json);
        init();
    }

    public AbstractRunner(Graph graph, Map<String, Object> env) {
//        this.env = new HashMap<>(env);
        this.graph = graph;
        init();
    }

    public AbstractRunner(String json, Map<String, Object> env) {
//        this.env = new HashMap<>(env);
        this.graph = new Graph().fromJson(json);
        init();
    }

    /**
     * Provides initialization of a new and restoration of the interrupted
     * processing
     */
    public final void init() {
        // Build Gnode map
        getGraph().getNodes().stream().forEach((Gnode node) -> {
            getNodeMap().put(node.getId(), node);
        });

        // Restore Color buckets
        getGraph().getNodes().stream().forEach((Gnode node) -> {
            String nodeColor = node.getColor();
            if (nodeColor.equalsIgnoreCase(GraphVocabulary.GREY)) {
                // We are moving all grey nodes to white to start process over. The "processNode(. . . )" method 
                // has to be an idempotent, so it should not create any duplicates in the target dataset, 
                // if we run grey node again.
                getColorBuckets().get(GraphVocabulary.WHITE).add(node);
            } else if (nodeColor.equalsIgnoreCase(GraphVocabulary.WHITE)) {
                getColorBuckets().get(GraphVocabulary.WHITE).add(node);
            } else if (nodeColor.equalsIgnoreCase(GraphVocabulary.RED)) {
                getColorBuckets().get(GraphVocabulary.RED).add(node);
            } else if (GraphVocabulary.START_NODE.equalsIgnoreCase(node.getLabel())) {
                // Check if the node is a START_NODE. If we got here, it means that we are just started,
                // in this case run processStartNode method.
                // This method will extract sample data from provided data source (in Env map)
                // by applying a specified sampling strategy
                markNode(node, GraphVocabulary.GREY);
                processNode(node, true);
                V status = getNodeStatus().get(node.getId());

                if (status == GraphVocabulary.COMPLETE) {
                    markNodeCompleteBlack(node);
                } else {
                    markNode(node, GraphVocabulary.RED);
                }
            } else if (nodeColor.equalsIgnoreCase(GraphVocabulary.UNTOUCHED)) {
                getColorBuckets().get(GraphVocabulary.UNTOUCHED).add(node);
            }
        });
    }

    public void execute() throws InterruptedException {
        // Run over the nodes and do it until white, blue and red queues would be empty
        ConcurrentLinkedQueue<Gnode> whiteBucket = getColorBuckets().get(GraphVocabulary.WHITE);
        ConcurrentLinkedQueue<Gnode> blueBucket = getColorBuckets().get(GraphVocabulary.BLUE);
        ConcurrentLinkedQueue<Gnode> redBucket = getColorBuckets().get(GraphVocabulary.RED);

        while (!(whiteBucket.isEmpty() && redBucket.isEmpty() && blueBucket.isEmpty())) {
            // Iterate over white queue and process each nodeWhite that gets there 
            while (!whiteBucket.isEmpty()) {
                Gnode nodeWhite = (Gnode) whiteBucket.poll();
                if (nodeWhite != null) {
                    String nodeColor = nodeWhite.getColor();
                    // Check for the node color - it maybe changed while staying in the queue
                    if (GraphVocabulary.WHITE.equalsIgnoreCase(nodeColor)) {
                        // TODO: move markNode to the processNode body
//                        markNode(nodeWhite, GraphVocabulary.GREY);
                        processNode(nodeWhite, true);
                        V status = getNodeStatus().get(nodeWhite.getId());

                        if (status == GraphVocabulary.COMPLETE) {
                            markNodeCompleteBlack(nodeWhite);
                        } else {
                            markNode(nodeWhite, GraphVocabulary.RED);
                        }
                    }
                }
            }
            while (!blueBucket.isEmpty()) {
                Gnode nodeBlue = (Gnode) blueBucket.poll();
                if (nodeBlue != null) {
                    String nodeColor = nodeBlue.getColor();
                    // Check for the node color - it could be chaged while staying in the queue
                    if (GraphVocabulary.BLUE.equalsIgnoreCase(nodeColor)) {
                        // TODO: move markNode to the processNode body
//                        markNode(nodeBlue, GraphVocabulary.GREY);
                        processNode(nodeBlue, false);
                        V status = getNodeStatus().get(nodeBlue.getId());
                        if (status == GraphVocabulary.COMPLETE) {
                            markNodeCompleteBlack(nodeBlue);
                        } else {
                            markNode(nodeBlue, GraphVocabulary.RED);
                        }
                    }
                }
            }
            // When we'll finish with direct processing we have to check, if we have any unfinished work in "red" queue.
            // "Red" becomes a new "white" and we are running processNode one more time, but backward. 
            while (!redBucket.isEmpty()) {
                Gnode node = (Gnode) redBucket.poll();
                if (node != null) {
                    String nodeColor = node.getColor();
                    // Check for the node color - it maybe changed while staying in the queue
                    if (GraphVocabulary.RED.equalsIgnoreCase(nodeColor)) {
                        Set<Edge> inEdges = this.getBackwardRel().get(node.getId());
                        if (inEdges != null) {
                            Iterator<Edge> iterator = inEdges.iterator();
                            while (iterator.hasNext()) {
                                Edge edge = iterator.next();
                                String[] source = edge.getSource().split(delim);
                                Gnode _node = getNodeMap().get(source[1]);
                                // At this point nodes can be only: BLACK (complete), 
                                // RED (unfinished) or UNTOUCHED. Mark all untouched nodes to BLUE
                                String _nodeColor = _node.getColor();
                                if (GraphVocabulary.UNTOUCHED.equalsIgnoreCase(_nodeColor)) {
                                    markNode(_node, GraphVocabulary.BLUE);
                                }
                            }
                        }
                        if (!(hasUntouchedInNodes(node) && hasUntouchedOutNodes(node))) {
                            markNodeCompleteBlack(node);
                        }
                    }
                }
            }
        }
        System.out.println("Processing Results: " + getNodeStatus());
    }

    /**
     * Marks nodeWhite to the specified color. Important. Node should reference
     * to the nodeWhite instance in the graph.
     *
     * @param node
     * @param color
     */
    public void markNode(Gnode node, String color) {
        // 1. Move nodeWhite to the "color" bucket
        moveNode(node, color);
        // 2. Update nodeWhite color in the graph
        node.setColor(color);
    }

    public void markNodeCompleteBlack(Gnode node) {
        // 1. Mark all connected out-nodes to the "white", if they are not "black"        
        Set<Edge> set = getForwardRel().get(node.getId());
        if (set != null) {
            set.stream().forEach((edge) -> {
                String[] target = edge.getTarget().split(delim);
                Gnode _node = getNodeMap().get(target[1]);
                String _nodeColor = _node.getColor();
                if (GraphVocabulary.UNTOUCHED.equalsIgnoreCase(_nodeColor)) {
                    markNode(_node, GraphVocabulary.WHITE);
                }
            });
        }

        // 2. Update color of current nodeWhite. If there is no "untouched" among in-nodes - change to black,
        //    otherwise - change to red.
        if (hasUntouchedInNodes(node)) {
            moveNode(node, GraphVocabulary.RED);
            node.setColor(GraphVocabulary.RED);
        } else {
            moveNode(node, GraphVocabulary.BLACK);
            node.setColor(GraphVocabulary.BLACK);
        }
    }

    private void markNodeCompleteRed(Gnode node) {
        // 1. Mark all connected out-nodes to the "white"        
        Set<Edge> set = getForwardRel().get(node.getId());
        if (set != null) {
            set.stream().forEach((edge) -> {
                String[] target = edge.getTarget().split(delim);
                Gnode _node = getNodeMap().get(target[1]);
                markNode(_node, GraphVocabulary.WHITE);
            });
        }

        // 2. Update color of current nodeWhite. If there is no "untouched" among in-nodes - change to black,
        //    otherwise - change to red.
        if (hasUntouchedInNodes(node)) {
            moveNode(node, GraphVocabulary.RED);
            node.setColor(GraphVocabulary.RED);
        } else {
            moveNode(node, GraphVocabulary.BLACK);
            node.setColor(GraphVocabulary.BLACK);
        }
    }

    private void moveNode(Gnode node, String color) {
        // 1. Remove nodeWhite from current bucket
        String curColor = node.getColor();
        if (getColorBuckets().get(curColor) != null) {
            getColorBuckets().get(curColor).remove(node);
        }
        // 2. Add it to the new color bucket
        getColorBuckets().get(color).add(node);
    }

    public Map<String, ConcurrentLinkedQueue<Gnode>> getColorBuckets() {
        if (this.colorBuckets == null) {
            this.colorBuckets = new HashMap<>();
            colorBuckets.put(GraphVocabulary.UNTOUCHED, new ConcurrentLinkedQueue<>());
            colorBuckets.put(GraphVocabulary.WHITE, new ConcurrentLinkedQueue<>());
            colorBuckets.put(GraphVocabulary.BLUE, new ConcurrentLinkedQueue<>());
            colorBuckets.put(GraphVocabulary.GREY, new ConcurrentLinkedQueue<>());
            colorBuckets.put(GraphVocabulary.BLACK, new ConcurrentLinkedQueue<>());
            colorBuckets.put(GraphVocabulary.RED, new ConcurrentLinkedQueue<>());
        }
        return colorBuckets;
    }

    /**
     * @return the nodeMap
     */
    public Map<String, Gnode> getNodeMap() {
        if (nodeMap == null) {
            this.nodeMap = new HashMap<>();
            getGraph().getNodes().stream().forEach((node) -> {
                nodeMap.put(node.getId(), node);
            });
        }
        return nodeMap;
    }

    /**
     * @return the nodeResults
     */
    public Map<String, V> getNodeStatus() {
        if (nodeStatus == null) {
            nodeStatus = new ConcurrentHashMap<>();
        }
        return nodeStatus;
    }

    /**
     * @param nodeResults the nodeResults to set
     */
    public void setNodeStatus(ConcurrentHashMap<String, V> nodeResults) {
        this.nodeStatus = nodeResults;
    }

    /**
     * @return the graph
     */
    public Graph getGraph() {
        return graph;
    }
    
    /**
     * @param graph the graph to set
     */
    public void setGraph(Graph graph) {
        this.graph = graph;
    }
    
    
    /**
     * @return the koCache
     */
    public RepoCache getKoCache() {
        if(koCache == null){
            throw new NullPointerException("KosCache instance cannot be null.");
        }
        return koCache;
    }

    /**
     * @param koCache the koCache to set
     */
    public void setKoCache(RepoCache koCache) {
        this.koCache = koCache;
    }

    public Map<String, Set<Edge>> getForwardRel() {
        if (forwardRels == null) {
            List<Edge> edges = getGraph().getEdges();
            this.forwardRels = new HashMap<>();
            edges.stream().forEach((edge) -> {
                String[] source = edge.getSource().split(delim);
                if (forwardRels.get(source[1]) == null) {
                    forwardRels.put(source[1], new HashSet<>());
                }
                forwardRels.get(source[1]).add(edge);
            });
        }
        return forwardRels;
    }

    public Map<String, Set<Edge>> getBackwardRel() {
        if (backwardRels == null) {
            List<Edge> edges = getGraph().getEdges();
            this.backwardRels = new HashMap<>();
            edges.stream().forEach((edge) -> {
                String[] target = edge.getTarget().split(delim);
                if (backwardRels.get(target[1]) == null) {
                    backwardRels.put(target[1], new HashSet<>());
                }
                backwardRels.get(target[1]).add(edge);
            });
        }
        return backwardRels;
    }

    public boolean hasUntouchedInNodes(Gnode node) {
        String id = node.getId();
        Set<Edge> set = getBackwardRel().get(id);

        return checkUntouched(set);
    }

    public boolean hasUntouchedOutNodes(Gnode node) {
        String id = node.getId();
        Set<Edge> set = getForwardRel().get(id);

        return checkUntouched(set);
    }

    public boolean checkUntouched(Set<Edge> set) {
        boolean bool = false;
        if (set != null) {
            List<Edge> iterator = new ArrayList<>(set);
            for (Edge edge : iterator) {
                String[] source = edge.getSource().split(delim);
                Gnode _node = getNodeMap().get(source[1]);
                String _nodeColor = _node.getColor();
                if (GraphVocabulary.UNTOUCHED.equalsIgnoreCase(_nodeColor)) {
                    bool = true;
                    break;
                }
            }
        }
        return bool;
    }
}
