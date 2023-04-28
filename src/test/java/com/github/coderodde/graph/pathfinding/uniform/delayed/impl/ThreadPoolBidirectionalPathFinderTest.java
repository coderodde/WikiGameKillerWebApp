package com.github.coderodde.graph.pathfinding.uniform.delayed.impl;

import com.github.coderodde.graph.pathfinding.uniform.delayed.AbstractDelayedGraphPathFinder;
import com.github.coderodde.graph.pathfinding.uniform.delayed.AbstractNodeExpander;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Apr 24, 2023)
 * @since 1.6 (Apr 24, 2023)
 */
public final class ThreadPoolBidirectionalPathFinderTest {
    
    private static final long SEED = 13L;
    private static final int NODES = 100_000;
    private static final int DISCONNECTED_GRAPH_NODES = 1000;
    private static final int MINIMUM_DISCONNECTED_GRAPH_DEGREE = 2;
    private static final int MAXIMUM_DISCONNECTED_GRAPH_DEGREE = 5;
    private static final int MINIMUM_DEGREE = 10;
    private static final int MAXIMUM_DEGREE = 30;
    private static final int MINIMUM_DELAY = 3;
    private static final int MAXIMUM_DELAY = 40;
    private static final int REQUESTED_NUMBER_OF_THREADS = 8;
    private static final int MASTER_THREAD_SLEEP_DURATION = 10;
    private static final int SLAVE_THREAD_SLEEP_DURATION = 20;
    private static final int MASTER_THREAD_TRIALS = 20;
    private static final int ITERATIONS = 5;
    
    private final List<DirectedGraphNode> delayedDirectedGraph;
    private final List<DirectedGraphNode> nondelayedDirectedGraph;
    
    private final List<DirectedGraphNode> disconnectedDelayedDirectedGraph;
    private final List<DirectedGraphNode> disconnectedNondelayedDirectedGraph;
    
    private final List<DirectedGraphNode> failingNodeGraph;
    
    private final Random random = new Random(SEED);
    private final AbstractDelayedGraphPathFinder<DirectedGraphNode> 
            testPathFinder = 
                new ThreadPoolBidirectionalPathFinder<>(
                        REQUESTED_NUMBER_OF_THREADS,
                        MASTER_THREAD_SLEEP_DURATION,
                        SLAVE_THREAD_SLEEP_DURATION,
                        MASTER_THREAD_TRIALS);
    
    private final ReferencePathFinder referencePathFinder =
            new ReferencePathFinder();
    
    public ThreadPoolBidirectionalPathFinderTest() {
        final DirectedGraphBuilder directedGraphBuilder = 
                new DirectedGraphBuilder(
                        NODES, 
                        MINIMUM_DEGREE, 
                        MAXIMUM_DEGREE,
                        MINIMUM_DELAY, 
                        MAXIMUM_DELAY, 
                        random);
        
        final DirectedGraphBuilder disconnectedGraphBuilder =
                new DirectedGraphBuilder(
                        DISCONNECTED_GRAPH_NODES,
                        MINIMUM_DISCONNECTED_GRAPH_DEGREE,
                        MAXIMUM_DISCONNECTED_GRAPH_DEGREE,
                        MINIMUM_DELAY,
                        MAXIMUM_DELAY,
                        random);
        
        final GraphPair graphPair = 
                directedGraphBuilder.getConnectedGraphPair();
        
        final GraphPair disconnectedGraphPair =
                disconnectedGraphBuilder.getDisconnectedGraphPair();
        
        this.delayedDirectedGraph = graphPair.delayedGraph;
        this.nondelayedDirectedGraph = graphPair.nondelayedGraph;
        
        this.disconnectedDelayedDirectedGraph =
                disconnectedGraphPair.delayedGraph;
        
        this.disconnectedNondelayedDirectedGraph =
                disconnectedGraphPair.nondelayedGraph;
        
        this.failingNodeGraph = directedGraphBuilder.getFailingGraph();
    }
    
    // This test may take a several seconds.
    @Test
    public void testCorrectness() {
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            final int sourceNodeIndex = 
                    random.nextInt(delayedDirectedGraph.size());
            
            final int targetNodeIndex = 
                    random.nextInt(delayedDirectedGraph.size());

            final DirectedGraphNode nondelayedGraphSource =
                    nondelayedDirectedGraph.get(sourceNodeIndex);

            final DirectedGraphNode nondelayedGraphTarget =
                    nondelayedDirectedGraph.get(targetNodeIndex);

            final DirectedGraphNode delayedGraphSource =
                    delayedDirectedGraph.get(sourceNodeIndex);

            final DirectedGraphNode delayedGraphTarget =
                    delayedDirectedGraph.get(targetNodeIndex);

            final List<DirectedGraphNode> testPath = 
                    testPathFinder
                            .search(delayedGraphSource,
                                    delayedGraphTarget, 
                                    new ForwardNodeExpander(),
                                    new BackwardNodeExpander(),
                                    null,
                                    null,
                                    null);

            final List<DirectedGraphNode> referencePath = 
                    referencePathFinder
                            .search(nondelayedGraphSource, nondelayedGraphTarget);
            
            assertEquals(referencePath.size(), testPath.size());
            assertEquals(referencePath.get(0), testPath.get(0));
            assertEquals(referencePath.get(referencePath.size() - 1),
                         testPath.get(testPath.size() - 1));
        }
    }   
    
    // This test may take a several seconds too .
//    @Test
    public void returnsEmptyPathOnDisconnectedGraph() {
        final int nodes = disconnectedDelayedDirectedGraph.size();
        final int sourceNodeIndex = random.nextInt(nodes / 2);
        final int targetNodeIndex = nodes / 2 + random.nextInt(nodes / 2);
        
        final DirectedGraphNode nondelayedGraphSource =
                    disconnectedNondelayedDirectedGraph.get(sourceNodeIndex);

        final DirectedGraphNode nondelayedGraphTarget =
                disconnectedNondelayedDirectedGraph.get(targetNodeIndex);

        final DirectedGraphNode delayedGraphSource =
                disconnectedDelayedDirectedGraph.get(sourceNodeIndex);

        final DirectedGraphNode delayedGraphTarget =
                disconnectedDelayedDirectedGraph.get(targetNodeIndex);

        final List<DirectedGraphNode> testPath = 
                testPathFinder
                        .search(delayedGraphSource,
                                delayedGraphTarget, 
                                new ForwardNodeExpander(),
                                new BackwardNodeExpander(),
                                null,
                                null,
                                null);

        final List<DirectedGraphNode> referencePath = 
                referencePathFinder
                        .search(nondelayedGraphSource, nondelayedGraphTarget);

        assertTrue(referencePath.isEmpty());
        assertTrue(testPath.isEmpty());
    }
    
//    @Test
    public void haltsOnFailingNodes() {
        
        final DirectedGraphNode sourceNode = 
                this.failingNodeGraph
                    .get(random.nextInt(this.failingNodeGraph.size()));
        
        final DirectedGraphNode targetNode = 
                this.failingNodeGraph
                    .get(random.nextInt(this.failingNodeGraph.size()));
        
        testPathFinder.search(sourceNode, 
                              targetNode,
                              new FailingForwardNodeExpander(), 
                              new FailingBackwardNodeExpander(), 
                              null, 
                              null, 
                              null);
    }
}

final class DirectedGraphNode {
    private final int id;
    private final boolean isDelayed;
    private final int delayMilliseconds;
    private final List<DirectedGraphNode> children = new ArrayList<>();
    private final List<DirectedGraphNode> parents  = new ArrayList<>();
    
    DirectedGraphNode(final int id) {
        this(id, false, Integer.MAX_VALUE);
    }
    
    DirectedGraphNode(final int id, 
                      final boolean isDelayed,
                      final int delayMilliseconds) {
        this.id = id;
        this.isDelayed = isDelayed;
        this.delayMilliseconds = delayMilliseconds;
    }
    
    void addChild(final DirectedGraphNode child) {
        children.add(child);
        child.parents.add(this);
    }
    
    List<DirectedGraphNode> getChildren() {
        if (isDelayed) {
            // Simulate network access.
            sleep(delayMilliseconds);
        }
        
        return children;
    }
    
    List<DirectedGraphNode> getParents() {
        if (isDelayed) {
            // Simulate network access.
            sleep(delayMilliseconds);
        }
        
        return parents;
    }
    
    @Override
    public boolean equals(Object obj) {
        DirectedGraphNode other = (DirectedGraphNode) obj;
        return id == other.id;
    }
    
    @Override
    public int hashCode() {
        return id;
    }
    
    @Override
    public String toString() {
        return "[Node id = " + id + "]";
    }
    
    private static void sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ex) {
            
        }
    }
}

final class DirectedGraphBuilder {
    
    private final int nodes;
    private final int minimumNodeDegree;
    private final int maximumNodeDegree;
    private final int minimumDelay;
    private final int maximumDelay;
    private final Random random;
    
    DirectedGraphBuilder(final int nodes,
                         final int minimumNodeDegree,
                         final int maximumNodeDegree,
                         final int minimumDelay,
                         final int maximumDelay,
                         final Random random) {
        this.nodes = nodes;
        this.minimumNodeDegree = minimumNodeDegree;
        this.maximumNodeDegree = maximumNodeDegree;
        this.minimumDelay = minimumDelay;
        this.maximumDelay = maximumDelay;
        this.random = random;
    }
    
    List<DirectedGraphNode> getFailingGraph() {
        List<DirectedGraphNode> graph = new ArrayList<>();
        
        for (int i = 0; i < nodes; i++) {
            graph.add(new DirectedGraphNode(i, true, Integer.MAX_VALUE));
        }
        
        for (int i = 0; i < 6 * nodes; i++) {
            final int headNodeIndex = random.nextInt(nodes);
            final int tailNodeIndex = random.nextInt(nodes);
            graph.get(tailNodeIndex).addChild(graph.get(headNodeIndex));
        }
        
        return graph;
    }
  
    GraphPair getDisconnectedGraphPair() {
        List<DirectedGraphNode> delayedDirectedGraph = 
                new ArrayList<>(nodes / 2);
        
        List<DirectedGraphNode> nondelayedDirectedGraph = 
                new ArrayList<>(nodes / 2);
        
        for (int id = 0; id < nodes; id++) {
            int delayMilliseconds = 
                    random.nextInt(maximumDelay - minimumDelay + 1) 
                    + 
                    minimumDelay;
            
            DirectedGraphNode delayedDirectedGraphNode = 
                    new DirectedGraphNode(
                            id, 
                            true,
                            delayMilliseconds);
            
            DirectedGraphNode nondelayedDirectedGraphNode =
                    new DirectedGraphNode(id);
            
            delayedDirectedGraph.add(delayedDirectedGraphNode);
            nondelayedDirectedGraph.add(nondelayedDirectedGraphNode);
        }
        
        final int totalNumberOfArcs =
                getTotalNumberOfArcs(
                        random, 
                        minimumNodeDegree, 
                        maximumNodeDegree);
        
        for (int i = 0; i < totalNumberOfArcs / 2; i++) {
            final int tailDirectedGraphNodeId = random.nextInt(nodes / 2);
            final int headDirectedGraphNodeId = random.nextInt(nodes / 2);
            
            if (headDirectedGraphNodeId != tailDirectedGraphNodeId) {
                final DirectedGraphNode nondelayedHeadDirectedGraphNode = 
                        nondelayedDirectedGraph.get(headDirectedGraphNodeId);
                
                final DirectedGraphNode nondelayedTailDirectedGraphNode = 
                        nondelayedDirectedGraph.get(tailDirectedGraphNodeId);
                
                nondelayedTailDirectedGraphNode
                        .addChild(nondelayedHeadDirectedGraphNode);
                
                final DirectedGraphNode delayedHeadDirectedGraphNode =
                        delayedDirectedGraph.get(headDirectedGraphNodeId);
                
                final DirectedGraphNode delayedTailDirectedGraphNode =
                        delayedDirectedGraph.get(tailDirectedGraphNodeId);
                
                delayedTailDirectedGraphNode
                        .addChild(delayedHeadDirectedGraphNode);
            }
        }
        
        for (int i = 0; i < totalNumberOfArcs / 2; i++) {
            final int tailDirectedGraphNodeId = nodes / 2 + 
                                                random.nextInt(nodes / 2);
            
            final int headDirectedGraphNodeId = nodes / 2 + 
                                                random.nextInt(nodes / 2);
            
            if (headDirectedGraphNodeId != tailDirectedGraphNodeId) {
                final DirectedGraphNode nondelayedHeadDirectedGraphNode = 
                        nondelayedDirectedGraph.get(headDirectedGraphNodeId);
                
                final DirectedGraphNode nondelayedTailDirectedGraphNode = 
                        nondelayedDirectedGraph.get(tailDirectedGraphNodeId);
                
                nondelayedTailDirectedGraphNode
                        .addChild(nondelayedHeadDirectedGraphNode);
                
                final DirectedGraphNode delayedHeadDirectedGraphNode =
                        delayedDirectedGraph.get(headDirectedGraphNodeId);
                
                final DirectedGraphNode delayedTailDirectedGraphNode =
                        delayedDirectedGraph.get(tailDirectedGraphNodeId);
                
                delayedTailDirectedGraphNode
                        .addChild(delayedHeadDirectedGraphNode);
            }
        }
        
        return new GraphPair(delayedDirectedGraph, nondelayedDirectedGraph);
    }
    
    GraphPair getConnectedGraphPair() {
        List<DirectedGraphNode> delayedDirectedGraph    = new ArrayList<>();
        List<DirectedGraphNode> nondelayedDirectedGraph = new ArrayList<>();
        
        for (int id = 0; id < nodes; id++) {
            int delayMilliseconds = 
                    random.nextInt(maximumDelay - minimumDelay + 1) 
                    +
                    minimumDelay;
            
            DirectedGraphNode delayedDirectedGraphNode = 
                    new DirectedGraphNode(
                            id, 
                            true,
                            delayMilliseconds);
            
            DirectedGraphNode nondelayedDirectedGraphNode =
                    new DirectedGraphNode(id);
            
            delayedDirectedGraph.add(delayedDirectedGraphNode);
            nondelayedDirectedGraph.add(nondelayedDirectedGraphNode);
        }
        
        final int totalNumberOfArcs =
                getTotalNumberOfArcs(
                        random, 
                        minimumNodeDegree, 
                        maximumNodeDegree);
        
        for (int i = 0; i < totalNumberOfArcs; i++) {
            final int tailDirectedGraphNodeId = random.nextInt(nodes);
            final int headDirectedGraphNodeId = random.nextInt(nodes);
            
            if (headDirectedGraphNodeId != tailDirectedGraphNodeId) {
                final DirectedGraphNode nondelayedHeadDirectedGraphNode = 
                        nondelayedDirectedGraph.get(headDirectedGraphNodeId);
                
                final DirectedGraphNode nondelayedTailDirectedGraphNode = 
                        nondelayedDirectedGraph.get(tailDirectedGraphNodeId);
                
                nondelayedTailDirectedGraphNode
                        .addChild(nondelayedHeadDirectedGraphNode);
                
                final DirectedGraphNode delayedHeadDirectedGraphNode =
                        delayedDirectedGraph.get(headDirectedGraphNodeId);
                
                final DirectedGraphNode delayedTailDirectedGraphNode =
                        delayedDirectedGraph.get(tailDirectedGraphNodeId);
                
                delayedTailDirectedGraphNode
                        .addChild(delayedHeadDirectedGraphNode);
            }
        }
        
        return new GraphPair(delayedDirectedGraph, nondelayedDirectedGraph);
    }
    
    private int getTotalNumberOfArcs(Random random, 
                                            int minimumNodeDegree,
                                            int maximumNodeDegree) {
        int totalNumberOfArcs = 0;
        
        for (int id = 0; id < nodes; id++) {
            final int outdegree = 
                    random.nextInt(
                            maximumNodeDegree - minimumNodeDegree)
                                + minimumNodeDegree 
                                + 1;
            
            totalNumberOfArcs += outdegree;
        }
        
        return totalNumberOfArcs;
    }
}

final class GraphPair {
    final List<DirectedGraphNode> delayedGraph;
    final List<DirectedGraphNode> nondelayedGraph;
    
    GraphPair(List<DirectedGraphNode> delayedGraph,
              List<DirectedGraphNode> nondelayedGraph) {
        this.delayedGraph = delayedGraph;
        this.nondelayedGraph = nondelayedGraph;
    }
}

final class Utils {
    
    static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            
        }
    }
}

final class FailingForwardNodeExpander
        extends AbstractNodeExpander<DirectedGraphNode> {

    @Override
    public List<DirectedGraphNode> expand(final DirectedGraphNode node) {
        Utils.sleep(1_000_000);
        return node.getChildren();
    }

    @Override
    public boolean isValidNode(final DirectedGraphNode node) {
        return true;
    }
}

final class FailingBackwardNodeExpander
        extends AbstractNodeExpander<DirectedGraphNode> {

    @Override
    public List<DirectedGraphNode> expand(final DirectedGraphNode node) {
        Utils.sleep(1_000_000);
        return node.getParents();
    }

    @Override
    public boolean isValidNode(final DirectedGraphNode node) {
        return true;
    }
}

final class ForwardNodeExpander
        extends AbstractNodeExpander<DirectedGraphNode> {

    @Override
    public List<DirectedGraphNode> expand(final DirectedGraphNode node) {
        return node.getChildren();
    }

    @Override
    public boolean isValidNode(final DirectedGraphNode node) {
        return true;
    }
}

final class BackwardNodeExpander
        extends AbstractNodeExpander<DirectedGraphNode> {

    @Override
    public List<DirectedGraphNode> expand(final DirectedGraphNode node) {
        return node.getParents();
    }

    @Override
    public boolean isValidNode(final DirectedGraphNode node) {
        return true;
    }
}

final class ReferencePathFinder  {

    public List<DirectedGraphNode> 
        search(final DirectedGraphNode source, 
               final DirectedGraphNode target) {
            
        if (source.equals(target)) {
            return Arrays.asList(target);
        }
        
        final Queue<DirectedGraphNode> queueA = new ArrayDeque<>();
        final Queue<DirectedGraphNode> queueB = new ArrayDeque<>();
        
        final Map<DirectedGraphNode, DirectedGraphNode> parentMapA = 
                new HashMap<>();
        
        final Map<DirectedGraphNode, DirectedGraphNode> parentMapB = 
                new HashMap<>();
        
        final Map<DirectedGraphNode, Integer> distanceMapA =
                new HashMap<>();
        
        final Map<DirectedGraphNode, Integer> distanceMapB =
                new HashMap<>();
        
        queueA.add(source);
        queueB.add(target);
        
        parentMapA.put(source, null);
        parentMapB.put(target, null);
        
        distanceMapA.put(source, 0);
        distanceMapB.put(target, 0);
        
        int bestCost = Integer.MAX_VALUE;
        DirectedGraphNode touchNode = null;
        
        while (!queueA.isEmpty() && !queueB.isEmpty()) {
            final int distanceA = distanceMapA.get(queueA.peek());
            final int distanceB = distanceMapB.get(queueB.peek());
            
            if (touchNode != null && bestCost < distanceA + distanceB) {
                return tracebackPath(touchNode, parentMapA, parentMapB);
            }
            
            if (distanceA < distanceB) {
                // Trivial load balancing.
                final DirectedGraphNode current = queueA.poll();
                
                if (distanceMapB.containsKey(current) 
                        && bestCost > distanceMapA.get(current) +
                                      distanceMapB.get(current)) {
                    
                    bestCost = distanceMapA.get(current) + 
                               distanceMapB.get(current);
                    
                    touchNode = current;
                }
                
                for (final DirectedGraphNode child : current.getChildren()) {
                    if (!distanceMapA.containsKey(child)) {
                        distanceMapA.put(child, distanceMapA.get(current) + 1);
                        parentMapA.put(child, current);
                        queueA.add(child);
                    }
                }
            } else {
                final DirectedGraphNode current = queueB.poll();
                
                if (distanceMapA.containsKey(current) 
                        && bestCost > distanceMapA.get(current) +
                                      distanceMapB.get(current)) {
                    
                    bestCost = distanceMapA.get(current) + 
                               distanceMapB.get(current);
                    
                    touchNode = current;
                }
                
                for (final DirectedGraphNode parent : current.getParents()) {
                    if (!distanceMapB.containsKey(parent)) {
                        distanceMapB.put(parent, distanceMapB.get(current) + 1);
                        parentMapB.put(parent, current);
                        queueB.add(parent);
                    }
                }
            }
        }
        
        return Arrays.asList();
    }
        
    private static List<DirectedGraphNode>
         tracebackPath(
                 final DirectedGraphNode touchNode, 
                 final Map<DirectedGraphNode, DirectedGraphNode> parentMapA,
                 final Map<DirectedGraphNode, DirectedGraphNode> parentMapB) {
        final List<DirectedGraphNode> path = new ArrayList<>();
        DirectedGraphNode current = touchNode;
        
        while (current != null) {
            path.add(current);
            current = parentMapA.get(current);
        }
        
        Collections.<DirectedGraphNode>reverse(path);
        
        current = parentMapB.get(touchNode);
        
        while (current != null) {
            path.add(current);
            current = parentMapB.get(current);
        }
        
        return path;
    } 
}