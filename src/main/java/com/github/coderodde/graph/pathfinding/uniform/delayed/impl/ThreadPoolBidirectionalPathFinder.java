package com.github.coderodde.graph.pathfinding.uniform.delayed.impl;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;
import java.util.logging.Level;
import com.github.coderodde.graph.pathfinding.uniform.delayed.AbstractDelayedGraphPathFinder;
import com.github.coderodde.graph.pathfinding.uniform.delayed.AbstractNodeExpander;
import com.github.coderodde.graph.pathfinding.uniform.delayed.ProgressLogger;

/**
 * This class implements a parallel, bidirectional breadth-first search in order
 * to find an unweighted shortest path from a given source node to a given 
 * target node. The underlying algorithm is the bidirectional breadth-first
 * search. However, multiple threads may work on a single search direction in
 * order to speed up the computation: for each search direction (forward and 
 * backward), the algorithm maintains concurrent state, such as the frontier 
 * queue; many threads may pop the queue, expand the node and append the 
 * neighbours to that queue.
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Aug 4, 2016)
 * @param <N> the actual graph node type.
 */
public class ThreadPoolBidirectionalPathFinder<N> 
extends AbstractDelayedGraphPathFinder<N> {

    /**
     * The default number of milliseconds a master thread sleeps when it finds
     * the frontier queue empty.
     */
    private static final int DEFAULT_MASTER_THREAD_SLEEP_DURATION = 10;

    /**
     * The default number of milliseconds a slave thread sleeps when it finds
     * the frontier queue empty.
     */
    private static final int DEFAULT_SLAVE_THREAD_SLEEP_DURATION = 10;

    /**
     * The default upper bound on the number of times a master thread hibernates
     * due to the frontier queue being empty before the entire search is 
     * terminated.
     */
    private static final int DEFAULT_NUMBER_OF_TRIALS = 50;

    /**
     * The minimum number of threads to allow. One thread per each of the two
     * search directions.
     */
    private static final int MINIMUM_NUMBER_OF_THREADS = 4;

    /**
     * The minimum number of milliseconds a <b>master thread</b> sleeps when it 
     * finds the frontier queue empty.
     */
    private static final int MINIMUM_MASTER_THREAD_SLEEP_DURATION = 1;

    /**
     * The minimum number of milliseconds a <b>slave thread</b> sleeps when it 
     * finds the frontier queue empty.
     */
    private static final int MINIMUM_SLAVE_THREAD_SLEEP_DURATION = 1;

    /**
     * The lower bound on the amount of trials.
     */
    private static final int MINIMUM_NUMBER_OF_TRIALS = 1;

    /**
     * Caches the requested number of threads to use in the each search process.
     */
    private final int numberOfThreads;

    /**
     * The duration of sleeping in milliseconds for the master threads.
     */
    private final int masterThreadSleepDuration;

    /**
     * The duration of sleeping in milliseconds for the slave threads.
     */
    private final int slaveThreadSleepDuration;

    /**
     * While a master thread waits the frontier queue to become non-empty, the
     * master thread makes at most {@code masterThreadTrials} sleeping sessions
     * before giving up and terminating the search.
     */
    private final int masterThreadTrials;
    
    /**
     * The logging facility used to log abnormal activity.
     */
    private static final Logger LOGGER = 
            Logger.getLogger(
                    ThreadPoolBidirectionalPathFinder
                            .class
                            .getSimpleName());

    /**
     * Constructs this path finder.
     * 
     * @param requestedNumberOfThreads  the requested number of search threads.
     * @param masterThreadSleepDuration the number of milliseconds a master 
     *                                  thread sleeps whenever it discovers the
     *                                  frontier queue being empty.
     * @param slaveThreadSleepDuration  the number of milliseconds a slave
     *                                  thread sleeps whenever it discovers the
     *                                  frontier queue being empty.
     * @param masterThreadTrials        the number of times the master thread
     *                                  hibernates itself before terminating the
     *                                  entire search.
     */
    public ThreadPoolBidirectionalPathFinder(
            final int requestedNumberOfThreads,
            final int masterThreadSleepDuration,
            final int slaveThreadSleepDuration,
            final int masterThreadTrials) {
        this.numberOfThreads = Math.max(requestedNumberOfThreads, 
                                        MINIMUM_NUMBER_OF_THREADS);

        this.masterThreadSleepDuration = 
                Math.max(masterThreadSleepDuration,
                         MINIMUM_MASTER_THREAD_SLEEP_DURATION);

        this.slaveThreadSleepDuration = 
                Math.max(slaveThreadSleepDuration,
                         MINIMUM_SLAVE_THREAD_SLEEP_DURATION);

        this.masterThreadTrials = 
                Math.max(masterThreadTrials,
                         MINIMUM_NUMBER_OF_TRIALS);
    }

    /**
     * Construct this path finder using default sleeping duration.
     * 
     * @param requestedNumberOfThreads the requested number of search threads.
     */
    public ThreadPoolBidirectionalPathFinder(final int requestedNumberOfThreads) {
        this(requestedNumberOfThreads, 
             DEFAULT_MASTER_THREAD_SLEEP_DURATION,
             DEFAULT_SLAVE_THREAD_SLEEP_DURATION,
             DEFAULT_NUMBER_OF_TRIALS);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public List<N> 
        search(final N source, 
               final N target, 
               final AbstractNodeExpander<N> forwardSearchNodeExpander, 
               final AbstractNodeExpander<N> backwardSearchNodeExpander, 
               final ProgressLogger<N> forwardSearchProgressLogger, 
               final ProgressLogger<N> backwardSearchProgressLogger, 
               final ProgressLogger<N> sharedSearchProgressLogger) {
            
        Objects.requireNonNull(forwardSearchNodeExpander, 
                               "The forward search node expander is null.");

        Objects.requireNonNull(backwardSearchNodeExpander,
                               "The backward search node expander is null.");

        // Check the validity of the source node:
        if (!forwardSearchNodeExpander.isValidNode(source)) {
            final String exceptionMessage = 
                    "The source node (" + source + ") was rejected by the " +
                    "forward search node expander.";
            
            LOGGER.log(Level.SEVERE, exceptionMessage);
            
            throw new IllegalArgumentException(exceptionMessage);
        }

        // Check the validity of the target node:
        if (!backwardSearchNodeExpander.isValidNode(target)) {
            final String exceptionMessage = 
                    "The target node (" + target + ") was rejected by the " +
                    "backward search node expander.";
            
            LOGGER.log(Level.SEVERE, exceptionMessage);
            
            throw new IllegalArgumentException(
                    exceptionMessage);
        }

        // Possibly log the beginning of the search:
        if (sharedSearchProgressLogger != null) {
            sharedSearchProgressLogger.onBeginSearch(source, target);
        }

        // This path finder collects some performance related statistics:
        this.duration = System.currentTimeMillis();

        // Compute the numbers of threads for each of the search direction:
        final int forwardSearchThreadCount  = numberOfThreads / 2;
        final int backwardSearchThreadCount = numberOfThreads - 
                                              forwardSearchThreadCount;

        // Create the state object shared by both the search direction:
        final SharedSearchState<N> sharedSearchState = 
                new SharedSearchState<>(source, 
                                        target, 
                                        sharedSearchProgressLogger);

        // Create the state obj6/ect shared by all the threads working on forward
        // search direction:
        final SearchState<N> forwardSearchState = 
                new SearchState<>(source,
                                  sharedSearchState);

        // Create the state object shared by all the threads working on backward
        // search direction:
        final SearchState<N> backwardSearchState = 
                new SearchState<>(target,
                                  sharedSearchState);
        
        sharedSearchState.setForwardSearchState(forwardSearchState);
        sharedSearchState.setBackwardSearchState(backwardSearchState);

        // The array holding all forward search threads:
        final ForwardSearchThread[] forwardSearchThreads =
                new ForwardSearchThread[forwardSearchThreadCount];

        // Below, the value of 'sleepDuration' is ignored since the thread being 
        // created is a master thread that never sleeps.
        forwardSearchThreads[0] = 
                new ForwardSearchThread(0, 
                                        forwardSearchNodeExpander,
                                        forwardSearchState,
                                        sharedSearchState,
                                        true,
                                        forwardSearchProgressLogger,
                                        masterThreadSleepDuration,
                                        masterThreadTrials);

        // Spawn the forward search master thread:
        forwardSearchState.introduceThread(forwardSearchThreads[0]);           
        forwardSearchThreads[0].start();

        // Create and spawn all the slave threads working on forward search 
        // direction.
        for (int i = 1; i < forwardSearchThreadCount; ++i) {
            forwardSearchThreads[i] = 
                    new ForwardSearchThread(i,
                                            forwardSearchNodeExpander,
                                            forwardSearchState,
                                            sharedSearchState,
                                            false,
                                            forwardSearchProgressLogger,
                                            slaveThreadSleepDuration,
                                            masterThreadTrials);

            forwardSearchState.introduceThread(forwardSearchThreads[i]);
            forwardSearchThreads[i].start();
        }

        // The array holding all backward search threads:
        final BackwardSearchThread[] backwardSearchThreads =
                new BackwardSearchThread[backwardSearchThreadCount];

        // Below, the value of 'sleepDuration' is ignored since the thread being
        // created is a master thread that never sleeps.
        backwardSearchThreads[0] = 
                new BackwardSearchThread(forwardSearchThreads.length,
                                         backwardSearchNodeExpander,
                                         backwardSearchState,
                                         sharedSearchState,
                                         true,
                                         backwardSearchProgressLogger,
                                         masterThreadSleepDuration,
                                         masterThreadTrials);

        // Spawn the backward search master thread:
        backwardSearchState.introduceThread(backwardSearchThreads[0]);
        backwardSearchThreads[0].start();

        // Create and spawn all the slave threads working on backward search
        // direction:
        for (int i = 1; i < backwardSearchThreadCount; ++i) {
            backwardSearchThreads[i] = 
                    new BackwardSearchThread(forwardSearchThreads.length + i,
                                             backwardSearchNodeExpander,
                                             backwardSearchState,
                                             sharedSearchState,
                                             false,
                                             backwardSearchProgressLogger,
                                             slaveThreadSleepDuration,
                                             masterThreadTrials);

            backwardSearchState.introduceThread(backwardSearchThreads[i]);
            backwardSearchThreads[i].start();
        }

        // Wait all forward search threads to finish their work:
        try {
            for (final ForwardSearchThread thread : forwardSearchThreads) {
                thread.join();
            }
        } catch (final InterruptedException ex) {
            final String exceptionMessage =
                    "The forward thread threw " +
                    ex.getClass().getSimpleName() + ": " +
                    ex.getMessage();
            
            LOGGER.log(Level.SEVERE, exceptionMessage);
            
            throw new IllegalStateException(exceptionMessage, ex);
        }

        // Wait all backward search threads to finish their work: 
        try {
            for (final BackwardSearchThread thread : backwardSearchThreads) {
                thread.join();
            }
        } catch (final InterruptedException ex) {
            final String exceptionMessage =
                    "The backward thread threw " +
                    ex.getClass().getSimpleName() + ": " +
                    ex.getMessage();
            
            LOGGER.log(Level.SEVERE, exceptionMessage);
            
            throw new IllegalStateException(exceptionMessage, ex);
        }

        // Record the duration of the search:
        this.duration = System.currentTimeMillis() - this.duration;

        // Count the number of expanded nodes over all threads:
        this.numberOfExpandedNodes = 0;

        for (final ForwardSearchThread thread : forwardSearchThreads) {
            this.numberOfExpandedNodes += thread.getNumberOfExpandedNodes();
        }

        for (final BackwardSearchThread thread : backwardSearchThreads) {
            this.numberOfExpandedNodes += thread.getNumberOfExpandedNodes();
        }

        // Construct and return the path:
        sharedSearchState.lock();
        List<N> path = sharedSearchState.getPath();
        sharedSearchState.unlock();
        return path;
    }

    /**
     * This class holds the state shared by the two search directions.
     */
    private static final class SharedSearchState<N> {

        /**
         * The source node.
         */
        private final N source;

        /**
         * The target node. 
         */
        private final N target;

        /**
         * The state of all the forward search threads.
         */
        private SearchState<N> forwardSearchState;

        /**
         * The state of all the backward search threads.
         */
        private SearchState<N> backwardSearchState;
        
        /**
         * The mutex to use in order to synchronize all the concurrent 
         * operations.
         */
        private final Semaphore mutex = new Semaphore(1, true);

        /**
         * Caches the best known length from the source to the target nodes.
         */
        private volatile int bestPathLengthSoFar = Integer.MAX_VALUE;

        /**
         * The best search frontier touch node so far.
         */
        private volatile N touchNode;

        /**
         * The progress logger for reporting the progress.
         */
        private final ProgressLogger<N> sharedProgressLogger;

        /**
         * Constructs a shared state information object for the search.
         * 
         * @param source               the source node.
         * @param target               the target node.
         * @param forwardSearchState   the state of the forward search
         *                             direction.
         * @param backwardSearchState  the state of the backward search
         *                             direction.
         * @param sharedProgressLogger the progress logger for logging the 
         *                             overall progress of the path finder.
         */
        SharedSearchState(final N source,
                          final N target, 
                          final ProgressLogger<N> sharedProgressLogger) {
            this.source = source;
            this.target = target;
            this.sharedProgressLogger = sharedProgressLogger;
        }
        
        void setForwardSearchState(final SearchState<N> forwardSearchState) {
            this.forwardSearchState = forwardSearchState;
        }
        
        void setBackwardSearchState(final SearchState<N> backwardSearchState) {
            this.backwardSearchState = backwardSearchState;
        }
        
        /**
         * Locks this shared state.
         * 
         * @throws Exception if mutex acquisition fails.
         */
        void lock() {
            
            try {
                mutex.acquire();
            } catch (InterruptedException ex) {
                final String exceptionMessage =
                        "Mutex lock threw, permits = " 
                        + mutex.availablePermits() 
                        + ".";
                
                LOGGER.log(Level.SEVERE, exceptionMessage);
                throw new RuntimeException();
            }
        }
        
        /**
         * Unlocks this shared state.
         */
        void unlock() {
            mutex.release();
        }
        
        void killAllThreads() {
            for (final Thread thread : forwardSearchState.runningThreadSet) {
                if (thread != Thread.currentThread()) {
                    thread.interrupt();
                }
            }
            
            for (final Thread thread : forwardSearchState.sleepingThreadSet) {
                if (thread != Thread.currentThread()) {
                    thread.interrupt();
                }
            }
            
            for (final Thread thread : backwardSearchState.runningThreadSet) {
                if (thread != Thread.currentThread()) {
                    thread.interrupt();
                }
            }
            
            for (final Thread thread : backwardSearchState.sleepingThreadSet) {
                if (thread != Thread.currentThread()) {
                    thread.interrupt();
                }
            }
        }
        
        /**
         * Attempts to update the best known path.
         * 
         * @param current the touch node candidate.
         * @throws Exception if mutex acquisition fails.
         */
        void updateSearchState(final N current) {
            if (forwardSearchState.containsNode(current) &&
                backwardSearchState.containsNode(current)) {
                
                final int currentDistance = 
                        forwardSearchState .getDistance(current) +
                        backwardSearchState.getDistance(current);

                if (bestPathLengthSoFar > currentDistance) {
                    bestPathLengthSoFar = currentDistance;
                    touchNode = current;
                    System.out.println("fdds = " + bestPathLengthSoFar);
                }
            }
            System.out.println("yeah " + bestPathLengthSoFar);
        }

        boolean pathIsOptimal() {
            if (touchNode == null) {
                System.out.println("touchNode == null");
                return false;
            }

            final N forwardSearchHead = forwardSearchState.getQueueHead();

            if (forwardSearchHead == null) {
                return false;
            }

            final N backwardSearchHead = backwardSearchState.getQueueHead();

            if (backwardSearchHead == null) {
                return false;
            }

            final int distance =
                  forwardSearchState .getDistance(forwardSearchHead) +
                  backwardSearchState.getDistance(backwardSearchHead);
            
            System.out.println("hello " + (distance > bestPathLengthSoFar));
            return distance > bestPathLengthSoFar;
        }

        /**
         * Asks every single thread to exit.
         */
        void requestExit() {
            forwardSearchState .requestThreadsToExit();
            backwardSearchState.requestThreadsToExit();
        }

        /**
         * Constructs a shortest path and returns it as a list. If the target
         * node is unreachable from the source node, returns an empty list.
         * 
         * @return a shortest path found, or an empty list if target node is not 
         *         reachable from the source node.
         * @throws Exception if mutex acquisition fails.
         */
        List<N> getPath() {
            if (touchNode == null) {
                if (sharedProgressLogger != null) {
                    sharedProgressLogger.onTargetUnreachable(source, target);
                }
                
                LOGGER.log(Level.INFO, "Returning empty path.");
                return new ArrayList<>();
            }
            
            final List<N> path = new ArrayList<>();

            N current = touchNode;

            while (current != null) {
                path.add(current);
                current = forwardSearchState.getParent(current);
            }

            Collections.<String>reverse(path);
            current = backwardSearchState.parents.get(touchNode);

            while (current != null) {
                path.add(current);
                current = backwardSearchState.getParent(current);
            }

            if (sharedProgressLogger != null) {
                sharedProgressLogger.onShortestPath(path);
            }

            LOGGER.log(
                    Level.INFO, 
                    "Returning a path of {0} nodes.", 
                    path.size());
            
            return path;
        }
    }

    /**
     * This class holds all the state of a single search direction.
     */
    private static final class SearchState<N> {
        
        /**
         * The shared search direction. Contains the facilities relating to both
         * of the search directions.
         */
        private final SharedSearchState<N> sharedSearchState;

        /**
         * This FIFO queue contains the queue of nodes reached but not yet 
         * expanded. It is called the <b>search frontier</b>.
         */
        private final Deque<N> queue = new ArrayDeque<>();
        
        /**
         * This map maps each discovered node to its predecessor on the shortest 
         * path.
         */
        private final Map<N, N> parents = new HashMap<>();

        /**
         * This map maps each discovered node to its shortest path distance from
         * the source node.
         */
        private final Map<N, Integer> distance = new HashMap<>();

        /**
         * The set of all the threads working on this particular direction.
         */
        private final Set<SleepingThread> runningThreadSet = new HashSet<>();

        /**
         * The set of all <b>slave</b> threads that are currently sleeping.
         */
        private final Set<SleepingThread> sleepingThreadSet = new HashSet<>();

        /**
         * Constructs the search state object.
         * 
         * @param initialNode          the node from which the search begins. If
         *                             this state object is used in the forward
         *                             search, this node should be the source 
         *                             node. Otherwise, if this state object is
         *                             used in the backward search, this node
         *                             should be the target node.
         * @param totalNumberOfThreads the number of threads working on a 
         *                             particular search direction.
         */
        SearchState(final N initialNode,
                    final SharedSearchState<N> sharedSearchState) {
            queue.add(initialNode);
            parents.put(initialNode, null);
            distance.put(initialNode, 0);
            this.sharedSearchState = sharedSearchState;
        }
        
        N removeQueueHead() {
            if (queue.isEmpty()) {
                return null;
            }
            
            return queue.remove();
        }
        
        N getQueueHead() {
            return queue.peek();
        }
        
        boolean queueIsEmpty() {
            return queue.isEmpty();
        }
        
        boolean containsNode(final N node) {
            return distance.containsKey(node);
        }
        
        Integer getDistance(final N node) {
            return distance.get(node);
        }
        
        N getParent(final N node) {
            return parents.get(node);
        }
        
        /**
         * Tries to set the new node in the data structures.
         * 
         * @param node        the node to process.
         * @param predecessor the predecessor node. In the forward search 
         *                    direction, it is the tail of the arc, and in
         *                    the backward search direction, it is the head of 
         *                    the arc.
         * @return {@code true} if the {@code node} was not added, {@code false}
         *         otherwise.
         */
        private boolean trySetNodeInfo(final N node, final N predecessor) {
            if (distance.containsKey(node)) {
                // Nothing to set.
                return false;
            }
            
            distance.put(node, distance.get(predecessor) + 1);
            parents.put(node, predecessor);
            queue.addLast(node);
            return true;
        }
        
        private void tryUpdateIfImprovementPossible(
                final N node, 
                final N predecessor) {
            
            if (distance.get(node) > distance.get(predecessor) + 1) {
                distance.put(node, distance.get(predecessor) + 1);
                parents.put(node, predecessor);
            }
        }
        
        /**
         * Introduces a new thread to this search direction.
         * 
         * @param thread the thread to introduce.
         */
        void introduceThread(final SleepingThread thread) {
            thread.putThreadToSleep(false);
            runningThreadSet.add(thread);
        }

        /**
         * Asks the argument thread to go to sleep and adds it to the set of
         * sleeping slave threads.
         * 
         * @param thread the <b>slave</b> thread to hibernate.
         */
        void putThreadToSleep(final SleepingThread thread) {
            thread.putThreadToSleep(true);
            runningThreadSet.remove(thread);
            sleepingThreadSet.add(thread);
        }
        
        /**
         * Wakes up all the sleeping slave threads.
         */
        void wakeupAllThreads() { 
            for (final SleepingThread thread : sleepingThreadSet) {
                thread.putThreadToSleep(false);
                runningThreadSet.add(thread);
            }
            
            sleepingThreadSet.clear();
        }

        /**
         * Tells all the thread working on current direction to exit so that the
         * threads may be joined.
         */
        void requestThreadsToExit() {
            for (final StoppableThread thread : runningThreadSet) {
                thread.requestThreadToExit();
            }
            
            for (final StoppableThread thread : sleepingThreadSet) {
                thread.requestThreadToExit();
            }
        }
    }

    /**
     * This abstract class defines a thread that may be asked to terminate.
     */
    private abstract static class StoppableThread extends Thread {

        /**
         * If set to {@code true}, this thread should exit.
         */
        protected volatile boolean exit;

        /**
         * Sends a request to finish the work.
         */
        void requestThreadToExit() {
            exit = true;
        }
    }

    /**
     * This abstract class defines a thread that may be asked to go to sleep.
     */
    private abstract static class SleepingThread extends StoppableThread {

        /**
         * Holds the flag indicating whether this thread is put to sleep.
         */
        protected volatile boolean sleepRequested;

        /**
         * The number of milliseconds to sleep during each hibernation.
         */
        protected final int threadSleepDuration;

        /**
         * The maximum number of times a master thread hibernates itself before
         * giving up and terminating the entire search.
         */
        protected final int threadSleepTrials;
        
        /**
         * The boolean flag indicating whether this thread is a master thread. 
         * If not, it is called a slave thread.
         */
        protected final boolean isMasterThread;

        /**
         * Constructs this thread supporting sleeping.
         * 
         * @param threadSleepDuration the number of milliseconds to sleep each 
         *                            time.
         * @param threadSleepTrials   the maximum number of trials to hibernate
         *                            a master thread before giving up.
         */
        SleepingThread(final int threadSleepDuration,
                       final int threadSleepTrials,
                       final boolean isMasterThread) {
            this.threadSleepDuration = threadSleepDuration;
            this.threadSleepTrials   = threadSleepTrials;
            this.isMasterThread = isMasterThread;
        }

        /**
         * Sets the current sleep status of this thread.
         * 
         * @param toSleep indicates whether to put this thread to sleep or 
         *                wake it up.
            return isMasterThread;
         */
        void putThreadToSleep(final boolean toSleep) {
            this.sleepRequested = toSleep;
        }
    }

    /**
     * This class defines all the state that should appear in threads working in
     * both search direction.
     * 
     * @param <N> the actual node type.
     */
    private abstract static class AbstractSearchThread<N> 
            extends SleepingThread {

        /**
         * The ID of this thread.
         */
        protected final int id;

        /**
         * Holds the reference to the class responsible for computing the 
         * neighbour nodes of a given node.
         */
        protected final AbstractNodeExpander<N> nodeExpander;

        /**
         * The entire state of this search thread, shared possibly with other
         * threads working on the same search direction.
         */
        protected final SearchState<N> searchState;

        /**
         * The state shared by both the directions.
         */
        protected final SharedSearchState<N> sharedSearchState;

        /**
         * The progress logger.
         */
        protected final ProgressLogger<N> searchProgressLogger;

        /**
         * Caches the amount of nodes expanded by this thread.
         */
        protected int numberOfExpandedNodes;

        /**
         * Construct this search thread.
         * 
         * @param id                   the ID number of this thread. Must be
         *                             unique over <b>all</b> search threads.
         * @param nodeExpander         the node expander responsible for 
         *                             generating the neighbours in this search
         *                             thread.
         * @param searchState          the search state object.
         * @param sharedSearchState    the search state object shared with both
         *                             forward search threads and backward
         *                             search threads.
         * @param isMasterThread       indicates whether this search thread is a
         *                             master thread or a slave thread.
         * @param searchProgressLogger the progress logger for the search 
         *                             direction of this search thread.
         * @param threadSleepDuration  the duration of sleeping in milliseconds
         *                             always when a thread finds the frontier 
         *                             queue empty.
         * @param threadSleepTrials    the maximum number of hibernation trials
         *                             before a master thread gives up and 
         *                             terminates the entire search process. If
         *                             this thread is a slave thread, this 
         *                             parameter is ignored.
         */
        AbstractSearchThread(final int id,
                     final AbstractNodeExpander<N> nodeExpander,
                     final SearchState<N> searchState, 
                     final SharedSearchState<N> sharedSearchState,
                     final boolean isMasterThread,
                     final ProgressLogger<N> searchProgressLogger,
                     final int threadSleepDuration,
                     final int threadSleepTrials) {
            
            super(threadSleepDuration, 
                  threadSleepTrials, 
                  isMasterThread);
            
            this.id                   = id;
            this.nodeExpander         = nodeExpander;
            this.searchState          = searchState;
            this.sharedSearchState    = sharedSearchState;
            this.searchProgressLogger = searchProgressLogger;
        }

        @Override
        public void run() {
            while (true) {
                if (exit) {
                    return;
                }
                
                if (sleepRequested) {
                    mysleep(threadSleepDuration);
                    continue;
                }
                
                lock();
                N current = searchState.removeQueueHead();
                unlock();
                
                if (current != null) {
                    processCurrent(current);
                } else {
                    processEmptyQueue();
                }
            }
        }
        
        /**
         * {@inheritDoc }
         */
        @Override
        public boolean equals(final Object other) {
            if (other == null) {
                return false;
            }

            if (!getClass().equals(other.getClass())) {
                return false;
            }

            return id == ((AbstractSearchThread) other).id;
        }

        /**
         * {@inheritDoc }
         */
        @Override
        public int hashCode() {
            return id;
        }

        /**
         * {@inheritDoc }
         */
        @Override
        public String toString() {
            return "[Thread ID: " + id + ", master: " + isMasterThread + "]";
        }

        /**
         * Returns the number of nodes expanded by this search thread.
         * 
         * @return the number of nodes.
         */
        int getNumberOfExpandedNodes() {
            return numberOfExpandedNodes;
        }
        
        private void lock() {
            sharedSearchState.lock();
        }
        
        private void unlock() {
            sharedSearchState.unlock();
        }
        
        private void processCurrent(final N current) {
            if (isMasterThread) {
                processCurrentInMasterThread(current);
            } else {
                processCurrentInSlaveThread(current);
            }
        }
        
        private void processEmptyQueue() {
            if (isMasterThread) {
                processCurrentInMasterThread(null);
            } else {
                processCurrentInSlaveThread(null);
            }
        }
        
        /**
         * Processes the current node in the master thread.
         * 
         * @param head the candidate frontier queue head node.
         */
        private void processCurrentInMasterThread(final N head) {
            if (head != null) {
                return;
            }
            
            N currentHead = null;
            
            for (int trials = 0; trials < threadSleepTrials; trials++) {
                mysleep(threadSleepDuration);
                
                lock();
                currentHead = searchState.getQueueHead();
                unlock();
                
                if (currentHead != null) {
                    break;
                }
            }
            
            if (currentHead == null) {
                // We have run out of trials and the queue is still empty; halt.
                sharedSearchState.requestExit();
                sharedSearchState.killAllThreads();
            }
        }
        
        private void processCurrentInSlaveThread(final N current) {
            if (current == null) {
                // Nothing to do, go to sleep.
                searchState.putThreadToSleep(this);
                return;
            }
            
            searchState.wakeupAllThreads();
            lock();
            sharedSearchState.updateSearchState(current);
            
            if (sharedSearchState.pathIsOptimal()) {
                unlock();
                sharedSearchState.requestExit();
                sharedSearchState.killAllThreads();
                return;
            }
            
            unlock();
            
            if (searchProgressLogger != null) {
                searchProgressLogger.onExpansion(current);
            }
            
            numberOfExpandedNodes++;
            
            expand(current);
        }
        
        /**
         * Expands the current node.
         * 
         * @param current the node of which to generate the successor nodes.
         */
        private void expand(final N current) {
            for (final N successor : 
                    nodeExpander.generateSuccessors(current)) {
                
                lock();
                
                if (!searchState.trySetNodeInfo(
                        successor, 
                        current)) {
                    
                    if (searchProgressLogger != null) {
                        searchProgressLogger
                                .onNeighborGeneration(successor);
                    }
                } else {
                    if (searchProgressLogger != null) {
                        searchProgressLogger
                                .onNeighborImprovement(successor);
                    }
                    
                    searchState.tryUpdateIfImprovementPossible(
                            successor,
                            current);
                }
                
                unlock();
            }
        }
    }
    
    /**
     * This class implements a search thread searching in forward direction.
     */
    private static final class ForwardSearchThread<N> 
            extends AbstractSearchThread<N> {

        /**
         * Constructs a forward search thread.
         * 
         * @param id                   the ID of this thread. Must be unique 
         *                             over <b>all</b> search threads.
         * @param nodeExpander         the node expander responsible for 
         *                             generating the neighbour nodes of a given
         *                             node.
         * @param searchState          the search state object.
         * @param sharedSearchState    the shared search state object.
         * @param isMasterThread       indicates whether this thread is a master
         *                             or a slave thread.
         * @param searchProgressLogger the progress logger for logging the 
         *                             progress of this thread.
         * @param threadSleepDuration  the number of milliseconds to sleep 
         *                             whenever a thread finds the frontier 
         *                             queue empty.
         * @param threadSleepTrials    the maximum number of times a master
         *                             thread hibernates itself before giving 
         *                             up.
         */
        ForwardSearchThread(
                final int id,
                final AbstractNodeExpander<N> nodeExpander,
                final SearchState<N> searchState, 
                final SharedSearchState<N> sharedSearchState,
                final boolean isMasterThread,
                final ProgressLogger<N> searchProgressLogger,
                final int threadSleepDuration,
                final int threadSleepTrials) {
            
            super(id,
                  nodeExpander,
                  searchState, 
                  sharedSearchState,
                  isMasterThread,
                  searchProgressLogger,
                  threadSleepDuration,
                  threadSleepTrials);
        }
    }

    /**
     * This class implements a search thread searching in backward direction.
     */
    private static final class BackwardSearchThread<N> 
            extends AbstractSearchThread<N> {

        /**
         * Constructs a backward search thread.
         * 
         * @param id                   the ID of this thread. Must be unique 
         *                             over <b>all</b> search threads.
         * @param nodeExpander         the node expander responsible for 
         *                             generating the neighbour nodes of a given
         *                             node.
         * @param searchState          the search state object.
         * @param sharedSearchState    the shared search state object.
         * @param isMasterThread       indicates whether this thread a master or
         *                             a slave thread.
         * @param searchProgressLogger the progress logger for logging the 
         *                             progress of this thread.
         * @param threadSleepDuration  the number of milliseconds to sleep 
         *                             whenever a slave thread finds the
         *                             frontier queue empty.
         * @param threadSleepTrials    the maximum number of times a master
         *                             thread hibernates itself before giving 
         *                             up.
         */
        BackwardSearchThread(final int id,
                             final AbstractNodeExpander<N> nodeExpander,
                             final SearchState<N> searchState, 
                             final SharedSearchState<N> sharedSearchState,
                             final boolean isMasterThread,
                             final ProgressLogger<N> searchProgressLogger,
                             final int threadSleepDuration,
                             final int threadSleepTrials) {
           super(id,
                 nodeExpander,
                 searchState,
                 sharedSearchState,
                 isMasterThread,
                 searchProgressLogger,
                 threadSleepDuration,
                 threadSleepTrials);
        }
    }
    
    /**
     * This method puts the calling thread to sleep for {@code milliseconds}
     * milliseconds.
     * 
     * @param milliseconds the number of milliseconds to sleep for.
     */
    private static void mysleep(final int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (final InterruptedException ex) {
            LOGGER.log(Level.WARNING, "Interrupted while sleeping.");
        }
    }
}
