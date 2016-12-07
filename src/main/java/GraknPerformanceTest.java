import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.graph.internal.AbstractGraknGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Pattern;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static ai.grakn.graql.Graql.*;

/**
 *
 */
public class GraknPerformanceTest {

    int numberOfThreads = 16; // size of the thread pool for concurrent queries
    int rateOfQuery = 1000; // the number of milliseconds between executing queries
    int runTime = 3600000; // the total number of milliseconds to run the test
    int totalNumberQueries;
    long startTime;
    final int defaultResultLimit = 100; // a default number of results for long queries
    final int updatePeriod = 60000; // frequency of updates on progress
    final String filepathPersonId = "/tmp/matchGetInstance.csv";
    final String filepathRelationId = "/tmp/matchGetRelation.csv";
    final AtomicLong totalTime = new AtomicLong(0L);
    final AtomicLong queryNumber = new AtomicLong(0L);
    final Logger LOGGER;

    GraknPerformanceTest() {
        // prepare logger
        ((Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)).setLevel(Level.OFF);
        LOGGER = (Logger) org.slf4j.LoggerFactory.getLogger(GraknPerformanceTest.class);
        LOGGER.setLevel(Level.DEBUG);
    }

    GraknPerformanceTest(int numberOfThreads, int rateOfQuery, int runTime, boolean debug) {
        this();
        this.numberOfThreads = numberOfThreads;
        this.rateOfQuery = rateOfQuery;
        this.runTime = runTime;
        if (!debug) LOGGER.setLevel(Level.INFO);
    }

    public void queryLoadTesting() throws Exception {
        final GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, "grakn").getGraph();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(numberOfThreads);
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final FileReader personIdReader = new FileReader(filepathPersonId);
        final FileReader relationIdReader = new FileReader(filepathRelationId);
        totalNumberQueries = numberOfThreads * runTime / rateOfQuery;

        // save ids in memory
        List<CSVRecord> personIdsList = CSVFormat.DEFAULT.parse(personIdReader).getRecords();
        List<CSVRecord> relationIdsList = CSVFormat.DEFAULT.withDelimiter('|').parse(relationIdReader).getRecords();

        // get iterators
        Iterator<CSVRecord> personIds = personIdsList.iterator();
        Iterator<CSVRecord> relationIds = relationIdsList.iterator();

        // load the queries into a queue randomly selecting between query type
        final Queue<Pattern> queries = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < totalNumberQueries; i++) {
            int picker = new Random().nextInt(5);
            String personId = null;
            CSVRecord relationId = null;
            switch (picker) {
                case 0:
                    if (!personIds.hasNext()) {
                        personIds = personIdsList.iterator();
                    }
                    personId = personIds.next().get(0);
                    queries.add(getEntityById("x", personId));
                    break;
                case 1:
                    if (!personIds.hasNext()) {
                        personIds = personIdsList.iterator();
                    }
                    personId = personIds.next().get(0);
                    queries.add(getFriendsOfEntity("x", personId));
                    break;
                case 2:
                    if (!personIds.hasNext()) {
                        personIds = personIdsList.iterator();
                    }
                    personId = personIds.next().get(0);
                    queries.add(getMessagesOfEntity("x", personId));
                    break;
                case 3:
                    if (!relationIds.hasNext()) {
                        relationIds = relationIdsList.iterator();
                    }
                    relationId = relationIds.next();
                    queries.add(getRelationById("z", relationId.get(0), relationId.get(1)));
                    break;
                case 4:
                    if (!relationIds.hasNext()) {
                        relationIds = relationIdsList.iterator();
                    }
                    relationId = relationIds.next();
                    queries.add(getRepliesToMessageAndKnowsCreator("z", relationId.get(0), relationId.get(1)));
                    break;
                default:
                    new RuntimeException("random number is bad");
            }
        }

        // get rid of the rest
        personIdsList.clear();
        relationIdsList.clear();
        personIdReader.close();
        relationIdReader.close();

        // set up the scheduled query executors
        Set<ScheduledFuture<?>> handles = new HashSet<>();
        for (int i = 0; i < numberOfThreads; i++) {
            handles.add(scheduler.scheduleAtFixedRate(
                    () -> executeQueryFromQueue(graph, queries), rateOfQuery, rateOfQuery, TimeUnit.MILLISECONDS));
        }

        // schedule the jobs to be terminated
        scheduler.schedule(() -> {
            for (ScheduledFuture handle : handles) {
                handle.cancel(true);
            }
        }, runTime, TimeUnit.MILLISECONDS);

        // schedule an update of the statistics
        scheduler.scheduleAtFixedRate(() -> {
            logCurrentPerformance();
        }, updatePeriod, updatePeriod, TimeUnit.MILLISECONDS);

//        executor.submit(() -> continuousExecuteQuery(graph, queries, executor));

        LOGGER.info("Started Querying");
        startTime = System.currentTimeMillis();
        Thread.sleep(runTime);
        LOGGER.info("Runtime Over");

//        executor.shutdown();
//        executor.awaitTermination(1000, TimeUnit.MILLISECONDS);

        scheduler.shutdown();
        boolean completed = scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS);
        while (!completed) {
            completed = scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS);
            LOGGER.debug("Waiting to finish queries.");
        }

        logCurrentPerformance();
        LOGGER.info("The run time for this test is: " + String.valueOf(runTime / 1000) + " s");

        ((AbstractGraknGraph) graph).getTinkerPopGraph().close();
    }

    private void executeSingleQuery(GraknGraph graph, Pattern query) {
        Long startTime = System.currentTimeMillis();
        LOGGER.debug("Start query at: " + startTime);
        List<Map<String, Concept>> result = graph.graql().match(query).limit(defaultResultLimit).execute();
        LOGGER.debug(String.valueOf("Got result of size: " + result.size()));
        Long runTime = System.currentTimeMillis() - startTime;
        LOGGER.debug("Ended query after: " + runTime);
        totalTime.getAndAdd(runTime);
        queryNumber.getAndIncrement();
    }

    private void executeQueryFromQueue(GraknGraph graph, Queue<Pattern> queries) {
        executeSingleQuery(graph, queries.poll());
    }

    private void continuousExecuteQuery(GraknGraph graph, Queue<Pattern> queries, ExecutorService executor) {
        Pattern currentQuery = queries.poll();
        if (currentQuery!=null) {
            executeSingleQuery(graph, currentQuery);
        } else {
            executor.shutdown();
        }
    }

    private Pattern getEntityById(String varName, String id) {
        return var(varName).isa("person").has("snb-id", id);
    }

    private Pattern getRelationById(String varName, String id1, String id2) {
        assert varName != "y";
        assert varName != "x";
        return and(var("x").isa("comment").has("snb-id", id1),
                var("y").isa("comment").has("snb-id", id2),
                var(varName).rel("x").rel("y"));
    }

    private Pattern getRepliesToMessageAndKnowsCreator(String varName, String id1, String id2) {
        return and(getRelationById(varName, id1, id2),
                var("p1").isa("person"),
                var("p2").isa("person"),
                var().rel("p1").rel("p2").isa("knows"),
                var().rel("p1").rel("id1").isa("writes"),
                var().rel("p2").rel("id2").isa("writes"));
    }

    private Pattern getFriendsOfEntity(String varName, String id) {
        assert varName != "z";
        return and(getEntityById(varName, id), var("z").isa("person"), var().rel(varName).rel("z").isa("knows"));
    }

    private Pattern getMessagesOfEntity(String varName, String id) {
        assert varName != "z";
        return and(getEntityById(varName, id), var("z").isa("message"), var().rel(varName).rel("z").isa("writes"));
    }

    private void logCurrentPerformance() {
        LOGGER.info("The total number of queries attempted is: " + String.valueOf(totalNumberQueries));
        LOGGER.info("The total number of queries completed is: " + String.valueOf(queryNumber.get()));
        LOGGER.info("The average query execution time is: " + String.valueOf((double) totalTime.get() / (double) queryNumber.get()) + " ms");
        LOGGER.info("The rate of query execution is: " + String.valueOf((double) queryNumber.get() / (double) (System.currentTimeMillis()-startTime) * 1000.0) + " s^-1");
    }

}
