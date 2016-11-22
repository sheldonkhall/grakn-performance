import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    final int numberOfThreads = 12; // size of the thread pool for concurrent queries
    final int rateOfQuery = 50; // the number of milliseconds between executing queries
    final int runTime = 1000; // the total number of milliseconds to run the test
    final String filepathPersonId = "/tmp/matchGetInstance.csv";
    final String filepathRelationId = "/tmp/matchGetRelation.csv";
    final AtomicLong totalTime = new AtomicLong(0L);
    final AtomicLong queryNumber = new AtomicLong(0L);

    public void queryLoadTesting() throws InterruptedException, IOException {
        final GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, "grakn").getGraph();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(numberOfThreads);
        final FileReader personIdReader = new FileReader(filepathPersonId);
        final FileReader relationIdReader = new FileReader(filepathRelationId);

        // save ids in memory
        List<CSVRecord> personIdsList = CSVFormat.DEFAULT.parse(personIdReader).getRecords();
        List<CSVRecord> relationIdsList = CSVFormat.DEFAULT.withDelimiter('|').parse(relationIdReader).getRecords();

        // get iterators
        Iterator<CSVRecord> personIds = personIdsList.iterator();
        Iterator<CSVRecord> relationIds = relationIdsList.iterator();

        // load the queries into a queue randomly selecting between query type
        final Queue<MatchQuery> queries = new ConcurrentLinkedQueue<>();
        int totalNumberQueries = numberOfThreads * runTime / rateOfQuery;
        for (int i=0; i<totalNumberQueries; i++) {
            int picker = new Random().nextInt(2);
            switch (picker) {
                case 0:
                    if (!personIds.hasNext()) {
                        personIds = personIdsList.iterator();
                    }
                    String personId = personIds.next().get(0);
                    queries.add(getEntityById(personId));
                case 1:
                    if (!relationIds.hasNext()) {
                        relationIds = relationIdsList.iterator();
                    }
                    CSVRecord relationId = relationIds.next();
                    queries.add(getRelationById(relationId.get(0),relationId.get(1)));
                default: new RuntimeException("random number is bad");
            }
        }

        // get rid of the rest
        personIdsList.clear();
        relationIdsList.clear();

        // set up the scheduled query executors
        Set<ScheduledFuture<?>> handles = new HashSet<>();
        for (int i=0; i<numberOfThreads; i++) {
            handles.add(scheduler.scheduleAtFixedRate(
                    () -> executeQuery(graph, queries), rateOfQuery, rateOfQuery, TimeUnit.MILLISECONDS));
        }

        // schedule the jobs to be terminated
        scheduler.schedule(() -> {
            for (ScheduledFuture handle:handles) {
                handle.cancel(true);
            }
        }, runTime, TimeUnit.MILLISECONDS);

        Thread.sleep(runTime);

        scheduler.shutdown();
        scheduler.awaitTermination(rateOfQuery,TimeUnit.MILLISECONDS);

        System.out.println("The total number of queries executed is: "+String.valueOf(queryNumber.get()));
        System.out.println("The average query execution time is: "+String.valueOf((double) totalTime.get()/(double) queryNumber.get())+" ms");
        System.out.println("The rate of query execution is: "+String.valueOf((double) queryNumber.get()/(double) runTime*1000.0)+" s^-1");
        System.out.println("The run time for this test is: "+String.valueOf(runTime/1000)+" s");
    }

    private void executeQuery(GraknGraph graph, Queue<MatchQuery> queries) {
        Long startTime = System.currentTimeMillis();
        System.out.println(queries.poll().withGraph(graph).execute());
        Long runTime = System.currentTimeMillis() - startTime;
        totalTime.getAndAdd(runTime);
        queryNumber.getAndIncrement();
    }

    private MatchQuery getEntityById(String id) {
        QueryBuilder qb = Graql.withoutGraph();
        return qb.match(var("x").isa("person").has("snb-id", id));
    }

    private MatchQuery getRelationById(String id1, String id2) {
        QueryBuilder qb = Graql.withoutGraph();
        return qb.match(
                var("x").isa("comment").has("snb-id", id1),
                var("y").isa("comment").has("snb-id", id2),
                var("z").rel("x").rel("y"));
    }

}
