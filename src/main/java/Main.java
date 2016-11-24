import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class Main {
    public static void main(String[] args) {
        try {
            if (args.length==4) {
                int numberOfThreads = Integer.valueOf(args[0]);
                int rateOfQuery = Integer.valueOf(args[1]);
                int runTime = Integer.valueOf(args[2]);
                boolean debug = Boolean.valueOf(args[3]);
                new GraknPerformanceTest(numberOfThreads, rateOfQuery, runTime, debug).queryLoadTesting();
            } else {
                new GraknPerformanceTest().queryLoadTesting();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
