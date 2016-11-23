import java.io.IOException;

/**
 *
 */
public class Main {
    public static void main(String[] args) {
        try {
            (new GraknPerformanceTest()).queryLoadTesting();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
