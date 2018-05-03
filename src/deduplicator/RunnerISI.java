package deduplicator;

import java.io.IOException;
import java.sql.SQLException;

public class RunnerISI {
    // Only accept ISI articles as input
    public static void main(String[] args) throws IOException, SQLException {
        int id = Integer.parseInt(args[0]);
        InterDeduplicator.deduplicate("isi", id);
    }
}
