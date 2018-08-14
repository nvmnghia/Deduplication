package config;

public class Config {
    public static final boolean ENABLE_BITAP = true;

    // ES - Transport mode
    public static final class ES {
        public static final String INDEX = "vci";
        public static final String TYPE = "isi";

        public static final String URL = "localhost";
        public static final int PORT = 9300;
        public static final String SEARCH_URI = Config.ES.URL + ":" + Config.ES.PORT + "/" + Config.ES.INDEX + "/" + Config.ES.TYPE + "/_search?q=";
    }

    public static final class DB {
        public static final String INPUT = "isi_documents";
        public static final String OUTPUT = "vci_scholar";

        public static final String USERNAME = "root";
        public static final String PASSWORD = "";

        // Always check for this
        public static final boolean NUCLEAR_OPTION = true;
    }

    public static final int START_IMPORT_ISI_FROM = 0;
    public static final int START_IMPORT_SCOPUS_FROM = 0;

    // Look at the comments at the end of Deduplicator.deduplicate()
    public static final boolean CONTINUOUS_MATCHING = false;
}
