package config;

public class Config {
    public static boolean ENABLE_BITAP = true;

    // ES - Transport mode
    public static class ES {
        public static String INDEX = "vci";
        public static String TYPE = "isi";

        public static String URL = "localhost";
        public static int PORT = 9300;
        public static String SEARCH_URI = Config.ES.URL + ":" + Config.ES.PORT + "/" + Config.ES.INDEX + "/" + Config.ES.TYPE + "/_search?q=";
    }

    public static class DB {
        public static String DBNAME = "vci_scholar";

        public static String USERNAME = "root";
        public static String PASSWORD = "";
    }

    public static int START_IMPORT_ISI_FROM = 0;
    public static int START_IMPORT_SCOPUS_FROM = 0;
}
