package config;

public class Config {
    public static boolean ENABLE_BITAP = true;

    public enum SOURCE {
        ES_TRANSPORT_6_2_3,
        ES_REST,
        DB
        }
    private static SOURCE DATA_SOURCE = SOURCE.ES_TRANSPORT_6_2_3;

    // ES - Transport
    public static String ES_CLUSTER_NAME = "vci-scholar";
    public static String ES_INDEX = "vci";
    public static String ES_TYPE = "isi";

    public static String ES_URL = "localhost";
    public static int ES_PORT = 9300;

    public static String ES_SEARCH_URI = Config.ES_URL + ":" + Config.ES_PORT + "/" + Config.ES_INDEX + "/" + Config.ES_TYPE + "/_search?q=";

    // DB
    public static String DB_NAME = "vci_scholar";


    // Field list
    public static String FIELD_ID, FIELD_TITLE, FIELD_AUTHORS, FIELD_DOI, FIELD_YEAR, FIELD_JOURNAL, FIELD_VOLUME, FIELD_ISSN, FIELD_ISI, FIELD_SCOPUS;

    static {
        setDataSourceES_TRANSPORT();
    }

    public static SOURCE getDataSource() {
        return DATA_SOURCE;
    }

    public static void setDataSource(SOURCE dataSource) {
        DATA_SOURCE = dataSource;

        switch (DATA_SOURCE) {
            case ES_TRANSPORT_6_2_3:
                setDataSourceES_TRANSPORT();
                break;

            case ES_REST:
                setDataSourceES_REST();
                break;

            case DB:
                break;
        }
    }

    private static void setDataSourceES_TRANSPORT() {
        FIELD_ID = "original_id";
        FIELD_TITLE = "title";
        FIELD_AUTHORS = "author";
        FIELD_DOI = "doi";
        FIELD_YEAR = "year";
        FIELD_JOURNAL = "journal";
        FIELD_VOLUME = "volume";
        FIELD_ISSN = "issn";
        FIELD_ISI = "is_isi";
        FIELD_SCOPUS = "is_scopus";
    }

    private static void setDataSourceES_REST() {
        FIELD_ID = "id";
        FIELD_TITLE = "title";
        FIELD_AUTHORS = "authors";
        FIELD_DOI = "doi";
        FIELD_YEAR = "year";
        FIELD_JOURNAL = "journal";
        FIELD_VOLUME = "volume";
        FIELD_ISSN = "";
        FIELD_ISI = "is_isi";
        FIELD_SCOPUS = "is_scopus";

    }
}
