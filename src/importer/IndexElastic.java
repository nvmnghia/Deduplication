package importer;

import config.Config;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import util.DataUtl;
import util.StringUtl;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class IndexElastic {

    // Create indices for subsequent processing
    static {
        try {
            DataUtl.deleteIndex("vci");
            System.out.println("Deleted ES index vci");
        } catch (Exception e) {
            if (! (e instanceof IndexNotFoundException)) {
                e.printStackTrace();
            }
        }
        try {
            DataUtl.createIndex("vci");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        try {
            DataUtl.deleteIndex("available_articles");
            System.out.println("Deleted ES index available_articles");
        } catch (Exception e) {
            if (! (e instanceof IndexNotFoundException)) {
                e.printStackTrace();
            }
        }
        try {
            DataUtl.createIndex("available_articles");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        try {
            DataUtl.deleteIndex("available_journals");
            System.out.println("Deleted ES index available_journals");
        } catch (Exception e) {
            if (! (e instanceof IndexNotFoundException)) {
                e.printStackTrace();
            }
        }
        try {
            DataUtl.createIndex("available_journals");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        try {
            DataUtl.deleteIndex("available_organizations");
            System.out.println("Deleted ES index available_organizations");
        } catch (Exception e) {
            if (! (e instanceof IndexNotFoundException)) {
                e.printStackTrace();
            }
        }
        try {
            DataUtl.createIndex("available_organizations");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * Index ISI and Scopus DB into ES. This index will be used as the input source for the deduplication process
     * @throws IOException
     * @throws SQLException
     */
    public static void indexCrawledISIAndScopus() throws IOException, SQLException {
        Client client = DataUtl.getESClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        // Index ISI
        // Shall never forget to escape MySQL keywords by backtick
        // Those fucker
        String query = "SELECT id, affiliation, author, doi, funding_text, isbn, issn, " +
                "journal, journal_iso, `language`, pages, publisher, title, " +
                "type, volume, year, authors_json, unique_id, abstract, cited_references, number, keyword FROM isi_documents";
        ResultSet articleSet = DataUtl.queryDB(Config.DB.INPUT, query);

        // Read the received ResultSet to Elastic
        int counter = 0;
        while (articleSet.next()) {
            bulkRequest.add(client.prepareIndex(Config.ES.INDEX, "articles", String.valueOf(counter++))
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("original_id", articleSet.getInt(1))
                            .field("affiliation", StringUtl.correct(articleSet.getString(2)))
                            .field("author", articleSet.getString(3))
                            .field("doi", articleSet.getString(4))
                            .field("issn", StringUtl.normalizeISSN(articleSet.getString(7)))
                            .field("journal", articleSet.getString(8))
                            .field("journal_iso", articleSet.getString(9))
                            .field("language", articleSet.getString(10))
                            .field("pages", articleSet.getString(11))
                            .field("publisher", articleSet.getString(12))
                            .field("title", articleSet.getString(13).replace("\n", " "))
                            .field("type", articleSet.getString(14))
                            .field("volume", articleSet.getString(15))
                            .field("year", articleSet.getString(16))
                            .field("authors_json", StringUtl.correct(articleSet.getString(17)))
                            .field("uri", articleSet.getString(18))
                            .field("abstract", articleSet.getString(19))
                            .field("reference", articleSet.getString(20))
                            .field("number", articleSet.getString(21))
                            .field("keyword", articleSet.getString(22))
                            .field("is_isi", true)
                            .field("is_scopus", false)
                            .endObject()
                    )
            );
        }

        System.out.println("Imported " + counter + " ISI\n");

        // Why this much data is indexed? Future-proof =)))))

        // Index Scopus
        // The input source just has to store ISI. Why Scopus is also indexed?
        // Future-proof too (merging ISI into Scopus) =)))))
        query = "SELECT id, `authors`, title, year, source_title, volume, page_start, " +
                "page_end, doi, affiliations, funding_text, " +
                "publisher, issn, isbn, language_of_original_document, " +
                "abbreviated_source_title, document_type, authors_json, abstract, `references`, index_keywords, link, issue FROM scopus_documents";

        articleSet = DataUtl.queryDB(Config.DB.INPUT, query);

        while (articleSet.next()) {
            bulkRequest.add(client.prepareIndex(Config.ES.INDEX, "articles", String.valueOf(counter++))
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("original_id", articleSet.getInt(1))
                            .field("affiliation", StringUtl.correct(articleSet.getString(10)))
                            .field("author", articleSet.getString(2))
                            .field("doi", articleSet.getString(9))
                            .field("funding_text", articleSet.getString(11))
                            .field("isbn", articleSet.getString(14))
                            .field("issn", StringUtl.normalizeISSN(articleSet.getString(13)))
                            .field("journal", articleSet.getString(5))
                            .field("journal_iso", articleSet.getString(16))
                            .field("language", articleSet.getString(15))
                            .field("pages", StringUtl.getPages(articleSet.getString(7), articleSet.getString(8)))
                            .field("publisher", articleSet.getString(12))
                            .field("title", articleSet.getString(3))
                            .field("type", articleSet.getString(17))
                            .field("volume", articleSet.getString(6))
                            .field("year", articleSet.getString(4))
                            .field("authors_json", StringUtl.correct(articleSet.getString(18)))
                            .field("abstract", articleSet.getString(19))
                            .field("reference", articleSet.getString(20))
                            .field("keyword", articleSet.getString(21))
                            .field("uri", articleSet.getString(22))
                            .field("number", articleSet.getString(23))
                            .field("is_isi", false)
                            .field("is_scopus", true)
                            .endObject()
                    )
            );
        }

        System.out.println("Imported " + counter + " ISI + Scopus");

        bulkRequest.get();
        DataUtl.flushES(Config.ES.INDEX);
    }

    /**
     * Index all available articles into ES
     *
     * @throws IOException
     * @throws SQLException
     */
    public static void indexAvailableArticles() throws IOException, SQLException {
        String query = "SELECT ar.id, ar.title, ar.year, j.name, ar.doi, ar.is_isi, ar.is_scopus, ar.uri, ar.journal_id FROM articles ar " +
                "JOIN journals j ON ar.journal_id = j.id";
        ResultSet articleSet = DataUtl.queryDB(Config.DB.OUPUT, query);

        Client client = DataUtl.getESClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        while (articleSet.next()) {
            bulkRequest.add(client.prepareIndex("available_articles", "articles", String.valueOf(articleSet.getInt(1)))
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("original_id", articleSet.getInt(1))
                            .field("title", articleSet.getString(2))
                            .field("year", articleSet.getString(3))
                            .field("journal", articleSet.getString(4))
                            .field("doi", articleSet.getString(5))
                            .field("is_isi", articleSet.getString(6) != null && articleSet.getString(6).equals("1"))
                            .field("is_scopus", articleSet.getString(7) != null && articleSet.getString(7).equals("1"))
                            .field("uri", articleSet.getString(8))
                            .field("journal_id", articleSet.getInt(9))
                            .endObject()
                    )
            );
        }

        bulkRequest.get();
        DataUtl.flushES("available_articles");
    }

    /**
     * Index all journals of the output DB into ES
     *
     * @throws IOException
     * @throws SQLException
     */
    public static void indexAvailableJournals() throws IOException, SQLException {
        String query = "SELECT id, name, issn, is_isi, is_scopus, is_vci from journals";
        ResultSet articleSet = DataUtl.queryDB(Config.DB.OUPUT, query);

        Client client = DataUtl.getESClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        while (articleSet.next()) {
            bulkRequest.add(client.prepareIndex("available_journals", "articles", String.valueOf(articleSet.getInt(1)))
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("original_id", articleSet.getInt(1))
                            .field("name", articleSet.getString(2))
                            .field("issn", StringUtl.normalizeISSN(articleSet.getString(3)))
                            .field("is_isi", articleSet.getString(4) != null && articleSet.getString(4).equals("1"))
                            .field("is_scopus", articleSet.getString(5) != null && articleSet.getString(5).equals("1"))
                            .field("is_vci", articleSet.getString(6) != null && articleSet.getString(6).equals("1"))
                            .endObject()
                    )
            );
        }

        bulkRequest.get();
        DataUtl.flushES("available_journals");
    }

    /**
     * Index all organizations of the output DB into ES
     *
     * @throws IOException
     * @throws SQLException
     */
    public static void indexAvailableOrganizations() throws IOException, SQLException {
        String query = "SELECT id, name FROM organizes";
        ResultSet articleSet = DataUtl.queryDB(Config.DB.OUPUT, query);

        Client client = DataUtl.getESClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        while (articleSet.next()) {
            bulkRequest.add(client.prepareIndex("available_organizations", "articles", articleSet.getString(1))
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("original_id", articleSet.getInt(1))
                            .field("name", articleSet.getString(2))
                            .endObject()
                    )
            );
        }

        bulkRequest.get();
        DataUtl.flushES("available_organizations");
    }

    public static void cleanTemporaryIndices() {
        try {
            DataUtl.deleteIndex("vci");
            System.out.println("Deleted ES index vci");
        } catch (Exception e) {
            if (!(e instanceof IndexNotFoundException)) {
                e.printStackTrace();
            }
        }

        try {
            DataUtl.deleteIndex("available_articles");
            System.out.println("Deleted ES index available_articles");
        } catch (Exception e) {
            if (! (e instanceof IndexNotFoundException)) {
                e.printStackTrace();
            }
        }

        try {
            DataUtl.deleteIndex("available_journals");
            System.out.println("Deleted ES index available_journals");
        } catch (Exception e) {
            if (! (e instanceof IndexNotFoundException)) {
                e.printStackTrace();
            }
        }

        try {
            DataUtl.deleteIndex("available_organizations");
            System.out.println("Deleted ES index available_organizations");
        } catch (Exception e) {
            if (! (e instanceof IndexNotFoundException)) {
                e.printStackTrace();
            }
        }
    }
}
