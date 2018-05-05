package importer;

import config.Config;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import util.DataUtl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ImportElastic {
    public static void main(String[] args) throws IOException, SQLException {
        TransportAddress address = new TransportAddress(InetAddress.getByName("localhost"), 9300);
        Settings settings = Settings.builder()
                .put("cluster.name", Config.ES_CLUSTER_NAME)
                .build();

        org.elasticsearch.client.Client client = new PreBuiltTransportClient(settings).addTransportAddress(address);

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        // Shall never forget to escape MySQL keywords by backtick
        // Those fucker

        // Import ISI
        String query = "SELECT id, affiliation, author, doi, funding_text, isbn, issn, " +
                        "journal, journal_iso, `language`, pages, publisher, title, " +
                        "type, volume, year, authors_json, unique_id, abstract, cited_references, number, keyword FROM isi_documents";

        ResultSet articleSet = DataUtl.queryDB("isi", query);

        // Read the received ResultSet to Elastic
        int counter = 0;
        while (articleSet.next()) {
            bulkRequest.add(client.prepareIndex(Config.ES_INDEX, "articles", String.valueOf(counter++))
                .setSource(jsonBuilder()
                    .startObject()
                        .field("original_id", articleSet.getInt(1))
                        .field("affiliation", articleSet.getString(2))
                        .field("author", articleSet.getString(3))
                        .field("doi", articleSet.getString(4))
                        .field("issn", normalizeISSN(articleSet.getString(7)))
                        .field("journal", articleSet.getString(8))
                        .field("journal_iso", articleSet.getString(9))
                        .field("language", articleSet.getString(10))
                        .field("pages", articleSet.getString(11))
                        .field("publisher", articleSet.getString(12))
                        .field("title", articleSet.getString(13))
                        .field("type", articleSet.getString(14))
                        .field("volume", articleSet.getString(15))
                        .field("year", articleSet.getString(16))
                        .field("authors_json", articleSet.getString(17))
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


        // Import Scopus
        query = "SELECT id, `authors`, title, year, source_title, volume, page_start, " +
                "page_end, doi, affiliations, funding_text, " +
                "publisher, issn, isbn, language_of_original_document, " +
                "abbreviated_source_title, document_type, authors_json, abstract, `references`, index_keywords, link, issue FROM scopus_documents";

        articleSet = DataUtl.queryDB("scopus", query);

        while (articleSet.next()) {
            bulkRequest.add(client.prepareIndex(Config.ES_INDEX, "articles", String.valueOf(counter++))
                .setSource(jsonBuilder()
                    .startObject()
                        .field("original_id", articleSet.getInt(1))
                        .field("affiliation", articleSet.getString(10))
                        .field("author", articleSet.getString(2))
                        .field("doi", articleSet.getString(9))
                        .field("funding_text", articleSet.getString(11))
                        .field("isbn", articleSet.getString(14))
                        .field("issn", normalizeISSN(articleSet.getString(13)))
                        .field("journal", articleSet.getString(5))
                        .field("journal_iso", articleSet.getString(16))
                        .field("language", articleSet.getString(15))
                        .field("pages", getPages(articleSet.getString(7), articleSet.getString(8)))
                        .field("publisher", articleSet.getString(12))
                        .field("title", articleSet.getString(3))
                        .field("type", articleSet.getString(17))
                        .field("volume", articleSet.getString(6))
                        .field("year", articleSet.getString(4))
                        .field("authors_json", articleSet.getString(18))
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

        // Import available articles
        query = "SELECT ar.id, ar.title, ar.year, j.name, ar.doi, ar.is_isi, ar.is_scopus FROM articles ar " +
                "JOIN journals j ON ar.journal_id = j.id";

        articleSet = DataUtl.queryDB("vci_scholar", query);

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
                    .endObject()
                )
            );
        }

        // Import available journals
        query = "SELECT id, name, issn, is_isi, is_scopus, is_vci from journals";

        articleSet = DataUtl.queryDB("vci_scholar", query);

        while (articleSet.next()) {
            bulkRequest.add(client.prepareIndex("available_journals", "articles", String.valueOf(articleSet.getInt(1)))
                .setSource(jsonBuilder()
                    .startObject()
                        .field("original_id", articleSet.getInt(1))
                        .field("name", articleSet.getString(2))
                        .field("issn", normalizeISSN(articleSet.getString(3)))
                        .field("is_isi", articleSet.getString(4) != null && articleSet.getString(4).equals("1"))
                        .field("is_scopus", articleSet.getString(5) != null && articleSet.getString(5).equals("1"))
                        .field("is_vci", articleSet.getString(6) != null && articleSet.getString(6).equals("1"))
                    .endObject()
                )
            );

            System.out.println(articleSet.getString(4));
        }

        // Import available organizations
        query = "SELECT id, name FROM organizes";

        articleSet = DataUtl.queryDB("vci_scholar", query);

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

        BulkResponse bulkResponse = bulkRequest.get();

        System.out.println("\ncounter " + counter);

    }

    private static String getPages(String start, String finish) {
        if (start != null && finish != null) {
            return start + "-" + finish;
        }

        return null;
    }

    private static String normalizeISSN(String ISSN) {
        if (ISSN == null || ISSN.length() < 8) {
            return null;
        }

        ISSN = ISSN.replace('x', 'X').trim();

        if (ISSN.length() == 8) {
            return ISSN.substring(0, 4) + "-" + ISSN.substring(4, 8);
        } else if (ISSN.length() == 9) {
            return ISSN;
        } else {
            return null;
        }
    }
}
