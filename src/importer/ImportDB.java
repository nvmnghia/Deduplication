package importer;

import com.google.gson.Gson;
import comparator.Lev;
import config.Config;
import data.Article;
import data.Author;
import data.Organization;
import deduplicator.Deduplicator;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import util.DataUtl;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Import Article to DB
 * Roughly equivalent to these ones from tuantmtb
 * https://github.com/tuantmtb/vci-scholar/blob/master/database/seeds/ImportISIArticle.php
 * https://github.com/tuantmtb/vci-scholar/blob/master/database/seeds/ImportScopusArticle.php
 *
 * This implementation is a convoluted version of tuantmtb's merging and importing code
 * If the article isNeedInsert, call insert()
 */

public class ImportDB {

    private static int numOfOrganization;

    static {
        try {
            numOfOrganization = getNumOfOrganizations();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Main insert function
     * One call to rule them all
     * First insert the article (theirs journals will be inserted if needed)
     * Then insert the authors (theirs organizations will be inserted if needed)
     * Then link articles to theirs authors
     */
    private static PreparedStatement pstmInsertArticle = null;
    private static PreparedStatement pstmInsertArticleAuthors = null;
    public static void createArticle(Article article) throws IOException, SQLException {
        // Insert the article
        if (pstmInsertArticle == null) {
            pstmInsertArticle = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.articles (title, author, volume, number, year, uri, abstract, usable, reference, journal_id, language, is_reviewed, keyword, doi, document_type, is_scopus, is_isi, is_vci, is_international, slug) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        }

        pstmInsertArticle.setString(1, article.getTitle());
        pstmInsertArticle.setString(2, article.getRawAuthorStr());
        pstmInsertArticle.setString(3, article.getVolume());
        pstmInsertArticle.setString(4, article.getNumber());
        pstmInsertArticle.setString(5, String.valueOf(article.getYear()));
        pstmInsertArticle.setString(6, article.getURI());
        pstmInsertArticle.setString(7, article.getAbstract());
        pstmInsertArticle.setInt(8, 1);
        pstmInsertArticle.setString(9, article.getReference());
        pstmInsertArticle.setInt(10, findOrCreateJournal(article));
        pstmInsertArticle.setString(11, "en");
        pstmInsertArticle.setInt(12, 1);
        pstmInsertArticle.setString(13, article.getKeywords());
        pstmInsertArticle.setString(14, article.getDOI());
        pstmInsertArticle.setString(15, article.getType());
        pstmInsertArticle.setInt(16, article.isScopus() ? 1 : 0);
        pstmInsertArticle.setInt(17, article.isISI() ? 1 : 0);
        pstmInsertArticle.setInt(18, article.isVCI() ? 1 : 0);
        pstmInsertArticle.setInt(19, 1);
        pstmInsertArticle.setString(20, "add-slug-here");

        pstmInsertArticle.executeUpdate();

        int articleID = DataUtl.getAutoIncrementID(pstmInsertArticle);

        // Link articles and authors
        if (pstmInsertArticleAuthors == null) {
            pstmInsertArticleAuthors = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.articles_authors (author_id, article_id) VALUES(?, ?)");
        }

        List<Integer> authorIDs = createAuthors(article);

        for (Integer authorID : authorIDs) {
            pstmInsertArticleAuthors.setInt(1, authorID);
            pstmInsertArticleAuthors.setInt(2, articleID);

            pstmInsertArticleAuthors.addBatch();
        }
        pstmInsertArticleAuthors.executeBatch();

        System.out.println("created " + (article.isScopus() ? "scopus " : "isi ") + articleID + "    " + article.getTitle());
    }

    /**
     * Equivalent to createOrFindJournal
     * Get the id of the journal given its article
     * If the journal is found, returns its id
     * If not, create a new journal, insert it into DB, index it into ES, and then returns the id
     *
     * @param article
     * @return the id of the found or newly created journal
     * @throws UnknownHostException
     */
    private static PreparedStatement pstmInsertJournal = null;
    public static int findOrCreateJournal(Article article) throws IOException, SQLException {
        if (article.getJournal() == null) {
            // AKA "Chưa phân loại"
            return 1;
        }

        if (article.getISSN() != null) {
            QueryBuilder builder = QueryBuilders.matchQuery("issn", article.getISSN());
            SearchHits hits = DataUtl.queryES("available_journals", builder);

            for (SearchHit hit : hits) {
                Map map = hit.getSourceAsMap();
                String hitISSN = (String) map.get("issn");

                // God almighty
                if (hitISSN != null && hitISSN.equals(article.getISSN())) {
                    boolean is_isi = map.get("is_isi").equals(Boolean.TRUE);
                    boolean is_scopus = map.get("is_scopus").equals(Boolean.TRUE);

                    if (is_isi != article.isISI() || is_scopus != article.isScopus()) {
                        // Need to update
                        updateJournal(article, (Integer) map.get("original_id"));
                    }

                    article.setVCI(map.get("is_vci").equals(Boolean.TRUE));

                    return (Integer) map.get("original_id");
                }
            }
        }

        QueryBuilder builder = QueryBuilders.matchQuery("name", article.getJournal());
        SearchHits hits = DataUtl.queryES("available_journals", builder);

        for (SearchHit hit : hits) {
            // The condition is quite strict, as journal names are messy and need a separate deduplication
            // - Only the closest match is examined
            // - error_threshold is pretty low

            Map map = hit.getSourceAsMap();

            String issn = (String) map.get("issn");    // Ha ha ha java in a nutshell: Foo bar = (Foo) null;
            String name = (String) map.get("name");

            if (Lev.distanceNormStr(article.getJournal(), name) < 0.1d) {
                boolean is_isi = map.get("is_isi").equals(Boolean.TRUE);
                boolean is_scopus = map.get("is_scopus").equals(Boolean.TRUE);

                if (issn != null && article.getISSN() == null) {
                    article.setISSN(issn);
                }

                if (is_isi != article.isISI() || is_scopus != article.isScopus() || issn == null) {
                    // Need update
                    updateJournal(article, (Integer) map.get("original_id"));
                }

                article.setVCI(map.get("is_vci").equals(Boolean.TRUE));

                return (Integer) map.get("original_id");
            }
        }

        // No match, create a new one
        if (pstmInsertJournal == null) {
            pstmInsertJournal = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.journals (name, issn, is_scopus, is_isi, is_vci, is_international, type, slug, type_platform, archive_url, is_new_article) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        }

        pstmInsertJournal.setString(1, article.getJournal());
        pstmInsertJournal.setString(2, article.getISSN());
        pstmInsertJournal.setInt(3, article.isScopus() ? 1 : 0);
        pstmInsertJournal.setInt(4, article.isISI() ? 1 : 0);
        pstmInsertJournal.setInt(5, 0);
        pstmInsertJournal.setInt(6, 1);
        pstmInsertJournal.setString(7, article.getType());
        pstmInsertJournal.setString(8, "add-slug-here");
        pstmInsertJournal.setString(9, "");
        pstmInsertJournal.setString(10, "");
        pstmInsertJournal.setInt(11, 0);

        pstmInsertJournal.executeUpdate();

        int journalID = DataUtl.getAutoIncrementID(pstmInsertJournal);

        // Index it in ES
        DataUtl.indexES("available_journals", "articles", String.valueOf(journalID),
            jsonBuilder()
                .startObject()
                    .field("original_id", journalID)
                    .field("name", article.getJournal())
                    .field("issn", article.getISSN())
                    .field("is_isi", article.isISI())
                    .field("is_scopus", article.isScopus())
                    .field("is_vci", false)
                .endObject()
        );

        return journalID;
    }

    /**
     * Update journal information
     * If an article is duplicated in both ISI and Scopus data, its journal must also be marked is_isi and is_scopus
     * If an article has a text issn
     *
     * @param article
     * @param journalID
     * @param isNeedUpdateISSN
     * @throws SQLException
     * @throws IOException
     */
    private static PreparedStatement pstmUpdateJournal = null;
    public static void updateJournal(Article article, int journalID) throws SQLException, IOException {
        // Update in DB
        if (pstmUpdateJournal == null) {
            pstmUpdateJournal = DataUtl.getDBConnection().prepareStatement("UPDATE vci_scholar.journals SET is_isi = ?, is_scopus = ?, issn = ? WHERE id = ?");
        }

        pstmUpdateJournal.setInt(1, article.isISI() ? 1 : 0);
        pstmUpdateJournal.setInt(2, article.isScopus() ? 1 : 0);
        pstmUpdateJournal.setString(3, article.getISSN());
        pstmUpdateJournal.setInt(4, journalID);

        pstmUpdateJournal.executeUpdate();

        // Update in ES
        UpdateRequest request = new UpdateRequest("available_journals", "articles", String.valueOf(journalID));

        request.doc(jsonBuilder()
                .startObject()
                .field("is_isi", article.isISI())
                .field("is_scopus", article.isScopus())
                .field("issn", article.getISSN())
                .endObject());

        DataUtl.getESClient().update(request);
    }

    /**
     * Insert all authors to DB
     * Return the list of theirs IDs
     *
     * @param article
     * @return IDs of new authors
     * @throws IOException
     * @throws SQLException
     */
    public static List<Integer> createAuthors(Article article) throws IOException, SQLException {
        HashSet<String> organizationSet = new HashSet<>();
        List<Author> authors = article.getListAuthors();

        for (Author author : authors) {
            for (String organization : author.getOrganizations()) {
                organizationSet.add(organization);
            }
        }

        HashMap<String, Integer> idOfOrganizations = findOrCreateOrganizations(organizationSet);

        if (pstmInsertAuthor == null) {
            pstmInsertAuthor = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.authors (name) VALUES(?)", Statement.RETURN_GENERATED_KEYS);
        }

        for (Author author : authors) {
            pstmInsertAuthor.setString(1, author.getFullName());
            pstmInsertAuthor.addBatch();
        }
        pstmInsertAuthor.executeBatch();

        List<Integer> authorIDs = DataUtl.getAutoIncrementIDs(pstmInsertAuthor);

        if (pstmInsertAuthorOrganization == null) {
            pstmInsertAuthorOrganization = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.authors_organizes (author_id, organize_id) VALUES(?, ?)");
        }

        for (int i = 0; i < authorIDs.size(); ++i) {
            Author author = authors.get(i);
            int authorID = authorIDs.get(i);

            for (String organization : author.getOrganizations()) {
                pstmInsertAuthorOrganization.setInt(1, authorID);
                pstmInsertAuthorOrganization.setInt(2, idOfOrganizations.get(organization));

                pstmInsertAuthorOrganization.addBatch();
            }

            try {
                pstmInsertAuthorOrganization.executeBatch();
            } catch (SQLException e) {
                if (e instanceof BatchUpdateException) {
                    System.out.println("stupid org " + article.getId());
                }
            }
        }

        return authorIDs;
    }

    /**
     * Insert a single author to DB
     * First insert the authors, then insert organization if needed and link them
     *
     * @param author
     * @return ID of the new article
     * @throws SQLException
     * @throws IOException
     */
    private static PreparedStatement pstmInsertAuthor = null;
    private static PreparedStatement pstmInsertAuthorOrganization = null;
    public static int createAuthor(Author author) throws SQLException, IOException {
        // Insert author to DB
        if (pstmInsertAuthor == null) {
            pstmInsertAuthor = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.authors (name) VALUES(?)", Statement.RETURN_GENERATED_KEYS);
        }

        pstmInsertAuthor.setString(1, author.getFullName());

        pstmInsertAuthor.executeUpdate();

        int authorID = DataUtl.getAutoIncrementID(pstmInsertAuthor);

        // Link authors and organizations
        if (pstmInsertAuthorOrganization == null) {
            pstmInsertAuthorOrganization = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.authors_organizes (author_id, organize_id) VALUES(?, ?)", Statement.RETURN_GENERATED_KEYS);
        }

        int[] organizationIDs = findOrCreateOrganizations(author.getOrganizations());

        for (int organizationID : organizationIDs) {
            pstmInsertAuthorOrganization.setInt(1, authorID);
            pstmInsertAuthorOrganization.setInt(2, organizationID);

            try {
                pstmInsertAuthorOrganization.executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return authorID;
    }

    public static int[] findOrCreateOrganizations(String[] organizations) throws IOException, SQLException {
        int[] organizationIDs = new int[organizations.length];

        for (int i = 0; i < organizations.length; ++i) {
            organizationIDs[i] = findOrCreateOrganization(organizations[i]);
        }

        return organizationIDs;
    }

    private static PreparedStatement pstmInsertOrganization = null;
    public static HashMap<String, Integer> findOrCreateOrganizations(HashSet<String> organizationSet) throws IOException, SQLException {
        HashMap<String, Integer> mapping = new HashMap<>();

        // This is bullshit
//        for (String organization : organizationSet) {
//            int organizationID = findOrganization(organization);
//
//            if (organizationID != -1) {
//                mapping.put(organization, organizationID);
//                organizationSet.remove(organization);
//            }
//        }

        for (Iterator<String> it = organizationSet.iterator(); it.hasNext(); ) {
            String organization = it.next();
            int organizationID = findOrganization(organization);

            if (organizationID != -1) {
                mapping.put(organization, organizationID);
                it.remove();
            }
        }

        if (pstmInsertOrganization == null) {
            pstmInsertOrganization = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.organizes (name, _lft, _rgt, slug) VALUES(?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        }

        for (String organization : organizationSet) {
            int _rgt = numOfOrganization++ << 1;

            pstmInsertOrganization.setString(1, organization);
            pstmInsertOrganization.setInt(3, _rgt);
            pstmInsertOrganization.setInt(2, --_rgt);
            pstmInsertOrganization.setString(4, "add-slug-here");

            pstmInsertOrganization.addBatch();
        }

        pstmInsertOrganization.executeBatch();

        List<Integer> organizationIDs = DataUtl.getAutoIncrementIDs(pstmInsertOrganization);
        int counter = 0;

        for (String organization : organizationSet) {
            mapping.put(organization, organizationIDs.get(counter++));
        }

        // Index into ES
        Client client = DataUtl.getESClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        for (Map.Entry<String, Integer> entry : mapping.entrySet()) {
            bulkRequest.add(client.prepareIndex("available_organizations", "articles", String.valueOf(entry.getValue()))
                .setSource(jsonBuilder()
                    .startObject()
                        .field("original_id", entry.getValue())
                        .field("name", entry.getKey())
                    .endObject()
                )
            );
        }

        bulkRequest.get();

        return mapping;
    }

    public static int findOrCreateOrganization(String organization) throws IOException {
        if (organization == null) {
            // AKA "Chưa phân loại"
            return 1;
        }

        int organizationID = findOrganization(organization);
        if (organizationID != -1) {
            return organizationID;
        }

        // No match, create a new one
        organizationID = createOrganizationInServer(organization);
        System.out.println("server created organizationID " + organizationID + "    " + organization);

        // Index it in ES
        DataUtl.indexES("available_organizations", "articles", String.valueOf(organizationID),
                jsonBuilder()
                    .startObject()
                        .field("original_id", organizationID)
                        .field("name", organization)
                    .endObject()
        );

        return organizationID;
    }

    public static int findOrganization(String organization) throws UnknownHostException {
        QueryBuilder builder = QueryBuilders.matchQuery("name", organization);
        SearchHits hits = DataUtl.queryES("available_organizations", builder);

        for (SearchHit hit : hits) {
            // The condition is quite strict, as journal names are messy and need a separate deduplication
            // - Only the closest match is examined
            // - error_threshold is pretty low

            Map map = hit.getSourceAsMap();
            if (Lev.distanceNormStr(organization, (String) map.get("name")) < 0.05d) {
                System.out.println("Found in elas: organizationID " + hit.getSourceAsMap().get("original_id") + "    " + organization);
                return (Integer) map.get("original_id");
            }
        }

        return -1;
    }

    private static CloseableHttpClient httpClient = HttpClients.createDefault();
    private static Gson gson = new Gson();

    private static int createOrganizationInServer(String organization) throws IOException {
        String json = gson.toJson(new Organization(organization));

        HttpPost httpPOST = new HttpPost("http://localhost:8000/api/organizes/createByName");
        httpPOST.setEntity(new StringEntity(json, "UTF-8"));
        httpPOST.setHeader("Content-type", "application/json; charset=utf-8");    // hmmm

        HttpResponse response = httpClient.execute(httpPOST);
        Organization org = gson.fromJson(EntityUtils.toString(response.getEntity(), "UTF-8"), Organization.class);

        return org.getId();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String organization = "Centre de Physique Théorique, Case 907 Luminy, 13288 Marseille Cedex 9, France, Université de la Méditérannée, 13288 Marseille Cedex 9, France";
        int organizationID = createOrganizationInServer(organization);
        System.out.println("exact id " + organizationID);

        DataUtl.indexES("available_organizations", "articles", String.valueOf(organizationID),
                jsonBuilder()
                        .startObject()
                        .field("original_id", organizationID)
                        .field("name", organization)
                        .endObject()
        );

        QueryBuilder builder = QueryBuilders.matchQuery("name", organization);
        SearchHits hits = DataUtl.queryES("available_organizations", builder);

        System.out.println("size " + hits.getTotalHits());

        for (SearchHit hit : hits) {
            Map map = hit.getSourceAsMap();
            System.out.println("- " + map.get("name"));

            if (Lev.distanceNormStr(organization, (String) map.get("name")) < 0.05d) {
                System.out.println("    - " + map.get("name"));
                System.out.println("    - " + map.get("original_id"));
            }
        }
    }

    public static int getNumOfOrganizations() throws SQLException {
        ResultSet rs = DataUtl.queryDB("vci_scholar", "SELECT COUNT(*) FROM organizes");
        rs.next();
        return rs.getInt(1);
    }
}
