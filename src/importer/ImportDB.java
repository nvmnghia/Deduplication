package importer;

import com.google.gson.Gson;
import comparator.LCS;
import comparator.Lev;
import data.Article;
import data.Author;
import data.Organization;
import deduplicator.Deduplicator;
import javafx.util.Pair;
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

        try {
            pstmInsertArticle.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        int articleID = DataUtl.getAutoIncrementID(pstmInsertArticle);
        article.setMergedID(articleID);

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

        System.out.println("Created " + (article.isScopus() ? "Scopus-" : "ISI-") + article.getID() + " as " + articleID + " in DB:    " + article.getTitle());
    }

    /**
     * Equivalent to createOrFindJournal
     * Get the id of the journal given its article
     * If the journal is found, return its ID
     * If not, create a new journal, insert it into DB, index it into ES, insert its look-alike into DB and then returns its ID
     * The tricky part is that each journal will be compared to the others to find look-alike once - when it is first inserted
     *
     * @param article
     * @return the id of the found or newly created journal
     * @throws UnknownHostException
     */
    private static PreparedStatement pstmInsertJournal = null;
    private static PreparedStatement pstmInsertPossiblyDuplicatedJournals = null;
    public static int findOrCreateJournal(Article article) throws IOException, SQLException {
        if (article.getJournal() == null || article.getJournal().trim().equals("")) {
            // AKA "Chưa phân loại"
            article.setJournalID(1);
            return 1;
        }

        if (article.getISSN() != null) {
            QueryBuilder builder = QueryBuilders.matchQuery("issn", article.getISSN());
            SearchHits hits = DataUtl.queryES("available_journals", builder);

            for (SearchHit hit : hits) {
                Map map = hit.getSourceAsMap();
                String hitISSN = (String) map.get("issn");

                // Found by ISSN
                if (hitISSN != null && hitISSN.equalsIgnoreCase(article.getISSN())) {
                    // Remember to set additional variables
                    article.setJournalID((Integer) map.get("original_id"));
//                    article.setVCI((Boolean) map.get("is_vci"));

                    return article.getJournalID();
                }
            }
        }

        // List of look-alike
        List<Pair<Integer, Float>> listPossibleCandidates = new ArrayList<>();

        QueryBuilder builder = QueryBuilders.matchQuery("name", article.getJournal());
        SearchHits hits = DataUtl.queryES("available_journals", builder);

        for (SearchHit hit : hits) {
            Map map = hit.getSourceAsMap();
            String name = (String) map.get("name");

            double normalNameScore = 1.0d - LCS.approxDistance(article.getJournal(), name);

            // Found by name
            // The condition is quite strict, as journal names are messy and need a separate deduplication
            // - Only the closest match is examined
            // - error_threshold is pretty low
            if (normalNameScore > 0.9d) {
                article.setJournalID((Integer) map.get("original_id"));
//                article.setVCI(map.get("is_vci").equals(Boolean.TRUE));

                return article.getJournalID();
            } else if (normalNameScore >= 0.8d) {
                listPossibleCandidates.add(new Pair<>((Integer) map.get("original_id"), (float) normalNameScore));
            }
        }

        // No match, create a new one
        if (pstmInsertJournal == null) {
            pstmInsertJournal = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.journals (name, issn, is_scopus, is_isi, is_vci, is_international, type, slug, type_platform, archive_url, is_new_article, name_en) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
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
        pstmInsertJournal.setString(12, article.getJournal());

        pstmInsertJournal.executeUpdate();

        int journalID = DataUtl.getAutoIncrementID(pstmInsertJournal);
        article.setJournalID(journalID);

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

        // Insert its look-alike into DB
        if (pstmInsertPossiblyDuplicatedJournals == null) {
            pstmInsertPossiblyDuplicatedJournals = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.possibly_duplicated_journals (journal_a, journal_b, journal_score) VALUES(?, ?, ?)");
        }

        for (Pair<Integer, Float> candidate : listPossibleCandidates) {
            pstmInsertPossiblyDuplicatedJournals.setInt(1, journalID);
            pstmInsertPossiblyDuplicatedJournals.setInt(2, candidate.getKey());
            pstmInsertPossiblyDuplicatedJournals.setFloat(3, candidate.getValue());

            pstmInsertPossiblyDuplicatedJournals.addBatch();
        }

        if (journalID % 100 == 0) {
            if (pstmInsertPossiblyDuplicatedJournals != null) {
                pstmInsertPossiblyDuplicatedJournals.executeBatch();
            }

            if (pstmInsertPossiblyDuplicatedOrganizations != null) {
                pstmInsertPossiblyDuplicatedOrganizations.executeBatch();
            }
        }

        return journalID;
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
    private static PreparedStatement pstmInsertAuthor = null;
    private static PreparedStatement pstmInsertAuthorOrganization = null;
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
                    System.out.println("stupid org " + article.getID());
                }
            }
        }

        return authorIDs;
    }

    /**
     * Given a set of organizations, find or create theirs corresponding IDs
     *
     * @param organizationSet
     * @return
     * @throws IOException
     * @throws SQLException
     */
    private static PreparedStatement pstmInsertOrganization = null;
    private static PreparedStatement pstmInsertPossiblyDuplicatedOrganizations = null;
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

        // List of organization, along with their look-alike, along with their scores
        List<Pair<String, List<Pair<Integer, Float>>>> wowLookAtME = new ArrayList<>();

        // Remove created organizations
        for (Iterator<String> it = organizationSet.iterator(); it.hasNext(); ) {
            String organization = it.next();
            List<Pair<Integer, Float>> possibleOrganizationIDs = findOrganization(organization);

            if (possibleOrganizationIDs.size() != 0 && possibleOrganizationIDs.get(0).getValue() >= 0.95) {
                mapping.put(organization, possibleOrganizationIDs.get(0).getKey());
                it.remove();
            } else {
                wowLookAtME.add(new Pair<>(organization, possibleOrganizationIDs));
            }
        }

        if (pstmInsertOrganization == null) {
            pstmInsertOrganization = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.organizes (name, _lft, _rgt, slug, name_en) VALUES(?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        }

        // NestedSet logic:
        // New node, without parent information, will be inserted into the root node as the last node
        // So, the _rgt value will be the largest _rgt, exactly doubling numOfOrganization
        // and the _lft equals _rgt - 1
        for (String organization : organizationSet) {
            int _rgt = numOfOrganization++ << 1;

            pstmInsertOrganization.setString(1, organization);
            pstmInsertOrganization.setInt(3, _rgt);
            pstmInsertOrganization.setInt(2, --_rgt);
            pstmInsertOrganization.setString(4, "add-slug-here");
            pstmInsertOrganization.setString(5, organization);

            pstmInsertOrganization.addBatch();
        }

        pstmInsertOrganization.executeBatch();

        List<Integer> organizationIDs = DataUtl.getAutoIncrementIDs(pstmInsertOrganization);
        int counter = 0;

        // A good example of de-facto standard: although not guaranteed, the iteration order of a set will stay the same, as long as it isn't modified
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

        if (pstmInsertPossiblyDuplicatedOrganizations == null) {
            pstmInsertPossiblyDuplicatedOrganizations = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.possibly_duplicated_organizations (organization_a, organization_b, organization_score) VALUES(?, ?, ?)");
        }

        for (Pair<String, List<Pair<Integer, Float>>> pair : wowLookAtME) {
            int organization_a = mapping.get(pair.getKey());

            for (Pair<Integer, Float> candidate : pair.getValue()) {
                pstmInsertPossiblyDuplicatedOrganizations.setInt(1, organization_a);
                pstmInsertPossiblyDuplicatedOrganizations.setInt(2, candidate.getKey());
                pstmInsertPossiblyDuplicatedOrganizations.setFloat(3, candidate.getValue());

                pstmInsertPossiblyDuplicatedOrganizations.addBatch();
            }
        }

        return mapping;
    }

    public static List<Pair<Integer, Float>> findOrganization(String organization) throws UnknownHostException {
        List<Pair<Integer, Float>> listPossbleCandidates = new ArrayList<>();

        if (organization == null || organization.trim().equals("")) {
            listPossbleCandidates.add(new Pair<>(1, 1.0f));
            return listPossbleCandidates;
        }

        QueryBuilder builder = QueryBuilders.matchQuery("name", organization);
        SearchHits hits = DataUtl.queryES("available_organizations", builder);

        for (SearchHit hit : hits) {
            // The condition is quite strict, as journal names are messy and need a separate deduplication
            // - Only the closest match is examined
            // - error_threshold is pretty low
            Map map = hit.getSourceAsMap();
            double nameScore = 1.0d - LCS.approxDistance(organization, (String) map.get("name"));

            if (nameScore >= 0.95d) {
                System.out.println("Found in elas: organizationID " + hit.getSourceAsMap().get("original_id") + "    " + organization);

                listPossbleCandidates.clear();
                listPossbleCandidates.add(new Pair<>((Integer) map.get("original_id"), (float) nameScore));

                return listPossbleCandidates;
            } else if (nameScore >= 0.8d) {
                listPossbleCandidates.add(new Pair<>((Integer) map.get("original_id"), (float) nameScore));
            }
        }

        return listPossbleCandidates;
    }

    public static int getNumOfOrganizations() throws SQLException {
        ResultSet rs = DataUtl.queryDB("vci_scholar", "SELECT COUNT(*) FROM organizes");
        rs.next();
        return rs.getInt(1);
    }

    public static void finishHim() throws SQLException {
        if (pstmInsertPossiblyDuplicatedJournals != null) {
            pstmInsertPossiblyDuplicatedJournals.executeBatch();
        }

        if (pstmInsertPossiblyDuplicatedOrganizations != null) {
            pstmInsertPossiblyDuplicatedOrganizations.executeBatch();
        }
    }
}
