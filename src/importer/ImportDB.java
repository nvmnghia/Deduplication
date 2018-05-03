package importer;

import com.github.slugify.Slugify;
import comparator.Lev;
import config.Config;
import data.Article;
import data.Author;
import deduplicator.InterDeduplicator;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import util.DataUtl;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

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

    private static Slugify slg = new Slugify();

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
        pstmInsertArticle.setString(20, slg.slugify(article.getTitle()));

        int articleID = pstmInsertArticle.executeUpdate();

        // Link articles and authors
        if (pstmInsertArticleAuthors == null) {
            pstmInsertArticleAuthors = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.articles_authors (author_id, article_id) VALUES(?, ?)");
        }

        int[] authorIDs = createAuthors(article);
        for (int authorID : authorIDs) {
            pstmInsertArticleAuthors.setInt(1, authorID);
            pstmInsertArticleAuthors.setInt(2, articleID);
            pstmInsertArticleAuthors.addBatch();
        }

        pstmInsertArticleAuthors.executeBatch();
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

            if (hits.getTotalHits() != 0) {
                Map map = hits.getAt(0).getSourceAsMap();
                String hitISSN = (String) map.get("issn");

                // God almighty
                if (hitISSN != null && hitISSN.equals(article.getISSN())) {
                    boolean is_isi = ((String) map.get("is_isi")).equals("1");
                    boolean is_scopus = ((String) map.get("is_scopus")).equals("1");

                    if (is_isi != article.isISI() || is_scopus != article.isScopus() ) {
                        // Need to update
                        updateJournal(article, (String) map.get("original_id"), false);
                    }

                    article.setVCI(((String) map.get("is_vci")).equals("1"));

                    return Integer.valueOf((String) hits.getAt(0).getSourceAsMap().get("original_id"));
                }
            }
        }

        QueryBuilder builder = QueryBuilders.matchQuery("name", article.getJournal());
        SearchHits hits = DataUtl.queryES("available_journals", builder);

        if (hits.getTotalHits() != 0) {
            // The condition is quite strict, as journal names are messy and need a separate deduplication
            // - Only the closest match is examined
            // - error_threshold is pretty low

            Map map = hits.getAt(0).getSourceAsMap();

            boolean is_isi = ((String) map.get("is_isi")).equals("1");
            boolean is_scopus = ((String) map.get("is_scopus")).equals("1");
            String issn = (String) map.get("issn");    // Ha ha ha java in a nutshell: Foo bar = (Foo) null;

            if (is_isi != article.isISI() || is_scopus != article.isScopus() || issn == null) {
                // Need update
                updateJournal(article, (String) map.get("original_id"), issn == null);
            }

            if (Lev.distanceNormStr(article.getJournal(), (String) map.get("name")) < 0.1d) {
                article.setVCI(((String) map.get("is_vci")).equals("1"));

                return Integer.valueOf((String) map.get("original_id"));
            }
        }

        // No match, create a new one
        if (pstmInsertJournal == null) {
            pstmInsertJournal = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.journals (name, issn, is_scopus, is_isi, is_vci, is_international, type) VALUES(?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        }

        pstmInsertJournal.setString(1, article.getJournal());
        pstmInsertJournal.setString(2, article.getISSN());
        pstmInsertJournal.setInt(3, article.isScopus() ? 1 : 0);
        pstmInsertJournal.setInt(4, article.isISI() ? 1 : 0);
        pstmInsertJournal.setInt(5, 0);
        pstmInsertJournal.setInt(6, 1);
        pstmInsertJournal.setString(7, article.getType());

        int journalID = pstmInsertJournal.executeUpdate();

        // Index it in ES
        DataUtl.indexES("available_journals", "articles", String.valueOf(journalID),
            jsonBuilder()
                .startObject()
                    .field("original_id", journalID)
                    .field("name", article.getJournal())
                    .field("issn", article.getISSN())
                .endObject()
        );

        return journalID;
    }

    /**
     *
     *
     * @param article
     * @param journalID
     * @param isNeedUpdateISSN
     * @throws SQLException
     * @throws IOException
     */
    private static PreparedStatement pstmUpdateJournal = null;
    public static void updateJournal(Article article, String journalID, boolean isNeedUpdateISSN) throws SQLException, IOException {
        // Update in DB
        if (pstmUpdateJournal == null) {
            pstmUpdateJournal = DataUtl.getDBConnection().prepareStatement("UPDATE vci_scholar.journals SET is_isi = ?, is_scopus = ? WHERE id = ?");
        }

        pstmUpdateJournal.setInt(1, article.isISI() ? 1 : 0);
        pstmUpdateJournal.setInt(2, article.isScopus() ? 1 : 0);
        pstmUpdateJournal.setInt(3, Integer.parseInt(journalID));

        pstmUpdateJournal.executeUpdate();

        // Update in ES
        UpdateRequest request = new UpdateRequest("available_journals", "articles", journalID);

        if (isNeedUpdateISSN) {
            request.doc(jsonBuilder()
                    .startObject()
                    .field("is_isi", article.isISI() ? "1" : "0")
                    .field("is_scopus", article.isScopus() ? "1" : "0")
                    .field("issn", article.getISSN())
                    .endObject());
        } else {
            request.doc(jsonBuilder()
                    .startObject()
                    .field("is_isi", article.isISI() ? "1" : "0")
                    .field("is_scopus", article.isScopus() ? "1" : "0")
                    .endObject());
        }

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
    public static int[] createAuthors(Article article) throws IOException, SQLException {
        List<Author> authors = article.getListAuthors();
        int[] authorIDs = new int[authors.size()];

        for (int i = 0; i < authors.size(); ++i) {
            authorIDs[i] = createAuthor(authors.get(i));
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
        int authorID = pstmInsertAuthor.executeUpdate();

        // Link authors and organizations
        if (pstmInsertAuthorOrganization == null) {
            pstmInsertAuthorOrganization = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.authors_organizes (author_id, organize) VALUES(?, ?)", Statement.RETURN_GENERATED_KEYS);
        }

        int organizationID = findOrCreateOrganization(author.getOrganization());

        pstmInsertAuthorOrganization.setInt(1, authorID);
        pstmInsertAuthorOrganization.setInt(2, organizationID);

        pstmInsertAuthorOrganization.executeUpdate();

        return authorID;
    }

    private static PreparedStatement pstmInsertOrganization = null;

    public static int findOrCreateOrganization(String organization) throws IOException, SQLException {
        if (organization == null) {
            // AKA "Chưa phân loại"
            return 1;
        }

        QueryBuilder builder = QueryBuilders.matchQuery("name", organization);
        SearchHits hits = DataUtl.queryES("available_organizations", builder);

        if (hits.getTotalHits() != 0) {
            // The condition is quite strict, as journal names are messy and need a separate deduplication
            // - Only the closest match is examined
            // - error_threshold is pretty low

            Map map = hits.getAt(0).getSourceAsMap();
            if (Lev.distanceNormStr(organization, (String) map.get("name")) < 0.1d) {
                return Integer.valueOf((String) map.get("original_id"));
            }
        }

        // No match, create a new one
        if (pstmInsertOrganization == null) {
            pstmInsertOrganization = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.organizes (name) VALUES(?)");
        }

        pstmInsertOrganization.setString(1, organization);

            pstmInsertOrganization.executeUpdate();

        ResultSet rs = pstmInsertOrganization.getGeneratedKeys();
        int organizationID = -1;
        if (rs.next()) {
            organizationID = rs.getInt(1);
        }

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

    /**
     * Check if the article exists in the current DB
     * The whole DB was indexed in ES (see ImportElastic class),
     * so ES will be searched, instead of the original DB
     * If the ISI or Scopus version have existed in the DB, skip them
     *
     * @param input the input article
     * @return the result of the check
     * @throws UnknownHostException when ES fucked up
     */
    public static boolean isNeedInsert(Article input) throws UnknownHostException {
        QueryBuilder filterByField = QueryBuilders.matchQuery("title", input.getTitle());
        SearchHits hits = DataUtl.queryES(Config.ES_INDEX, filterByField);

        if (hits == null || hits.getTotalHits() == 0) {
            return true;
        }

        for (SearchHit hit : hits) {
            Article article = new Article();

            article.setId(Integer.valueOf((String) hit.getSourceAsMap().get("original_id")));
            article.setTitle((String) hit.getSourceAsMap().get("title"));
            article.setYear((String) hit.getSourceAsMap().get("year"));
            article.setJournal((String) hit.getSourceAsMap().get("journal"));

            if (InterDeduplicator.isDuplicate(article, input)) {
                return false;
            }
        }

        return true;
    }
}
