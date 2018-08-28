package importer;

import com.google.common.collect.*;
import comparator.LCS;
import config.Config;
import data.Article;
import data.Author;
import data.Organization;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import util.DataUtl;
import util.StringUtl;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static util.DataUtl.getDBStatement;
import static util.DataUtl.refreshES;

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

    // Used to set initial NestedSet _rgt and _lft of organizes table
    private static int numOfOrganization;

    private static int UNCATEGORIZED_JOURNAL_ID = -1;
    private static int UNCATEGORIZED_ORGANIZATION_ID = -1;

    static {
        try {
            if (Config.DB.NUCLEAR_OPTION) {
                String[] truncateTables = {"articles", "articles_authors", "authors", "authors_organizes", "journals", "merge_logs", "organizes", "organize_representative"};

                for (String table : truncateTables) {
                    DataUtl.truncate(Config.DB.OUTPUT, table);
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static {
        try {
            numOfOrganization = getNumOfOrganizations();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Prepare uncategorized values for journals and organizes table
        try {
            ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT,
                    "SELECT * FROM journals WHERE name_en LIKE 'Uncategorized'");
            while (rs.next()) {
                UNCATEGORIZED_JOURNAL_ID = rs.getInt("id");
            }
            rs.close();

            if (UNCATEGORIZED_JOURNAL_ID == -1) {
                UNCATEGORIZED_JOURNAL_ID = DataUtl.insertAndGetID(Config.DB.OUTPUT,
                        "INSERT INTO journals (name, name_en, slug) VALUES('Chưa phân loại', 'Uncategorized', '')");
            }

            System.out.println("UNCATEGORIZED_JOURNAL_ID: " + UNCATEGORIZED_JOURNAL_ID);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT,
                    "SELECT id FROM organizes WHERE name_en LIKE 'Uncategorized'");
            while (rs.next()) {
                UNCATEGORIZED_ORGANIZATION_ID = rs.getInt(1);
            }
            rs.close();

            if (UNCATEGORIZED_ORGANIZATION_ID == -1) {
                int _rgt = ++numOfOrganization << 1;    // Look at the comments in findOrCreateOrganizations
                UNCATEGORIZED_ORGANIZATION_ID = DataUtl.insertAndGetID(Config.DB.OUTPUT,
                        "INSERT INTO organizes (name, _rgt, _lft, slug, name_en) VALUES('Chưa phân loại', " + _rgt + ", " + --_rgt + ", '', 'Uncategorized')");
            }

            System.out.println("UNCATEGORIZED_ORGANIZATION_ID: " + UNCATEGORIZED_ORGANIZATION_ID);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static List<String> organizationSuffixes;
    static {
        try {
            organizationSuffixes = createOrganizationSuffixes();
        } catch (Exception e) {
            System.err.println("WHAT");
            e.printStackTrace();
        }


        System.out.println("List suffixes:");
        for (String suffix : organizationSuffixes) {
            System.out.print(suffix + "    ");
        }
        System.out.println("\n");
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
            pstmInsertArticle = DataUtl.getDBConnection().prepareStatement(
                    "INSERT INTO " + Config.DB.OUTPUT + ".articles (title, author, volume, number, year, uri, abstract, usable, reference, journal_id, language, is_reviewed, keyword, doi, document_type, is_scopus, is_isi, is_vci, is_international, slug, raw_scopus_id, raw_isi_id) " +
                            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
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
        pstmInsertArticle.setString(20, "");

        if (article.isScopus()) {
            pstmInsertArticle.setInt(21, article.getID());
            pstmInsertArticle.setNull(22, Types.INTEGER);
        } else {
            pstmInsertArticle.setInt(22, article.getID());
            pstmInsertArticle.setNull(21, Types.INTEGER);
        }

        try {
            pstmInsertArticle.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }

        int articleID = DataUtl.getAutoIncrementID(pstmInsertArticle);
        article.setMergedID(articleID);    // No, don't set ID!

        // Link articles and authors
        if (pstmInsertArticleAuthors == null) {
            pstmInsertArticleAuthors = DataUtl.getDBConnection().prepareStatement(
                    "INSERT INTO " + Config.DB.OUTPUT + ".articles_authors (author_id, article_id) VALUES(?, ?)");
        }

        List<Integer> authorIDs = createAuthors(article);

        for (Integer authorID : authorIDs) {
            pstmInsertArticleAuthors.setInt(1, authorID);
            pstmInsertArticleAuthors.setInt(2, articleID);

            pstmInsertArticleAuthors.addBatch();
        }
        pstmInsertArticleAuthors.executeBatch();
    }

    /**
     * Equivalent to createOrFindJournal
     * Given an article, get the id of the journal
     * If the journal is found, return its ID
     * If not, create a new journal, insert it into DB, index it into ES, and then returns its ID
     *
     * @param article
     * @return the id of the found or newly created journal
     * @throws UnknownHostException
     */
    private static PreparedStatement pstmInsertJournal = null;
    public static Integer findOrCreateJournal(Article article) throws IOException, SQLException {
        if (article.getJournal() == null || article.getJournal().trim().equals("")) {
            // AKA "Chưa phân loại"
            article.setJournalID(UNCATEGORIZED_JOURNAL_ID);
            return UNCATEGORIZED_JOURNAL_ID;
        }

        if (article.getISSN() != null) {
            QueryBuilder builder = QueryBuilders.matchQuery("issn", article.getISSN());
            SearchHits hits = DataUtl.queryES("available_journals", builder);

            for (SearchHit hit : hits) {
                Map map = hit.getSourceAsMap();
                String hitISSN = (String) map.get("issn");

                // Found by ISSN
                if (hitISSN != null && hitISSN.equalsIgnoreCase(article.getISSN())) {
                    article.setJournalID((Integer) map.get("original_id"));
                    // If the journal is a VCI, set the article's is_vci to true
//                    article.setVCI((Boolean) map.get("is_vci"));

                    return article.getJournalID();
                }
            }
        }

        QueryBuilder builder = QueryBuilders.matchQuery("name", article.getJournal());
        SearchHits hits = DataUtl.queryES("available_journals", builder);

        Integer ID = null;
        double highestScore = -1d;

        for (SearchHit hit : hits) {
            Map map = hit.getSourceAsMap();
            String name = (String) map.get("name");

            double nameScore = 1.0d - LCS.approxDistance(article.getJournal(), name);

            if (nameScore > 0.9d && nameScore > highestScore) {
//                article.setJournalID((Integer) map.get("original_id"));
//
//                return article.getJournalID();

                highestScore = nameScore;
                ID = (Integer) map.get("original_id");
            }
        }

        if (ID != null) {
            article.setJournalID(ID);
            return ID;
        }

        // No match, create a new one
        if (pstmInsertJournal == null) {
            pstmInsertJournal = DataUtl.getDBConnection().prepareStatement(
                    "INSERT INTO " + Config.DB.OUTPUT + ".journals (name, issn, is_scopus, is_isi, is_vci, is_international, type, slug, type_platform, archive_url, is_new_article, name_en) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        }

        pstmInsertJournal.setString(1, article.getJournal());
        pstmInsertJournal.setString(2, article.getISSN());
        pstmInsertJournal.setInt(3, article.isScopus() ? 1 : 0);
        pstmInsertJournal.setInt(4, article.isISI() ? 1 : 0);
        pstmInsertJournal.setInt(5, 0);
        pstmInsertJournal.setInt(6, 1);
        pstmInsertJournal.setString(7, article.getType());
        pstmInsertJournal.setString(8, "");
        pstmInsertJournal.setString(9, "");
        pstmInsertJournal.setString(10, "");
        pstmInsertJournal.setInt(11, 0);
        pstmInsertJournal.setString(12, article.getJournal());

        pstmInsertJournal.executeUpdate();

        ID = DataUtl.getAutoIncrementID(pstmInsertJournal);
        article.setJournalID(ID);

        // Index it in ES
        DataUtl.indexES("available_journals", "articles", String.valueOf(ID),
            jsonBuilder()
                .startObject()
                    .field("original_id", ID)
                    .field("name", article.getJournal())
                    .field("issn", article.getISSN())
                    .field("is_isi", article.isISI())
                    .field("is_scopus", article.isScopus())
                    .field("is_vci", false)
                .endObject()
        );

        return ID;
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
        List<Author> authors = article.getListAuthors();

        HashMap<String, Integer> idOfOrganizations = findOrCreateOrganizations(article);

        if (pstmInsertAuthor == null) {
            pstmInsertAuthor = DataUtl.getDBConnection().prepareStatement(
                    "INSERT INTO " + Config.DB.OUTPUT + ".authors (name) VALUES(?)", Statement.RETURN_GENERATED_KEYS);
        }

        for (Author author : authors) {
            pstmInsertAuthor.setString(1, author.getFullName());
            pstmInsertAuthor.addBatch();
        }
        pstmInsertAuthor.executeBatch();

        List<Integer> authorIDs = DataUtl.getAutoIncrementIDs(pstmInsertAuthor);

        if (pstmInsertAuthorOrganization == null) {
            pstmInsertAuthorOrganization = DataUtl.getDBConnection().prepareStatement(
                    "INSERT INTO " + Config.DB.OUTPUT + ".authors_organizes (author_id, organize_id) VALUES(?, ?)");
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
                e.printStackTrace();
            }
        }

        return authorIDs;
    }

    /**
     * Given a set of organizations, find or create theirs corresponding IDs
     *
     * @param organizationSet
     * @return input organizations along with theirs IDs
     * @throws IOException
     * @throws SQLException
     */
    private static PreparedStatement pstmInsertOrganization = null;
    public static HashMap<String, Integer> findOrCreateOrganizations(Article article) throws IOException, SQLException {
        List<Author> authors = article.getListAuthors();
        Set<String> organizationSet = new HashSet<>();

        for (Author author : authors) {
            try {
                organizationSet.addAll(Arrays.asList(author.getOrganizations()));
            } catch (Exception e) {
                System.out.println(article.getID());
                e.printStackTrace();
            }
        }

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

        // Remove organizations which are already stored in the output DB
        for (Iterator<String> it = organizationSet.iterator(); it.hasNext(); ) {
            String organization = it.next();
            Integer ID = findOrganization(organization);

            if (ID != null) {
                mapping.put(organization, ID);
                it.remove();
            }
        }

        if (pstmInsertOrganization == null) {
            pstmInsertOrganization = DataUtl.getDBConnection().prepareStatement(
                    "INSERT INTO " + Config.DB.OUTPUT + ".organizes (name, _lft, _rgt, slug, name_en) VALUES(?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        }

        // NestedSet logic:
        // New node, without parent information, will be inserted into the root node as the last node
        // So, the _rgt value will be the largest _rgt, exactly doubling the number of element (including the new one)
        // and the _lft equals _rgt - 1
        for (String organization : organizationSet) {
            int _rgt = ++numOfOrganization << 1;    // Feel like an ACM fellow

            pstmInsertOrganization.setString(1, organization);
            pstmInsertOrganization.setInt(3, _rgt);
            pstmInsertOrganization.setInt(2, --_rgt);
            pstmInsertOrganization.setString(4, "");
            pstmInsertOrganization.setString(5, organization);

            pstmInsertOrganization.addBatch();
        }

        pstmInsertOrganization.executeBatch();

        List<Integer> organizationIDs = DataUtl.getAutoIncrementIDs(pstmInsertOrganization);
        int counter = 0;

        // A good example of de-facto standard: although not guaranteed,
        // the iteration order of a set will remain the same, as long as it isn't modified
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
        refreshES("available_organizations");

        return mapping;
    }

    /**
     * Given an organization's name, returns its ID if it is created in the output DB
     * Return null otherwise
     * @param organization
     * @return
     * @throws UnknownHostException
     */
    public static Integer findOrganization(String organization) throws UnknownHostException {

        if (organization == null || organization.trim().equals("")) {
            return UNCATEGORIZED_ORGANIZATION_ID;
        }

        QueryBuilder builder = QueryBuilders.matchQuery("name", organization);
        SearchHits hits = DataUtl.queryES("available_organizations", builder);
        String temp = null;

        Integer ID = null;
        double highestScore = 0.94d;    // This condition is quite strict, as organization names are messy and need a separate deduplication

        for (SearchHit hit : hits) {
            Map map = hit.getSourceAsMap();
            double nameScore = Organization.nameSimilarity(organization, (String) map.get("name"));

            if (nameScore >= highestScore) {
                temp = (String) map.get("name");
                highestScore = nameScore;
                ID = (Integer) map.get("original_id");
            }
        }

        if (ID != null) {
            System.out.println(organization);
            System.out.println(temp);
            System.out.println();
        }

        return ID;
    }

    private static int getNumOfOrganizations() throws SQLException {
        ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT, "SELECT COUNT(*) FROM organizes");
        rs.next();
        int temp = rs.getInt(1);
        rs.close();
        return temp;
    }

    /**
     * Suffix is the last word of the organization name.
     * Usually there will be one word in the segment, but
     * It will be used to split lumped together organization names.
     * @return
     */
    private static List<String> createOrganizationSuffixes() {
        HashMultiset<String> suffixCounter = HashMultiset.create();
        List<String> organizationSuffixes = new ArrayList<>();

        int total = 0;
        String[] tables = {"isi_documents", "scopus_documents"};

        for (String table : tables) {
            String query = "SELECT authors_json FROM " + table;

            try {
                ResultSet rs = DataUtl.queryDB(Config.DB.INPUT, query);

                while (rs.next()) {
                    Article article = new Article();
                    article.setAuthorsJSON(rs.getString(1));

                    List<String> organizations = article.getRawOrganizations();

                    for (String organization : organizations) {
                        String suffix = Organization.getSuffix(organization);
                        if (suffix != null) {
                            suffixCounter.add(Organization.getSuffix(organization));
                            ++total;
                        }
                    }
                }

                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // Get the 95% most popular
        total = total * 95 / 100;
        Iterator<Multiset.Entry<String>> entriesSortedByCount = Multisets.copyHighestCountFirst(suffixCounter).entrySet().iterator();
        while (total >= 0) {
            Multiset.Entry<String> entry = entriesSortedByCount.next();

            total -= entry.getCount();
            organizationSuffixes.add(", " + entry.getElement() + ",");
        }

        return organizationSuffixes;
    }

    public static List<String> getOrganizationSuffixes() {
        return organizationSuffixes;
    }

    /**
     * Split the organization if possible
     * If not, the original string is still added to the output list, intact
     * @param organization
     * @return
     */
    public static List<String> splitLumpedOrganizations(String organization) {
        List<String> output = new ArrayList<>();

        Stack<String> splitedParts = new Stack<>();
        splitedParts.push(organization);

//        if (organization.contains("Department of Otolaryngology Head and Neck")) {
//            System.out.println("ha");
//        }

        boolean isSplitted;
        int threshold;
        while (! splitedParts.empty()) {
            String current = splitedParts.pop();
            isSplitted = false;

            if (current.length() > 65) {
                threshold = current.length() * 75 / 100;

                for (String suffix : organizationSuffixes) {
                    int index = current.indexOf(suffix);
                    if (index == -1) {
                        continue;
                    }

                    int endIndex = current.indexOf(suffix) + suffix.length();
                    if (endIndex <= threshold) {
                        // Split!
                        splitedParts.push(StringUtl.cleanComma(current.substring(0, endIndex)));
                        splitedParts.push(StringUtl.cleanComma(current.substring(endIndex)));

                        ++tempCounter;
                        isSplitted = true;
                        break;
                    }
                }
            }

            // No splitting occurred, so just add to the output
            if (! isSplitted) {
                output.add(current);
            }
        }

        if (output.size() > 1) {
            System.out.println("Split: " + organization);
            for (String split : output) {
                System.out.println(split);
            }
            System.out.println();
        }

        return output;
    }

    public static int tempCounter = 0;
}