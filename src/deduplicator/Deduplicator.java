package deduplicator;

import com.google.gson.JsonArray;
import comparator.LCS;
import config.Config;
import data.*;

import importer.ImportDB;
import org.elasticsearch.action.update.UpdateRequest;
import util.DataUtl;
import util.StringUtl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Deduplicator {

    private static int counterISI = 0;

    /**
     * Main deduplication function
     * Given the document type (either ISI or Scopus) and its id in the original DB, return a list of Match
     * Normally, this function is used to merge ISI into Scopus, which means it expects ISI to be passed
     * If the given ISI article has a Scopus match, the match's is_isi will be switched to true
     * Else, the given article will be inserted in the output DB.
     *
     * @param type
     * @param id
     * @return a List of Match couple, for logging purpose
     * @throws IOException
     * @throws SQLException
     */
    public static List<Match> deduplicate(int type, int id) throws IOException, SQLException {
        // First get the article
        Article article = ArticleSource.getArticleByID(Config.ES.INDEX, type, id);
        if (article == null) {
            return new ArrayList<>();
        }

        // Then search for its name in ES
        List<Article> candidates = ArticleSource.getArticles("available_articles",
                type == Article.ISI ? Article.SCOPUS : Article.ISI,
                "title", article.getTitle());

        filterByYear(candidates, article.getYear());

        List<Match> listMatches = new ArrayList<>();
        boolean duplicated = false;

        for (Article candidate : candidates) {
            // This check is not needed:
            // candidates are from available_articles, which are articles in the merged DB,
            // while the article is an article in the crawled, original DB
//            if (candidate.getID() == article.getID()) {
//                continue;
//            }

            Match match = areSameArticles(article, candidate);

            if (match == null) {
                continue;
            }

            listMatches.add(match);
            if (match.getMatchType() == Match.DUPLICATED) {
                duplicated = true;
//                printDebug(article, candidate, match.titleScore, match.journalScore);

                // Candidates are Scopus, which are already imported
                // Now they and their journals need to be updated: is_isi = true
                updateArticleAndJournal(candidate);

                System.out.println("Duplicated: ISI: " + article.getTitle() + "    Scopus: " + candidate.getTitle());

                // The quest for duplicated articles ends here, as the duplicated one was found
                // But the program should continue to find POSSIBLY_DUPLICATED articles
                // This has a surprise side effect:
                // More DUPLICATED matched can be found,
                // therefore the number of is_isi in the merged DB could be larger than the number records in isi_documents
                // However, the mergedID only stores one value: the last duplicated candidate's ID

                article.setMergedID(candidate.getID());
            }
        }

        if (duplicated) {
//            insertOrganizationsOfDuplicatedArticle(article);
        } else {
            ImportDB.createArticle(article);
            System.out.println("Inserted " + ++counterISI + " ISI articles");
        }

        return listMatches;
    }

    /**
     * Duplicated articles aren't inserted, but theirs Organizations can be inserted using this function
     */
    private static PreparedStatement pstmInsertDuplicatedISIArticleAuthor = null;
    private static void insertOrganizationsOfDuplicatedArticle(Article article) throws SQLException, IOException {
        if (pstmInsertDuplicatedISIArticleAuthor == null) {
            pstmInsertDuplicatedISIArticleAuthor = DataUtl.getDBConnection().prepareStatement("INSERT INTO duplicated_articles_authors (author_id, isi_article_id) VALUES(?, ?)");
        }

        List<Integer> authorIDs = ImportDB.createAuthors(article);

        for (Integer authorID : authorIDs) {
            pstmInsertDuplicatedISIArticleAuthor.setInt(1, authorID);
            pstmInsertDuplicatedISIArticleAuthor.setInt(2, article.getID());

            pstmInsertDuplicatedISIArticleAuthor.addBatch();
        }
        pstmInsertDuplicatedISIArticleAuthor.executeBatch();
    }

    private static PreparedStatement pstmUpdateArticle = null;
    private static PreparedStatement pstmUpdateJournal = null;
    private static void updateArticleAndJournal(Article candidate) throws SQLException, IOException {
        // Update articles
        if (pstmUpdateArticle == null) {
            pstmUpdateArticle = DataUtl.getDBConnection().prepareStatement("UPDATE " + Config.DB.DBNAME + ".articles SET is_isi = 1 WHERE id = ?");
        }
        pstmUpdateArticle.setInt(1, candidate.getID());
        pstmUpdateArticle.executeUpdate();

        UpdateRequest request = new UpdateRequest("available_articles", "articles", String.valueOf(candidate.getID()));
        request.doc(jsonBuilder()
                .startObject()
                    .field("is_isi", true)
                    .field("is_scopus", true)
                .endObject());
        DataUtl.getESClient().update(request);

        // Update journals
        if (pstmUpdateJournal == null) {
            pstmUpdateJournal = DataUtl.getDBConnection().prepareStatement("UPDATE " + Config.DB.DBNAME + ".journals SET is_isi = 1 WHERE id = ?");
        }

        int journalID = candidate.getJournalID();

        pstmUpdateJournal.setInt(1, journalID);
        pstmUpdateJournal.executeUpdate();

        request = new UpdateRequest("available_journals", "articles", String.valueOf(journalID));
        request.doc(jsonBuilder()
            .startObject()
                .field("is_isi", true)
                .field("is_vci", true)
            .endObject());
        DataUtl.getESClient().update(request);
    }

    private static void printDebug(Article article, Article candidate, double titleScore, double journalScore) {
        try {
            String output_debug = article.toString() + "\n" + titleScore + "    " + journalScore + "\n" + candidate.toString() + "\n\n\n\n";
            Files.write(Paths.get("outfile_relaxed.txt"), output_debug.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get author from the corresponding JsonObject
     * @param authorsObject containing author information
     * @return A list of author
     */
    private static String[] getAuthors(JsonArray authorsObject) {
        if (authorsObject.size() == 0) {
            return new String[0];
        }

        String[] authors = new String[authorsObject.size()];

        for (int i = 0; i < authorsObject.size(); ++i) {
            authors[i] = authorsObject.get(i).getAsJsonObject().get("name").getAsString();
        }

        return authors;
    }

    /**
     * Given the year of the input article, filter out the candidates which has the same year
     * @param candidates candidates of the input article
     * @param year publication year of the input article
     */
    private static void filterByYear(List<Article> candidates, int year) {
        // Null year is read as -1
        if (year == -1) {
            return;
        }

        for (int i = 0; i < candidates.size(); ++i) {
            if (candidates.get(i).getYear() != year && candidates.get(i).getYear() != -1) {
                // Definitely not
                candidates.remove(i--);
            }
        }
    }

    /**
     * Check if the 2 articles are the same (match)
     * If not, return null
     * @param article an Article of either ISI or Scopus matchType
     * @param candidate an Article of the other matchType
     * @return the result of the check
     */
    public static Match areSameArticles(Article article, Article candidate) {
        // Quick deny / accept
        if (candidate.getDOI() != null && StringUtl.isDOI(candidate.getDOI())
                && article.getDOI() != null && StringUtl.isDOI(article.getDOI())) {

            return article.getDOI().equals(candidate.getDOI()) ?
                    new Match(Match.DUPLICATED,
                            article.isISI() ? article.getID() : candidate.getID(),
                            article.isISI() ? candidate.getID() : article.getID(),
                            1.0d, 1.0d)
                    : null;
        }

        // One Journal can have multiple ISSN ==
//        if (candidate.getISSN() != null && article.getISSN() != null
//                && ! candidate.equals(article.getISSN())) {
//            return (new double[]{NOT_DUPLICATED, 0.0d, 0.0d});
//        }

        // Approx Matching
        double titleScore = titleScore(article, candidate);
        double journalScore = journalScore(article, candidate);

        // Title score is more important as different articles will sure have different name,
        // and the journal may be abbreviated (which lowers journalScore)
        // This case is considered a POSSIBLY_DUPLICATED: titleScore = 0.7d, journalScore = 0.0d
        if (titleScore == 1.0d) {
            return new Match(Match.DUPLICATED,
                    article.isISI() ? article.getID() : candidate.getID(),
                    article.isISI() ? candidate.getID() : article.getID(),
                    titleScore, journalScore);

        } else if (titleScore >= 0.85d) {
            return new Match(journalScore >= 0.85d ? Match.DUPLICATED : Match.POSSIBLY_DUPLICATED,
                    article.isISI() ? article.getID() : candidate.getID(),
                    article.isISI() ? candidate.getID() : article.getID(),
                    titleScore, journalScore);

        } else if (titleScore >= 0.7d) {
            return new Match(Match.POSSIBLY_DUPLICATED,
                    article.isISI() ? article.getID() : candidate.getID(),
                    article.isISI() ? candidate.getID() : article.getID(),
                    titleScore, journalScore);

        } else {
            // No match
            return null;
        }
    }

    public static boolean areSameJournals(Article article, Article candidate) {
        return LCS.isMatch(article.getJournal(), candidate.getJournal(), 0.2d)
                || LCS.isMatch(article.getAbbrJourna(), candidate.getAbbrJourna(), 0.2d);
    }

    public static boolean areSameTitles(Article article, Article candidate) {
        return LCS.isMatch(article.getTitle(), candidate.getTitle(), 0.3d);
    }

    public static double titleScore(Article article, Article candidate) {
        return 1.0d - LCS.approxDistance(article.getTitle(), candidate.getTitle());
    }

    public static double journalScore(Article article, Article candidate) {
        double normalNameScore = 1.0d - LCS.approxDistance(article.getJournal(), candidate.getJournal());
        if (normalNameScore >= 0.8d) {
            return normalNameScore;
        }

        double abbrNameScore = 1.0d - LCS.approxDistance(article.getAbbrJourna(), candidate.getAbbrJourna());
        if (abbrNameScore >= 0.8d) {
            return abbrNameScore;
        } else {
            return normalNameScore > abbrNameScore ? normalNameScore : abbrNameScore;
        }
    }

    public static boolean areSameAuthors(Article article, Article candidate) {

        List<Author> articleListAuthor = article.getListAuthors();
        List<Author> candidateListAuthor = candidate.getListAuthors();

        int match = 0;
        for (Author articleAuthor : articleListAuthor) {
            for (int i = 0; i < candidateListAuthor.size(); ++i) {
                Author candidateAuthor = candidateListAuthor.get(i);

                if (Name.isMatch(articleAuthor, candidateAuthor)) {
                    ++match;
                    candidateListAuthor.remove(i--);
                }
            }
        }

        // Old implementation, where only whole authors string is available
//        String rawAuthorStr;
//        String[] authors;
//
//        if (article.getListAuthors().length > candidate.getListAuthors().length) {
//            rawAuthorStr = article.getRawAuthorStr();
//            authors = candidate.getRawAuthorStr();
//        } else {
//            rawAuthorStr = candidate.getRawAuthorStr();
//            authors = article.getRawAuthorStr();
//        }
//
//        int deduplicate = 0;
//        for (String author : authors) {
//            if (rawAuthorStr.contains(author)) {
//                ++deduplicate;
//            } else {
//                ArrayList<String> possibleAbbrNames = Name.generateAbbrNames(author);
//                for (String abbrName : possibleAbbrNames) {
//                    if (rawAuthorStr.contains(abbrName)) {
//                        ++deduplicate;
//                        break;
//                    }
//                }
//            }
//        }

        // Should've deduplicate / (float)rawAuthorStr.split(", ").length
        // Instead, author.length relax the condition to allow more matches
        return match / (float)Math.min(articleListAuthor.size(), candidateListAuthor.size()) > 0.8f;
    }

    /**
     * Result of areSameArticles()
     */
    public static class Match {
        // 2 types of Match
        public static final int DUPLICATED = 1;
        public static final int POSSIBLY_DUPLICATED = 0;

        private int matchType;
        private int ISI, Scopus;    // IDs of the matched articles
        private double titleScore, journalScore;

        public Match(int type, int isi_id, int scopus_id, double titleScore, double journalScore) {
            this.matchType = type;

            this.ISI = isi_id;
            this.Scopus = scopus_id;

            this.titleScore = titleScore;
            this.journalScore = journalScore;
        }

        public int getISI() {
            return ISI;
        }

        public int getScopus() {
            return Scopus;
        }

        public int getMatchType() {
            return matchType;
        }

        public double getTitleScore() {
            return titleScore;
        }

        public double getJournalScore() {
            return journalScore;
        }
    }
}
