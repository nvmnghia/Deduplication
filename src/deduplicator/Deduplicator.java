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

    private static final double DUPLICATED = 1d;
    private static final double POSSIBLY_DUPLICATED = 0d;
    private static final double NOT_DUPLICATED = -1d;

    private static int counterISI = 0;

    public static List<Match> deduplicate(String type, Integer id) throws IOException, SQLException {
        // First get the article
        Article article = ArticleSource.getArticleByID(Config.ES_INDEX, type, id);
        if (article == null) {
            return new ArrayList<>();
        }

        // Then search for its name, in the merged Elas
        List<Article> candidates = ArticleSource.getArticles("available_articles", type.equals("isi") ? "scopus" : "isi", Config.FIELD_TITLE, article.getTitle());

        // Filter by year
        filterByYear(candidates, article.getYear());

        List<Match> listMatches = new ArrayList<>();
        boolean duplicated = false, possiblyDuplicated = false;

        List<Article> listPossibleCandidates = new ArrayList<>();
        List<Double> listPossibleCandidateTitleScore = new ArrayList<>();
        List<Double> listPossibleCandidateJournalScore = new ArrayList<>();

        for (Article candidate : candidates) {
            // This can't be the case: candidates are from available_article which are articles in the merged DB, and the article is an article in the crawled DB
//            if (candidate.getID() == article.getID()) {
//                continue;
//            }

            double[] result = areSameArticles(article, candidate);
            if (result[0] == DUPLICATED) {
                duplicated = true;

                if (article.isISI()) {
                    listMatches.add(new Match(article.getID(), candidate.getID()));
                } else {
                    listMatches.add(new Match(candidate.getID(), article.getID()));
                }

                // Candidates are Scopus, which are already imported
                // But they and their journals need to be updated: is_isi = true
                updateArticleAndJournal(candidate);

                System.out.println("Duplicated: ISI: " + article.getTitle() + "    Scopus: " + candidate.getTitle());

                // Normally, the quest for duplicated articles ends here, as the duplicated one was found
                // But the quest should continue to find POSSIBLY_DUPLICATED articles
                // This has some surprise side effect:
                // There may be multiple matches, therefore the number of is_isi in the merged DB could be larger than the number of row in the crawled DB

            } else if (result[0] == POSSIBLY_DUPLICATED) {
                possiblyDuplicated = true;

                listPossibleCandidates.add(candidate);
                listPossibleCandidateTitleScore.add(result[1]);
                listPossibleCandidateJournalScore.add(result[2]);
            }
        }

        if (possiblyDuplicated || ! duplicated) {
            ImportDB.createArticle(article);
            System.out.println("Inserted " + ++counterISI + " ISI articles");
        }

        if (possiblyDuplicated) {
            for (int i = 0; i < listPossibleCandidates.size(); ++i) {
                addPossiblyDuplicatedArticles(article, listPossibleCandidates.get(i),
                        listPossibleCandidateTitleScore.get(i), listPossibleCandidateJournalScore.get(i));
            }
        }

        return listMatches;
    }

    private static PreparedStatement pstmInsertPossiblyDuplicatedArticles = null;
    public static void addPossiblyDuplicatedArticles(Article article, Article candidate, double titleScore, double journalScore) throws SQLException {
        if (pstmInsertPossiblyDuplicatedArticles == null) {
            pstmInsertPossiblyDuplicatedArticles = DataUtl.getDBConnection().prepareStatement("INSERT INTO vci_scholar.possibly_duplicated_articles (isi_id, scopus_id, title_score, journal_score) VALUES(?, ?, ?, ?)");
        }

        pstmInsertPossiblyDuplicatedArticles.setInt(1, article.getID());
        pstmInsertPossiblyDuplicatedArticles.setInt(2, candidate.getID());
        pstmInsertPossiblyDuplicatedArticles.setFloat(3, (float) titleScore);
        pstmInsertPossiblyDuplicatedArticles.setFloat(4, (float) journalScore);

        pstmInsertPossiblyDuplicatedArticles.addBatch();
    }

    private static PreparedStatement pstmUpdateArticle = null;
    private static PreparedStatement pstmUpdateJournal = null;
    private static void updateArticleAndJournal(Article candidate) throws SQLException, IOException {
        // Update articles
        if (pstmUpdateArticle == null) {
            pstmUpdateArticle = DataUtl.getDBConnection().prepareStatement("UPDATE vci_scholar.articles SET is_isi = 1 WHERE id = ?");
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
            pstmUpdateJournal = DataUtl.getDBConnection().prepareStatement("UPDATE vci_scholar.journals SET is_isi = 1 WHERE id = ?");
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

    private static void printDebug(Article article, Article candidate) {
        try {
            String output_debug = "";

            if (article.isISI() && candidate.isISI()) {
                output_debug += "Both ISI\n";
            }
            if (article.isScopus() && candidate.isScopus()) {
                output_debug += "Both Scopus\n";
            }

            output_debug += article.toString() + "\n\n" + candidate.toString() + "\n\n\n\n";
            Files.write(Paths.get("D:\\VCI\\Deduplication\\src\\outfile_relaxed.txt"), output_debug.getBytes(), StandardOpenOption.APPEND);
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

    public static void filterByYear(List<Article> articles, int year) {
        if (year == -1) {
            return;
        }

        for (int i = 0; i < articles.size(); ++i) {
            if (articles.get(i).getYear() != year && articles.get(i).getYear() != -1) {
                // Definitely not
                articles.remove(i--);
            }
        }
    }

    /**
     * Check if the 2 articles are the same
     * The result is of the following format
     * [IS_DUPLICATED, titleScore, journalScore]
     * IS_DUPLICATED: can be DUPLICATED (1.0d), POSSIBLY_DUPLICATED (0.0d), or NOT_DUPLICATED (-1.0d)
     * titleScore, journalScore: similarity scores, ranging from 0.0d to to 1.0d
     *
     * @param article an Article of either ISI or Scopus type
     * @param candidate an Article of the other type
     * @return the result of the check
     */
    public static double[] areSameArticles(Article article, Article candidate) {
        // Quick deny / accept
        if (candidate.getDOI() != null && StringUtl.isDOI(candidate.getDOI())
                && article.getDOI() != null && StringUtl.isDOI(article.getDOI())) {

            return article.getDOI().equals(candidate.getDOI()) ? (new double[]{DUPLICATED, 1.0d, 1.0d}) : (new double[]{NOT_DUPLICATED, 0.0d, 0.0d});
        }

        // One Journal can have multiple ISSN ==
//        if (candidate.getISSN() != null && article.getISSN() != null
//                && ! candidate.equals(article.getISSN())) {
//            return (new double[]{NOT_DUPLICATED, 0.0d, 0.0d});
//        }

        // Approx Matching
        double titleScore = titleScore(article, candidate);
        double journalScore = journalScore(article, candidate);

        if (titleScore >= 0.8d) {
            if (journalScore >= 0.8d) {
                return new double[]{DUPLICATED, titleScore, journalScore};
            } else {
                return new double[]{POSSIBLY_DUPLICATED, titleScore, journalScore};
            }
        } else if (titleScore >= 0.7d) {
            return new double[]{POSSIBLY_DUPLICATED, titleScore, journalScore};
        } else {
            return new double[]{NOT_DUPLICATED, 0.0d, 0.0d};
        }
    }

    public static boolean areSameJournals(Article article, Article candidate) {
        return LCS.isMatch(article.getJournal(), candidate.getJournal(), 0.2d) || LCS.isMatch(article.getAbbrJourna(), candidate.getAbbrJourna(), 0.2d);
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
     * Everything related to batch update/insert goes here
     * One final blow to import them all
     */
    public static void finishHim() throws SQLException {
        if (pstmInsertPossiblyDuplicatedArticles != null) {
            pstmInsertPossiblyDuplicatedArticles.executeUpdate();
        }

        ImportDB.finishHim();
    }
}
