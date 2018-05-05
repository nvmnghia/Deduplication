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
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Deduplicator {

    public static List<Match> deduplicate(String type, Integer id) throws IOException, SQLException {
        // First get the article
        Article article = ArticleSource.getArticleByID(Config.ES_INDEX, type, id);
        if (article == null) {
            return new ArrayList<>();
        }

        // Then search for its name, in the merged DB
        List<Article> candidates = ArticleSource.getArticles("available_articles", type.equals("isi") ? "scopus" : "isi", Config.FIELD_TITLE, article.getTitle());

        // Filter by year
        filterByYear(candidates, article.getYear());

        List<Match> listMatches = new ArrayList<>();
        boolean duplicated = false;

        for (Article candidate : candidates) {
            if (candidate.getId() == article.getId()) {
                // Search result will include the original article
                continue;
            }

            if (isDuplicate(article, candidate)) {
                duplicated = true;

                if (article.isISI()) {
                    listMatches.add(new Match(article.getId(), candidate.getId()));
                } else {
                    listMatches.add(new Match(candidate.getId(), article.getId()));
                }

                // Candidates are Scopus, which are already imported
                // But they and their journals need to be updated: is_isi = true;
                updateArticleAndJournal(candidate);
            }
        }

        if (! duplicated) {
            ImportDB.createArticle(article);
        }

        return listMatches;
    }

    private static PreparedStatement pstmUpdateArticle = null;
    private static PreparedStatement pstmUpdateJournal = null;
    private static void updateArticleAndJournal(Article candidate) throws SQLException, IOException {
        // Update articles
        if (pstmUpdateArticle == null) {
            pstmUpdateArticle = DataUtl.getDBConnection().prepareStatement("UPDATE vci_scholar.articles SET is_isi = 1 WHERE id = ?");
        }
        pstmUpdateArticle.setInt(1, candidate.getId());
        pstmUpdateArticle.executeUpdate();

        UpdateRequest request = new UpdateRequest("available_articles", "articles", String.valueOf(candidate.getId()));
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
        int journalID = ImportDB.findOrCreateJournal(candidate);
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
            if (articles.get(i).getYear() != year) {
                if (articles.get(i).getYear() != -1) {
                    // Definitely not
                    articles.remove(i);
                    --i;
                } else {
                    // Give it a chance! Give it a chance! LMAO
                }
            }
        }
    }

    public static boolean isDuplicate(Article article, Article candidate) {
        // Quick deny / accept
        if (candidate.getDOI() != null && StringUtl.isDOI(candidate.getDOI())
                && article.getDOI() != null && StringUtl.isDOI(article.getDOI())) {

            return article.getDOI().equals(candidate.getDOI());
        }

        // One Journal can have multiple ISSN ==
//        if (candidate.getISSN() != null && article.getISSN() != null
//                && ! candidate.equals(article.getISSN())) {
//            return false;
//        }

        // Approx Matching
        boolean isSameTitle = isSameTitle(article, candidate);
        boolean isSameJournal = isSameJournal(article, candidate);
//        boolean isSameAuthors = isSameAuthors(article, candidate);

        // Look suspicious, need more investigation
        if (isSameTitle && ! isSameJournal) {
            try {
                Files.write(Paths.get("D:\\VCI\\Deduplication\\src\\almost.txt"), (article.toString() + "\n\n" + candidate.toString() + "\n\n\n\n").getBytes(), StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return isSameTitle && isSameJournal;
    }

    public static boolean isSameJournal(Article article, Article candidate) {
        return LCS.isMatch(article.getJournal(), candidate.getJournal(), 0.2d) || LCS.isMatch(article.getJournal_abbr(), candidate.getJournal_abbr(), 0.2d);
    }

    public static boolean isSameTitle(Article article, Article candidate) {
        return LCS.isMatch(article.getTitle(), candidate.getTitle(), 0.3d);
    }

    public static boolean isSameAuthors(Article article, Article candidate) {

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
}
