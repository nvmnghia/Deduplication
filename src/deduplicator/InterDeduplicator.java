package deduplicator;

import com.google.gson.JsonArray;
import comparator.LCS;
import config.Config;
import data.*;

import importer.ImportDB;
import util.StringUtl;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InterDeduplicator {

    private static Set<Integer> insertedScopusArticle = new HashSet<>();

    public static List<Match> deduplicate(String type, Integer id) throws IOException, SQLException {
        // First get the article
        Article article = ArticleSource.getArticleByID(type, id);
        if (article == null) {
            return new ArrayList<>();
        }

        // Then search for its name
        // In the OTHER DB
        List<Article> candidates = ArticleSource.getArticles(type.equals("isi") ? "scopus" : "isi", Config.FIELD_TITLE, article.getTitle());

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
                printDebug(article, candidate);

                if (article.isISI()) {
                    listMatches.add(new Match(article.getId(), candidate.getId()));
                } else {
                    listMatches.add(new Match(candidate.getId(), article.getId()));
                }

                if (! duplicated) {
                    duplicated = true;

                    Article theChosenOne = candidate.isScopus() ? candidate : article;
                    theChosenOne.setDuplicate();

                    if (ImportDB.isNeedInsert(theChosenOne)) {
                        ImportDB.createArticle(theChosenOne);
                    }

                    insertedScopusArticle.add(theChosenOne.getId());

                    System.out.print("Import scopus " + theChosenOne.getId() + " for");
                    if (type.equals("isi")) {
                        System.out.print(" isi " + id);
                    }
                } else {
                    if (type.equals("isi")) {
                        System.out.print(" scopus " + candidate.getId());
                    } else {
                        System.out.print(" isi " + candidate.getId());
                    }
                }
            }
        }

        if (! duplicated) {
            if (article.isScopus()) {
                if (! insertedScopusArticle.contains(article.getId())) {
                    insertedScopusArticle.add(article.getId());
                    ImportDB.createArticle(article);
                }

            } else {
                ImportDB.createArticle(article);
            }
        }

        return listMatches;
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

        if (candidate.getISSN() != null && candidate.getISSN().length() != 0
                && article.getISSN() != null && article.getISSN().length() != 0
                && ! candidate.getISSN().replace("-", "").equals(article.getISSN().replace("-", ""))) {
            return false;
        }

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
