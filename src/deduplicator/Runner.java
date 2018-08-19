package deduplicator;

import config.Config;
import data.Article;
import data.ArticleSource;
import importer.ImportDB;
import importer.IndexElastic;
import util.DataUtl;
import util.Sluginator;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static util.DataUtl.getMaxIDOfTable;

public class Runner {

    /**
     * How it works:
     * Input: Original ISI and Scopus DB
     * Output: DB in which the 2 inputs are merged (i.e. without duplicated articles)
     * Those 3 DBs have 3 different schema.
     *
     * - ISI and Scopus articles are indexed into ES
     * - Scopus articles are imported to the output DB
     * - For each ISI article, check if it is duplicated with a Scopus article by searching in ES
     *     + If it is duplicated, set is_isi of the duplicated Scopus article in the output DB to true
     *     + If it is not duplicated, import the ISI article into the output DB
     * - The deduplication function returns a Match containing the id (in the original DB)
     *   of the duplicated ISI article and its corresponding Scopus article.
     *   These information is used only for logging.
     *
     *  The Scopus DB's quality is better, so basically ISI is MERGED INTO Scopus.
     *
     * tl;dr: import all Scopus, loop over ISI, check if the ISI is duplicated with the Scopus.
     * If no duplication found, import ISI
     */

    public static void main(String[] args) throws IOException, SQLException, InterruptedException {
        System.setOut(new PrintStream(new FileOutputStream(new File("log.txt"))));

        if (Config.DB.NUCLEAR_OPTION) {
            String myDearestWarning = "NUCLEAR_OPTION ENABLED... RUN FOR YOUR FUCKIN LIFE (i.e. ask yourself if you ABSOLUTELY need this)\n" +
                    "THIS OPTION SHOULD BE USED ONLY FOR TESTING. IT WILL DELETE THESE TABLES TO START A FRESH DEDUPLICATION/MERGE:\n" +
                    "articles, articles_authors, authors, authors_organizes, journals, merge_logs, organizes\n" +
                    "YOU HAVE 20 SECONDS TO THINK ABOUT YOUR LIFE...";

            System.out.println(myDearestWarning);
            System.err.println(myDearestWarning);

            Thread.sleep(20000);
        }

        long start = System.nanoTime();

        IndexElastic.indexCrawledISIAndScopus();

        importAllScopus();

        IndexElastic.indexAvailableArticles();
        IndexElastic.indexAvailableJournals();
        IndexElastic.indexAvailableOrganizations();

        int maxIDOfISI = DataUtl.getMaxIDOfTable(Config.DB.INPUT, "isi_documents");
        if (Config.START_IMPORT_ISI_FROM > maxIDOfISI) {
            throw new RuntimeException("Config.START_IMPORT_ISI_FROM is larger than the current max ID of isi_documents.");
        }

        for (int i = Config.START_IMPORT_ISI_FROM; i <= maxIDOfISI; ++i) {

            try {
                List<Deduplicator.Match> matches = Deduplicator.deduplicate(Article.ISI, i);

                if (matches.size() != 0) {
                    writeLogToDB(matches);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Sluginator.slugifyAll();

        IndexElastic.cleanTemporaryIndices();

        long elapsed = System.nanoTime() - start;

        System.out.println("All took " + elapsed + " nanoseconds");
    }

    private static PreparedStatement pstmLogMergedArticle = null;
    public static void writeLogToDB(List<Deduplicator.Match> matches) throws SQLException {
        if (pstmLogMergedArticle == null) {
            pstmLogMergedArticle = DataUtl.getDBConnection().prepareStatement(
                    "INSERT INTO " + Config.DB.OUTPUT + ".merge_logs (isi_id, duplication_of, possible_duplication_of, title_score, journal_score, is_merged) VALUES(?, ?, ?, ?, ?, ?)");
        }

        for (Deduplicator.Match match : matches) {
            pstmLogMergedArticle.setInt(1, match.getISI());
            pstmLogMergedArticle.setInt(match.getMatchType() == Deduplicator.Match.DUPLICATED ? 2 : 3, match.getScopus());
            pstmLogMergedArticle.setNull(match.getMatchType() == Deduplicator.Match.DUPLICATED ? 3 : 2, java.sql.Types.INTEGER);

            if (match.getTitleScore() == -1d) {
                pstmLogMergedArticle.setNull(4, Types.FLOAT);
            } else {
                pstmLogMergedArticle.setFloat(4, (float) match.getTitleScore());
            }

            if (match.getJournalScore() == -1d) {
                pstmLogMergedArticle.setNull(5, Types.FLOAT);
            } else {
                pstmLogMergedArticle.setFloat(5, (float) match.getJournalScore());
            }

            pstmLogMergedArticle.setBoolean(6, match.getMatchType() == Deduplicator.Match.DUPLICATED);

            pstmLogMergedArticle.addBatch();
        }

        pstmLogMergedArticle.executeBatch();
    }

    /**
     * Import all Scopus articles into the output DB
     *
     * @throws SQLException
     * @throws IOException
     */
    private static void importAllScopus() throws SQLException, IOException {
        int maxIDOfScopus = getMaxIDOfTable(Config.DB.INPUT, "scopus_documents");

        if (Config.START_IMPORT_SCOPUS_FROM > maxIDOfScopus) {
            throw new RuntimeException("Config.START_IMPORT_SCOPUS_FROM is larger than the current max ID of scopus_documents.");
        }

        for (int i = Config.START_IMPORT_SCOPUS_FROM; i <= maxIDOfScopus; ++i) {
            Article Scopus = ArticleSource.getArticleByID(Config.ES.INDEX, Article.SCOPUS, i);

            if (Scopus != null) {
                ImportDB.createArticle(Scopus);
                System.out.println("   Created:  " + Scopus.toShortString() + "  as  DB-" + Scopus.getMergedID());
            }
        }
    }
}
