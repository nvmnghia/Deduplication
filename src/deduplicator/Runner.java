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
     * Those 3 DBs have 3 similar, but different schema.
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

    public static void main(String[] args) throws IOException, SQLException {
        System.setOut(new PrintStream(new FileOutputStream(new File("log.txt"))));

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

        Set<Integer> suspicious = new HashSet<>();
        int[] ids = {59, 469, 484, 628, 669, 772, 838, 871, 915, 1060, 1128, 1235, 1238, 1240, 1453, 1568, 1609, 1645, 1776, 2021, 2089, 2097, 2237, 2277, 2595, 2597, 2689, 2796, 2807, 2856, 2900, 2922, 2943, 2944, 2947, 2974, 2977, 2984, 2990, 2991, 2992, 2993, 2994, 2995, 2996, 2997, 2998, 2999, 3000, 3001, 3002, 3003, 3004, 3005, 3006, 3007, 3008, 3009, 3010, 3011, 3012, 3013, 3014, 3015, 3038, 3048, 3126, 3146, 3215, 3234, 3278, 3279, 3281, 3283, 3284, 3285, 3286, 3287, 3288, 3341, 3342, 3368, 3390, 3402, 3403, 3477, 3520, 3563, 3588, 3614, 3626, 3648, 3649, 3650, 3669, 3722, 3723, 3724, 3725, 3726, 3727, 3728, 3729, 3730, 3731, 3732, 3733, 3734, 3735, 3736, 3737, 3762, 3763, 3764, 3765, 3766, 3767, 3768, 3769, 3770, 3771, 3772, 3773, 3774, 3775, 3776, 3782, 3826, 3827, 3830, 3831, 3832, 3833, 3834, 3835, 3836, 3837, 3838, 3839, 3840, 3841, 3842, 3849, 3854, 3855, 3863, 3867, 3868, 3869, 3913, 3915, 3917, 3918, 3919, 3920, 3921, 3922, 3924, 3925, 3926, 3927, 3928, 3929, 3930, 3931, 3932, 3933, 3934, 3935, 3936, 3937, 3938, 3939, 3940, 3941, 3942, 3943, 3944, 3945, 3946, 3947, 3960, 3961, 3973, 3975, 3988, 3992, 4000, 4001, 4012, 4020, 4021, 4054, 4068, 4072, 4084, 4087, 4113, 4130, 4135, 4172, 4174, 4175, 4176, 4177, 4178, 4179, 4191, 4205, 4273, 4286, 4294, 4301, 4302, 4361, 4406, 4424, 4447, 4456, 4457, 4458, 4459, 4460, 4461, 4462, 4463, 4464, 4465, 4466, 4467, 4468, 4469, 4470, 4471, 4472, 4473, 4474, 4475, 4476, 4477, 4478, 4479, 4480, 4481, 4482, 4483, 4484, 4485, 4486, 4487, 4488, 4489, 4490, 4491, 4492, 4493, 4494, 4495, 4496, 4497, 4498, 4499, 4500, 4501, 4502, 4503, 4504, 4505, 4506, 4507, 4533, 4534, 4560, 4566, 4631, 4632, 4659, 4660, 4726, 4794, 4857, 5140, 5194, 5217, 5268};
        for (int id : ids) {
            suspicious.add(id);
        }

        for (int i = Config.START_IMPORT_ISI_FROM; i <= maxIDOfISI; ++i) {
            if (suspicious.contains(i)) {
                System.err.println("now what");
            }

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

        long elapsed = System.nanoTime() - start;

        IndexElastic.cleanTemporaryIndices();

        System.out.println("All took " + elapsed + " nanoseconds");
    }

    private static PreparedStatement pstmLogMergedArticle = null;
    public static void writeLogToDB(List<Deduplicator.Match> matches) throws SQLException {
        if (pstmLogMergedArticle == null) {
            pstmLogMergedArticle = DataUtl.getDBConnection().prepareStatement(
                    "INSERT INTO " + Config.DB.OUPUT + ".merge_logs (isi_id, duplication_of, possible_duplication_of, title_score, journal_score, is_merged) VALUES(?, ?, ?, ?, ?, ?)");
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
                if (Scopus.getID() != i) {
                    System.out.println("WRONG ISI ID, ES PROBLEM");
                    continue;
                }

                ImportDB.createArticle(Scopus);
                System.out.println("   Created:  " + Scopus.toShortString() + "  as  DB-" + Scopus.getMergedID());
            }
        }
    }
}
