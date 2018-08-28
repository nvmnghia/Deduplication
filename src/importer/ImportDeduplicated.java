package importer;

import config.Config;
import data.Article;
import data.ArticleSource;
import data.Organization;
import deduplicator.Deduplicator;
import deduplicator.Runner;
import representative.Representative;
import util.DataUtl;
import util.Sluginator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static util.DataUtl.getMaxIDOfTable;

public class ImportDeduplicated {
    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(new FileOutputStream(new File("log.txt"))));

        if (Config.DB.NUCLEAR_OPTION) {
            String myDearestWarning = "NUCLEAR_OPTION ENABLED... RUN FOR YOUR FUCKIN LIFE (i.e. ask yourself if you ABSOLUTELY need this)\n" +
                    "THIS OPTION SHOULD BE USED ONLY FOR TESTING. IT WILL WIPE THESE TABLES OF " + Config.DB.OUTPUT + " TO START A FRESH DEDUPLICATION/MERGE:\n" +
                    "articles, articles_authors, authors, authors_organizes, journals, merge_logs, organizes\n" +
                    "CONTINUE? (y/n)";

            System.out.println(myDearestWarning);
            System.err.println(myDearestWarning);

            Scanner scanner = new Scanner(System.in);
            if (! scanner.nextLine().equals("y")) {
                return;
            }
        }

        long start = System.nanoTime();

        IndexElastic.indexCrawledISIAndScopus();
        importAllScopus();
        importNonDuplicatedISI();
        markDuplicatedScopus();

        Sluginator.slugifyAll();
        Representative.apply();

        IndexElastic.cleanTemporaryIndices();

        long elapsed = System.nanoTime() - start;

        System.out.println("All took " + elapsed + " nanoseconds");
    }

    public static void importAllScopus() throws IOException, SQLException {
        int maxIDOfScopus = getMaxIDOfTable(Config.DB.INPUT, "scopus_documents");

        int counter = 0;
        for (int i = 0; i <= maxIDOfScopus; ++i) {

            Article scopus = ArticleSource.getArticleByID(Config.ES.INDEX, Article.SCOPUS, i);

            if (scopus != null) {
                if (scopus.getID() != i) {
                    System.out.println("WRONG SCOPUS ID, ES PROBLEM");
                    continue;
                }

                ImportDB.createArticle(scopus);
                System.out.println("   Created:  " + scopus.toShortString() + "  as  DB-" + scopus.getMergedID());
                ++counter;
            }
        }

        System.out.println("Created " + counter + " Scopus articles");
    }

    public static void importNonDuplicatedISI() throws SQLException, IOException {
        String query = "SELECT id FROM " + Config.DB.INPUT + ".isi_documents WHERE raw_scopus_id = ''";
        ResultSet rs = DataUtl.queryDB(Config.DB.INPUT, query);
        List<Integer> IDs = new ArrayList<>();

        while (rs.next()) {
            IDs.add(rs.getInt(1));
        }
        rs.close();

        int counter = 0;
        for (Integer ID : IDs) {
            Article ISI = ArticleSource.getArticleByID(Config.ES.INDEX, Article.ISI, ID);

            if (ISI != null) {
                if (ISI.getID() != ID) {
                    System.out.println("WRONG ISI ID, ES PROBLEM");
                    continue;
                }

                ImportDB.createArticle(ISI);
                System.out.println("   Created:  " + ISI.toShortString() + "  as  DB-" + ISI.getMergedID());

                ++counter;
            } else {
                if (! IndexElastic.unavailableISI.contains(ID)) {
                    System.out.println("NULL ISI ARTICLE, ES PROBLEM");
                }
            }
        }

        System.out.println("Created " + counter + " non-duplicated ISI articles");
    }

    public static void markDuplicatedScopus() throws Exception {
        List<Deduplicator.Match> matches = new ArrayList<>();

        String query = "SELECT id, raw_scopus_id FROM " + Config.DB.INPUT + ".isi_documents WHERE raw_scopus_id != ''";
        ResultSet rs = DataUtl.queryDB(Config.DB.INPUT, query);
        Map<Integer, Integer> ISI2Scopus = new HashMap<>();

        while (rs.next()) {
            ISI2Scopus.put(rs.getInt(1), Integer.parseInt(rs.getString(2)));
        }
        rs.close();

        int counter = 0;
        for (Map.Entry<Integer, Integer> entry : ISI2Scopus.entrySet()) {
            Article Scopus = ArticleSource.getArticleByID(Config.ES.INDEX, Article.SCOPUS, entry.getValue());

            if (Scopus != null) {
                if (Scopus.getID() != entry.getValue()) {
                    System.out.println("WRONG SCOPUS ID, ES PROBLEM");
                    continue;
                }

                Deduplicator.updateArticleAndJournal(Scopus, entry.getKey());
                System.out.println("Duplicated: ISI-" + entry.getKey() + "    Scopus-" + entry.getValue());

                matches.add(new Deduplicator.Match(Deduplicator.Match.DUPLICATED, entry.getKey(), entry.getValue(), -1d, -1d));
                ++counter;

//                corrector(fetchOrganizations(false, entry.getKey()),
//                        fetchOrganizations(true, entry.getValue()));
            } else {
                System.out.println("NULL SCOPUS ARTICLE, ES PROBLEM");
            }
        }

        System.out.println("There're " + counter + " duplicated couples. These are the ISI ID of them:");
        for (Map.Entry<Integer, Integer> entry : ISI2Scopus.entrySet()) {
            System.out.println(entry.getKey() + ", ");
        }

        Runner.writeLogToDB(matches);
    }

    private static List<String> fetchOrganizations(boolean isScopus, int ID) throws SQLException {
        String table = isScopus ? "scopus_documents" : "isi_documents";
        String query = "SELECT authors_json FROM " + table + " WHERE id = " + ID;

        ResultSet rs = DataUtl.queryDB(Config.DB.INPUT, query);
        rs.next();

        Article temp = new Article();
        temp.setAuthorsJSON(rs.getString(1));

        return temp.getOrganizations();
    }

    private static Map<String, String> corrector(List<String> ISI, List<String> Scopus) throws Exception {
        if (ISI == null) {
            ISI = new ArrayList<>();
        }
        if (Scopus == null) {
            Scopus = new ArrayList<>();
        }

        for (int i = 0; i < ISI.size(); ++i) {
            String isi = ISI.get(i);

            for (int j = 0; j < Scopus.size(); ++j) {
                String scopus = Scopus.get(j);

                if (Organization.nameSimilarity(isi, scopus) >= 0.9d) {
                    isOfDifferentRepresentative(ISI.remove(i--), Scopus.remove(j--));
                    break;
                }
            }
        }

        return null;
    }

    private static Representative representative = null;
    private static boolean isOfDifferentRepresentative(String A, String B) throws Exception {
        if (representative == null) {
            representative = new Representative("Affiliations.xml");
        }

        List<Integer> representativeA = representative.getRepresentativesOf(A);
        List<Integer> representativeB = representative.getRepresentativesOf(A);

        if (representativeA.size() != representativeB.size() || (representativeA.size() != 0 && representativeA.get(0) != representativeB.get(0))) {
            System.out.println("Fuck");

            System.out.println(A + "    "  +representativeA);
            System.out.println(B + "    "  +representativeB);
        }

        return false;
    }
}
