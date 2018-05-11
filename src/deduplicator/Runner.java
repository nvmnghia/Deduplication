package deduplicator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import config.Config;
import data.Article;
import data.ArticleSource;
import data.Match;
import importer.ImportDB;
import importer.ImportElastic;
import util.DataUtl;
import util.Sluginator;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Runner {
    public static void main(String[] args) throws IOException, SQLException, InterruptedException {

        System.setOut(new PrintStream(new FileOutputStream(new File("D:\\VCI\\Deduplication\\src\\log.txt"))));

        long start = System.nanoTime();

        ImportElastic.importISIAndScopus();

//        importAllScopus();

//        ImportElastic.importAvailableArticles();
        ImportElastic.importAvailableJournals();
//        ImportElastic.importAvailableOrganizations();
//
//        Thread.sleep(1000);
//
//        List<Match> listMatches = new ArrayList<>();
//
//        int maxIDOfISI = getMaxIDOfDB("isi", "isi_documents");
//        for (int i = 0; i <= maxIDOfISI; ++i) {
//
//            try {
//                addToListMatches(listMatches, Deduplicator.deduplicate("isi", i));
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        Deduplicator.finishHim();
//
//        Sluginator.slugifyAll();
//
//        Gson gson = new GsonBuilder().setPrettyPrinting().create();
//
//        File output = new File("D:\\VCI\\Deduplication\\src\\output.txt");
//
//        BufferedWriter writer = new BufferedWriter(new FileWriter(output));
//        writer.write(gson.toJson(listMatches));
//        writer.close();
//
//        long elapsed = System.nanoTime() - start;
//
//        System.out.println("All took " + elapsed + " nanoseconds");
    }

    public static void importAllScopus() throws SQLException, IOException, InterruptedException {
        int maxIDOfScopus = getMaxIDOfDB("scopus", "scopus_documents");
        for (int i = 0; i <= maxIDOfScopus; ++i) {
            Article scopus = ArticleSource.getArticleByID(Config.ES_INDEX, "scopus", i);
            if (scopus != null) {
                ImportDB.createArticle(scopus);
            }
        }
    }

    public static int getMaxIDOfDB(String dbName, String tableName) throws SQLException {
        ResultSet rs = DataUtl.queryDB(dbName, "SELECT id FROM " + tableName + " ORDER BY id DESC LIMIT 1");
        rs.next();

        int maxIDOfMergedDB = rs.getInt(1);
        System.out.println("Max ID of " + dbName + "." + tableName + ": " + maxIDOfMergedDB);

        return maxIDOfMergedDB;
    }

    public static void addToListMatches(List<Match> currentMatches, List<Match> newMatches) {
        for (int i = 0; i < newMatches.size(); ++i) {
            Match newMatch = newMatches.get(i);

            for (Match currentMatch : currentMatches) {
                if (currentMatch.getISI() == newMatch.getISI() && currentMatch.getScopus() == newMatch.getScopus()) {
                    newMatches.remove(i--);
                    break;
                }
            }
        }

        currentMatches.addAll(newMatches);
    }
}
