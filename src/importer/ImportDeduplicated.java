package importer;

import config.Config;
import data.Article;
import data.ArticleSource;
import deduplicator.Deduplicator;
import deduplicator.Runner;
import importer.ImportDB;
import util.DataUtl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static util.DataUtl.getMaxIDOfTable;

public class ImportDeduplicated {
    public static void main(String[] args) throws IOException, SQLException {
        System.setOut(new PrintStream(new FileOutputStream(new File("log.txt"))));

        IndexElastic.indexCrawledISIAndScopus();
        importAllScopus();
        importNonDuplicatedISI();
        markDuplicatedScopus();
    }

    public static void importAllScopus() throws IOException, SQLException {
        int maxIDOfScopus = getMaxIDOfTable(Config.DB.INPUT, "scopus_documents");

        for (int i = 0; i <= maxIDOfScopus; ++i) {
            Article scopus = ArticleSource.getArticleByID(Config.ES.INDEX, Article.SCOPUS, i);

            if (scopus != null) {
                if (scopus.getID() != i) {
                    System.out.println("WRONG SCOPUS ID, ES PROBLEM");
                    continue;
                }

                ImportDB.createArticle(scopus);
                System.out.println("   Created:  " + scopus.toShortString() + "  as  DB-" + scopus.getMergedID());
            }
        }
    }

    public static void importNonDuplicatedISI() throws SQLException, IOException {
        String query = "SELECT id FROM " + Config.DB.INPUT + ".isi_documents WHERE raw_scopus_id = ''";
        ResultSet rs = DataUtl.queryDB(Config.DB.INPUT, query);
        List<Integer> IDs = new ArrayList<>();

        while (rs.next()) {
            IDs.add(rs.getInt(1));
        }

        for (Integer ID : IDs) {
            Article ISI = ArticleSource.getArticleByID(Config.ES.INDEX, Article.ISI, ID);

            if (ISI != null) {
                if (ISI.getID() != ID) {
                    System.out.println("WRONG ISI ID, ES PROBLEM");
                    continue;
                }

                ImportDB.createArticle(ISI);
                System.out.println("   Created:  " + ISI.toShortString() + "  as  DB-" + ISI.getMergedID());
            } else {
                System.out.println("NULL ISI ARTICLE, ES PROBLEM");
            }
        }
    }

    public static void markDuplicatedScopus() throws SQLException, IOException {
        List<Deduplicator.Match> matches = new ArrayList<>();

        String query = "SELECT id, raw_scopus_id FROM " + Config.DB.INPUT + ".isi_documents WHERE raw_scopus_id != ''";
        ResultSet rs = DataUtl.queryDB(Config.DB.INPUT, query);
        Map<Integer, Integer> ISI2Scopus = new HashMap<>();

        while (rs.next()) {
            ISI2Scopus.put(rs.getInt(1), Integer.parseInt(rs.getString(2)));
        }

        for (Map.Entry<Integer, Integer> entry : ISI2Scopus.entrySet()) {
            Article Scopus = ArticleSource.getArticleByID(Config.ES.INDEX, Article.SCOPUS, entry.getValue());

            if (Scopus != null) {
                if (Scopus.getID() != entry.getValue()) {
                    System.out.println("WRONG SCOPUS ID, ES PROBLEM");
                    continue;
                }

                Deduplicator.updateArticleAndJournal(Scopus, entry.getKey());
                matches.add(new Deduplicator.Match(Deduplicator.Match.DUPLICATED, entry.getKey(), entry.getValue(), -1d, -1d));
            } else {
                System.out.println("NULL SCOPUS ARTICLE, ES PROBLEM");
            }
        }

        Runner.writeLogToDB(matches);
    }
}
