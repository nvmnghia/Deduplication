package comparator;

import config.Config;
import data.Article;
import data.ArticleSource;
import importer.ImportDB;
import util.DataUtl;
import util.StringUtl;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static util.DataUtl.getMaxIDOfTable;

public class Test {
    public static void main(String[] args) throws UnknownHostException, SQLException {
        searchByID();
    }

    public static void searchByTitle() throws SQLException {
        Set<String> duplicatedISITitlesHieu = new HashSet<>();
        Set<String> duplicatedISITitlesNghia = new HashSet<>();

        String query = "SELECT title FROM isi_documents WHERE raw_scopus_id != ''";
        ResultSet rs = DataUtl.queryDB(Config.DB.OUPUT, query);

        while (rs.next()) {
//            if (rs.getString(1).contains("Surveillance of")) {
//                System.out.println(StringUtl.normalize(rs.getString(1)));
//            }

            duplicatedISITitlesHieu.add(StringUtl.normalize(rs.getString(1)));
        }

        query = "SELECT title FROM articles WHERE is_scopus = 1 AND is_isi = 1";
        rs = DataUtl.queryDB(Config.DB.OUPUT, query);

        int counter = 0;
        while (rs.next()) {
            if (! duplicatedISITitlesHieu.contains(StringUtl.normalize(rs.getString(1)))) {
                System.out.println(rs.getString(1));
                counter++;
            }

//            if (rs.getString(1).startsWith("Surveillance of dengue ")) {
//                System.out.println(StringUtl.normalize(rs.getString(1)));
//            }
        }

        System.out.println(counter);
    }

    public static void searchByID() throws SQLException {
        // All mapping is of this type: ISI -> Scopus
        Map<Integer, Integer> nghia = new HashMap<>();

        String query = "SELECT raw_isi_id, raw_scopus_id FROM articles WHERE raw_scopus_id IS NOT NULL AND raw_isi_id IS NOT NULL";
        ResultSet rs = DataUtl.queryDB(Config.DB.OUPUT, query);

        while (rs.next()) {
            nghia.put(rs.getInt(1), rs.getInt(2));
        }

        Map<Integer, Integer> hieu = new HashMap<>();

        query = "SELECT id, raw_scopus_id FROM isi_documents WHERE raw_scopus_id != ''";
        rs = DataUtl.queryDB(Config.DB.OUPUT, query);

        while (rs.next()) {
            hieu.put(rs.getInt(1), rs.getInt(2));
        }

        System.out.println("WHAT H FOUND BUT N DIDN'T");
        int counter = 0;
        for (Integer isi : hieu.keySet()) {
            if (! nghia.containsKey(isi)) {
                System.out.print(isi + "    ");
                ++counter;
            }
        }
        System.out.println("\n" + counter);

        System.out.println("WHAT N FOUND BUT H DIDN'T");
        counter = 0;
        for (Integer isi : nghia.keySet()) {
            if (! hieu.containsKey(isi)) {
                System.out.print(isi + "    ");
                ++counter;
            }
        }
        System.out.println("\n" + counter);

        System.out.println("WHAT H FOUND BUT DIFFERS FROM N");
        counter = 0;
        for (Map.Entry<Integer, Integer> entry : hieu.entrySet()) {
            if (nghia.containsKey(entry.getKey()) && ! nghia.get(entry.getKey()).equals(entry.getValue())) {
                System.out.print(entry.getKey() + "    ");
                ++counter;
            }
        }
        System.out.println("\n" + counter);

        System.out.println("WHAT N FOUND BUT DIFFERS FROM H");
        counter = 0;
        for (Map.Entry<Integer, Integer> entry : nghia.entrySet()) {
            if (hieu.containsKey(entry.getKey()) && ! hieu.get(entry.getKey()).equals(entry.getValue())) {
                System.out.print(entry.getKey() + "    ");
                ++counter;
            }
        }
        System.out.println("\n" + counter);
    }

    public static void importSensei() throws SQLException, IOException {
        // Import all Scopus


        // Import ISI which has raw_scopus_id is '' (i.e. no duplication)
    }
}
