package comparator;

import com.sun.org.apache.bcel.internal.generic.TABLESWITCH;
import config.Config;
import data.Article;
import data.Organization;
import util.DataUtl;
import util.StringUtl;

import javax.swing.text.TabExpander;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Test {
    public static void main(String[] args) throws SQLException, FileNotFoundException {
        System.setOut(new PrintStream(new FileOutputStream(new File("log.txt"))));

        testSuffix();
    }

    /**
     * Use suffix list to split clumped organization string
     */
    public static void testSuffix() throws SQLException {
        Set<String> organizationSuffixes = getOrganizationSuffixes();
        for (String suffix : organizationSuffixes) {
            System.out.print(suffix + "    ");
        }
        System.out.println();

        String[] tables = {"isi_documents", "scopus_documents"};

        for (String table : tables) {
            String query = "SELECT authors_json FROM " + table;
            ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT, query);

            while (rs.next()) {
                Article article = new Article();
                article.setAuthorsJSON(rs.getString(1));

                List<String> organizations = article.getListOrganizations();
                if (organizations == null) {
                    continue;
                }

                for (String organization : organizations) {
                    organization = StringUtl.correct(organization).toLowerCase().trim();
                    int suffixCounter = 0;

                    for (String suffix : organizationSuffixes) {
                        suffixCounter += StringUtl.countOccurences(organization, suffix);
                    }

                    if (suffixCounter > 1) {
                        System.out.println(suffixCounter + "    " + organization);
                    }
                }
            }
        }
    }

    public static Set<String> getOrganizationSuffixes() throws SQLException {
        Map<String, Integer> counter = new HashMap<>();
        String[] tables = {"isi_documents", "scopus_documents"};

        for (String table : tables) {
            String query = "SELECT authors_json FROM " + table;
            ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT, query);

            while (rs.next()) {
                Article article = new Article();
                article.setAuthorsJSON(rs.getString(1));

                List<String> organizations = article.getListOrganizations();
                if (organizations == null) {
                    continue;
                }

                for (String organization : organizations) {
                    String suffix = Organization.getSuffix(organization);
                    if (suffix != null) {
                        if (counter.containsKey(suffix)) {
                            counter.put(suffix, counter.get(suffix) + 1);
                        } else {
                            counter.put(suffix, 1);
                        }
                    }
                }
            }
        }

        int sum = 0;
        for (Map.Entry<String, Integer> entry : counter.entrySet()) {
            sum += entry.getValue();
        }
        int threshold = sum / counter.entrySet().size();

        Set<String> organizationSuffixes = new HashSet<>();
        for (Map.Entry<String, Integer> entry : counter.entrySet()) {
            if (entry.getValue() > threshold) {
                organizationSuffixes.add(entry.getKey());
            }
        }

        return organizationSuffixes;
    }

    public static void showPossiblyWrongOrganizationSet() throws SQLException {
        String query = "SELECT isi.id, isi.affiliation AS isi_crawled_organizes, GROUP_CONCAT(DISTINCT o.name SEPARATOR '\\n') AS merged_organizes, scopus.id, scopus.affiliations AS scopus_crawled_organizes FROM merge_logs ml " +
                "JOIN isi_documents isi ON isi.id = ml.isi_id " +
                "JOIN articles ar ON ar.id = ml.duplication_of " +
                "JOIN articles_authors aa ON aa.article_id = ar.id " +
                "JOIN authors_organizes ao ON ao.author_id = aa.author_id " +
                "JOIN organizes o ON o.id = ao.organize_id " +
                "JOIN scopus_documents scopus ON scopus.id = isi.raw_scopus_id " +
                "GROUP BY ml.isi_id " +
                "HAVING NOT ( " +
                "(isi_crawled_organizes LIKE '%ton duc thang%' AND merged_organizes LIKE '%ton duc thang%' AND scopus_crawled_organizes LIKE '%ton duc thang%') OR " +
                "(isi_crawled_organizes NOT LIKE '%ton duc thang%' AND merged_organizes NOT LIKE '%ton duc thang%' AND scopus_crawled_organizes NOT LIKE '%ton duc thang%') " +
                ")";
        DataUtl.getDBStatement().setQueryTimeout(0);
        ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT, query);

        List<Integer> isiIDs = new ArrayList<>(), scopusIDs = new ArrayList<>();
        List<String> mergedOrganizations = new ArrayList<>();
        while (rs.next()) {
            isiIDs.add(rs.getInt(1));
            scopusIDs.add(rs.getInt(4));
            mergedOrganizations.add(rs.getString(3));
        }
        System.out.println(isiIDs.size());

        for (int i = 0; i < isiIDs.size(); ++i) {
            System.out.println(isiIDs.get(i));

            Article article = new Article();
            article.setAuthorsJSON(getAuthorJSON(isiIDs.get(i), "isi_documents"));
            for (String organization : article.getListOrganizations()) {
                System.out.println(organization);
            }
            System.out.println();

            System.out.println(mergedOrganizations.get(i));
            System.out.println();

            article = new Article();
            article.setAuthorsJSON(getAuthorJSON(scopusIDs.get(i), "scopus_documents"));
            for (String organization : article.getListOrganizations()) {
                System.out.println(organization);
            }
            System.out.println();

            System.out.println("\n\n\n");
        }
    }

    public static String getAuthorJSON(int ID, String table) throws SQLException {
        String query = "SELECT authors_json FROM " + table + " WHERE id = " + ID;
        ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT, query);
        rs.next();
        return rs.getString(1);
    }

    public static void compareByID() throws SQLException {
        // Both maps are of this type: ISI ID -> Scopus ID
        Map<Integer, Integer> nghia = new HashMap<>();

        String query = "SELECT raw_isi_id, raw_scopus_id FROM articles WHERE raw_scopus_id IS NOT NULL AND raw_isi_id IS NOT NULL";
        ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT, query);

        while (rs.next()) {
            nghia.put(rs.getInt(1), rs.getInt(2));
        }

        Map<Integer, Integer> hieu = new HashMap<>();

        query = "SELECT id, raw_scopus_id FROM isi_documents WHERE raw_scopus_id != ''";
        rs = DataUtl.queryDB(Config.DB.OUTPUT, query);

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

        System.out.println("WHAT BOTH FOUND BUT DIFFERS FROM EACH OTHER");
        counter = 0;
        for (Map.Entry<Integer, Integer> entry : hieu.entrySet()) {
            if (nghia.containsKey(entry.getKey()) && ! nghia.get(entry.getKey()).equals(entry.getValue())) {
                System.out.print(entry.getKey() + "    ");
                ++counter;
            }
        }
        System.out.println("\n" + counter);
    }
}
