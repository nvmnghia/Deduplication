package util;

import com.github.slugify.Slugify;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

public class Sluginator {
    private static Slugify sluginator = new Slugify();

    public static void main(String[] args) throws SQLException {
        slugifyAll();
    }

    public static void slugifyAll() throws SQLException{
        slugifyArticles();
        slugifyJournals();
        slugifyOrganizations();
    }

    public static void slugifyArticles() throws SQLException {
        HashSet<String> articleSlugs = new HashSet<>();

        ResultSet rs = DataUtl.queryDB("vci_scholar", "SELECT id, title, slug FROM articles");
        PreparedStatement pstmSlugifyArticles = DataUtl.getDBConnection().prepareStatement("UPDATE articles SET slug = ? WHERE id = ?");

        while (rs.next()) {
            String currentSlug = rs.getString(3);

            if (currentSlug == null || currentSlug.length() == 0 || currentSlug.equals("add-slug-here")) {
                pstmSlugifyArticles.setString(1, slugify(articleSlugs, rs.getString(2)));
                pstmSlugifyArticles.setInt(2, rs.getInt(1));

                pstmSlugifyArticles.addBatch();
            } else {
                articleSlugs.add(currentSlug);
            }
        }

        pstmSlugifyArticles.executeBatch();
    }

    public static void slugifyJournals() throws SQLException {
        HashSet<String> journalSlugs = new HashSet<>();

        ResultSet rs = DataUtl.queryDB("vci_scholar", "SELECT id, name, slug FROM journals");
        PreparedStatement pstmSlugifyJournals = DataUtl.getDBConnection().prepareStatement("UPDATE journals SET slug = ? WHERE id = ?");

        while (rs.next()) {
            String currentSlug = rs.getString(3);

            if (currentSlug == null || currentSlug.length() == 0 || currentSlug.equals("add-slug-here")) {
                pstmSlugifyJournals.setString(1, slugify(journalSlugs, rs.getString(2)));
                pstmSlugifyJournals.setInt(2, rs.getInt(1));

                pstmSlugifyJournals.addBatch();
            } else {
                journalSlugs.add(currentSlug);
            }
        }

        pstmSlugifyJournals.executeBatch();
    }

    public static void slugifyOrganizations() throws SQLException {
        HashSet<String> organizationSlugs = new HashSet<>();

        ResultSet rs = DataUtl.queryDB("vci_scholar", "SELECT id, name, slug FROM organizes");
        PreparedStatement pstmSlugifyOrganizes = DataUtl.getDBConnection().prepareStatement("UPDATE organizes SET slug = ? WHERE id = ?");

        while (rs.next()) {
            String currentSlug = rs.getString(3);

            if (currentSlug == null || currentSlug.length() == 0 || currentSlug.equals("add-slug-here")) {
                pstmSlugifyOrganizes.setString(1, slugify(organizationSlugs, rs.getString(3)));
                pstmSlugifyOrganizes.setInt(2, rs.getInt(1));

                pstmSlugifyOrganizes.addBatch();
            } else {
                organizationSlugs.add(currentSlug);
            }
        }

        pstmSlugifyOrganizes.executeBatch();
    }

    public static String slugify(HashSet<String> slugSet, String str) {
        String slug = sluginator.slugify(str);

        if (slugSet.contains(slug)) {
            slug += '-';
            String newSlug;
            int counter = 1;

            do {
                newSlug = slug + String.valueOf(counter++);
            } while (! slugSet.contains(newSlug));

            slugSet.add(newSlug);
            return newSlug;
        } else {
            slugSet.add(slug);
            return slug;
        }
    }
}
