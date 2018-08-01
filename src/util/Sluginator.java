package util;

import com.github.slugify.Slugify;
import config.Config;

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

        ResultSet rs = DataUtl.queryDB(Config.DB.OUPUT, "SELECT id, title, slug FROM articles");
        PreparedStatement pstmSlugifyArticles = DataUtl.getDBConnection().prepareStatement("UPDATE " + Config.DB.OUPUT + ".articles SET slug = ? WHERE id = ?");

        int counter = 0;

        while (rs.next()) {
            String currentSlug = rs.getString(3);

            if (currentSlug == null || currentSlug.length() == 0 || currentSlug.equals("add-slug-here")) {
                pstmSlugifyArticles.setString(1, slugify(articleSlugs, rs.getString(2)));
                pstmSlugifyArticles.setInt(2, rs.getInt(1));

                pstmSlugifyArticles.addBatch();
            } else {
                articleSlugs.add(currentSlug);
            }

            if (++counter % 100 == 0) {
                pstmSlugifyArticles.executeBatch();
            }
        }

        pstmSlugifyArticles.executeBatch();
    }

    public static void slugifyJournals() throws SQLException {
        HashSet<String> journalSlugs = new HashSet<>();

        ResultSet rs = DataUtl.queryDB(Config.DB.OUPUT, "SELECT id, name, slug FROM journals");
        PreparedStatement pstmSlugifyJournals = DataUtl.getDBConnection().prepareStatement("UPDATE " + Config.DB.OUPUT + ".journals SET slug = ? WHERE id = ?");

        int counter = 0;

        while (rs.next()) {
            String currentSlug = rs.getString(3);

            if (currentSlug == null || currentSlug.length() == 0 || currentSlug.equals("add-slug-here")) {
                pstmSlugifyJournals.setString(1, slugify(journalSlugs, rs.getString(2)));
                pstmSlugifyJournals.setInt(2, rs.getInt(1));

                pstmSlugifyJournals.addBatch();
            } else {
                journalSlugs.add(currentSlug);
            }

            if (++counter % 100 == 0) {
                pstmSlugifyJournals.executeBatch();
            }
        }

        pstmSlugifyJournals.executeBatch();
    }

    public static void slugifyOrganizations() throws SQLException {
        HashSet<String> organizationSlugs = new HashSet<>();

        ResultSet rs = DataUtl.queryDB(Config.DB.OUPUT, "SELECT id, name, slug FROM organizes");
        PreparedStatement pstmSlugifyOrganizes = DataUtl.getDBConnection().prepareStatement("UPDATE " + Config.DB.OUPUT + ".organizes SET slug = ? WHERE id = ?");

        int counter = 0;

        while (rs.next()) {
            String currentSlug = rs.getString(3);

            if (currentSlug == null || currentSlug.length() == 0 || currentSlug.equals("add-slug-here")) {
                pstmSlugifyOrganizes.setString(1, slugify(organizationSlugs, rs.getString(2)));
                pstmSlugifyOrganizes.setInt(2, rs.getInt(1));

                pstmSlugifyOrganizes.addBatch();
            } else {
                organizationSlugs.add(currentSlug);
            }

            if (++counter % 100 == 0) {
                pstmSlugifyOrganizes.executeBatch();
            }
        }

        pstmSlugifyOrganizes.executeBatch();
    }

    /**
     * Generate ASCII, lower case slug.
     * THe maximum length of the slug is 255 chars, as MariaDB cannot index anything larger than 767 bytes
     *
     * @param slugSet
     * @param str
     * @return
     */
    private static final int MAX_SLUG_LENGTH = 255;
    public static String slugify(HashSet<String> slugSet, String str) {
        String slug = sluginator.slugify(str);
        slug = slug.substring(0, slug.length() > MAX_SLUG_LENGTH ? MAX_SLUG_LENGTH : slug.length());

        if (slugSet.contains(slug)) {
            String temp = slug;
            int counter = 1;

            do {
                String count = String.valueOf(counter++);

                if (temp.length() + 1 + count.length() > MAX_SLUG_LENGTH) {
                    slug = temp.substring(0, MAX_SLUG_LENGTH - count.length() - 1) + "-" + count;
                } else {
                    slug = temp + "-" + count;
                }
            } while (slugSet.contains(slug));

            slugSet.add(slug);
            System.out.println("Slug with suffix " + slug);
            return slug;

        } else {
            slugSet.add(slug);
            return slug;
        }
    }
}
