package representative;

import config.Config;
import util.DataUtl;
import util.StringUtl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Representative {

    private List<Affiliation> affiliations;

    public Representative(String input) throws Exception {
        affiliations = new Affiliations(input).getAffiliations();
    }

    public List<Integer> getRepresentativesOf(String organization) throws SQLException {
        organization = StringUtl.correct(organization).trim().toLowerCase();

        List<Integer> representativeIDs = new ArrayList<>();

        for (Affiliation affiliation : affiliations) {
            if (affiliation.process(organization)) {
                representativeIDs.add(affiliation.getId());
                break;
            }
        }

        return representativeIDs;
    }

    public static void apply() throws SQLException {
        Config.DB.NUCLEAR_OPTION = false;
        DataUtl.truncate(Config.DB.OUTPUT, "organize_representative");

        // Make sure it has UNIQUE on both key
        String query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS " +
                "WHERE TABLE_SCHEMA = '" + Config.DB.OUTPUT + "' AND TABLE_NAME = 'organize_representative' AND INDEX_NAME = 'organize_representative_pair'";
        ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT, query);

        rs.next();
        if (rs.getInt(1) == 0) {
            query = "ALTER TABLE organize_representative ADD UNIQUE `organize_representative_pair` (organize_id, representative_id)";

            try {
                DataUtl.updateDB(Config.DB.OUTPUT, query);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        rs.close();

        try {
            List<Affiliation> affiliations = new Affiliations("Affiliations.xml").getAffiliations();

            for (Affiliation affiliation : affiliations) {
                affiliation.process();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(new FileOutputStream(new File("log.txt"))));

        Representative.apply();

//        Representative representative = new Representative(Config.REPRESENTATIVE_INPUT);
//        System.out.println(representative.getRepresentativesOf(
//                "University of Engineering and Technology, Viet Nam"
//        ));
    }
}
