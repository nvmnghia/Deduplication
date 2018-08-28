package representative;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import config.Config;
import org.checkerframework.checker.units.qual.A;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import util.DataUtl;
import util.StringUtl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Vo Dinh Hieu
 * Modified by me
 */
public class Affiliation {

    private int id = -1;

    private String representative;

    private List<IDS> idss = new ArrayList<>();
    private List<EXCL> excls = new ArrayList<>();
    private List<CIDS> cidss = new ArrayList<>();

    public Affiliation(Node affiliationNode) {
        NodeList nl = affiliationNode.getChildNodes();

        for (int i = 0; i < nl.getLength(); ++i) {
            Node node = nl.item(i);
            String name = node.getNodeName();

            switch (name) {
                case "ids":
                    idss.add(new IDS(node));
                    break;

                case "excl":
                    excls.add(new EXCL(node));
                    break;

                case "cids":
                    cidss.add(new CIDS(node));
                    break;

                case "representative":
                    representative = node.getTextContent();
                    break;
            }
        }

        // Representative itself is a IDS
        idss.add(new IDS(representative));
    }

    public String getRepresentative() {
        return representative;
    }

    public List getIDSs() {
        return idss;
    }

    public List getCIDSs() {
        return cidss;
    }

    public int getId() {
        return id;
    }

    public void print() {
        System.out.println("Representative: " + representative);

        System.out.println("IDS: ");
        for (IDS ids : idss) {
            System.out.println(ids.toString());
        }

        System.out.println("EXCL: ");
        for (EXCL excl : excls) {
            System.out.println(excl.toString());
        }

        System.out.println("CIDS: ");
        for (CIDS cids : cidss) {
            System.out.println(cids.toString());
        }
    }

    public void process() throws Exception {
        System.out.println("Representative: " + representative);
        String query;
        ResultSet rs;

        if (id == -1) {
            query = "SELECT id FROM representatives WHERE name = '" + representative + "'";
            rs = DataUtl.queryDB(Config.DB.OUTPUT, query);

            if (rs.next()) {
                id = rs.getInt(1);
                System.out.println("Representative ID: " + id);
            } else {
                // Representative not found, create a new one
                query = "INSERT IGNORE INTO representatives (name) VALUES ('" + representative + "')";
                id = DataUtl.insertAndGetID(Config.DB.OUTPUT, query);

                System.out.println("New representative with ID: " + id);
            }

            rs.close();
        }

        // Find and mark using IDS
        System.out.println("Processing IDS:");
        for (IDS ids : idss) {
            System.out.println(ids.toString());

            // This table already has UNIQUE on these 2 columns
            query = "INSERT IGNORE INTO organize_representative (organize_id, representative_id) " +
                    "SELECT o.id, " + id + " FROM organizes o " +
                    "WHERE " + ids.getWHEREClause("o.name");
            try {
                DataUtl.updateDB(Config.DB.OUTPUT, query);
            } catch (SQLException e) {
                if (! (e instanceof MySQLIntegrityConstraintViolationException)) {
                    e.printStackTrace();
                }
            }

            // Add to segments for debug purpose
            // Read its comments for more information
            query = "SELECT id, name FROM organizes " +
                    "WHERE " + ids.getWHEREClause("name");
            rs = DataUtl.queryDB(Config.DB.OUTPUT, query);
            while (rs.next()) {
                addToSegmentList(ids, rs.getInt(1), rs.getString(2));
            }
            rs.close();
        }

        // Find and mark using CIDS
        System.out.println("Processing CIDS:");
        for (CIDS cids : cidss) {
            System.out.println(cids.toString());

            query = "INSERT IGNORE INTO organize_representative (organize_id, representative_id) " +
                    "SELECT o.id, " + id + " FROM organizes o " +
                    "WHERE " + cids.getWHEREClause("o.name");
            try {
                DataUtl.updateDB(Config.DB.OUTPUT, query);
            } catch (SQLException e) {
                if (! (e instanceof MySQLIntegrityConstraintViolationException)) {
                    e.printStackTrace();
                }
            }

            query = "SELECT id, name FROM organizes " +
                    "WHERE " + cids.getWHEREClause("name");
            rs = DataUtl.queryDB(Config.DB.OUTPUT, query);
            while (rs.next()) {
                addToSegmentList(cids.getIDS(), rs.getInt(1), rs.getString(2));
            }
            rs.close();
        }

        // Delete the EXCL that was accidentally added
        for (EXCL excl : excls) {
            query = "DELETE org_rep FROM organize_representative org_rep " +
                    "JOIN organizes o ON o.id = org_rep.organize_id " +
                    "WHERE org_rep.representative_id = " + id + " AND " + excl.getWHEREClause("o.name");
            DataUtl.updateDB(Config.DB.OUTPUT, query);
        }

        for (Iterator<Map.Entry<Integer, String>> itr = segments.entrySet().iterator(); itr.hasNext();) {
            Map.Entry<Integer, String> entry = itr.next();

            for (EXCL excl : excls) {
                if (! excl.isNotSubstringOf(entry.getValue())) {
                    itr.remove();
                    break;
                }
            }
        }

        System.out.println("\nOrganizations of this representative have one of those " + segments.size() + " segments:");
        for (String segment : segments.values()) {
            System.out.println(segment);
        }

        System.out.println("\n\n");
    }

    protected boolean process(String organization) throws SQLException {

        if (id == -1) {
            String query = "SELECT id FROM representatives WHERE name = '" + representative + "'";
            ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT, query);

            if (rs.next()) {
                id = rs.getInt(1);
                System.out.println("Representative ID: " + id);
            } else {
                // Representative not found, create a new one
                query = "INSERT IGNORE INTO representatives (name) VALUES ('" + representative + "')";
                id = DataUtl.insertAndGetID(Config.DB.OUTPUT, query);

                System.out.println("New representative with ID: " + id);
            }

            rs.close();
        }

        // It must first pass all EXCL
        for (EXCL excl : excls) {
            if (! excl.isNotSubstringOf(organization)) {
                return false;
            }
        }

        // And pass one of the IDS or CIDS
        for (IDS ids : idss) {
            if (ids.isSubstringOf(organization)) {
                return true;
            }
        }

        for (CIDS cids : cidss) {
            if (cids.isSubstringOf(organization)) {
                return true;
            }
        }
        
        return false;
    }

    private Map<Integer, String> segments = new HashMap();
    private void addToSegmentList(IDS IDS,int ID, String organization) {
        organization = organization.toLowerCase();

        List<Integer> position = IDS.getMatchPosition(organization);

        // The match will be extended to both end until a ',' is reached
        if (position.size() > 0) {
            int start = position.get(0);
            while (start >= 0 && organization.charAt(start) != ',') {
                --start;
            }
            ++start;    // Avoid the ',' itself

            int end = position.get(1);
            while (end < organization.length() && organization.charAt(end) != ',') {
                ++end;
            }
//            --end;    // Exclusive end

            String segment = organization.substring(start, end).trim();
            String tempSegment = segments.get(ID);

            if (tempSegment != null) {
                // If a segment of this organization has already been added, update if this segment is longer
                // (avoid segments of the same organization be added multiple times, due to multiple match)
                if (tempSegment.length() < segment.length()) {
                    segments.put(ID, segment);
                }
            } else if (! segments.containsValue(segment)) {
                // Else, insert if the particular segment hasn't been added
                // (avoid same segment of different organizations be added)
                segments.put(ID, segment);
            }
        }
    }

    private void addToSegmentList(List<IDS> idss, int ID, String organization) {
        for (IDS ids : idss) {
            addToSegmentList(ids, ID, organization);
        }
    }

    public static class IDS {
        private String ids;
        private Pattern idsPattern;

        public IDS(String s) {
            this.ids = s.toLowerCase();
            this.idsPattern = createPattern(this.ids);
        }

        public IDS(Node n) {
            this.ids = n.getTextContent().toLowerCase();
            this.idsPattern = createPattern(this.ids);
        }

        public String getIDS() {
            return ids;
        }

        public boolean isSubstringOf(String str) {
            str = str.toLowerCase();
            return idsPattern != null ? idsPattern.matcher(str).find() : str.contains(ids);
        }

        @Override
        public String toString() {
            return ids;
        }

        public String getWHEREClause(String col) {
            return col + " LIKE " + "'%" + this.ids + "%'";
        }

        public List<Integer> getMatchedArticles() throws SQLException {
            String query = "SELECT DISTINCT ar.id FROM articles ar " +
                    "JOIN articles_authors aa ON aa.article_id = ar.id " +
                    "JOIN authors_organizes ao ON ao.author_id = aa.author_id " +
                    "JOIN organizes o ON o.id = ao.organize_id " +
                    "WHERE " + getWHEREClause("o.name");
            ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT, query);
            List<Integer> matches = new ArrayList<>();

            while (rs.next()) {
                matches.add(rs.getInt(1));
            }
            rs.close();

            return matches;
        }

        public List<Integer> getMatchPosition(String str) {
            List<Integer> position = Collections.emptyList();

            if (idsPattern == null) {
                int start = str.indexOf(this.ids);

                if (start != -1) {
                    position = Arrays.asList(start, start + this.ids.length());
                }
            } else {
                Matcher matcher = idsPattern.matcher(str);

                if (matcher.find()) {
                    position = Arrays.asList(matcher.start(), matcher.end());
                }
            }

            return position;
        }
    }

    public static class EXCL {
        private String excl;
        private Pattern exclPattern;

        public EXCL(Node n) {
            this.excl = n.getTextContent().toLowerCase();
            this.exclPattern = createPattern(this.excl);
        }

        public boolean isNotSubstringOf(String str) {
            str = str.toLowerCase();
            return ! (exclPattern != null ? exclPattern.matcher(str).find() : str.contains(excl));
        }

        @Override
        public String toString() {
            return excl;
        }

        public String getWHEREClause(String col) {
//            return col + " NOT LIKE " + "'%" + this.excl + "%'";    // WHERE for DELETE, NOT is so so stupid
            return col + " LIKE " + "'%" + this.excl + "%'";
        }

        public String getWHERENOTClause(String col) {
            return col + " NOT LIKE " + "'%" + this.excl + "%'";
        }

        public List<Integer> getMatchedArticles() throws SQLException {
            String query = "SELECT DISTINCT ar.id FROM articles ar " +
                    "JOIN articles_authors aa ON aa.article_id = ar.id " +
                    "JOIN authors_organizes ao ON ao.author_id = aa.author_id " +
                    "JOIN organizes o ON o.id = ao.organize_id " +
                    "WHERE " + getWHEREClause("o.name");
            ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT, query);
            List<Integer> matches = new ArrayList<>();

            while (rs.next()) {
                matches.add(rs.getInt(1));
            }
            rs.close();

            return matches;
        }
    }

    public class CIDS {
        private List<IDS> idss;
        private List<EXCL> excls;

        public CIDS(Node n) {
            this.idss = new ArrayList<>();
            this.excls = new ArrayList<>();

            NodeList nl = n.getChildNodes();

            for (int i = 0; i < nl.getLength(); ++i) {
                Node node = nl.item(i);
                String name = node.getNodeName();

                if (name.equals("ids")) {
                    this.idss.add(new IDS(node));
                }
                if (name.equals("excl")) {
                    this.excls.add(new EXCL(node));
                }
            }
        }

        public List<IDS> getIDS() {
            return idss;
        }

        public List<EXCL> getEXCL() {
            return excls;
        }

        public boolean isSubstringOf(String str) {
            for (EXCL excl : excls) {
                if (! excl.isNotSubstringOf(str)) {
                    return false;
                }
            }

            for (IDS ids : idss) {
                if (! ids.isSubstringOf(str)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public String toString() {
            return "IDS: " + idss.toString() + ";    EXCL: " + excls.toString();
        }

        public String getWHEREClause(String col) {
            String clause = "";

            for (int i = 0; i < idss.size(); ++i) {
                clause += idss.get(i).getWHEREClause(col);

                if (i != idss.size() - 1) {
                    clause += " AND ";
                }
            }

            if (idss.size() != 0 && excls.size() != 0) {
                clause += " AND ";
            }

            for (int i = 0; i < excls.size(); ++i) {
                clause += excls.get(i).getWHERENOTClause(col);

                if (i != excls.size() - 1) {
                    clause += " AND ";
                }
            }

            return clause;
        }

        public List<Integer> getMatchedArticles() throws SQLException {
            String query = "SELECT DISTINCT ar.id FROM articles ar " +
                    "JOIN articles_authors aa ON aa.article_id = ar.id " +
                    "JOIN authors_organizes ao ON ao.author_id = aa.author_id " +
                    "JOIN organizes o ON o.id = ao.organize_id " +
                    "WHERE " + getWHEREClause("o.name");
            ResultSet rs = DataUtl.queryDB(Config.DB.OUTPUT, query);
            List<Integer> matches = new ArrayList<>();

            while (rs.next()) {
                matches.add(rs.getInt(1));
            }
            rs.close();

            return matches;
        }

        public List<Integer> getMatchPosition(String str) {
            for (IDS ids : idss) {
                List<Integer> temp = ids.getMatchPosition(str);

                if (temp.size() != 0) {
                    return temp;
                }
            }

            return Collections.emptyList();
        }
    }

    private static Pattern createPattern(String str) {
        if (str.contains("%")) {
            str = StringUtl.removeConsecutiveDuplicated(str, '%');
            String patternStr = "";

            String[] parts = str.split("%");
            for (String part : parts) {
                if (part.length() != 0) {
                    patternStr += "(" + part + ").*";
                }
            }
            patternStr = patternStr.substring(0, patternStr.length() - 2);

            System.err.println(patternStr);

            return Pattern.compile(patternStr, Pattern.DOTALL);
        }

        return null;
    }
}
