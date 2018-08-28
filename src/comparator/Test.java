package comparator;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import config.Config;
import data.Article;
import importer.ImportDB;
import representative.Representative;
import util.DataUtl;
import util.StringUtl;
import util.Utl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Test {
    public static void main(String[] args) throws Exception {
//        System.setOut(new PrintStream(new FileOutputStream(new File("log.txt"))));

//        yetAnotherCompareToCrawledDB(true);

        compareByID();

//        int[] a = {14, 15, 19, 21, 24, 25, 26, 51, 59, 118, 124, 149, 154, 173, 178, 179, 201, 206, 209, 240, 241, 244, 252, 261, 262, 273, 274, 278, 281, 292, 295, 298, 301, 311, 319, 333, 334, 349, 359, 362, 371, 373, 374, 375, 376, 383, 391, 394, 398, 401, 402, 418, 426, 433, 447, 449, 465, 508, 512, 514, 526, 539, 544, 548, 550, 555, 560, 562, 566, 578, 610, 611, 615, 616, 622, 624, 627, 630, 633, 637, 638, 639, 655, 659, 661, 669, 670, 673, 674, 684, 713, 721, 744, 749, 759, 760, 771, 774, 794, 795, 803, 804, 814, 823, 830, 831, 857, 862, 868, 876, 877, 884, 919, 976, 977, 984, 989, 994, 1011, 1068, 1104, 1105, 1108, 1123, 1124, 1134, 1135, 1137, 1143, 1144, 1157, 1163, 1165, 1174, 1176, 1184, 1229, 1264, 1282, 1308, 1311, 1315, 1320, 1389, 1403, 1405, 1406, 1412, 1465, 1475, 1494, 1500, 1503, 1515, 1516, 1522, 1525, 1526, 1528, 1536, 1551, 1554, 1568, 1577, 1587, 1651, 1716, 1722, 1748, 1758, 1759, 1760, 1771, 1783, 1796, 1804, 1809, 1814, 1834, 1841, 1853, 1854, 1856, 1867, 1915, 1925, 1932, 1947, 1950, 1976, 2003, 2008, 2015, 2016, 2019, 2024, 2032, 2042, 2064, 2069, 2075, 2086, 2087, 2105, 2116, 2117, 2119, 2132, 2133, 2138, 2210, 2211, 2238, 2248, 2251, 2261, 2283, 2301, 2302, 2314, 2318, 2336, 2343, 2355, 2364, 2366, 2368, 2369, 2390, 2417, 2436, 2442, 2453, 2499, 2513, 2534, 2539, 2563, 2566, 2569, 2583, 2584, 2593, 2596, 2597, 2600, 2602, 2603, 2607, 2608, 2615, 2622, 2633, 2643, 2672, 2676, 2678, 2712, 2724, 2729, 2747, 2748, 2762, 2768, 2770, 2775, 2777, 2806, 2807, 2812, 2821, 2841, 2842, 2849, 2915, 2936, 2937, 2946, 2950, 2955, 2984, 3003, 3040, 3042, 3053, 3063, 3079, 3086, 3091, 3110, 3132, 3143, 3155, 3186, 3200, 3203, 3222, 3241, 3280, 3292, 3304, 3341, 3393, 3410, 3412, 3415, 3416, 3433, 3435, 3473, 3477, 3489, 3542, 3572, 3612, 3644, 3645, 3647, 3656, 3659, 3678, 3717, 3720, 3748, 3750, 3782, 3826, 3839, 3854, 3856, 3857, 3864, 3866, 3867, 3868, 3875, 3882, 3895, 3908, 3925, 3932, 3933, 3940, 3942, 3952, 3961, 3977, 3983, 4056, 4073, 4121, 4164, 4174, 4185, 4205, 4266, 4317, 4437, 4505, 4507, 4514, 4515, 4519, 4544, 4547, 4548, 4552, 4553, 4559, 4560, 4587, 4608, 4664, 4701, 4713, 4724, 4732, 4736, 4737, 4741, 4752, 4765, 4767, 4790, 4793, 4801, 4813, 4815, 4818, 4830, 4854, 4857, 4858, 4859, 4865, 4875, 4880, 4885, 4911, 4981, 4991, 5000, 5006, 5008, 5038, 5039, 5049, 5053, 5065, 5073, 5082, 5094, 5097, 5117, 5131, 5133, 5136, 5137, 5157, 5180, 5200, 5203, 5213, 5228, 5229, 5246, 5258, 5305, 5306, 5317, 5328, 5486, 5570, 5581, 5593, 5601, 5660, 5665, 5701, 5766, 5791, 5792, 5793, 5794, 12275, 12281, 12291, 12294, 12310, 12314, 12318, 12319, 12320, 12321, 12322, 12333, 12337, 12338, 12340, 12341, 12354, 12364, 12367, 12372, 12373, 12394, 12396, 12398, 12399, 12424, 12430, 12454, 12456, 12457, 12463, 12464, 12468, 12472, 12505, 12538, 12541, 12544, 12546, 12579, 12580, 12581, 12582, 12583, 12590, 12593, 12601, 12608, 12632, 12645, 12663, 12669, 12681, 12725, 12726, 12727, 12728, 12741, 12792, 12793, 12798, 12816, 12817, 12828, 12853, 12856, 12865, 12872, 12889, 12911, 12919, 12933, 12936, 12952, 12965, 13005};
//        List<Integer> A = new ArrayList<>();
//        for (int i = 0; i < a.length; ++i) {
//            A.add(a[i]);
//        }
//
//        int[] b = {14, 15, 19, 21, 24, 25, 26, 51, 59, 118, 124, 149, 154, 173, 178, 179, 201, 206, 209, 223, 240, 241, 244, 252, 261, 262, 272, 273, 274, 278, 281, 292, 295, 298, 301, 311, 319, 331, 333, 334, 343, 349, 359, 362, 371, 373, 374, 375, 376, 383, 391, 394, 398, 401, 402, 418, 426, 433, 447, 449, 465, 508, 512, 514, 523, 526, 539, 544, 548, 550, 555, 560, 562, 566, 578, 610, 611, 615, 616, 622, 624, 627, 630, 633, 637, 638, 639, 655, 659, 661, 668, 669, 670, 673, 674, 684, 686, 713, 721, 744, 749, 759, 760, 771, 774, 794, 795, 803, 804, 814, 823, 830, 831, 857, 862, 868, 876, 877, 884, 919, 966, 976, 977, 984, 989, 994, 1011, 1068, 1072, 1104, 1105, 1108, 1123, 1124, 1134, 1135, 1137, 1143, 1144, 1157, 1163, 1165, 1174, 1176, 1184, 1219, 1229, 1264, 1282, 1308, 1311, 1315, 1320, 1389, 1403, 1405, 1406, 1412, 1465, 1475, 1494, 1500, 1503, 1515, 1516, 1522, 1524, 1525, 1526, 1528, 1536, 1551, 1554, 1568, 1577, 1581, 1587, 1651, 1716, 1721, 1722, 1723, 1729, 1748, 1758, 1759, 1760, 1771, 1783, 1791, 1792, 1796, 1804, 1809, 1814, 1825, 1834, 1841, 1849, 1853, 1854, 1856, 1867, 1915, 1925, 1932, 1947, 1950, 1972, 1976, 2003, 2008, 2015, 2016, 2019, 2024, 2032, 2042, 2064, 2069, 2075, 2086, 2087, 2105, 2116, 2117, 2119, 2126, 2132, 2133, 2138, 2210, 2211, 2234, 2238, 2239, 2244, 2248, 2251, 2261, 2268, 2283, 2301, 2302, 2314, 2318, 2336, 2337, 2343, 2355, 2364, 2366, 2368, 2369, 2390, 2417, 2436, 2442, 2453, 2499, 2504, 2513, 2534, 2539, 2563, 2566, 2569, 2583, 2584, 2593, 2596, 2597, 2600, 2602, 2603, 2607, 2608, 2615, 2622, 2633, 2643, 2672, 2676, 2678, 2712, 2724, 2729, 2747, 2748, 2762, 2768, 2770, 2775, 2777, 2805, 2806, 2807, 2808, 2812, 2821, 2830, 2841, 2842, 2848, 2849, 2853, 2859, 2863, 2889, 2893, 2915, 2918, 2934, 2936, 2937, 2946, 2947, 2950, 2952, 2955, 2984, 2990, 2994, 3003, 3037, 3040, 3042, 3053, 3058, 3063, 3079, 3086, 3091, 3104, 3110, 3114, 3132, 3143, 3148, 3155, 3186, 3200, 3203, 3222, 3236, 3241, 3256, 3280, 3292, 3304, 3337, 3341, 3370, 3375, 3393, 3395, 3410, 3411, 3412, 3415, 3416, 3433, 3435, 3473, 3477, 3489, 3542, 3572, 3612, 3644, 3645, 3647, 3648, 3656, 3659, 3676, 3678, 3688, 3696, 3698, 3717, 3720, 3748, 3750, 3782, 3826, 3839, 3854, 3855, 3856, 3857, 3864, 3866, 3867, 3868, 3875, 3882, 3895, 3905, 3908, 3925, 3932, 3933, 3936, 3940, 3942, 3948, 3952, 3961, 3970, 3977, 3983, 4056, 4073, 4121, 4154, 4162, 4164, 4174, 4185, 4186, 4189, 4194, 4205, 4266, 4313, 4317, 4350, 4424, 4433, 4437, 4505, 4507, 4514, 4515, 4519, 4544, 4547, 4548, 4552, 4553, 4559, 4560, 4587, 4608, 4664, 4701, 4713, 4724, 4730, 4732, 4736, 4737, 4741, 4752, 4765, 4767, 4790, 4793, 4798, 4801, 4804, 4810, 4813, 4815, 4818, 4830, 4854, 4857, 4858, 4859, 4865, 4875, 4880, 4885, 4911, 4981, 4991, 5000, 5005, 5006, 5007, 5008, 5038, 5039, 5049, 5053, 5065, 5073, 5082, 5094, 5097, 5117, 5131, 5133, 5136, 5137, 5157, 5180, 5200, 5203, 5213, 5228, 5229, 5246, 5258, 5259, 5305, 5306, 5317, 5328, 5332, 5333, 5337, 5342, 5346, 5414, 5422, 5459, 5486, 5487, 5497, 5511, 5512, 5513, 5558, 5563, 5566, 5570, 5581, 5589, 5593, 5600, 5601, 5615, 5626, 5660, 5665, 5701, 5763, 5766, 5791, 5792, 5793, 5794, 12275, 12281, 12291, 12294, 12310, 12314, 12318, 12319, 12320, 12321, 12322, 12333, 12337, 12338, 12340, 12341, 12354, 12364, 12367, 12372, 12373, 12394, 12396, 12398, 12399, 12424, 12430, 12454, 12456, 12457, 12463, 12464, 12468, 12472, 12505, 12538, 12541, 12544, 12546, 12579, 12580, 12581, 12582, 12583, 12590, 12593, 12601, 12608, 12632, 12645, 12663, 12669, 12681, 12725, 12726, 12727, 12728, 12741, 12792, 12793, 12798, 12816, 12817, 12828, 12853, 12856, 12865, 12872, 12889, 12911, 12919, 12933, 12936, 12952, 12965, 13005};
//        List<Integer> B = new ArrayList<>();
//        for (int i = 0; i < b.length; ++i) {
//            B.add(b[i]);
//        }
//
//        System.out.println(Utl.setDiff(A, B));
    }

    public static void compareCrawledScopusToCrawledISI() throws Exception {
        Config.DB.NUCLEAR_OPTION = false;

        ImportDB.getOrganizationSuffixes();
        Representative representative = new Representative(Config.REPRESENTATIVE_INPUT);

        // Process Scopus
        SetMultimap<Integer, Integer> crawledScopus = MultimapBuilder.hashKeys().hashSetValues().build();

        String query = "SELECT id, authors_json FROM scopus_documents";
        ResultSet rs = DataUtl.queryDB(Config.DB.INPUT, query);

        while (rs.next()) {
            int crawledArticleID = rs.getInt(1);

            // For the ease of debugging
//            if (crawledArticleID == 13) {
//                System.out.println("let's debug");
//            }

            Article article = new Article();
            article.setAuthorsJSON(StringUtl.correct(rs.getString(2)));

            List<String> organizations = article.getOrganizations();
            if (organizations == null) {
                continue;
            }

            for (String organization : organizations) {
                List<Integer> representativeIDs = representative.getRepresentativesOf(organization);

                if (representativeIDs.size() > 0) {
                    for (Integer representativeID : representativeIDs) {
                        crawledScopus.put(representativeID, crawledArticleID);
                    }
                }
            }
        }
        rs.close();

        // Process ISI
        SetMultimap<Integer, Integer> crawledISI = MultimapBuilder.hashKeys().hashSetValues().build();

        query = "SELECT raw_scopus_id, authors_json FROM isi_documents";
        rs = DataUtl.queryDB(Config.DB.INPUT, query);

        while (rs.next()) {
            int crawledArticleID = rs.getInt(1);

            Article article = new Article();
            article.setAuthorsJSON(StringUtl.correct(rs.getString(2)));

            List<String> organizations = article.getOrganizations();
            if (organizations == null) {
                continue;
            }

            for (String organization : organizations) {
                List<Integer> representativeIDs = representative.getRepresentativesOf(organization);

                if (representativeIDs.size() > 0) {
                    for (Integer representativeID : representativeIDs) {
                        crawledISI.put(representativeID, crawledArticleID);
                    }
                }
            }
        }
        rs.close();

        // Display the difference
        Set<Integer> allRepresentativeIDs = new HashSet<>();
        allRepresentativeIDs.addAll(crawledScopus.keySet());
        allRepresentativeIDs.addAll(crawledISI.keySet());

        for (Integer representativeID : allRepresentativeIDs) {
            System.out.println("Comparing for representative " + representativeID);

            List<List<Integer>> diff = Utl.setDiff(crawledScopus.get(representativeID), crawledISI.get(representativeID));

            System.out.println("Scopus, not ISI");
            List<Integer> scopusNotISI = diff.get(0);
            for (Integer articleID : scopusNotISI) {
                System.out.print(articleID + ", ");
            }
            System.out.println();

            System.out.println("ISI, not Scopus");
            List<Integer> isiNotScopus = diff.get(1);
            for (Integer articleID : isiNotScopus) {
                System.out.print(articleID + ", ");
            }
            System.out.println();

            System.out.println("\n");
        }
    }

    /**
     *
     * @param useCrawledScopus compare merged DB to original Scopus if true, compare to original ISI otherwise
     * @throws Exception
     */
    public static void yetAnotherCompareToCrawledDB(boolean useCrawledScopus) throws Exception {
        Config.DB.NUCLEAR_OPTION = false;

        // This is so ugly
        // I promise. I will fix this. Here's a
        // TODO: fix this god damn. Make a new connection for it, inside the static block.
        // See?
        ImportDB.getOrganizationSuffixes();

        // Process crawled
        String table = useCrawledScopus ? "scopus_documents" : "isi_documents";

        SetMultimap<Integer, Integer> crawled = MultimapBuilder.hashKeys().hashSetValues().build();

        Representative representative = new Representative(Config.REPRESENTATIVE_INPUT);

        String query = "SELECT id, authors_json FROM " + table;
        ResultSet rs = DataUtl.queryDB(Config.DB.INPUT, query);

        while (rs.next()) {
            int crawledArticleID = rs.getInt(1);

            if (crawledArticleID == 3562) {
                System.out.println("let's fucking debug");
            }

            Article article = new Article();
            article.setAuthorsJSON(StringUtl.correct(rs.getString(2)));

            List<String> organizations = article.getOrganizations();
            if (organizations == null) {
                continue;
            }

            for (String organization : organizations) {
                List<Integer> representativeIDs = representative.getRepresentativesOf(organization);

                if (representativeIDs.size() > 0) {
                    for (Integer representativeID : representativeIDs) {
                        crawled.put(representativeID, crawledArticleID);
                    }
                }
            }
        }
        rs.close();

        // Process merged
        String column_type = useCrawledScopus ? "is_scopus" : "is_isi";
        String column_raw_id = useCrawledScopus ? "raw_scopus_id" : "raw_isi_id";

        ListMultimap<Integer, Integer> merged = MultimapBuilder.hashKeys().arrayListValues().build();

        List<Integer> representativeIDs = new ArrayList<>();
        rs = DataUtl.queryDB(Config.DB.OUTPUT, "SELECT * FROM representatives");
        while (rs.next()) {
            representativeIDs.add(rs.getInt(1));
        }
        rs.close();

        for (Integer representativeID : representativeIDs) {
            query = "SELECT DISTINCT ar." + column_raw_id + " FROM articles ar " +
                    "LEFT JOIN articles_authors aa ON aa.article_id = ar.id " +
                    "LEFT JOIN authors_organizes ao ON ao.author_id = aa.author_id " +
                    "LEFT JOIN organize_representative org_rep ON org_rep.organize_id = ao.organize_id " +
                    "WHERE ar." + column_type + " = 1 AND org_rep.representative_id = " + representativeID;
            System.out.println(query);
            rs = DataUtl.queryDB(Config.DB.OUTPUT, query);

            while (rs.next()) {
                merged.putAll(representativeID, StringUtl.strToListInt(rs.getString(1)));
            }
            rs.close();
        }

        // Display the difference
        Set<Integer> allRepresentativeIDs = new HashSet<>();
        allRepresentativeIDs.addAll(crawled.keySet());
        allRepresentativeIDs.addAll(merged.keySet());

        for (Integer representativeID : allRepresentativeIDs) {
            System.out.println("Comparing for representative " + representativeID);

            List<List<Integer>> diff = Utl.setDiff(crawled.get(representativeID), merged.get(representativeID));

            System.out.println("Crawled, not merged");
            List<Integer> crawledNotMerged = diff.get(0);
            for (Integer articleID : crawledNotMerged) {
                System.out.print(articleID + ", ");
            }
            System.out.println();

            System.out.println("Merged, not crawled");
            List<Integer> mergedNotCrawled = diff.get(1);
            for (Integer articleID : mergedNotCrawled) {
                System.out.print(articleID + ", ");
            }
            System.out.println();

            System.out.println("\n");
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
        rs.close();

        Map<Integer, Integer> hieu = new HashMap<>();

        query = "SELECT id, raw_scopus_id FROM isi_documents WHERE raw_scopus_id != ''";
        rs = DataUtl.queryDB(Config.DB.OUTPUT, query);

        while (rs.next()) {
            hieu.put(rs.getInt(1), rs.getInt(2));
        }
        rs.close();

        System.out.println("WHAT H FOUND BUT N DIDN'T");
        int counter = 0;
        for (Integer isi : hieu.keySet()) {
            if (! nghia.containsKey(isi)) {
                System.out.print(isi + ", ");
                ++counter;
            }
        }
        System.out.println("\n" + counter);

        System.out.println("WHAT N FOUND BUT H DIDN'T");
        counter = 0;
        for (Integer isi : nghia.keySet()) {
            if (! hieu.containsKey(isi)) {
                System.out.print(isi + ", ");
                ++counter;
            }
        }
        System.out.println("\n" + counter);

        System.out.println("WHAT BOTH FOUND BUT DIFFERS FROM EACH OTHER");
        counter = 0;
        for (Map.Entry<Integer, Integer> entry : hieu.entrySet()) {
            if (nghia.containsKey(entry.getKey()) && ! nghia.get(entry.getKey()).equals(entry.getValue())) {
                System.out.print(entry.getKey() + ", ");
                ++counter;
            }
        }
        System.out.println("\n" + counter);
    }
}
