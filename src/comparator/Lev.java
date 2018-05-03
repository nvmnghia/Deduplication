package comparator;

import info.debatty.java.stringsimilarity.Levenshtein;
import util.StringUtl;

public class Lev {
    private static Levenshtein levenshtein = new Levenshtein();

    public static double distance(String a, String b) {
        return levenshtein.distance(a, b) / Math.max(a.length(), b.length());
    }

    public static double distanceNormStr(String a, String b) {
        return distance(StringUtl.clean(a), StringUtl.clean(b));
    }

    public static int absoluteDistance(String a, String b) {
        return (int)levenshtein.distance(a, b);
    }
}
