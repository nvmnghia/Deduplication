package comparator;

import info.debatty.java.stringsimilarity.Levenshtein;
import util.StringUtl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static util.StringUtl.generateTokenizedString;

public class Lev {
    private static Levenshtein levenshtein = new Levenshtein();

    public static double approxDistance(String A, String B) {
        if (A == null || B == null) {
            return 1.0d;
        }

        if (A.equals(B)) {
            return 0.0d;
        }

        String[] tokenized = generateTokenizedString(A, B);
        String tokenizedA = tokenized[0];
        String tokenizedB = tokenized[1];

        return preciseDistance(tokenizedA, tokenizedB);
    }

    public static double preciseDistance(String a, String b) {
        return levenshtein.distance(a, b) / Math.max(a.length(), b.length());
    }

    public static double distanceNormStr(String a, String b) {
        return preciseDistance(StringUtl.clean(a), StringUtl.clean(b));
    }

    public static int absoluteDistance(String a, String b) {
        return (int)levenshtein.distance(a, b);
    }
}
