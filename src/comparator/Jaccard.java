package comparator;

import util.StringUtl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static util.StringUtl.generateTokenizedString;

public class Jaccard {
    private static info.debatty.java.stringsimilarity.Jaccard jaccard = new info.debatty.java.stringsimilarity.Jaccard();

    public static double approxSimilarity(String A, String B) {
        if (A == null || B == null) {
            return 0.0d;
        }

        if (A.equals(B)) {
            return 1.0d;
        }

        String[] tokenized = generateTokenizedString(A, B);
        String tokenizedA = tokenized[0];
        String tokenizedB = tokenized[1];

        return jaccard.similarity(tokenizedA, tokenizedB);
    }
}
