package comparator;

import info.debatty.java.stringsimilarity.LongestCommonSubsequence;
import info.debatty.java.stringsimilarity.MetricLCS;
import util.StringUtl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static util.StringUtl.generateTokenizedString;
import static util.StringUtl.removeDiacritics;

public class LCS {


    private static MetricLCS metricLCS = new MetricLCS();
    private static LongestCommonSubsequence LCS = new LongestCommonSubsequence();

    public static double preciseDistance(String A, String B) {
        return metricLCS.distance(A, B);
    }

    public static int length(String A, String B) {
        return LCS.length(A, B);
    }

    /**
     * Most important function
     * Allow to LCS deduplicate by prefix
     * For example, this 2 strings deduplicate
     * - Journal of psychosomatic research
     * - J-Psychosom-Res
     *
     * @param A
     * @param B
     * @return
     */
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

        // Calculate LCS
        return metricLCS.distance(tokenizedA, tokenizedB);
    }

    public static double approxSimilarityRelaxed(String A, String B) {
        if (A == null || B == null) {
            return 0.0d;
        }

        if (A.equals(B)) {
            return 1.0d;
        }

        String[] tokenized = generateTokenizedString(A, B);
        String tokenizedA = tokenized[0];
        String tokenizedB = tokenized[1];

        // Calculate LCS
        return ((double) LCS.length(tokenizedA, tokenizedB)) / Math.min(tokenizedA.length(), tokenizedB.length());
    }

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

        // Calculate LCS
        return ((double) LCS.length(tokenizedA, tokenizedB)) / Math.max(tokenizedA.length(), tokenizedB.length());
    }

    public static boolean isMatch(String A, String B, double error_threshold) {
        return approxDistance(A, B) <= error_threshold;
    }

    public static String subsequence(String A, String B) {
        int len_A = A.length(), len_B = B.length();
        int[][] L = new int[len_A+1][len_B+1];

        // Build L[len_A+1][len_B+1] in bottom up fashion.
        // Note that L[i][j] contains length of LCS of A[0..i-1] and B[0..j-1]
        for (int i = 0; i <= len_A; ++i)
        {
            for (int j = 0; j <= len_B; ++j)
            {
                if (i == 0 || j == 0)
                    L[i][j] = 0;
                else if (A.charAt(i-1) == B.charAt(j - 1))
                    L[i][j] = L[i-1][j-1] + 1;
                else
                    L[i][j] = Math.max(L[i - 1][j], L[i][j - 1]);
            }
        }

        //  Build LCS
        int index = L[len_A][len_B];

        // Create a character array to store the subsequence
        char[] lcs = new char[index];

        // Start from the bottom right corner and
        int i = len_A, j = len_B;
        while (i > 0 && j > 0)
        {
            // If current character in A[] and B are same, then
            // current character is part of LCS
            if (A.charAt(i - 1) == B.charAt(j - 1))
            {
                // Put the current character in result
                lcs[index - 1] = A.charAt(i - 1);

                // Reduce values of i, j and index
                --i;
                --j;
                --index;
            }

            // If they aren't the same, find the larger of two and go in its direction
            else if (L[i - 1][j] > L[i][j - 1])
                --i;
            else
                --j;
        }

        return new String(lcs);
    }
}
