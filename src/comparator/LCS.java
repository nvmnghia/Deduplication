package comparator;

import info.debatty.java.stringsimilarity.LongestCommonSubsequence;
import info.debatty.java.stringsimilarity.MetricLCS;
import util.StringUtl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class LCS {


    private static MetricLCS lcs = new MetricLCS();

    public static double preciseDistance(String A, String B) {
        return lcs.distance(A, B);
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
            return 0.0d;
        }

        if (A.equals(B)) {
            return 0.0d;
        }

        // Segment strings
        // Non alphanumeric stuffs are used as delimiters
//        List<String> segmentedA = Arrays.asList(A.toLowerCase().split("[^\\\\w']+"));
        ArrayList<String> segmentedA = new ArrayList<>(Arrays.asList(A.toLowerCase().split("[^\\w']+")));
        for (int i = 0; i < segmentedA.size(); ++i) {
            String segment = segmentedA.get(i);
            if (segment.length() == 0 || StringUtl.isStopWord(segment)) {
                segmentedA.remove(i);
            }
        }

        ArrayList<String> segmentedB = new ArrayList<>(Arrays.asList(B.toLowerCase().split("[^\\w']+")));
        for (int i = 0; i < segmentedB.size(); ++i) {
            String segment = segmentedB.get(i);
            if (segment.length() == 0 || StringUtl.isStopWord(segment)) {
                segmentedB.remove(i);
            }
        }

        // Tokenization
        // Words are converted into unique characters (token)
        // Word that is a prefix of a word of the other string are treated as the same word as the its extending word
        // Example: J for Journal, Res for research,...
        // However, in some case there could be >= 2 words in a string that has the same prefix, and that prefix appears in the other string
        // Then tie-break: choose the word that is the most similar in length with the prefix

        // This character is to
        char representChar = 'a';
        HashMap<String, Character> representCharMap = new HashMap<>();

        // Match segment and build token map
        for (int i = 0; i < segmentedA.size(); ++i) {
            String a = segmentedA.get(i);

            if (representCharMap.containsKey(a)) {
                continue;
            }

            float score = 0.0f;
            int posB = -1;

            for (int j = 0; j < segmentedB.size(); ++j) {
                String b = segmentedB.get(j);

                if (a.startsWith(b) || b.startsWith(a)) {
                    // Found possible deduplicate
                    float tempScore = Math.min(a.length(), b.length()) / Math.max(a.length(), b.length());
                    if (score < tempScore) {
                        score = tempScore;
                        posB = j;
                    }
                }
            }

            representCharMap.put(a, ++representChar);

            if (posB != -1) {
                // a and segmentedB[posB], one is the prefix of the other
                representCharMap.put(segmentedB.get(posB), representChar);
            }
        }

        // Tokenize using built token map
        StringBuilder builder = new StringBuilder();
        for (String segment : segmentedA) {
            builder.append(representCharMap.get(segment));
        }
        String tokenizedA = builder.toString();

        builder = new StringBuilder();
        for (String segment : segmentedB) {
            // After the loop, all segments in segmentedA are in the representCharMap
            // But there're segments in segmentedB which is not contained in representCharMap
            if (representCharMap.containsKey(segment)) {
                builder.append(representCharMap.get(segment));

            } else {
                representCharMap.put(segment, ++representChar);
                builder.append(representChar);
            }
        }
        String tokenizedB = builder.toString();

        // Calculate LCS
        return lcs.distance(tokenizedA, tokenizedB);
    }


    public static boolean isMatch(String A, String B, double error_threshold) {
        return approxDistance(A, B) <= error_threshold;
    }
}
