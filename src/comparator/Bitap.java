package comparator;

import util.BitSetUtl;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * The code below, which implements the comparator algorithm,
 * is an excerpt from the original google-diff-deduplicate-patch
 * Original code is commented out if modification is needed
 * @Author GerHobbelt
 * https://github.com/GerHobbelt/google-diff-match-patch
 *
 * The algorithm generates a deduplicate if there exist a substring in the text
 * which differs less than a given error threshold from the pattern
 *
 * The algorithm is modified to work with arbitrary long patterns
 * by storing the mask in a BitSet
 * Patterns having less than 64 chars still benefit from the fast bitwise version
 */

public class Bitap {

    /**
     * Main deduplicate function
     *
     * If the pattern length is less than 64 chars, match_bitap function will be used
     * Else, the match_bitap function will be used as the first pass, with pattern trimmed to the first 63 chars
     * If a deduplicate is detected, a confirmation deduplicate will be performed by match_bitap_extended
     * All this stuff is for performance reason, as the extended deduplicate is PAINFULLY slow
     *
     * @param text The text to search.
     * @param pattern The pattern to search for.
     * @param error_threshold Highest error beyond which we give up.
     * @return float [best_loc, similarity] array.
     *         If no deduplicate is detected, returns [-1, 0.0f]
     *         Obviously, the similarity returned only ranges from error_threshold away from 1.0f
     *         i.e., if the error_threshold is 0.1f, then similarity is >= 0.9f and <= 1.0f
     */
    public static float[] match(String text, String pattern, float error_threshold) {
        if (pattern.length() < 64) {
            return match_bitap(text, pattern, error_threshold);
        } else {
            float[] initial_result = match_bitap(text, pattern.substring(0, 63), error_threshold);
            if (initial_result[1] == 0f) {
                // If normal comparator can't deduplicate the first 63 chars, the chance for extended deduplicate is low too
                // Acceptable heuristic, since 64 is already quite long.
                return initial_result;
            }

            // Hardcore matcher, finally
            // For the more potential string
            return match_bitap_extended(text, pattern, error_threshold);
        }
    }

    /**
     * Same description as the above one, but return bool instead
     *
     * @param text The text to search.
     * @param pattern The pattern to search for.
     * @param error_threshold Highest error beyond which we give up.
     * @return float [best_loc, similarity] array.
     *         If no deduplicate is detected, returns [-1, 0.0f]
     *         Obviously, the similarity returned only ranges from error_threshold away from 1.0f
     *         i.e., if the error_threshold is 0.1f, then similarity is >= 0.9f and <= 1.0f
     */
    public static boolean isMatch(String text, String pattern, float error_threshold) {
        float[] result = match(text, pattern, error_threshold);
        return result[0] != -1;
    }

    /**
     * Locate the best instance of 'pattern' in 'text' near 'loc' using the Bitap algorithm.
     * Only support pattern less than 64 chars
     *
     * @param text The text to search.
     * @param pattern The pattern to search for.
     * @param score_threshold Highest score beyond which we give up.
     * @return float [best_loc, similarity] array
     */
    protected static float[] match_bitap(String text, String pattern, float score_threshold) {

        // Is there a nearby exact deduplicate? (speedup)
        int best_loc = text.indexOf(pattern, 0);
        if (best_loc != -1) {
            float[] results = {best_loc, 1f};
            return results;
        }

        // Initialise the alphabet.
        Map<Character, Long> s = match_alphabet(pattern);

        // Initialise the bit arrays.
        long matchmask = 1L << (pattern.length() - 1);

        int bin_min, bin_mid;
        int bin_max = pattern.length() + text.length();

        // Empty initialization added to appease Java compiler.
        long[] last_rd = new long[0];
        int max_err = (int) (score_threshold * pattern.length());

        int current_min_err = Integer.MAX_VALUE;
        for (int d = 0; d <= max_err; ++d) {
            // Scan for the best deduplicate; each iteration allows for one more error.

            // Run a binary search to determine how far from 'loc' we can stray at this error level.
            bin_min = 0;
            bin_mid = bin_max;
            while (bin_min < bin_mid) {
                if (d <= current_min_err) {
                    bin_min = bin_mid;
                } else {
                    bin_max = bin_mid;
                }
                bin_mid = (bin_max - bin_min) / 2 + bin_min;
            }

            // Use the result from this iteration as the maximum for the next.
            bin_max = bin_mid;
            int start = Math.max(1, 1 - bin_max);
            int finish = Math.min(bin_max, text.length()) + pattern.length();

            long[] rd = new long[finish + 2];
            rd[finish + 1] = (1 << d) - 1;

            for (int j = finish; j >= start; --j) {

                long charMatch;
                if (text.length() < j || !s.containsKey(text.charAt(j - 1))) {
                    // Out of range.
                    charMatch = 0;
                } else {
                    charMatch = s.get(text.charAt(j - 1));
                }

                if (d == 0) {
                    // First pass: exact deduplicate.
                    rd[j] = ((rd[j + 1] << 1) | 1) & charMatch;
                } else {
                    // Subsequent passes: fuzzy deduplicate.
                    rd[j] = (((rd[j + 1] << 1) | 1) & charMatch)
                            | (((last_rd[j + 1] | last_rd[j]) << 1) | 1) | last_rd[j + 1];
                }

                if ((rd[j] & matchmask) != 0) {
                    // This deduplicate will almost certainly be better than any existing deduplicate. But check anyway.
                    if (current_min_err >= d) {
                        // Told you so.
                        current_min_err = d;
                        best_loc = j - 1;

                        if (best_loc > 0) {
                            // When passing loc, don't exceed our current approxDistance from loc.
                            start = Math.max(1, 0 - best_loc);
                        } else {
                            // Already passed loc, downhill from here on in.
                            break;
                        }
                    }
                }
            }

            last_rd = rd;
        }

        if (current_min_err > max_err) {
            float[] results = {-1, 0f};
            return results;
        } else {
            float[] results = {best_loc, 1 - ((float) current_min_err) / pattern.length()};
            return results;
        }
    }

    /**
     * Extended version of the above block of code
     * Support patterns longer than 64 chars
     * Of course it is slower
     * PAINFULLY slower
     * Original, native bitwise code is commented out for reference
     *
     * @param text The text to search.
     * @param pattern The pattern to search for.
     * @param score_threshold Highest score beyond which we give up.
     * @return float [best_loc, similarity] array
     */
    public static float[] match_bitap_extended(String text, String pattern, float score_threshold) {

        // Is there a nearby exact deduplicate? (speedup)
        int best_loc = text.indexOf(pattern, 0);
        if (best_loc != -1) {
            float[] results = {best_loc, 1f};
            return results;
        }

        // Initialize BitSetUtl
        BitSetUtl bitSetUtl = new BitSetUtl(pattern.length());

        // Initialise the alphabet.
//        Map<Character, Long> s = match_alphabet(pattern);
        Map<Character, BitSet> s = match_alphabet_extended(pattern, bitSetUtl);

        // Initialise the bit arrays.
//        long matchmask = 1 << (pattern.length() - 1);
        BitSet matchmask = bitSetUtl.shiftLeft(bitSetUtl.getONE(), pattern.length() - 1);

        int bin_min, bin_mid;
        int bin_max = pattern.length() + text.length();

        // Empty initialization added to appease Java compiler.
//        long[] last_rd = new long[0];
        BitSet[] last_rd = new BitSet[0];
        int max_err = (int) (score_threshold * pattern.length());

        int current_min_err = Integer.MAX_VALUE;
        for (int d = 0; d < max_err; ++d) {

            // Scan for the best deduplicate; each iteration allows for one more error.
            // Run a binary search to determine how far from 'loc' we can stray at
            // this error level.
            bin_min = 0;
            bin_mid = bin_max;
            while (bin_min < bin_mid) {
                if (d <= current_min_err) {
                    bin_min = bin_mid;
                } else {
                    bin_max = bin_mid;
                }
                bin_mid = (bin_max - bin_min) / 2 + bin_min;
            }

            // Use the result from this iteration as the maximum for the next.
            bin_max = bin_mid;
            int start = Math.max(1, 1 - bin_mid);
            int finish = Math.min(bin_mid, text.length()) + pattern.length();

//            long[] rd = new long[finish + 2];
            BitSet[] rd = bitSetUtl.createBitSetArr(finish + 2);

//            rd[finish + 1] = (1 << d) - 1;
            rd[finish + 1] = bitSetUtl.shiftLeft(bitSetUtl.getONE(), d);
            rd[finish + 1].and(bitSetUtl.getNEGATED_ONE());

            for (int j = finish; j >= start; --j) {
//                long charMatch;
                BitSet charMatch;

                if (text.length() <= j - 1 || ! s.containsKey(text.charAt(j - 1))) {
                    // Out of range
//                    charMatch = 0;
                    charMatch = new BitSet(pattern.length());
                } else {
//                    charMatch = s.get(text.charAt(j - 1));
                    charMatch = s.get(text.charAt(j - 1));
                }

                if (d == 0) {
                    // First pass: exact deduplicate.
//                    rd[j] = ((rd[j + 1] << 1) | 1) & charMatch;
                    rd[j] = bitSetUtl.shiftLeft(rd[j + 1], 1);
                    rd[j].or(bitSetUtl.getONE());
                    rd[j].and(charMatch);

                } else {
                    // Subsequent passes: fuzzy deduplicate.
//                    rd[j] = (((rd[j + 1] << 1) | 1) & charMatch)
//                            | (((last_rd[j + 1] | last_rd[j]) << 1) | 1) | last_rd[j + 1];

                    BitSet term1 = bitSetUtl.shiftLeft(rd[j + 1], 1);
                    term1.or(bitSetUtl.getONE());
                    term1.and(charMatch);

                    BitSet term2 = last_rd[j + 1];
                    term2.or(last_rd[j]);
                    term2 = bitSetUtl.shiftLeft(term2, 1);
                    term2.or(bitSetUtl.getONE());

                    BitSet term3 = last_rd[j + 1];

                    rd[j] = term1;
                    rd[j].or(term2);
                    rd[j].or(term3);
                }

                BitSet temp = (BitSet) matchmask.clone();
                temp.and(rd[j]);

                if (! bitSetUtl.isEqual(temp, bitSetUtl.getZERO())) {
                    // This deduplicate will almost certainly be better than any existing deduplicate
                    // But check anyway
                    if (current_min_err >= d) {
                        // Told you so.
                        current_min_err = d;
                        best_loc = j - 1;
                        if (best_loc > 0) {
                            // When passing loc, don't exceed our current approxDistance from loc.
                            start = Math.max(1, 0 - best_loc);
                        } else {
                            // Already passed loc, downhill from here on in.
                            break;
                        }
                    }
                }
            }

//            last_rd = rd;
            last_rd = rd;
        }

        if (current_min_err > max_err) {
            float[] results = {-1, 0f};
            return results;
        } else {
            float[] results = {best_loc, 1 - ((float) current_min_err) / pattern.length()};
            return results;
        }
    }

    /**
     * Initialise the alphabet for the Bitap algorithm.
     * @param pattern The text to encode.
     * @return Hash of character locations.
     */
    private static Map<Character, Long> match_alphabet(String pattern) {
        Map<Character, Long> s = new HashMap<Character, Long>();
        for (int i = 0; i < pattern.length(); ++i) {
            s.put(pattern.charAt(i), 0L);
        }

        int i = 0;
        for (int j = 0; j < pattern.length(); ++j) {
            char c = pattern.charAt(j);
            s.put(c, s.get(c) | (1L << (pattern.length() - i - 1)));
            ++i;
        }

        return s;
    }

    /**
     * The same as the above function, but uses BitSet instead of Long
     * Original code is commented out for reference
     *
     * @param pattern The text to encode.
     * @return Hash of character locations.
     */
    private static Map<Character, BitSet> match_alphabet_extended(String pattern, BitSetUtl bitSetUtl) {
        Map<Character, BitSet> s = new HashMap<Character, BitSet>();
        for (int i = 0; i < pattern.length(); ++i) {
//            s.put(pattern.charAt(i), 0L);
            s.put(pattern.charAt(i), bitSetUtl.getZERO());
        }

        int i = 0;
        for (int j = 0; j < pattern.length(); ++j) {
            char c = pattern.charAt(j);
//            s.put(c, s.get(c) | (1 << (pattern.length() - i - 1)));
            BitSet temp = bitSetUtl.shiftLeft(bitSetUtl.getONE(), pattern.length() - i - 1);
            temp.or(s.get(c));
            s.put(c, temp);
            ++i;
        }

        return s;
    }
}