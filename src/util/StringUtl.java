package util;

import comparator.LCS;

import java.text.Normalizer;
import java.util.*;

public class StringUtl {
    /**
     * Clean the input subsequence
     * - Trim
     * - Convert to lower case
     * - Strip diacritics
     *
     * @param str input subsequence
     * @return cleaned subsequence
     */
    public static String clean(String str) {
        return str.trim().replaceAll("\\s+", " ").toLowerCase();
    }

    public static String[] vietnameseChars = {
            "ô", "ơ"     , "â", "ă", "ê",           "ư",
            "ó", "ồ", "ớ", "á", "ấ", "ắ", "ề", "é", "ụ", "ứ", "í", "ý",
            "đ", "ò", "ộ", "ờ", "à", "ầ", "ằ", "ế", "è", "ú", "ừ", "ì", "ỳ",
            "õ", "ổ", "ở", "ã", "ẫ", "ẵ", "ễ", "ẽ", "ũ", "ữ", "ĩ", "ỷ",
            "ỏ", "ỗ", "ợ", "ả", "ẩ", "ẳ", "ể", "ẻ", "ủ", "ử", "ỉ", "ỹ",
            "ọ", "ố", "ỡ", "ạ", "ậ", "ặ", "ệ", "ẹ", "ù", "ự", "ị", "ỵ"};

    public static String[] normalizedVietnameseChars = {
            "o", "o"     , "a", "a", "e",           "u",
            "o", "o", "o", "a", "a", "a", "e", "e", "", "u", "i", "y",
            "d", "o", "o", "o", "a", "a", "a", "e", "e", "u", "u", "i", "y",
            "o", "o", "o", "a", "a", "a", "e", "e", "u", "u", "i", "y",
            "o", "o", "o", "a", "a", "a", "e", "e", "u", "u", "i", "y",
            "o", "o", "o", "a", "a", "a", "e", "e", "u", "u", "i", "y"};


    public static boolean isVietnamese(String str) {
        for (String vn : vietnameseChars) {
            if (str.contains(vn)) {
                return true;
            }
        }
        return false;
    }

    public static List<Integer> strToListInt(String str) {
        if (str.endsWith(",")) {
            str = str.substring(0, str.length() - 1);
        }

        String[] temp = str.split(", ");
        List<Integer> listInt = new ArrayList<>(temp.length);

        for (int i = 0; i < temp.length; ++i) {
            listInt.add(Integer.valueOf(temp[i]));
        }

        return listInt;
    }

    /**
     * Remove all diacritics, accents,...
     * NFD is unnecessarily slow
     *
     * @param str
     * @return
     */
    public static String removeDiacritics(String str) {
        for (int i = 0; i < vietnameseChars.length; ++i) {
            str = str.replace(vietnameseChars[i], normalizedVietnameseChars[i]);
        }

        return str;
    }

    /**
     * Count the number of occurrences of a single letter in a subsequence
     * Used in comparator
     *
     * @param haystack
     * @param needle
     * @return number of needle in the hackstack
     */
    public static int countChar(String haystack, char needle) {
        int counter = 0;

        for (int i = 0; i < haystack.length(); ++i) {
            if (haystack.charAt(i) == needle) {
                ++counter;
            }
        }

        return counter;
    }

    /**
     * Clean control chars before manually append strings to a JSON
     * @param string
     * @return
     */
    public static String cleanJSON(String string) {
        if (string == null || string.length() == 0) {
            return "\"\"";
        }

        char c = 0;
        int i;
        int len = string.length();
        StringBuilder sb = new StringBuilder(len + 4);
        String t;

        sb.append('"');
        for (i = 0; i < len; i += 1) {
            c = string.charAt(i);
            switch (c) {
                case '\\':

                case '"':
                    sb.append('\\');
                    sb.append(c);
                    break;

                case '/':
                    //                if (b == '<') {
                    sb.append('\\');
                    //                }
                    sb.append(c);
                    break;

                case '\b':
                    sb.append("\\b");
                    break;

                case '\t':
                    sb.append("\\t");
                    break;

                case '\n':
                    sb.append("\\n");
                    break;

                case '\f':
                    sb.append("\\f");
                    break;

                case '\r':
                    sb.append("\\r");
                    break;

                default:
                    if (c < ' ') {
                        t = "000" + Integer.toHexString(c);
                        sb.append("\\u" + t.substring(t.length() - 4));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');

        return sb.toString();
    }

    private static HashSet<String> stopwords = new HashSet<>();
    static {
        String[] stopwordList = {"a", "able", "about", "across", "after", "all", "almost", "also", "am", "among",
                                    "an", "and", "any", "are", "as", "at", "be", "because", "been", "but", "by",
                                    "can", "cannot", "could", "dear", "did", "do", "does", "either", "else", "ever",
                                    "every", "for", "from", "get", "got", "had", "has", "have", "he", "her", "hers",
                                    "him", "his", "how", "however", "i", "if", "in", "into", "is", "it", "its", "just",
                                    "least", "let", "like", "likely", "may", "me", "might", "most", "must", "my",
                                    "neither", "no", "nor", "not", "of", "off", "often", "on", "only", "or", "other",
                                    "our", "own", "rather", "said", "say", "says", "she", "should", "since", "so", "some",
                                    "than", "that", "the", "their", "them", "then", "there", "these", "they", "this", "tis",
                                    "to", "too", "twas", "us", "wants", "was", "we", "were", "what", "when", "where",
                                    "which", "while", "whom", "why", "will", "with", "would", "yet", "you", "your"};
        for (String stopword : stopwordList) {
            stopwords.add(stopword);
        }
    }

    public static boolean isStopWord(String word) {
        return stopwords.contains(word);
    }

    public static boolean isDOI(String block) {
        return block.startsWith("10.");
    }

    public static String getPages(String start, String finish) {
        if (start != null && finish != null) {
            return start + "-" + finish;
        }

        return null;
    }

    public static String normalizeISSN(String ISSN) {
        if (ISSN == null || ISSN.length() < 8) {
            return null;
        }

        ISSN = ISSN.replace('x', 'X').trim();

        if (ISSN.length() == 8) {
            return ISSN.substring(0, 4) + "-" + ISSN.substring(4, 8);
        } else if (ISSN.length() == 9) {
            return ISSN;
        } else {
            return null;
        }
    }

    private static Map<String, String> corrector = new LinkedHashMap<>();    // To retain insertion order
    static {
        corrector.put("Viet nam", "Vietnam");
        corrector.put("Việt nam", "Vietnam");
        corrector.put("Viet Nam", "Vietnam");
        corrector.put("Việt Nam", "Vietnam");
        corrector.put("Vietnamese", "Vietnam");

        corrector.put("Ha noi", "Hanoi");
        corrector.put("Hà nội", "Hanoi");
        corrector.put("Ha Noi", "Hanoi");
        corrector.put("Hà Nội", "Hanoi");

        corrector.put("Ho chi minh", "Ho Chi Minh");
        corrector.put("Hochiminh", "Ho Chi Minh");
        corrector.put("Ho-Chi-Minh", "Ho Chi Minh");
        corrector.put("VNUHCM", "temp-V-N-U-H-C-M-temp");
        corrector.put("HCMUS", "temp-H-C-M-U-S-temp");
        corrector.put("HCMUT", "temp-H-C-M-U-T-temp");
        corrector.put("HCMC", "Ho Chi Minh City");
        corrector.put("Hcmc", "Ho Chi Minh City");
        corrector.put("HCM", "Ho Chi Minh");    // Sometimes, order is very important
        corrector.put("temp-H-C-M-U-S-temp", "HCMUS");
        corrector.put("temp-H-C-M-U-T-temp", "HCMUT");    // RegEx is too damn slow
        corrector.put("temp-V-N-U-H-C-M-temp", "VNUHCM");
        corrector.put("HoChiMinh", "Ho Chi Minh");
        corrector.put("HochiMinh", "Ho Chi Minh");
        corrector.put("HoChi Minh", "Ho Chi Minh");

        corrector.put("Danang", "Da Nang");
        corrector.put("Da nang", "Da Nang");
        corrector.put("DaNang", "Da Nang");

        corrector.put("Cantho", "Can Tho");
        corrector.put("Can tho", "Can Tho");
        corrector.put("CanTho", "Can Tho");

        corrector.put("Thainguyen", "Thai Nguyen");
        corrector.put("Thai nguyen", "Thai Nguyen");

        corrector.put("Quynhon", "Quy Nhon");

        corrector.put("Univ.", "University");
        corrector.put("Universities", "University");

        corrector.put("Sciences", "Science");

        corrector.put("Ton-Duc-Thang", "Ton Duc Thang");
        corrector.put("Ton Due Thang", "Ton Duc Thang");
        corrector.put("TonDucThang", "Ton Duc Thang");

        corrector.put("Nguyen-Tat-Thanh", "Nguyen Tat Thanh");
        corrector.put("NguyenTatThanh", "Nguyen Tat Thanh");

        corrector.put("DuyTan", "Duy Tan");
        corrector.put("Duytan", "Duy Tan");
    }

    public static String correct(String str) {
        for (String key : corrector.keySet()) {
            str = str.replace(key, corrector.get(key));
        }

        return str;
    }

    public static String removeConsecutiveDuplicated(String str, char c) {
        if (str == null || str.length() == 0) {
            return str;
        }

        StringBuilder out = new StringBuilder(str.substring(0, 1));

        for (int i = 1; i < str.length(); ++i) {
            if (str.charAt(i) != str.charAt(i - 1)) {
                out.append(str.charAt(i));
            }
        }

        return out.toString();
    }

    public static void main(String[] args) {
        System.out.println(removeConsecutiveDuplicated("%% a%%%sd asd%", '%'));
    }

    public static String normalizeSpaces(String str) {
        if (str != null) {
            return str.replaceAll("\\s+", " ").trim();
        } else {
            return null;
        }
    }

    public static String cleanComma(String str) {
        str = normalizeSpaces(str);

        if (str.startsWith(",")) {
            str = str.substring(1);
        }

        if (str.endsWith(",")) {
            str = str.substring(0, str.length() - 1);
        }

        return str.trim();
    }

    public static String[] generateTokenizedString(String A, String B) {
        // Segment strings
        // Non alphanumeric stuffs are used as delimiters
        ArrayList<String> segmentedA = new ArrayList<>(Arrays.asList(flattenToASCII(A).toLowerCase().split("[^\\w']+")));
        for (int i = 0; i < segmentedA.size(); ++i) {
            String segment = segmentedA.get(i);
            if (segment.length() == 0 || StringUtl.isStopWord(segment)) {
                segmentedA.remove(i--);
            }
        }

        ArrayList<String> segmentedB = new ArrayList<>(Arrays.asList(flattenToASCII(B).toLowerCase().split("[^\\w']+")));
        for (int i = 0; i < segmentedB.size(); ++i) {
            String segment = segmentedB.get(i);
            if (segment.length() == 0 || StringUtl.isStopWord(segment)) {
                segmentedB.remove(i--);
            }
        }

        // Tokenization
        // Words are converted into unique characters (tokens)
        // Word that is a prefix of a word of the other subsequence are treated as the same word as the its extending word
        // Example: J for Journal, Res for research,...
        // However, in some case there could be >= 2 words in a sentence that have the same prefix
        // Then do a tie-break: choose the word that is the most similar in length with the prefix

        // This character is to
        char representChar = 'a';
        HashMap<String, Character> representCharMap = new HashMap<>();

        // Match segment and build token corrector
        for (String a : segmentedA) {
            if (representCharMap.containsKey(a)) {
                continue;
            }

            float score = 0.0f;
            int posB = -1;

            for (int j = 0; j < segmentedB.size(); ++j) {
                String b = segmentedB.get(j);

                if (possiblySameWord(a, b)) {
                    // Found possible deduplicate
                    float tempScore = ((float) Math.min(a.length(), b.length()) ) / Math.max(a.length(), b.length());
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

        // Tokenize using built token corrector
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

//        System.out.println(tokenizedA + "\n" + tokenizedB);

        return new String[]{tokenizedA, tokenizedB};
    }

    /**
     * This function is strongly biased to some common patterns of abbreviation in ISI DB
     * For example: Natl for Naturelles,...
     *
     * @param A
     * @param B
     * @return
     */
    public static Map<String, String> abbr = new HashMap<>();
    private static boolean possiblySameWord(String A, String B) {
        String shorter, longer;

        if (A.length() < B.length()) {
            shorter = A;
            longer = B;
        } else {
            shorter = B;
            longer = A;
        }

        if (longer.startsWith(shorter)) {
            abbr.put(shorter, longer);
            return true;
        }

        int lengthLCS = LCS.length(longer, shorter);
        boolean isAbbr = shorter.charAt(0) == longer.charAt(0) && lengthLCS > shorter.length() * 0.9f;
        if (isAbbr) {
            abbr.put(shorter, longer);
        }

        return isAbbr;
    }

    /**
     * @author David Conrad
     * https://stackoverflow.com/questions/3322152/is-there-a-way-to-get-rid-of-accents-and-convert-a-whole-string-to-regular-lette
     *
     * @param str
     * @return
     */
    public static String flattenToASCII(String str) {
        char[] out = new char[str.length()];
        str = Normalizer.normalize(str, Normalizer.Form.NFD);

        int j = 0;
        for (int i = 0; i < str.length(); ++i) {
            char c = str.charAt(i);
            if (c <= '\u007F') {
                out[j++] = c;
            }
        }

        return new String(out);
    }

    public static String normalize(String str) {
        return str.trim().toLowerCase().replaceAll("[^A-Za-z0-9]", "");
    }

    public static boolean isNumber(String str) {
        try {
            Double.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static int countOccurences(String A, String B) {
        if (A.length() < B.length()) {
            return 0;
        }

        int indexOf = 0, counter = 0;

        while (true) {
            indexOf = A.indexOf(B, indexOf);

            if (indexOf < 0) {
                break;
            } else {
                if ((indexOf == 0 || !Character.isAlphabetic(A.charAt(indexOf - 1))) &&
                        (indexOf == A.length() - B.length() || !Character.isAlphabetic(A.charAt(indexOf + B.length())))) {
                    ++counter;
                }

                ++indexOf;
            }
        }

        return counter;
    }
}

