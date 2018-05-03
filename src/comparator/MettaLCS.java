package comparator;

import util.StringUtl;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class MettaLCS {
    public static double getSimilarity(String a, String b, boolean charCheck, boolean singleWord) {
        if (a != null && b != null) {
            String[] x = getWords(a, singleWord);
            String[] y = getWords(b, singleWord);
            int[][] lcs = new int[x.length + 1][y.length + 1];

            int i;
            for(i = 0; i < x.length + 1; ++i) {
                lcs[i][0] = 0;
            }

            for(i = 0; i < y.length + 1; ++i) {
                lcs[0][i] = 0;
            }

            for(i = 1; i < x.length + 1; ++i) {
                for(int j = 1; j < y.length + 1; ++j) {
                    String s1 = x[i - 1];
                    String s2 = y[j - 1];
                    boolean equal = false;
                    if (!charCheck || s1.matches("\\w+") && s2.matches("\\w+")) {
                        if ((s1.length() > s2.length() || !s2.startsWith(s1)) && !s1.startsWith(s2)) {
                            equal = false;
                        } else {
                            equal = true;
                        }
                    } else {
                        double sim = getSimilarity(s1, s2, false, true);
                        if (sim > 0.8D) {
                            equal = true;
                        }
                    }

                    if (equal) {
                        lcs[i][j] = lcs[i - 1][j - 1] + 1;
                    } else {
                        lcs[i][j] = Math.max(lcs[i - 1][j], lcs[i][j - 1]);
                    }
                }
            }

            i = lcs[x.length][y.length];
            return (double)i / (double)Math.min(x.length, y.length);
        } else {
            return 0.0D;
        }
    }

    private static String[] getWords(String s, boolean singleWord) {
        if (!singleWord) {
            StringTokenizer st = new StringTokenizer(s, " \t\n\r\f-&.,:()=[]'/");
            List<String> words = new ArrayList();

            while(st.hasMoreTokens()) {
                String word = st.nextToken();
                word = word.toLowerCase();
                if (word.endsWith("'s")) {
                    word = word.substring(0, word.length() - 2);
                }

                if (!StringUtl.isStopWord(word) && !word.matches("\\d+")) {
                    words.add(word);
                }
            }

            return (String[])words.toArray(new String[0]);
        } else {
            List<String> words = new ArrayList();
            char[] chars = s.toCharArray();
            char[] var7 = chars;
            int var6 = chars.length;

            for(int var5 = 0; var5 < var6; ++var5) {
                char c = var7[var5];
                words.add(new String(new char[]{c}));
            }

            return (String[])words.toArray(new String[0]);
        }
    }
}
