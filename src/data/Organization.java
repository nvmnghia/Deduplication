package data;

import comparator.LCS;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static util.StringUtl.generateTokenizedString;

public class Organization {
    private int id, _lft, _rgt;
    private String name, slug;

    public Organization() {
    }

    public Organization(int id, int _lft, int _rgt, String name, String slug) {
        this.id = id;
        this._lft = _lft;
        this._rgt = _rgt;
        this.name = name;
        this.slug = slug;
    }

    public Organization(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public Organization(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSlug() {
        return slug;
    }

    public void setSlug(String slug) {
        this.slug = slug;
    }

    public int get_lft() {
        return _lft;
    }

    public void set_lft(int _lft) {
        this._lft = _lft;
    }

    public int get_rgt() {
        return _rgt;
    }

    public void set_rgt(int _rgt) {
        this._rgt = _rgt;
    }

    public String getSuffix() {
        return getSuffix(this.name);
    }

    public static final int SUBSEQUENCE = 1;
    public static final int CONTAIN = 2;
    public static final int COMMON = 3;
    public static final int STD = 4;

    public static double nameSimilarity(String A, String B, int OPTION) {
        if (A == null || B == null) {
            return 0.0d;
        }

        if (A.equals(B)) {
            return 1.0d;
        }

        String[] tokenized = generateTokenizedString(A, B);
        String tokenizedA = tokenized[0];
        String tokenizedB = tokenized[1];

        // Not enough info to compare
        // Avoid the like of "Hanoi, Vietnam" to equals everything
        if (tokenizedA.length() <= 2 || tokenizedB.length() <= 2) {
            return 0.0d;
        }

        // There're cases where  several organizations are lumped together
        // This check avoid them to be matched with an individual organization
        // For example: "Ton Duc Thang, Dai hoc Quoc gia" won't be matched against either "Ton duc thang" or "Dai hoc quoc gia"
        if (tokenizedA.length() <= tokenizedB.length() / 2 ||
            tokenizedB.length() <= tokenizedA.length() / 2) {
            return 0.0d;
        }

        // Check for country: ... Vietnam and ... China are obviously not the same
        if (tokenizedA.charAt(tokenizedA.length() - 1) != tokenizedB.charAt(tokenizedB.length() - 1)) {
            return 0.0d;
        }

        String shorter, longer;
        if (tokenizedA.length() < tokenizedB.length()) {
            shorter = tokenizedA;
            longer = tokenizedB;
        } else {
            shorter = tokenizedB;
            longer = tokenizedA;
        }

        switch (OPTION) {
            case SUBSEQUENCE:
                return LCS.length(shorter, longer) / (double) shorter.length();

            case STD:
                String subsequence = LCS.subsequence(shorter, longer);

                // Find the position of each of the subsequence token in the longer string
                List<Integer> pos = new ArrayList<>();
                for (int i = 0; i < subsequence.length(); ++i) {
                    pos.add(longer.indexOf(subsequence.charAt(i)));
                }

                // Calculate the std
                int sum = 0;
                for (Integer i : pos) {
                    sum += i;
                }

                float mean = sum / (float)pos.size();
                float std = 0;
                for (Integer i : pos) {
                    std += (i - mean) * (i - mean);
                }

                return Math.sqrt(std / pos.size());

            default:
                return 0;
        }
    }

    public static double nameSimilarity(String A, String B) {
        return nameSimilarity(A, B, SUBSEQUENCE);
    }

    public static String getSuffix(String organization) {
        if (organization.length() < 2) {
            return null;
        }

        organization = organization.toLowerCase().trim().replace("viet nam", "vietnam");

        for (int i = organization.length() - 2; i >= 0; --i) {
            char character = organization.charAt(i);
            if (character == ' ' || character == ',' || character == '.' || character == ';') {
                return organization.substring(++i).trim();
            }
        }

        return null;
    }
}
