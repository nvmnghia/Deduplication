package data;

import comparator.LCS;
import util.StringUtl;

import java.util.ArrayList;
import java.util.List;

public class Name {
    public static ArrayList<String> generateAbbrNames(Author author) {
        ArrayList<String> abbrNames = new ArrayList<>();
        abbrNames.add(author.getFullName());

        // From full name
        abbrNames.addAll(generateAbbrNames(author.getFullName()));


        return abbrNames;
    }

    public static ArrayList<String> generateAbbrNames(String fullName) {
        String[] words = StringUtl.clean(fullName.replace(".", ". ")).split(" ");

        // If the full name was abbreviated already, return empty list
        // No shortening can be done
        if (StringUtl.countChar(fullName, '.') >= words.length - 1) {
            return new ArrayList<>();
        }

        ArrayList<String> abbrNames = new ArrayList<>();

        // Example Nguyen Viet Minh Nghia

        // Rule 1: nghia nguyen viet minh
        StringBuilder temp = new StringBuilder(words[words.length - 1]);
        for (int i = 0; i < words.length - 1; ++i) {
            temp.append(' ').append(words[i]);
        }
        abbrNames.add(temp.toString());

        // Rule 1: n. v. m. nghia
        temp = new StringBuilder("");
        for (int i = 0; i < words.length - 1; ++i) {
            temp.append(words[i].charAt(0)).append(". ");
        }
        temp.append(words[words.length - 1]);
        abbrNames.add(temp.toString());

        // Rule 2: nghia, n. v. m.
        temp = new StringBuilder(words[words.length - 1]).append(' ');
        for (int i = 0; i < words.length - 1; ++i) {
            temp.append(words[i].charAt(0)).append(". ");
        }
        abbrNames.add(temp.toString().trim());

        // Rule 3: nghia, n.
        temp = new StringBuilder(words[words.length - 1]).append(", ").append(words[0].charAt(0)).append('.');
        abbrNames.add(temp.toString());

        // Rule 4: n. nghia
        temp = new StringBuilder().append(words[0].charAt(0)).append(". ").append(words[words.length - 1]);
        abbrNames.add(temp.toString());

        return abbrNames;
    }

    public static boolean isMatch(Author author1, Author author2) {
//        List<String> listAbbrName1 = author1.getListAbbrName();
//        List<String> listAbbrName2 = author2.getListAbbrName();
//
//        for (String name1 : listAbbrName1) {
//            for (String name2 : listAbbrName2) {
//                if (name1.equals(name2)) {
//                    return true;
//                }
//            }
//        }
//
//        return false;

        // Name error threshold is a lil higher to compensate abbreviation
        return LCS.isMatch(author1.getFullName(), author2.getFullName(), 0.4d);
    }
}
