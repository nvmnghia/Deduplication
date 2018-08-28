package util;

import javafx.util.Pair;

import java.util.*;

public class Utl {

    /**
     * Return the set of A-and-not-B and B-and-not-A
     * @param A
     * @param B
     * @return
     */
    public static List<List<Integer>> setDiff(Collection<Integer> A, Collection<Integer> B) {
        if (A == null) {
            A = new ArrayList<>();
        }
        if (B == null) {
            B = new ArrayList<>();
        }

        List<List<Integer>> out = new ArrayList<>();

        Set<Integer> setB = new HashSet<>(B);
        List<Integer> AnotB = new ArrayList<>();
        for (Integer a : A) {
            if (! setB.contains(a)) {
                AnotB.add(a);
            }
        }
        out.add(AnotB);

        Set<Integer> setA = new HashSet<>(A);
        List<Integer> BnotA = new ArrayList<>();
        for (Integer b : B) {
            if (! setA.contains(b)) {
                BnotA.add(b);
            }
        }
        out.add(BnotA);

        return out;
    }

    public static void main(String[] args) {
        List<Integer> a = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            a.add(i);
        }

        List<Integer> b = new ArrayList<>();
        for (int i = 5; i < 10; ++i) {
            b.add(i);
        }

        System.out.println(setDiff(a, b));
    }
}
