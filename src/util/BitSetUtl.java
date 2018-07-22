package util;

import java.math.BigInteger;
import java.util.BitSet;

public class BitSetUtl {
    private int LENGTH;

    private BitSet ZERO;
    private BitSet ONE;
    private BitSet NEGATED_ONE;

    public BitSetUtl(int LENGTH) {
        this.LENGTH = LENGTH;

        ZERO = createZero();
        ONE = createOne();
        NEGATED_ONE = createNegatedOne();
    }

    public BitSet createNegatedOne() {
        BitSet negated_one = new BitSet(LENGTH);

        negated_one.set(0, negated_one.size() - 1);
        return negated_one;
    }

    public BitSet createOne() {
        BitSet one = new BitSet(LENGTH);
        one.set(one.size() - 1);
        return one;
    }

    public BitSet createZero() {
        return new BitSet(LENGTH);
    }

    public BitSet getZERO() {
        return ZERO;
    }

    public BitSet getONE() {
        return ONE;
    }


    public BitSet getNEGATED_ONE() {
        return NEGATED_ONE;
    }

    public BitSet shiftLeft(BitSet input, int shift) {
        BitSet shifted = new BitSet(input.size());
        int j = 0;
        for (int i = shift; i < input.size(); ++i) {
            shifted.set(j++, input.get(i));
        }
        return shifted;
    }

    public boolean isEqual(BitSet a, BitSet b) {
        if (a.size() != b.size()) {
            return false;
        }

        for (int i = 0; i < a.size(); ++i) {
            if (a.get(i) != b.get(i)) {
                return false;
            }
        }
        return true;
    }

    public BitSet[] createBitSetArr(int size) {
        BitSet[] arr = new BitSet[size];
        for (int i = 0; i < size; ++i) {
            arr[i] = new BitSet(LENGTH);
        }
        return arr;
    }

    public BitSet convert(int value) {
        BitSet temp = new BitSet(), bits = new BitSet(LENGTH);
        int index = 0;

        while (value != 0) {
            if (value % 2 != 0) {
                temp.set(index++);
            }
            value = value >>> 1;
        }

        int j = 0;
        for (int i = LENGTH - index; i < LENGTH; ++i) {
            bits.set(i, temp.get(j++));
        }

        return bits;
    }

    public static void print(BitSet a) {
        for (int i = 0; i < a.size(); ++i) {
            System.out.print(a.get(i) ? '1' : '0');
        }
    }

    public static void println(BitSet a) {
        for (int i = 0; i < a.size(); ++i) {
            System.out.print(a.get(i) ? '1' : '0');
        }
        System.out.println("");
    }

    public static String stringify(BitSet a) {
        StringBuilder builder = new StringBuilder("");
        boolean found = false;
        for (int i = 0; i < a.size(); ++i) {
            if (a.get(i)) {
                found = true;
            }

            if (found) {
                builder.append(a.get(i) ? '1' : '0');
            }
        }

        String temp = builder.toString();
        return temp.length() == 0 ? "0" : temp;
    }

    public static String toInteger(BitSet a) {
        return a.length() == 0 ? "0" : new BigInteger(a.toByteArray()).toString();
    }

    public static String toLongBin(BitSet a) {
        return stringify(a).substring(a.size() - 64, a.size());
    }
}
