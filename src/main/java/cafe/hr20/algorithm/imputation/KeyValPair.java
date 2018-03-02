package cafe.hr20.algorithm.imputation;

public class KeyValPair implements Comparable<KeyValPair> {
    int key;
    double value;
    public KeyValPair(int key, double val) {
        this.key = key;
        this.value = val;
    }
    public int compareTo(KeyValPair keyValPair) {
        return ((Double)(this.value)).compareTo((Double)keyValPair.value);
    }
}