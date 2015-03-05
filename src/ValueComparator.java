import java.util.Comparator;
import java.util.Map;

// Comparator to sort using values

class ValueComparator implements Comparator<String> {
 
    Map<String, Double> map;
 
    public ValueComparator(Map<String, Double> base) {
        this.map = base;
    }
 
    public int compare(String a, String b) {
        if (map.get(a) >= map.get(b)) {
            return -1;
        } else {
            return 1;
        } 
    }
}