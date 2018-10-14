package Project2.btree;

import java.util.ArrayList;
import java.util.List;

public class DataEntry implements Comparable<DataEntry>{

    int key;                // the value of k
    ArrayList<rid> RidList;

    public DataEntry(int key, List<rid> RidList) {
        this.key = key;
        this.RidList = new ArrayList<>(RidList);
    }

    /**
     * The method to compares to another Data Entry,
     * return 1 if it is greater than the compared one,
     * return -1 if smaller, return 0 if they are equal.
     *
     * @return the result of the comparison, -1, 0 or 1
     */
    @Override
    public int compareTo(DataEntry that) {
        if (this.key > that.key) return 1;
        else if (this.key < that.key) return -1;
        else return 0;
    }

    /**
     * Returns the string representation of the data entry.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("<[" + key +":");
        for (rid r : RidList) {
            sb.append(r.toString());
        }
        sb.append("]>");
        return sb.toString();
    }
}
