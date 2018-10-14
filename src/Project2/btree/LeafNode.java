package Project2.btree;

import java.util.ArrayList;
import java.util.List;

public class LeafNode extends TreeNode{

    List<DataEntry> dataEntries;    //the serialized representation of each data entry in the node
    int min;

    public LeafNode(int order, List<DataEntry> dataEntries) {
        super(order);
        this.dataEntries = new ArrayList<>(dataEntries);
        min = dataEntries.get(0).key;
    }

    /**
     * Returns the string representation of the Leaf Node.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Leaf Node[\n");
        for (DataEntry data : dataEntries) {
            sb.append(data.toString() + "\n");
        }
        sb.append("]\n");
        return sb.toString();
    }

    /**
     * the minimum key value for its parent
     */
    @Override
    public int getMin() {
        return min;
    }
}
