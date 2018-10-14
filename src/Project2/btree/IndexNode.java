package Project2.btree;

import java.util.ArrayList;
import java.util.List;

public class IndexNode extends TreeNode{
    List<TreeNode> children;    // the children nodes
    List<Integer> keys;         // the number of keys in the node
    List<Integer> address;      // the addresses of the children
    int min; //the minimum key value for its parent

    public IndexNode(int order, List<Integer> keys,
                     List<TreeNode> children, List<Integer> address) {

        super(order);
        if (children == null) {
            this.children = null;
            min = 0;
        } else {
            this.children = new ArrayList<>(children);
            min = children.get(0).getMin();
        }
        this.keys = new ArrayList<>(keys);
        this.address = new ArrayList<>(address);
    }

    /**
     * get the the minimum key value for its parent
     *
     */
    public int getMin() {
        return min;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Index Node with keys [");
        for (Integer key : keys) {
            sb.append(key).append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("] and child addresses [");
        for (Integer addr : address) {
            sb.append(addr).append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("]\n");
        return sb.toString();
    }
}
