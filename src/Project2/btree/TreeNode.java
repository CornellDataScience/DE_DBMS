package Project2.btree;

public abstract class TreeNode {

    int order;

    public TreeNode(int order){
        this.order = order;
    }

    abstract public int getMin();
}
