package Project2.btree;

import Project2.Operators.Physical.PhysicalExternalSortOperator;
import Project2.Operators.Physical.PhysicalOperator;
import Project2.Operators.Physical.PhysicalScanOperator;
import Project2.Tuple.Tuple;
import Project2.Tuple.TupleBinaryReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static Project2.DBCatalog.getPageSize;

public class BTree {

    TupleBinaryReader tr;
    int order;
    int capacity;// the total entries in each node
    int position;
    List<DataEntry> dataEntries; // dataentries for creating leaf nodes
    List<TreeNode> leafLayer; // leaflayer that stores all the leaf nodes
    TreeNode root;
    TreeSerializer ts;

    /**
     * constructor
     * @param fileName: the input file used for construct the tree
     * @param indexName: the index file used for construct the tree
     * @param position: the column index for the index
     * @param order: the half capacity of entries in each node,
     * say d is the order, then capacity of each node is 2d
     *
     * @throws IOException
     */
    public BTree(String fileName, ArrayList<String> columns, String indexName, int position, int order, boolean clustered) throws IOException {
        PhysicalOperator operator;
        if (clustered) {
            ArrayList<String> sortCol = new ArrayList<>();
            sortCol.add(columns.get(position));
            operator = new PhysicalExternalSortOperator(new PhysicalScanOperator
                          (fileName), columns, sortCol);
        } else {
            operator = new PhysicalScanOperator(fileName);
        }

        HashMap<Integer, ArrayList<rid>> map = new HashMap<>();
        Tuple tuple = operator.getNextTuple();

        int pageSize = getPageSize(tuple.getColNum());
        int currTuple = 0, currPage = 0;

        while (tuple != null) {
            if (currTuple == pageSize) {
                currPage++;
                currTuple = 0;
            }

            int key = tuple.getColValue(position);
            ArrayList<rid> currRid = map.computeIfAbsent(key, k -> new ArrayList<>());
            currRid.add(new rid(currPage, currTuple));

            currTuple ++;
            tuple = operator.getNextTuple();
        }

        dataEntries = new ArrayList<>();
        for (int x : map.keySet()) {
            dataEntries.add(new DataEntry(x, map.get(x)));
        }
        Collections.sort(dataEntries);

        leafLayer = new ArrayList<>();
        this.position = position;
        this.order = order;
        this.capacity = 2 * order;

        this.ts = new TreeSerializer(indexName);

        createLeafLayer();

        List<TreeNode> layer = new ArrayList<>(leafLayer);
        while(layer.size()!=1){
            layer = createIndexLayer(layer, ts);
        }
        root = layer.get(0);
        ts.serialize(root);
        ts.finishSerialization(order);
        ts.close();
    }

    /**
     * Generate the index layer according to the previous layer
     * @throws IOException
     */
    public List<TreeNode> createIndexLayer(List<TreeNode> preLayer, TreeSerializer ts) throws IOException{
        List<TreeNode> newLayer = new ArrayList<>();
        int cnt = 0;
        List<Integer> keys = new ArrayList<>();
        List<TreeNode> children = new ArrayList<>();
        List<Integer> address = new ArrayList<>(); // list of address
        if(preLayer.size() <= capacity){ // only one node to construct
            for (int i = 0; i < preLayer.size(); i++){
                int ads = ts.serialize(preLayer.get(i));
                address.add(ads);
            }
            children.addAll(preLayer);
            for (int i = 1; i < preLayer.size(); i++){
                keys.add(preLayer.get(i).getMin());
            }
            newLayer.add(new IndexNode (order, keys,children , address));

        } else {

            for(int i = 0; i < preLayer.size(); i++){
                if (cnt == capacity){
                    children.add(preLayer.get(i));
                    int ads = ts.serialize(preLayer.get(i));
                    address.add(ads);
                    //add last key
                    keys.add(preLayer.get(i).getMin());
                    //create a index node
                    IndexNode node = new IndexNode(order,keys,children,address);
                    newLayer.add(node);
                    cnt = 0;
                    keys.clear();
                    children.clear();
                    address.clear();

                    int remainNum = preLayer.size() - i - 1;
                    if(remainNum > (2 * order + 1) && remainNum < (3 * order + 2)) {
                        int secLastNodeNum = remainNum/2;

                        for (int j = i + 1; j < i + 1 + secLastNodeNum; j++){
                            ads = ts.serialize(preLayer.get(j));
                            address.add(ads);
                        }
                        children.addAll(preLayer.subList(i+1, i + 1 + secLastNodeNum));
                        for (int j = i + 2; j < i+1+secLastNodeNum;j++){
                            keys.add(preLayer.get(j).getMin());
                        }
                        newLayer.add(new IndexNode (order,keys,children,address));
                        keys.clear();
                        children.clear();
                        address.clear();
                        //  fill LastNode
                        for(int j = i + 1 + secLastNodeNum; j < preLayer.size(); j++) {
                            ads = ts.serialize(preLayer.get(j));
                            address.add(ads);
                        }
                        children.addAll(preLayer.subList(i+1+secLastNodeNum, preLayer.size()));
                        for (int j = i+2+secLastNodeNum; j < preLayer.size();j++){
                            keys.add(preLayer.get(j).getMin());
                        }
                        newLayer.add(new IndexNode ( order, keys,children,address));
                        keys.clear();
                        children.clear();
                        address.clear();
                        break;
                    }
                    continue;
                }

                if(cnt == 0){
                    children.add(preLayer.get(i));
                    int ads = ts.serialize(preLayer.get(i));
                    address.add(ads);
                    cnt++;
                } else if (cnt < capacity ){
                    //add key
                    keys.add(preLayer.get(i).getMin());
                    children.add(preLayer.get(i));
                    int ads = ts.serialize(preLayer.get(i));
                    address.add(ads);
                    cnt++;
                }

            }
            // check if there is a node left
            if(keys.size()!=0) {
                newLayer.add(new IndexNode(order, keys,children,address));
            }
        }

        return newLayer;
    }

    /**
     * Generate the leaf layer for the tree
     */
    public void createLeafLayer(){
        if (dataEntries == null) {
            throw new NullPointerException();
        }

        int i;
        for (i = 0; i < dataEntries.size() - 2 * capacity; i += capacity){
            leafLayer.add(new LeafNode(order, dataEntries.subList(i, i + capacity)));
        }

        if (dataEntries.size() - i < capacity * 1.5) {
            // test
            leafLayer.add(new LeafNode(order, dataEntries.subList(i, i + (dataEntries.size() - i) / 2)));
            i += (dataEntries.size() - i) / 2;
            leafLayer.add(new LeafNode(order, dataEntries.subList(i, dataEntries.size())));
        } else {
            leafLayer.add(new LeafNode(order, dataEntries.subList(i, i + capacity)));
            i += capacity;
            leafLayer.add(new LeafNode(order, dataEntries.subList(i, dataEntries.size())));
        }
    }
}
