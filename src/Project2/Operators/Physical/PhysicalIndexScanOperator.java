package Project2.Operators.Physical;

import Project2.Tuple.Tuple;
import Project2.Tuple.TupleWriter;
import Project2.btree.TreeDeserializer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static Project2.DBCatalog.getTmp;

public class PhysicalIndexScanOperator extends PhysicalOperator {

    private PhysicalScanOperator childOp;
    private PhysicalSortOperator sortOp;

    private Integer lowKey, highKey;
    private int attributeIndex;	    // The index of the attribute column.
    private File indexFile;         // the index file for deserializer to locate
    private boolean isClustered = false;
    private TreeDeserializer td;    // tree serializer used for get the data entry
    private String tableName, columnName;

    public PhysicalIndexScanOperator(PhysicalScanOperator op, ArrayList<String> columns, int attributeIndex, Integer lowKey,
                                     Integer highKey, boolean isClustered, String indexFileName) {
        if (isClustered) {
            ArrayList<String> sortCol = new ArrayList<>();
            sortCol.add(columns.get(attributeIndex));
            columnName = columns.get(attributeIndex);
            columnName = columnName.split("\\.").length == 1 ? columnName : columnName.split("\\.")[1];
            tableName = op.getTableName();
            sortOp = new PhysicalExternalSortOperator(op, columns, sortCol);
        } else {
            childOp = op;
            sortOp = null;
        }

        this.attributeIndex = attributeIndex;
        this.lowKey = lowKey;
        this.highKey = highKey;
        this.isClustered = isClustered;
        this.indexFile = new File(indexFileName);
        td = null;
    }

    @Override
    public Tuple getNextTuple() throws IOException {
        // call deserializer to fetch the first rid from the leafnode
        if (td == null) {
            if (sortOp != null) {
                String fileName = getTmp() + File.separator + "Sorted" + (int) Math.ceil(Math.random() * 100000);
                TupleWriter writer = new TupleWriter(fileName);

                sortOp.dump(writer);
                sortOp = null;
                childOp = new PhysicalScanOperator(fileName);
            }

            td = new TreeDeserializer(indexFile, lowKey, highKey);

            return childOp.getNextTuple(td.getNextRid());
        }

        if (isClustered) {
            Tuple curr = childOp.getNextTuple();
            if (curr != null && (highKey == null || curr.getColValue(attributeIndex) < highKey)) {
                return curr;
            }
            td.close();
            return null;
        } else {
            return childOp.getNextTuple(td.getNextRid());
        }
    }

    @Override
    public ArrayList<String> getColumns() {
        return childOp.getColumns();
    }

    @Override
    public void reset() throws IOException {
        childOp.reset();
        td = null;
    }

    @Override
    public String toString() {
        String low = lowKey == null ? "null": lowKey.toString();
        String high = highKey == null ? "null": highKey.toString();
        return "IndexScan[" + tableName + "," + columnName + "," + low + "," + high + "]";
    }
}
