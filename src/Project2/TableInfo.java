package Project2;

import java.util.HashMap;

public class TableInfo {
    int numTuples;
    private String tableName;
    HashMap<String, ColumnInfo> columnInfo;
    //Column, <Minimum, Maximum>



    public TableInfo(String name) {
        tableName = name;
        columnInfo = new HashMap<>();
        numTuples = 0;
    }

    public ColumnInfo getColumn(String s) {
        return columnInfo.get(s);
    }

    public Integer getMin(String s) {
        return columnInfo.get(s).getMin();
    }

    public Integer getMax(String s) {
        return columnInfo.get(s).getMax();
    }

    public Integer getVValue (String s) { return columnInfo.get(s).getVValue(); }

    public void setMin (String s, Integer i) {
        columnInfo.get(s).setMin(i);
    }

    public void setMax (String s, Integer i) {
        columnInfo.get(s).setMax(i);
    }

    public void increment() {
        numTuples++;
    }

    public void addNewCol(String s) {
        columnInfo.put(s, new ColumnInfo(Integer.MAX_VALUE, Integer.MIN_VALUE, tableName));
    }

    public void computeInitialVValues() {
        for (ColumnInfo ci : columnInfo.values()) {
            ci.setVValue(ci.getMax() - ci.getMin() + 1);
        }
    }

    public int getSize() {
        return numTuples;
    }
    public String toString() {
        String result = "";
        result += tableName + " " + numTuples + " ";
        for (String key : columnInfo.keySet())
        {
            result += key + "," + columnInfo.get(key).getMin() + "," + columnInfo.get(key).getMax() + " ";
        }
        result += "\n";
        return result;
    }
}
