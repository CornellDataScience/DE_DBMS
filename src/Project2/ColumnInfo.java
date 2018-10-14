package Project2;

public class ColumnInfo {
    private int min;
    private int max;
    private int VValue;
    private String tName;

    public ColumnInfo(int min, int max, String tName) {
        this.min = min;
        this.max = max;
        this.tName = tName;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public void setMax (int max){
        this.max = max;
    }

    public void setVValue(int VValue) {
        this.VValue = VValue;
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public int getVValue() {
        return  VValue;
    }

    public String gettName() { return tName; }
}
