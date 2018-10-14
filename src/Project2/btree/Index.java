package Project2.btree;

/**
 * Represents an index
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class Index {
   private boolean isClustered;
   private int order;
   private String table;
   private String column;
   private String fileName;

   /**
    * Creates an Index representation
    *
    * @param clustered 0 if the index is unclustered, 1 if clustered
    * @param treeOrder the tree order
    * @param col the column the index is on
    * @param file the file name of the index
    */
   public Index(int clustered, int treeOrder, String t, String col, String file) {
      isClustered = (clustered == 1);
      order = treeOrder;
      table = t;
      column = col;
      fileName = file;
   }

   /**
    * @return if the index is clustered
    */
   public boolean isClustered() {
      return isClustered;
   }

   /**
    * @return the tree order
    */
   public int getOrder() {
      return order;
   }

   /**
    * @return the column name
    */
   public String getColumn() {
      return column;
   }

   /**
    * @return the table name
    */
   public String getTable() {
      return table;
   }

   /**
    * @return the file name
    */
   public String getFileName() {
      return fileName;
   }
}
