package Project2;

import Project2.Tuple.Tuple;
import Project2.Tuple.TupleBinaryReader;
import Project2.btree.Index;

import java.io.*;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Reads in and catalogs all tables and corresponding columns in the database
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class DBCatalog {
   protected static volatile HashMap<String, AbstractMap.SimpleImmutableEntry<String, ArrayList<String>>> databases = null;
   protected static HashMap<String, Index> indices = null;
   protected static String tmp;
   protected static String input;
   protected static int bufferPages;
   protected static File stats;
   protected static HashMap<String, TableInfo> tableInfo;
   protected static BufferedWriter writer;
   private static final int PAGE_SIZE = 4096;

   /**
    * Parses all config files to build catalog of the database and set all variables. Should only
    * be called once
    *
    * @param inputDirectory the file path the directory can be found
    * @throws IOException if there is a problem reading the input directory
    */

   public static void initialize(String inputDirectory, String tmpDirectory) throws IOException {
      tableInfo = new HashMap<>();
      tmp = tmpDirectory;
      input = inputDirectory;
      stats = new File(input + File.separator + "db" + File.separator + "stats.txt");
      stats.createNewFile();
      writer = new BufferedWriter(new FileWriter(stats));

      bufferPages = 10;

      parseSchemaConfigFile();
      parseIndexConfigFile();

      writer.close();
   }

   private static void genStats(ArrayList<String> columns, String tName) {
      try {
         TupleBinaryReader tr = new TupleBinaryReader(databases.get(tName).getKey());
         TableInfo newTableInfo = new TableInfo(tName);
         ArrayList<String> colInfos = new ArrayList<>();
         for (String s : columns) {
            colInfos.add(s);
            newTableInfo.addNewCol(s);
         }

         while (true) {
            Tuple t = new Tuple(tr.read());
            try {
               int n = t.getSize();
            } catch (Exception e) {
               break;
            }
            newTableInfo.increment();
            for (int i = 0; i < t.getSize(); i++) {
               int newVal = t.getAllCols().get(i);
               String currCol = colInfos.get(i);
               if (newTableInfo.getMin(currCol) > newVal)
                  newTableInfo.setMin(currCol, newVal);
               else if (newTableInfo.getMax(currCol) < newVal)
                  newTableInfo.setMax(currCol, newVal);
            }

         }
         newTableInfo.computeInitialVValues();

         writer.write(newTableInfo.toString());
         tableInfo.put(tName, newTableInfo);

         tr.close();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   /**
    * Parses the index_info file to create a hashmap keyed to the column name with an Index
    * object value
    *
    * @throws IOException if there is an issue reading the config file
    */
   private static void parseIndexConfigFile() throws IOException {
      BufferedReader reader = new BufferedReader(new FileReader(input + File.separator +
            "db" + File.separator + "index_info.txt"));

      indices = new HashMap<>();
      String index;
      while ((index = reader.readLine()) != null) {
         String[] indexInfo = index.split("\\s+");
         String fullColName = indexInfo[0] + "." + indexInfo[1];
         indices.put(fullColName, new Index(Integer.parseInt(indexInfo[2]), Integer.parseInt
               (indexInfo[3]), indexInfo[0], indexInfo[1], input + File.separator + "db" +
               File.separator + "indexes" + File.separator + fullColName));
      }
      reader.close();
   }

   /**
    * Parses the schema file to create a hashmap keyed to the name of the table with pair values
    * of file path and a list of columns
    *
    * @throws IOException if there is an issue reading the config file
    */
   private static void parseSchemaConfigFile() throws IOException {
      BufferedReader fileReader = new BufferedReader(new FileReader(input +
            File.separator + "db" + File.separator + "schema.txt"));
      String db;

      databases = new HashMap<>();
      boolean first;
      while ((db = fileReader.readLine()) != null) {
         first = true;
         ArrayList<String> columns = new ArrayList<>();
         String name = "";
         for (String x : db.split("\\s+")) {
            if (!first) {
               columns.add(x);
            } else {
               first = false;
               name = x;
            }
         }
         databases.put(name, new AbstractMap.SimpleImmutableEntry<>(input + File
               .separator + "db" + File.separator + "data" + File.separator + name, columns));
         genStats(columns, name);
      }
      fileReader.close();
   }

   /**
    * @param dbName the database name, cannot be alias
    * @return the file path of the given database
    */
   public static String getFilename(String dbName) {
      return databases.get(dbName).getKey();
   }

   /**
    * Returns an arraylist of columns when given the database name
    *
    * @param dbName the database name, cannot be alias
    * @param alias the alias (can be null)
    * @return an arraylist of columns with the full name (alias.colName)
    */
   public static ArrayList<String> getColumns(String dbName, String alias) {
      if (alias == null) {
         alias = dbName;
      }
      ArrayList<String> fullColNames = new ArrayList<>();
      for (String name : databases.get(dbName).getValue()) {
         fullColNames.add(alias + "." + name);
      }
      return fullColNames;
   }

   /**
    * Returns an arraylist of columns when given the database name
    *
    * @param dbName the database name, cannot be alias
    * @return an arraylist of only column names
    */
   public static ArrayList<String> getColumns(String dbName) {
      return databases.get(dbName).getValue();
   }

   /**
    * @param tableName table name, must be full name
    * @param colName column name
    * @return the Index (or null if no Index) for the given full column name
    */
   public static Index getIndex(String tableName, String colName) {
      if (indices == null) return null;
      return indices.get(tableName + "." + colName);
   }

   /**
    * @return the indices hashmap
    */
   public static HashMap<String, Index> getIndices() {
      return indices;
   }

   /**
    * @return the number of buffer pages for sorting
    */
   public static int getBufferPages() {
      return bufferPages;
   }

   /**
    * @return the number of bytes to a page
    */
   public static int getPAGE_SIZE() {
      return PAGE_SIZE;
   }

   /**
    * @param colSize the number of columns per tuple
    * @return the number of tuples that fit on a page
    */
   public static int getPageSize(int colSize) {
      return (PAGE_SIZE / 4 - 2) / colSize;
   }

   /**
    * @return the name of the input directory
    */
   public static String getInput() {
      return input;
   }

   /**
    * @return the name of the temporary directory
    */
   public static String getTmp() {
      return tmp;
   }

   public static TableInfo getTableInfo(String t1) {
      return tableInfo.get(t1);
   }

   /**
    * Helper method to delete all files in a directory
    *
    * @param file the head of the directory to delete
    */
   public static void deleteDir(File file) {
      File[] contents = file.listFiles();
      if (contents != null) {
         for (File f : contents) {
            deleteDir(f);
         }
      }
      file.delete();
   }
}
