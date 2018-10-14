package Project2;

import Project2.Tuple.TupleWriter;
import Project2.btree.BTree;
import Project2.btree.Index;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

import static Project2.DBCatalog.*;

/**
 * Main class for executing an interpret on a queries file and database
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public class Interpreter {
   /**
    * Read in command line information and interpret commands
    *
    * @param args String array of command line directories
    */
   public static void main(String[] args) {
      long beginTime;
      try {
         // read Interpreter config file
         BufferedReader configReader = new BufferedReader(new FileReader(args[0]));

         // inside has queries.sql and db folder
         // inside db has schema.txt and data folder
         String inputDirectory = configReader.readLine();
         // output file named query#
         String outputDirectory = configReader.readLine();
         String tmpDirectory = configReader.readLine();

         initialize(inputDirectory, tmpDirectory);

         HashMap<String, Index> indices = getIndices();
         for (String x : indices.keySet()) {
            Index i = indices.get(x);
            String fileName = getInput() + File.separator + "db" + File.separator + "data" + File.separator + i.getTable();
            ArrayList<String> columns = getColumns(i.getTable());
            new BTree(fileName, columns, i.getFileName(), columns.indexOf(i.getColumn()), i.getOrder(), i.isClustered());
         }

         CCJSqlParser parser = new CCJSqlParser(new FileReader(inputDirectory + File.separator + "queries.sql"));

         Statement statement;
         int counter = 1;
         // loop through all statements
         while ((statement = parser.Statement()) != null) {
            beginTime = System.currentTimeMillis();
            // inner try catch prevents one erroneous query from halting the rest
            String tmpOutput;
            try {
               SQLQueryPlan tree = new SQLQueryPlan(statement, outputDirectory, counter);
               tree.getHead().dump(new TupleWriter(outputDirectory + File.separator + "query" + counter));
               counter++;
            } catch (Exception e) {
               System.err.println("Exception occurred during sql statement: " + counter);
               e.printStackTrace();
               counter++;
            }
            File file = new File(tmpDirectory);

            for (File child : file.listFiles()) {
               deleteDir(child);
            }
            System.out.println("Time Elapsed for Query " + (counter - 1) + ": " + (System.currentTimeMillis() - beginTime) + " msec");
         }
      } catch (Exception e) {
         System.err.println("Exception occurred during parsing input directory. Fatal error, quitting");
         e.printStackTrace();
      }
   }
}
