package DBSystem;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class dbCatalog {

	/**
	 * Top-level Catalog class which uses the schema file to parse the input
	 * file into databases of Tuples dbCatalog is a singleton type class, which
	 * can be accessed throughout the files and
	 * 
	 * @author Rong Tan
	 */

	private static ArrayList<Table> tables = new ArrayList<Table>();
	public static HashSet<String> names = new HashSet<String>();
	public static dbCatalog catalog;
	private static String Filename;
	private static HashMap<String, Table> alias = new HashMap<String, Table>();
	

	/**
	 * Method to return Tables with certain name
	 * 
	 * @return the Table with name tableName
	 */
	public static Table getTable(String tableName) {
		for (Table t : tables)
			if (t.getName().equals(tableName))
				return t;
		return null;
	}

	/**
	 * Method to construct dbCatalog singleton
	 */
	public dbCatalog(String d) {
		BufferedReader br = null;
		FileReader fr = null;
		FileReader tmp;
		BufferedReader brtmp;
		String data = d + "/db/data/";
		Filename = d + "/db/schema.txt";
		try {
			fr = new FileReader(Filename);
			br = new BufferedReader(fr);

			String sCurrentLine;
			String dataLine;

			while ((sCurrentLine = br.readLine()) != null) {
				String[] header = sCurrentLine.split("\\s+");
				String[] splitArray = sCurrentLine.split("\\s+");
				tmp = new FileReader(data + splitArray[0]);
				brtmp = new BufferedReader(tmp);
				ArrayList<Tuple> tuples = new ArrayList<Tuple>();
				while ((dataLine = brtmp.readLine()) != null) {
					List<Object> items = Arrays.asList(dataLine.split("\\s*,\\s*"));
					Tuple tuple = new Tuple(items);
					tuples.add(tuple);
				}
				ArrayList<Column> columns = new ArrayList<Column>();
				for (int i = 0; i < tuples.get(0).getSize(); i++) {
					ArrayList<Object> items = new ArrayList<Object>();
					for (int j = 0; j < tuples.size(); j++) {
						items.add(tuples.get(j).get(i));
					}
					Column column = new Column(splitArray[i + 1], items);
					columns.add(column);
				}
				Table table = new Table(header[0], tuples, columns);
				names.add(header[0]);
				tables.add(table);
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		} finally {

			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}
		}
	}

	/**
	 * Method to return the tables in this catalog
	 * 
	 * @return ArrayList of tables stored in the dbCatalog
	 */
	public static ArrayList<Table> getInstance() {
		return tables;
	}
	
	/**
	 * Method to set the Table "fromItem.0"'s alias to "fromItem.2"
	 * 
	 * @return ArrayList of tables stored in the dbCatalog
	 */
	public static String setAlias(Object fromItem) {
		String newAlias = fromItem.toString();
		String[] array = newAlias.split("\\s+");
		alias.put(array[2], getTable(array[0]));
		return array[0];
	}
	/**
	 * Method to find the table with alias of s in catalog 
	 * 
	 * @return Method to find the table in catalog of alias of s
	 */
	public static Table getAlias(String s) {
		return alias.get(s);
	}

}
