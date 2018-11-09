package DBSystem;
import java.util.*;

public class Table {
	private String name;
	private List<Tuple> tuples;
	private List<Column> columns;
	private HashMap<String, Integer> colIndicies = new HashMap<String, Integer>();

	/**
	 * Method to construct Tuple from name, list of tuples, list of columns
	 * 
	 * @return the constructed Tuple
	 */
	public Table(String name, List<Tuple> tuples, List<Column> columns) {
		this.name = name;
		this.tuples = tuples;
		this.columns = columns;
		for (int i = 0; i < columns.size(); i++) {
			colIndicies.put(columns.get(i).getName(), i);
		}
		
	}
	
	/**
	 * Method to construct Tuple from name, list of tuples
	 * 
	 * @return the constructed Tuple
	 */
	public Table(String name, ArrayList<Tuple> tuples) {
		this.name = name;
		ArrayList<ArrayList<Object>> columnsDataGenerated = new ArrayList<ArrayList<Object>>();
		int tupleSize = tuples.get(0).getSize();
		
		for (Tuple t : tuples) {
			for (int i = 0; i < tupleSize; i++) {
				columnsDataGenerated.get(0).add(t.get(i));// ith column will be added the data t.get(i)
			}
		}

		ArrayList<Column> columnsGenerated = new ArrayList<Column>();
		for (int i = 0; i < tupleSize; i++) {
			// TODO column name ???
			columnsGenerated.add(new Column("COLUMN", columnsDataGenerated.get(0)));
		}
		this.columns = columnsGenerated;
	}
	/**
	 * 
	 * @param name of the column needed to be accessed 
	 * @return the column from the name of the column  
	 */
	public Column getCol(String name) {
		return columns.get(colIndicies.get(name));
	}
	
	
	/**
	 * The function to get the name of the table
	 * 
	 * @return name of the table
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Method to get the list of columns in the table
	 * 
	 * @return the list of columns in the table
	 */
	public List<Column> getColumns() {
		return columns;
	}
}
