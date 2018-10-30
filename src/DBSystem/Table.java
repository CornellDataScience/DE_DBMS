package DBSystem;
import java.util.*;

public class Table {
	private String name;
	private List<Tuple> tuples;
	private List<Column> columns;
	private HashMap<String, Integer> colIndicies = new HashMap<String, Integer>();
	private HashMap<Column, Integer> columnIndexed = new HashMap<Column, Integer>();

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
			columnIndexed.put(columns.get(i), i);
		}
		
	}
	
	/**
	 * Method to construct Tuple from name, list of tuples
	 * 
	 * @return the constructed Tuple
	 */
	public Table(String name, ArrayList<Tuple> tuples) {
		this.name = name;
		// TODO convert the tuples to columns
		// this.tuples = tuples;
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
