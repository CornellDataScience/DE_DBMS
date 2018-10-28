package DBSystem;
import java.util.*;

public class Table {
	private String name;
	private List<Column> columns;
	
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
