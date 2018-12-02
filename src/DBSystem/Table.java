package DBSystem;
import java.util.*;

public class Table {
	private String name;
	private List<Column> columns;
	private HashMap<String, Integer> colIndicies = new HashMap<String, Integer>();

	/**
	 * Method to construct Tuple from name, list of tuples, list of columns
	 * 
	 * @return the constructed Tuple
	 */
	public Table(String name, List<Column> columns) {
		this.name = name;
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
	/** This function gets the rows at given indicies
	 * 
	 * @param indices indices of rows wanted
	 * @return a Table instance of the rows
	 */
	public void addColumn(Column c) {
		columns.add(c);
		colIndicies.put(c.getName(), colIndicies.size());
	}
	public Table getRows(List<Integer> indices) {
		Table retTable = new Table(getName(), new ArrayList<Column>());
		for(Column c: columns) {
			Column newCol = new Column(c.getName(), new ArrayList<Object>());
			for(Integer i: indices) {
				newCol.addData(c.getData(i));
			}	
			retTable.addColumn(newCol);
		}
		return retTable;
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
