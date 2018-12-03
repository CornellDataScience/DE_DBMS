package DBSystem;
import java.util.*;

public class Table {
	private String name;
	private List<ColumnTab> columns;
	private HashMap<String, Integer> colIndicies = new HashMap<String, Integer>();

	/**
	 * Method to construct Tuple from name, list of tuples, list of columns
	 * 
	 * @return the constructed Tuple
	 */
	public Table(String name, List<ColumnTab> columns) {
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
		ArrayList<ArrayList<Double>> columnsDataGenerated = new ArrayList<ArrayList<Double>>();
		int tupleSize = tuples.get(0).getSize();
		
		for (Tuple t : tuples) {
			for (int i = 0; i < tupleSize; i++) {
				columnsDataGenerated.get(0).add(t.get(i));// ith column will be added the data t.get(i)
			}
		}

		ArrayList<ColumnTab> columnsGenerated = new ArrayList<ColumnTab>();
		for (int i = 0; i < tupleSize; i++) {
			// TODO column name ???
			columnsGenerated.add(new ColumnTab("COLUMN", columnsDataGenerated.get(0)));
		}
		this.columns = columnsGenerated;
	}
	/**
	 * 
	 * @param name of the column needed to be accessed 
	 * @return the column from the name of the column  
	 */
	public ColumnTab getCol(String name) {
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
	public void addColumn(ColumnTab c) {
		columns.add(c);
		colIndicies.put(c.getName(), colIndicies.size());
	}
	
	public Table getRows(List<Integer> indices) {
		Table retTable = new Table(getName(), new ArrayList<ColumnTab>());
		for(ColumnTab c: columns) {
			ColumnTab newCol = new ColumnTab(c.getName(), new ArrayList<Double>());
			for(Integer i: indices) {
				newCol.addData(c.getData(i));
			}	
			retTable.addColumn(newCol);
		}
		return retTable;
	}
	
	public Table getRows(Set<Integer> indices) {
		Table retTable = new Table(getName(), new ArrayList<ColumnTab>());
		for(ColumnTab c: columns) {
			ColumnTab newCol = new ColumnTab(c.getName(), new ArrayList<Double>());
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
	public List<ColumnTab> getColumns() {
		return columns;
	}
	
	/**
	 * Method to get the number of tuples in the table
	 * 
	 * @return the number of tuples in the table
	 */
	public int getTupleNum() {
		return columns.get(0).getSize();
	}
	
	/**
	 * Method to convert the Table to String
	 * 
	 * @return the string representation of the Table
	 */
	public String toString() {
		int size = getTupleNum();
		for (int i = 0; i < size; i++) {
			for (int j = 0; j < columns.size(); j++) {
				ColumnTab c = columns.get(j);
				System.out.print(c.getData(i));
				if (j < columns.size() - 1) System.out.print(",");
			}
			System.out.println();
		}
		return "";
	}
}
