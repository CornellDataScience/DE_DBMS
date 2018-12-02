package DBSystem;
import java.util.ArrayList;
import java.util.HashMap;

public class ColumnTab {
	private String name;
	private ArrayList<Object> data;

	public ColumnTab(String name, ArrayList<Object> data) {
		this.name = name;
		this.data = data;
	}
	
	/**
	 * Method to get the name of the Column
	 * 
	 * @return the name of the Column
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Method to get size of the column
	 * 
	 * @return the size of the Column
	 */
	public int getSize() {
		return data.size();
	}
	
	/**
	 * Method to get nth data in the column
	 * 
	 * @return the nth data in the column
	 */
	public Object getData(int n) {
		return data.get(n);
	}
	
	/**
	 * Method to add data to the column
	 * 
	 */
	public void addData(Object o) {
		data.add(o);
	}
	
	/**
	 * Method to clear all the data in the column
	 * 
	 */
	public void resetColumn() {
		data = new ArrayList<Object>();
	}
	
	/**
	 * Method to convert the Column class to String
	 * 
	 * @return the column's name and data
	 */
	public String toString() {
		String output = name + "\n" + data;
		return output;
	}
	
}
