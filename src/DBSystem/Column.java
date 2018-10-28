package DBSystem;
import java.util.ArrayList;
import java.util.HashMap;

public class Column {
	private String name;
	private ArrayList<Object> data;

	public Column(String name, ArrayList<Object> data) {
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
	 * Method to convert the Column class to String
	 * 
	 * @return the column's name and data
	 */
	public String toString() {
		String output = name + "\n" + data;
		return output;
	}
}
