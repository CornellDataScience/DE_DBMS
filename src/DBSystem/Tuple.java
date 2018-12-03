package DBSystem;
import java.util.*;

public class Tuple {
	/**
	 * The Tuple Class, which is the basic component of the database
	 * 
	 */

	private List<Double> data = new ArrayList<Double>(); // Contains all the
															// data in the tuple
	/**
	 * Method to construct Tuple from items
	 * 
	 * @return the constructed Tuple
	 */
	public Tuple(List<Double> items) {
		data = items;
	}
	
	/**
	 * Method to clone the tuple
	 * 
	 * @return the cloned tupled
	 */
	public Tuple clone() {
		try {
			return (Tuple) super.clone();
		} catch (CloneNotSupportedException e) {
			// This should never happen
			throw new java.lang.Error("this should never happen");
		}
	}
	
	/**
	 * Method to this tuple
	 * 
	 * @return the tuple itself
	 */
	public List<Double> getTuple() {
		return data;
	}
	
	/**
	 * Method to get the size of the tuple
	 * 
	 * @return the size of the tuple
	 */
	public int getSize() {
		return data.size();
	}

	/**
	 * Method to get the ith position in the Tuple
	 * 
	 * @return the ith position in the Tuple
	 */
	public Double get(int i) {
		return data.get(i);
	}

	/**
	 * Method to add an Object to the Tuple
	 * 
	 */
	public void add(Double o) {
		data.add(o);
	}
	
	/**
	 * Method to convert the Tuple to String
	 * 
	 * @return the string representation of the Tuple
	 */
	public String toString() {
		String answer = "";
		for (Object o : data) {
			answer += o.toString();
			answer += ",";
		}
		return answer.substring(0, answer.length() - 1);
	}
}
