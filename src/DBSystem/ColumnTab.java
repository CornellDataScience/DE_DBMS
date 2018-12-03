package DBSystem;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

public class ColumnTab {
	private String name;
	private ArrayList<Double> data;

	public ColumnTab(String name, ArrayList<Double> data) {
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
	public Double getData(int n) {
		return data.get(n);
	}

	/**
	 * Method to add data to the column
	 * 
	 */
	public void addData(Double o) {
		data.add(o);
	}

	/**
	 * Method to clear all the data in the column
	 * 
	 */
	public void resetColumn() {
		data = new ArrayList<Double>();
	}

	/**
	 * Method to output the list of indices in order to sort this Column
	 * 
	 * @return the column's name and data
	 */
	public int[] sortedIndices() {
		System.out.print(data.toArray());
		int[] order = argsort(data.toArray(), true);
		// We assume everything to be double in the table
		return order;
	}

	/**
	 * Method to output a Column according to order given
	 * 
	 * @return a new column with the order in order array
	 */
	public ColumnTab sortWithIndices(int[] order) {
		Double[] new_data = new Double[getSize()];
		for (int i = 0; i < order.length; i++) {
			// fill order[i] with data[i]
			new_data[order[i]] = getData(i);
		}
		ColumnTab newColumn = new ColumnTab("SortedColumn", new ArrayList<Double>(Arrays.asList(new_data)));
		return newColumn;
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

	/**
	 * Method to convert the Column class to String
	 * 
	 * @return the column's name and data
	 */
	public ColumnTab clone() {
		ArrayList<Double> cloneData = new ArrayList<Double>();
		for (Double d : data) {
			cloneData.add(d.doubleValue());
		}
		ColumnTab colClone = new ColumnTab(name, cloneData);
		return colClone;
	}
	
	/**
	 * Method to clone an object
	 * 
	 * @return the cloned object
	 */
	private static Object cloneObject(Object obj) {
		try {
			Object clone = obj.getClass().newInstance();
			for (Field field : obj.getClass().getDeclaredFields()) {
				field.setAccessible(true);
				if (field.get(obj) == null || Modifier.isFinal(field.getModifiers())) {
					continue;
				}
				if (field.getType().isPrimitive() || field.getType().equals(String.class)
						|| field.getType().getSuperclass().equals(Number.class)
						|| field.getType().equals(Boolean.class)) {
					field.set(clone, field.get(obj));
				} else {
					Object childObj = field.get(obj);
					if (childObj == obj) {
						field.set(clone, clone);
					} else {
						field.set(clone, cloneObject(field.get(obj)));
					}
				}
			}
			return clone;
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * Helper function for argsort in Java
	 * 
	 * @param a
	 * @param ascending
	 * @return
	 */
	public static int[] argsort(Object[] a, final boolean ascending) {
		Integer[] indexes = new Integer[a.length];
		for (int i = 0; i < indexes.length; i++) {
			indexes[i] = i;
		}
		Arrays.sort(indexes, new Comparator<Integer>() {
			@Override
			public int compare(final Integer i1, final Integer i2) {
				return (ascending ? 1 : -1) * Double.compare((Double) a[i1], (Double) a[i2]);
			}
		});
		int[] ret = new int[indexes.length];
		for (int i = 0; i < ret.length; i++)
			ret[i] = indexes[i];
		return ret;
	}
}
