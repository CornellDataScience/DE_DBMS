package DBSystem;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javafx.util.Pair;
import java.lang.Object.*;

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
		double[] data_temp = new double[data.size()];
		for (int i = 0; i < data.size(); i++) {
			data_temp[i] = data.get(i);
		}
		int[] order = argSort(data_temp, true);
		// We assume everything to be double in the table
		return order;
	}

	/**
	 * Method to output a Column according to order given
	 * 
	 * @return a new column with the order in order array
	 */
	public void sortWithIndices(int[] order) {
		Double[] new_data = new Double[getSize()];
		for (int i = 0; i < order.length; i++) {
			new_data[i] = getData(order[i]);
		}
		data = new ArrayList<Double>(Arrays.asList(new_data));
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
		ColumnTab colClone = new ColumnTab(name + "clone", cloneData);
		return colClone;
	}
	
	public static int[] argSort(final double[] a, final boolean ascending) {
        final Integer[] indexes = new Integer[a.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
        }
        Arrays.sort(indexes, (i1, i2) -> (ascending ? 1 : -1) * Double.compare(a[i1], a[i2]));
        return Stream.of(indexes).mapToInt(i -> i).toArray();
    }
}
