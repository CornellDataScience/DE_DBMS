package operator;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import DBSystem.Table;
import DBSystem.Column;
import DBSystem.Tuple;

public abstract class Operator {
	/**
	 * Top-level abstract class for the Operation
	 * 
	 * @author Rong Tan
	 */

	private static int n = 0; // The indication of the number of query file it
								// is outputting to

	/**
	 * Method to get the next Tuple of the Operator
	 * 
	 * @return the next Tuple of the Operator
	 */
	public abstract Table operate();

	/**
	 * Method to tell the operator to reset its state and start returning its
	 * output again from the beginning
	 * 
	 */
	public abstract void reset();

	/**
	 * Method to repeatedly calls getNextTuple() until the next tuple is null
	 * (no more output) and writes each tuple to a suitable PrintStream
	 * 
	 * @throws FileNotFoundException
	 * 
	 */
	public void dump() throws FileNotFoundException {
		n++;
		Tuple t;
		PrintStream out = new PrintStream(new FileOutputStream("query" + n + ".txt"));
		/*
		while ((t = getNextTuple()) != null) {
			System.out.println(t);
		}
		out.close();
		*/
	}

}
