package Project2.Operators.Physical;

import Project2.Tuple.Tuple;
import Project2.Tuple.TupleWriter;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Top-level abstract class for a physical operator node
 *
 * @author Abby Beeler arb379, Akhil Kumar Gopu akg68, Rong Tan rt389
 */
public abstract class PhysicalOperator {

	/**
	 * @return next tuple from operator's output, null if there are no remaining tuples
	 */
	public abstract Tuple getNextTuple() throws IOException;

	/**
	 * @return the list of columns
	 */
	public abstract ArrayList<String> getColumns();

	/**
	 * Repeatedly calls getNextTuple() to read all output and writes results to file
	 *
	 * @param writer the TupleWriter for the output file
	 * @throws IOException if there is an issue reading the database file or writing to the output
	 * 						  file
	 */
	public void dump(TupleWriter writer) throws IOException {
		Tuple tuple = getNextTuple();
		int colNum = 0;
		if (tuple != null) {
			writer.write(tuple.getAllCols());
			colNum = tuple.getColNum();

			while ((tuple = getNextTuple()) != null) {
				writer.write(tuple.getAllCols());
			}
		}
		writer.end(colNum);
	}

	/**
	 * Resets file reader and thus operator to the beginning
	 */
	public abstract void reset() throws IOException;
}
