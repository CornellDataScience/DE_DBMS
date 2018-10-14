package Project2.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class ReadableTupleWriter {

    private File file;					// the file writing to.
    private BufferedWriter buff = null;	// the buffered writer for the file.

    public ReadableTupleWriter(String fileName) throws IOException {
        this.file = new File(fileName);
        buff = new BufferedWriter(new FileWriter(file));
    }


    public void write(ArrayList<Tuple> tuple) throws IOException {
        for (Tuple x : tuple) {
            for (int i = 0; i < x.getSize() - 1; i++) {
                buff.write(Integer.toString(x.getColValue(i)) + ",");
            }
            buff.write(Integer.toString(x.getColValue(x.getSize()-1)));
            buff.newLine();
        }
    }


    public void write(Tuple tuple) throws IOException {
        int i = 0;
        for (i = 0; i < tuple.getSize() - 1; i++) {
            buff.write(tuple.getColValue(i) + ",");
        }
        buff.write(Integer.toString(tuple.getColValue(i)));
        buff.newLine();
    }


    public void close() throws IOException {
        buff.close();
    }


}
