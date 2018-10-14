package Project2.Tuple;

import java.io.*;
import java.util.ArrayList;

public class ReadableTupleReader {
    private File file;	// the file reading from.
    private BufferedReader br = null;	// the buffered reader for the file.

    public ReadableTupleReader(String fileName) throws FileNotFoundException {
        this.file = new File(fileName);
        br = new BufferedReader(new FileReader(this.file));
    }

    public Tuple read() throws IOException {
        ArrayList<Integer> result = new ArrayList<>();
        String line = br.readLine();
        if (line == null) return null;
        String[] elems = line.split(",");
        int len = elems.length;
        for (int i = 0; i < len; i++) {
            result.add(Integer.valueOf(elems[i]));
        }
        return new Tuple(result);
    }

    public void reset() throws IOException {
        if (br != null) {
            br.close();
        }
        br = new BufferedReader(new FileReader(file));
    }

    public String getLoc() {
        return file.getName();
    }
}
