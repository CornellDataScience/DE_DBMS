package Project2.Tuple;

import Project2.btree.rid;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import static Project2.DBCatalog.getPAGE_SIZE;

public class TupleBinaryReader {

    private String fileName;
    private FileInputStream inputStream;
    private FileChannel fileChannel;
    private ByteBuffer byteBuffer;
    private boolean fileEnds;           // indicating whether the file ends
    private boolean pageEnds;           // indicating whether the page ends
    private int numOfAtr;               // the number of attributes in a Tuple
    private int numOfTp;                // the number of Tuples in a page
    private int index;
    private int pageSize;
    private int intSize = 4;

    /**
     * Creates a new TupleBinaryReader with an input file with the given file name
     *
     * @param fileName the file name to read input from
     * @throws IOException if there is an issue reading from the input file
     */
    public TupleBinaryReader(String fileName) throws IOException {
        // initialize fileChannel
        this.fileName = fileName;
        inputStream = new FileInputStream(fileName);
        fileChannel = inputStream.getChannel();

        pageSize = getPAGE_SIZE();
        byteBuffer = ByteBuffer.allocate(pageSize);
        numOfTp = byteBuffer.getInt(4);

        fileEnds = false;
        pageEnds = true;
        index = 0;
    }

    /**
     * return the next Tuple in file in form of an ArrayList
     *
     * @return the next Tuple in file in form of an ArrayList
     * @throws IOException if there is an issue reading from the input file
     */
    public ArrayList<Integer> read() throws IOException{
        while (!fileEnds) {
            // get page if the current page ends
            if (pageEnds) {
                getPage();
                // return null if the file ends
                if (fileEnds) return null;
            }

            while (byteBuffer.hasRemaining()) {

                ArrayList<Integer> list = new ArrayList<>();

                // create a new Tuple by reading certain amount of attributes
                for (int i = 0; i < numOfAtr; i++) {
                    list.add(byteBuffer.getInt());
                }
                index += 1;
                return list;

            }

            clearBuffer();
            pageEnds = true;
        }
        return null;
    }

    /**
     * Reset the buffer and prepare to read tuples from the next page
     *
     * @throws IOException if there is an issue reading from the input file
     */
    public void reset() throws IOException {
        fileChannel.close();
        inputStream = new FileInputStream(fileName);
        fileChannel = inputStream.getChannel();
        byteBuffer = ByteBuffer.allocate(pageSize);
        index = 0;
        fileEnds = false;
        pageEnds = true;
    }

    /**
     * Reset the buffer and prepare to read tuples from index. If index is past the end of the
     * file, read() will return null
     *
     * @throws IOException if there is an issue reading from the input file
     */
    public void reset(int index) throws IOException, IndexOutOfBoundsException {
        if (index < 0) {
            throw new IndexOutOfBoundsException("The index is negative");
        }

        // if we did not get any pages, initialize numOfAtr, numOfTp
        if (numOfAtr == 0){
            getPage();
        }
        int pageNum = index / ((pageSize / intSize - 2) / numOfAtr);
        fileChannel.position(pageSize * pageNum);

        fileEnds = false;
        pageEnds = true;

        clearBuffer();
        getPage();
        this.index = index % ((pageSize / intSize - 2) / numOfAtr);
        byteBuffer.position(8 + this.index * numOfAtr * 4);
    }
    
    /**
     * Helper function to reset the buffer and
     * prepare to read next page from the file
     */
    private void getPage() throws IOException {

        fileEnds = (fileChannel.read(byteBuffer) < 0);
        pageEnds = false;
        if (fileEnds) return;

        // byteBuffer starts at "write mode"
        byteBuffer.flip();
        numOfAtr = byteBuffer.getInt(0);
        numOfTp = byteBuffer.getInt(4);

        int newLimit = (numOfAtr * numOfTp + 2) * intSize;

        byteBuffer.limit(newLimit);
        byteBuffer.position(8);
        index = 0;
    }

    /**
     * Helper function to clear the buffer
     */
    private void clearBuffer() {
        byteBuffer.clear();
        byteBuffer.put(new byte[pageSize]);
        byteBuffer.clear();
    }

    /**
     * return the next Tuple in file with rid r in form of an ArrayList
     *
     * @return the next Tuple in file in form of an ArrayList
     * @throws IOException if there is an issue reading from the input file
     */
    public ArrayList<Integer> read(rid r) throws IOException{
        // check r precondition
        if (r == null) return null;
        int pageId = r.getPageId();
        int tupleId = r.getTupleid();
        // check pageId, tupleId precondition
        if (pageId < 0 || tupleId < 0)
            throw new IndexOutOfBoundsException("Index out of bound");
        clearBuffer();
        pageEnds = true;
        fileEnds = false;
        // fetch the page
        long position = pageSize * (long) pageId;
        fileChannel.position(position);
        getPage();
        int newPos = (tupleId * numOfAtr + 2) * intSize;
        byteBuffer.position(newPos);
        return read();
    }

    public void close() throws IOException {
        inputStream.close();
    }

    public String getFileName() {
        String[] name = fileName.split(File.separator);
        return name[name.length - 1];
    }
}
