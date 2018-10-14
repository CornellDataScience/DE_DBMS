package Project2.btree;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static Project2.DBCatalog.getPAGE_SIZE;

public class TreeSerializer {

    private FileOutputStream outputStream;  // output stream for reader
    private FileChannel fileChannel;        // file channel for reader
    private ByteBuffer buffer;              // file buffer
    private int pageNum;	                 // current page number
    private int numOfLeaves;                // the number of leaves

    private static final int BUFFER_SIZE = getPAGE_SIZE();	// the size of the buffer page
    private static final int LEAF_FLAG = 0; // the flag indicating this is leaf
    private static final int INDEX_FLAG = 1;  // the flag indicating this is index.
    private static final int HEADER_ID = 0; // the page id of header page.
    
    /**
     * The constructor of the serializer.
     * @param fileName the output index file
     * @throws FileNotFoundException
     */
    public TreeSerializer(String fileName) throws FileNotFoundException {
        outputStream = new FileOutputStream(fileName);
        fileChannel = outputStream.getChannel();
        buffer = ByteBuffer.allocate(BUFFER_SIZE);
        pageNum = 1;
        numOfLeaves = 0;
    }

    /**
     * Serialize the node into the index file
     *
     * @param node the node to be serialized
     * @return the address of the node to be serialized in the index file.
     * @throws IOException
     *
     */
    public int serialize(TreeNode node) throws IOException{
        long position = BUFFER_SIZE * (long) pageNum;

        // initialize the buffer
        fileChannel.position(position);
        clearBuffer();

        // serialize the Leaf Node
        if (node instanceof LeafNode) {
            numOfLeaves++;
            LeafNode curr = (LeafNode) node;
            int numOfEntries = curr.dataEntries.size();
            // start writing
            buffer.putInt(LEAF_FLAG);
            buffer.putInt(numOfEntries);
            for (DataEntry data : curr.dataEntries) {
                buffer.putInt(data.key);
                buffer.putInt(data.RidList.size());
                for (rid r : data.RidList) {
                    buffer.putInt(r.pageId);
                    buffer.putInt(r.tupleId);
                }
            }
        // serialize the Index Node
        } else if (node instanceof IndexNode) {
            IndexNode curr = (IndexNode) node;
            int numOfKeys = curr.keys.size();
            buffer.putInt(INDEX_FLAG);
            buffer.putInt(numOfKeys);
            for (Integer key : curr.keys) {
                buffer.putInt(key);
            }
            for (Integer address: curr.address) {
                buffer.putInt(address);
            }
        }

        // padding zeros at the end
        while (buffer.hasRemaining())
            buffer.putInt(0);

        buffer.flip();
        fileChannel.write(buffer);
        return pageNum++;
    }

    /**
     * set the current page as the root page,
     * and write the head page.
     *
     * @param order the order of the tree.
     * @throws IOException
     */
    public void finishSerialization(int order) throws IOException{
        long position = BUFFER_SIZE * (long) HEADER_ID;
        fileChannel.position(position);
        clearBuffer();

        buffer.putInt(pageNum - 1);	// The address of the root
        buffer.putInt(numOfLeaves);	// The number of leaves in the tree
        buffer.putInt(order);	    // The order of the tree

        // padding zeros at the end
        while(buffer.hasRemaining()){
            buffer.putInt(0);
        }

        buffer.flip();
        fileChannel.write(buffer);
    }

    /**
     * closes the file channel
     *
     * @throws IOException indicate any I/O error when calling close function
     */
    public void close() throws IOException {
        outputStream.close();
    }

    /**
     * Helper function to clear the buffer
     */
    private void clearBuffer() {
        buffer.clear();
        buffer.put(new byte[BUFFER_SIZE]);
        buffer.clear();
    }
}
