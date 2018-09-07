import org.apache.arrow.plasma.*;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.nio.ByteOrder;

public class JavaGetInt {
  public static void main(String[] args)throws Exception {
    System.load("<path_for_libplasma_java.so>");
    
    ObjectStoreLink pLink = new PlasmaClient("/tmp/plasma", "", 0);
    
    Path p = FileSystems.getDefault().getPath("", "<path_to_object_id_file>");
    byte[] objid={0};
    try {
      objid = Files.readAllBytes(p);
    } catch (java.io.IOException e) {
      System.out.println("error");
      System.out.println(e);
    }
    
    
    System.out.println(pLink.contains(objid));
    
    byte[] a = pLink.get(objid, 300, false);
    long startTime = System.nanoTime();
    
    ByteBuffer bb = ByteBuffer.wrap(a).order(ByteOrder.LITTLE_ENDIAN);
    LongBuffer lb = bb.asLongBuffer();
    int size = lb.capacity();
    lb.rewind();
    long data[] = new long[size];
    lb.get(data);
    
    long endTime = System.nanoTime() - startTime;
    double secs = endTime/1000000000.0;
    System.out.println("For size " + size + ": "+ secs);
  }
}

