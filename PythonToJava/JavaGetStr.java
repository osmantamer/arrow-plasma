import org.apache.arrow.plasma.*;

import java.io.FileInputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.BufferedReader;
import java.io.InputStream;

public class JavaGetStr {
  public static void main(String[] args)throws Exception {
    System.load("<path_for_libplasma_java.so>");
    
    ObjectStoreLink pLink = new PlasmaClient("/tmp/plasma", "", 0);
    
    Path p = FileSystems.getDefault().getPath("", "<path_to_object_id_file>");
    Path p_lenmax = FileSystems.getDefault().getPath("", "<path_to_obj_id_len_max_file>");
    byte[] objid={0};
    byte[] objid_lenmax={0};
    try {
      objid = Files.readAllBytes(p);
      objid_lenmax = Files.readAllBytes(p_lenmax);
    } catch (java.io.IOException e) {
      System.out.println("error");
      System.out.println(e);
    }
    
    System.out.println(pLink.contains(objid)); // check if plasma contains object id
    
    long startTime = System.nanoTime();
    byte[] a = pLink.get(objid, 300, false);
    byte[] len_max = pLink.get(objid_lenmax, 300, false);
    int itemlength = ByteBuffer.wrap(len_max).order(ByteOrder.LITTLE_ENDIAN).getInt(); // max length of the items
    System.out.println(itemlength);
    InputStream is = new ByteArrayInputStream(a);
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    
    int size = (a.length/itemlength);
    String data[] = new String[size]; // Array for items
    char[] temp = new char[itemlength]; // temproray char array to put each string in it then convert it to string
    // this for loop to get each item r
    for (int i = 0; i<size;i++){
      br.read(temp,0,itemlength); 
      data[i] = String.valueOf( temp );
    }
    
    
    long endTime = System.nanoTime() - startTime;
    double s = endTime/1000000000.0;
    System.out.println("For size " + size + ": "+ s);
  }
}
