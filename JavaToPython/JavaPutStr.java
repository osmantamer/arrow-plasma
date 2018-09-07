package osman.arrow;
import java.util.Arrays;
import org.apache.arrow.plasma.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.util.Random;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Writer;
import java.io.FileWriter;
import java.util.Locale;
import java.util.stream.Collectors;

public class JavaPutStr {
  public static String generateObjid(int len){
    String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    String lower = upper.toLowerCase(Locale.ROOT);
    String digits = "0123456789";
    String alphanum = upper + lower + digits;
    String str = new Random().ints(len, 0, alphanum.length())
      .mapToObj(i -> "" + alphanum.charAt(i))
      .collect(Collectors.joining());
    return str;
  }
  
  public static String[] generateNames(int pw)throws Exception{
    BufferedReader br = new BufferedReader(new FileReader("<path_to_file_to_generate_input>"));
    StringBuilder sb = new StringBuilder();
    int max_size = (int)Math.pow(2,pw);
    
    String[] arr = new String[max_size];
    int count = 0;
    while (count < max_size) {
      arr[count] = br.readLine();
      count++;
    }
    return arr;
  }
  
  public static void main(String[] args)throws Exception{
    int max_size = 17;
    System.load("<path_for_libplasma_java.so>");
    String[] strs = generateNames(max_size);
    String objid = generateObjid(20);
    long startTime = System.nanoTime();
    
    ObjectStoreLink pLink = new PlasmaClient("/tmp/plasma", "", 0);
    byte[] obj_id_data = objid.getBytes();
    
    // Put array to plasma with bytebuffer
    ByteBuffer bb = ByteBuffer.allocate(strs.length*8); // Strings size is 8. thats why we multiply by 8.
    
    for (String item:strs){
      bb.put((item+"\n").getBytes()); // after each item we also put "\n" to split it when we reach the data
    }
    
    pLink.put(obj_id_data,bb.array(),null);
    
    long endTime = System.nanoTime() - startTime;
    double secs = endTime/1000000000.0;
    
    System.out.println("For size " + max_size + ": "+ secs);
    Writer wr = new FileWriter("<path_to_object_id_file>");
    wr.write(objid);
    wr.close();
    
  }
}
