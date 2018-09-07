package osman.arrow;

import java.util.Arrays;
import org.apache.arrow.plasma.*;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Random;

import java.io.Writer;
import java.io.FileWriter;
import java.util.Locale;
import java.util.stream.Collectors;

public class JavaPutInt{
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
  
  public static int[] generateInput(int size){
    int seed = 0;
    Random rand = new Random(seed);
    int[] arr = new int[size];
    for (int i = 0; i < arr.length; i++){
      arr[i] = rand.nextInt(Integer.MAX_VALUE);
    }
    return arr;
  }
  
  public static void main(String[] args)throws Exception{
    int max_size = 131072;
    int[] ints = generateInput(max_size);
    String objid = generateObjid(20);
    long startTime = System.nanoTime();
    
    System.load("<path_for_libplasma_java.so>");
    
    ObjectStoreLink pLink = new PlasmaClient("/tmp/plasma", "", 0);
    byte[] obj_id = objid.getBytes();
    
    // Put integer array to intBuffer
    ByteBuffer byteBuffer = ByteBuffer.allocate(ints.length * 4); // integers size is 4. thats why we multiply by 4.  
    IntBuffer intBuffer = byteBuffer.asIntBuffer();
    intBuffer.put(ints);
    byte[] array = byteBuffer.array();
    
    pLink.put(obj_id,array,null);
    long endTime = System.nanoTime() - startTime;
    double secs = endTime/1000000000.0;
    System.out.println("For size " + max_size + ": "+ secs);
    
    Writer wr = new FileWriter("<path_to_object_id_file>");
    wr.write(objid);
    wr.close();
  }
}
