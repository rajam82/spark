package com.spark.wordcount;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.spark.launcher.SparkLauncher;

   public class MyLauncher {
     public static void main(String[] args) throws Exception {
      Process handle = new SparkLauncher()
         .setAppResource("/home/rajam/wordcount/target/wordcount-0.0.1-SNAPSHOT.jar")
         .setMainClass("com.spark.wordcount.WordCount")
         .setMaster("spark://rajam-VirtualBox:7077")
         .setConf(SparkLauncher.DRIVER_MEMORY, "1g").launch();
      // Get Spark driver log
      
   
     
System.out.println(convertInputStreamToString(handle.getErrorStream())+"ssssssssssssssssssssssssssssssssssssssssFinished! Exit code is "+convertInputStreamToString(handle.getInputStream())  );
                 }
     private static String convertInputStreamToString(InputStream inputStream)
    			throws IOException {

    		        ByteArrayOutputStream result = new ByteArrayOutputStream();
    		        byte[] buffer = new byte[1024];
    		        int length;
    		        while ((length = inputStream.read(buffer)) != -1) {
    		            result.write(buffer, 0, length);
    		        }

    		        return result.toString(StandardCharsets.UTF_8.name());

    		    }
   }