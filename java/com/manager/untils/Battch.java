package com.manager.untils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class Battch {
    //写文件，支持中文字符，在linux redhad下测试过
    public static void writeLog()
    {
        try
        {
        String path="D:\\test\\kmeans_data.txt";
        File file=new File(path);
        if(!file.exists())
            file.createNewFile();
        FileOutputStream out=new FileOutputStream(file,false);
       for(int j=1;j<=50000;j++)
        {
    	   
     
        	StringBuffer s=new StringBuffer();
        	 String sb=new String();
        	int ID=j;
        	int Age=(int) ((Math.random())*70+1);
        	int Gender= Math.random()>0.5?1:0;
        	int consumption=(int)(Math.random()*1000+1);
        	//long timestamp=System.currentTimeMillis();
        	sb=ID+" "+Age+" "+Gender+" "+consumption;
        	s.append(sb+"\n");
        	out.write(s.toString().getBytes());
        
        
      
        }
       out.close();
        }
        catch(IOException ex)
        {
            System.out.println(ex.getStackTrace());
        }
    }    
    public static String readLog()
    {
        StringBuffer sb=new StringBuffer();
        String tempstr=null;
        try
        {
            String path="/root/test/testfilelog.log";
            File file=new File(path);
            if(!file.exists())
                throw new FileNotFoundException();            
//            BufferedReader br=new BufferedReader(new FileReader(file));            
//            while((tempstr=br.readLine())!=null)
//                sb.append(tempstr);    
            //另一种读取方式
            FileInputStream fis=new FileInputStream(file);
            BufferedReader br=new BufferedReader(new InputStreamReader(fis));
            while((tempstr=br.readLine())!=null)
                sb.append(tempstr);
        }
        catch(IOException ex)
        {
            System.out.println(ex.getStackTrace());
        }
        return sb.toString();
    }
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        writeLog();
        
       
    }

}
