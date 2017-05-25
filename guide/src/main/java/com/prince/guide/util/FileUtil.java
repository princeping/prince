package com.prince.guide.util;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 文件操作工具类
 * Created by zhangxin on 2016/9/7.
 */
public class FileUtil {

    /**
     * 获取某个文件夹下的文件数量，仅包含该文件夹，不包含子文件夹
     * @param path 文件的完整绝对路径
     * @return 文件夹中的文件数量
     */
    public static int getFileNumber(String path) {
        if(!isFolder(path))
            return 0;
        File file = new File(path);
        String[] arrayList = file.list();
        if(arrayList==null)
            return 0;
        return arrayList.length;
    }

    /**
     * 删除一个文件夹
     * @param path 文件夹的完整绝对路径
     * @return 删除成功返回true，否则返回false
     */
    public static boolean delFolder(String path) {
        if(!isFolder(path))
            return false;
        return new File(path).delete();
    }

    /**
     * 删除一个文件
     * @param path 文件的完整绝对路径
     * @return 删除成功返回true，否则返回false
     */
    public static boolean delFile(String path) {
        if(!isFile(path))
            return false;
        return new File(path).delete();
    }

    /**
     * 删除文件夹下所有文件和子文件夹，但文件夹本身不会被删除
     * @param path 文件夹的完整绝对路径
     * @return 完全清空返回true，否则返回false
     */
    public static boolean delAllFile(String path) {
        if(!isFolder(path))
            return false;
        int count = getFileNumber(path);
        if(count<1)
            return true;
        File[] files = new File(path).listFiles();
        for(File tempfile : files) {
            if(tempfile.isFile())
                delFile(getFullPath(path,tempfile.getName()));
            if(tempfile.isDirectory()) {
                delAllFile(getFullPath(path,tempfile.getName()));
                tempfile.delete();
            }
        }
        return true;
    }

    /**
     * 获取文件大小，单位根据传入的参数决定
     * @param path 文件的完整绝对路径
     * @param units 单位：KB,MB,GB
     * @return 文件的尺寸
     */
    public static double getFileSize(String path, String units) {
        if(!isFile(path))
            return 0;
        double size = new File(path).length();

        if(units==null||"".equals(units))
            return size;
        else if("kb".equals(units.toLowerCase()))
            return size/1024;
        else if("mb".equals(units.toLowerCase()))
            return size/1024/1024;
        else if("gb".equals(units.toLowerCase()))
            return size/1024/1024/1024;
        else
            return size;
    }

    /**
     * 把文件路径和文件名组合为完整的文件路径
     * @param path 文件的完整绝对路径,不含文件名
     * @param fileName 文件名，含后缀
     * @return 组合后的完整路径
     */
    public static String getFullPath(String path, String fileName) {
        if(path.endsWith(File.separator))
            return path+fileName;
        else
            return path+File.separator+fileName;
    }

    /**
     * 检查文件夹是否都不为空
     * @param paths 文件夹路径集合
     * @return 如果都不为空，则返回true，只要有一个为空就返回false
     */
    public static boolean isNotNullForPaths(List<String> paths) {
        if(paths==null||paths.size()<1)
            return false;
        int fileSize = 0;
        for(String tempPath : paths) {
            fileSize = getFileNumber(tempPath);
            if(fileSize == 0)
                return false;
        }
        return true;
    }

    /**
     * 判断是否为文件
     * @param path 文件的完整绝对路径
     * @return 如果是，则返回true，否则返回false
     */
    private static boolean isFile(String path) {
        if(path==null||"".equals(path))
            return false;
        File file = new File(path);
        if(!file.exists())
            return false;
        if(!file.isFile())
            return false;
        return true;
    }

    /**
     * 判断是否为文件
     * @param path 文件夹的完整绝对路径
     * @return 如果是，则返回true，否则返回false
     */
    private static boolean isFolder(String path) {
        if(path==null||"".equals(path))
            return false;
        File file = new File(path);
        if(!file.exists())
            return false;
        if(!file.isDirectory())
            return false;
        return true;
    }

    /**
     * 保存文件到指定路径
     * @param data 文件的内容
     * @param path 文件的完整绝对路径
     * @return 保存成功返回true，否则返回false
     * @throws IOException
     */
    public static boolean writeByteToFile(byte[] data, String path) throws IOException {
        if(data==null)
            return false;
        if(!isFile(path))
            return false;
        FileOutputStream outf = new FileOutputStream(path);
        BufferedOutputStream bufferout = new BufferedOutputStream(outf);
        bufferout.write(data);
        bufferout.flush();
        bufferout.close();
        outf.close();
        return true;
    }

    /**
     * 添加内容到指定文件 如果该文件不存在则创建
     * @param path 文件的完整绝对路径
     * @param fileContent 要保存的内容
     * @param flag 如果为true，则向现有文件中添加，否则清空并新写入
     * @return 保存完成返回true，否则返回false
     * @throws IOException
     */
    public static boolean writeFile(String path, String fileContent, boolean flag) throws IOException {
        if(fileContent==null||"".equals(fileContent))
            return false;
        if(!createNewFile(path))
            return false;
        FileOutputStream fos = new FileOutputStream(path, flag);
        OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8");
        osw.write(fileContent + "\r\n");
        osw.flush();
        osw.close();
        return true;
    }

    /**
     * 添加内容到指定文件 如果该文件不存在则创建
     * @param path 文件的完整绝对路径
     * @param fileContent 要保存的内容集合
     * @param flag 如果为true，则向现有文件中添加，否则清空并新写入
     * @return 保存完成返回true，否则返回false
     * @throws IOException
     */
    public static boolean writeFile(String path,  List<String> fileContent, boolean flag) throws IOException {
        if(fileContent==null||fileContent.size()<1)
            return false;
        if(!createNewFile(path))
            return false;
        FileOutputStream fos = new FileOutputStream(path, flag);
        OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8");
        for(String str : fileContent)
            osw.write(str + "\r\n");
        osw.flush();
        osw.close();
        return true;
    }

    /**
     * 添加内容到指定文件 如果该文件不存在则创建
     * @param path 文件的绝对路径（不含文件名）
     * @param fileName 文件名
     * @param fileContent 要保存的内容集合
     * @param flag 如果为true，则向现有文件中添加，否则清空并新写入
     * @return 保存完成返回true，否则返回false
     * @throws IOException
     */
    public static boolean writeFile(String path,String fileName, List<String> fileContent, boolean flag) throws IOException {
        String allpath = getFullPath(path,fileName);
        return writeFile(allpath,fileContent,flag);
    }

    /**
     * 判断文件路径下是否是文件
     * @param path 文件的完整绝对路径
     * @return 是则返回true，没有则创建
     * @throws IOException
     */
    private static boolean createNewFile(String path) throws IOException {
        File file = new File(path);
        if(file.isDirectory())
            return false;
        if(file.exists()) {
            return true;
        }else{
            return file.createNewFile();
        }
    }

    /**
     * 读取一个文件
     * 如果路径错误、文件不存在、为空返回尺寸为0的list
     * @param path 文件的完整绝对路径
     * @return 读取到的文件内容
     * @throws IOException
     */
    public static List<String> readFile(String path) throws IOException {
        List<String> returnValue = new ArrayList<>();
        if(!isFile(path)) {
            return returnValue;
        }
        FileInputStream fis = new FileInputStream(path);
        InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
        LineNumberReader lnr = new LineNumberReader(isr);
        String tempStr;
        while(true) {
            tempStr = lnr.readLine();
            if(tempStr==null)
                break;
            returnValue.add(tempStr);
        }
        lnr.close();
        isr.close();
        fis.close();
        return returnValue;
    }

    /**
     * 读取一个文件
     * 如果路径错误、文件不存在、为空返回尺寸为0的list
     * @param file 要读取的文件
     * @return 读取到的文件内容
     * @throws IOException
     */
    public static List<String> readFile(File file) throws IOException {
        return readFile(file.getPath());
    }

    /**
     * 读取一个文件,并排重后返回
     * 如果路径错误、文件不存在、为空返回尺寸为0的set
     * @param path 文件的完整绝对路径
     * @return 读取到的文件内容
     * @throws IOException
     */
    public static Set<String> readFileNoDup(String path) throws IOException {
        List<String> list = readFile(path);
        Set<String> set = new HashSet<>(list);
        return set;
    }

    /**
     * 读取第一个文件，排重后写入第二个文件 并把排重结果返回
     * 如果路径错误、文件不存在、为空返回尺寸为0的list
     * @param path1 第一个文件的完整绝对路径
     * @param path2 第二个文件的完整绝对路径
     * @return 读取到的文件内容
     * @throws IOException
     */
    public static List<String> excludeDuplicates(String path1, String path2) throws IOException {
        Set<String> set = readFileNoDup(path1);
        List<String> list = new ArrayList<>(set);
        writeFile(path2,list,true);
        return list;
    }
}
