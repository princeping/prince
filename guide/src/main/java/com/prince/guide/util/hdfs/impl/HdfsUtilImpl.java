package com.prince.guide.util.hdfs.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.prince.guide.start.ConfigFactory;
import com.prince.guide.util.hdfs.HdfsUtil;

import jodd.util.StringUtil;

/**
 * hdfs工具类接口实现
 * @author princeping
 */
public class HdfsUtilImpl implements HdfsUtil{
	
	/**
	 * 查看一个目录或是文件是否存在
	 * @param hdfsPath 目录或文件完整路径
	 * @return 存在返回true，不存在返回false
	 * @throws IOException
	 */
	@Override
	public boolean existsFiles(String hdfsPath) throws IOException {
		if(StringUtil.isEmpty(hdfsPath))
			return false;
		FileSystem fileSystem = FileSystem.get(ConfigFactory.getInstance().getCoreSiteConf());
		Path path = new Path(hdfsPath);
		boolean flag = fileSystem.exists(path);
		fileSystem.close();
		return flag;
	}

	/**
	 * 创建文件夹
	 * 可以递归创建
	 * 如果文件夹存在，则直接返回true
	 * @param hdfsPath HDFS文件夹路径
	 * @return 创建成功返回true，失败返回false
	 * @throws IOException
	 */
	@Override
	public boolean createFolder(String hdfsPath) throws IOException {
		if(StringUtil.isEmpty(hdfsPath))
			return false;
		FileSystem fileSystem = FileSystem.get(ConfigFactory.getInstance().getCoreSiteConf());
		Path path = new Path(hdfsPath);
		boolean flag = fileSystem.mkdirs(path);
		fileSystem.close();
		return flag;
	}
	
	/**
	 * 创建文件
	 * 可以递归创建
	 * 如果已存在，则覆盖，不管是否有内容
	 * @param hdfsPath HDFS文件完整路径
	 * @return 创建成功返回true，失败返回false
	 * @throws IOException
	 */
	@Override
	public boolean createFile(String hdfsPath) throws IOException {
		if(StringUtil.isEmpty(hdfsPath))
			return false;
		FileSystem fileSystem = FileSystem.get(ConfigFactory.getInstance().getCoreSiteConf());
		Path path = new Path(hdfsPath);
		boolean flag = fileSystem.exists(path);
		if(flag)
			System.out.println("您要创建的文件已存在，默认进行了覆盖");
		fileSystem.create(path);
		fileSystem.close();
		return true;
	}
	
	/**
	 * 创建文件
	 * 可以递归创建
	 * 如果已存在，则覆盖，不管是否有内容
	 * @param hdfsPath HDFS文件完整路径
	 * @param message 传入的内容
	 * @return 创建成功返回true，失败返回false
	 * @throws IOException
	 */
	@Override
	public boolean createFile(String hdfsPath, String message) throws IOException {
		if(StringUtil.isEmpty(hdfsPath))
            return false;
        FileSystem fileSystem = FileSystem.get(ConfigFactory.getInstance().getCoreSiteConf());
        Path path = new Path(hdfsPath);
        FSDataOutputStream fSDataOutputStream = fileSystem.create(path);
        fSDataOutputStream.write(message.getBytes());
        fileSystem.close();
        return true;
	}
//	/**
//	 * 写文件
//	 * 不存在则返回false，存在则续写
//	 * @param hdfsPath HDFS文件完整路径
//	 * @param value 一行要写入的内容
//	 * @return 写入成功返回true，否则返回false
//	 * @throws IOException
//	 */
//	@Override
//	public boolean writeFile(String hdfsPath, String value) throws IOException {
//		if(StringUtil.isEmpty(hdfsPath))
//			return false;
//		FileSystem fileSystem = FileSystem.get(ConfigFactory.getInstance().getCoreSiteConf());
//		Path path = new Path(hdfsPath);
//		if(!fileSystem.exists(path)){
//			System.out.println("要写入的文件不存在，请检查文件路径或文件名是否有误");
//			return false;
//		}
//		FSDataOutputStream outputStream = fileSystem.append(path);
//		byte[] buff = value.getBytes();
//		outputStream.write(buff, 0, buff.length);
//		outputStream.close();
//		fileSystem.close();
//		return true;
//		
//	}
//	
//	/**
//	 * 写文件
//	 * 不存在则返回false，存在则续写
//	 * @param hdfsPath HDFS文件完整路径
//	 * @param value 一行要写入的内容
//	 * @return 写入成功返回true，否则返回false
//	 * @throws IOException
//	 */
//	@Override
//	public boolean writeFile(String hdfsPath, List<String> value) throws IOException {
//		if(StringUtil.isEmpty(hdfsPath))
//			return false;
//		FileSystem fileSystem = FileSystem.get(ConfigFactory.getInstance().getCoreSiteConf());
//		Path path = new Path(hdfsPath);
//		FSDataOutputStream outputStream = fileSystem.append(path);
//		List<String> tempStr = new ArrayList<>();
//		return false;
//	}
	
	/**
	 * 读文件
	 * 如果文件不存在，则返回null
	 * @param hdfsPath 文件完整路径
	 * @return 读取的内容，文件不存在或读取失败，返回null
	 */
	@Override
	public List<String> readFile(String hdfsPath) throws IOException {
        if(StringUtil.isEmpty(hdfsPath))
            return null;
        if(!existsFiles(hdfsPath))
            return null;
        FileSystem fileSystem = FileSystem.get(ConfigFactory.getInstance().getCoreSiteConf());
        Path path = new Path(hdfsPath);
        FSDataInputStream fsDataInputStream = fileSystem.open(path);
        InputStreamReader inputStreamReader = new InputStreamReader(fsDataInputStream);
        LineNumberReader lineNumberReader = new LineNumberReader(inputStreamReader);
        List<String> returnValue = new ArrayList<>();
        while(true){
            String tempStr = lineNumberReader.readLine();
            if(tempStr==null)
                break;
            returnValue.add(tempStr);
        }
        lineNumberReader.close();
        inputStreamReader.close();
        fsDataInputStream.close();
        fileSystem.close();
        return returnValue;
	}

    /**
     * 移动文件到另一个文件夹
     * @param sourcepath 原始文件夹
     * @param targetpath 目标文件夹
     * @return 移动成功返回true，否则返回false
     * @throws IOException
     */
    public boolean copyFiles(String sourcepath, String targetpath) throws IOException {
        if(StringUtil.isEmpty(sourcepath)||StringUtil.isEmpty(targetpath))
            return false;
        if(!existsFiles(sourcepath)||!existsFiles(targetpath))
            return false;
        FileSystem fileSystem = FileSystem.get(ConfigFactory.getInstance().getCoreSiteConf());
        Path path1 = new Path(sourcepath);
        Path path2 = new Path(targetpath);
        boolean flag = fileSystem.rename(path1,path2);
        fileSystem.close();
        return flag;
    }
    
	/**
	 * 上传本地文件到HDFS
	 * 如果文件已存在，则覆盖
	 * @param localFile 本地文件完整路径
	 * @param hdfsPath HDFS文件夹路径
	 * @return 上传成功，返回true，否则返回false
	 */
	@Override
	public boolean localFileUploadHDFS(String localFile, String hdfsPath) throws IOException {
		//如果输入的路径为空则返回false
		if(StringUtil.isEmpty(localFile)||StringUtil.isEmpty(hdfsPath))
			return false;
		File file = new File(localFile);
		//如果本地文件不存在或上传的是文件夹
		if(!file.exists()||file.isDirectory()){
			System.out.println("上传的不是文件或文件不存在");
			return false;
		}
		//如果HDFS文件夹路径不存在
		if(!existsFiles(hdfsPath))
			return false;
//		if(!existsFiles(hdfsFile))
//			createFolder(hdfsFile);
		FileSystem fileSystem = FileSystem.get(ConfigFactory.getInstance().getCoreSiteConf());
		Path path1 = new Path(localFile);
		Path path2 = new Path(hdfsPath);
		fileSystem.copyFromLocalFile(path1, path2);
		fileSystem.close();
		return true;
	}

	/**
	 * 上传本地文件夹到HDFS
	 * 如果文件夹已存在，则删除
	 * @param localFile 本地文件夹完整路径
	 * @param hdfsPath HDFS文件夹路径
	 * @return 上传成功，返回true，否则返回false
	 */
	@Override
	public boolean localFolderUploadHDFS(String localFolder, String hdfsPath) throws IOException {
		if(StringUtil.isEmpty(localFolder)||StringUtil.isEmpty(hdfsPath))
			return false;
		File file = new File(localFolder);
		if(!file.exists()||file.isFile()){
			System.out.println("上传的不是文件夹或文件夹不存在");
			return false;
		}
		if(!existsFiles(hdfsPath))
			return false;
		String[] filename = file.list();
        FileSystem fileSystem = FileSystem.get(ConfigFactory.getInstance().getCoreSiteConf());
        Path path2 = new Path(hdfsPath);
        if(!localFolder.endsWith(File.separator))
        	localFolder = localFolder + File.separator;
        for(String onename : filename){
            Path path1 = new Path(localFolder+onename);
            fileSystem.copyFromLocalFile(path1,path2);
        }
		fileSystem.close();
		return true;
	}

	/**
	* 下载HDFS文件到本地
	* 文件存在时覆盖，文件夹存在时替换
	* @param localPath 本地文件完整路径
	* @param hdfsFile HDFS文件夹路径
	* @return 下载成功，返回true，否则返回false
	*/
	@Override
	public boolean hdfsDownLocal(String localPath, String hdfsFile) throws IOException {
		if(StringUtil.isEmpty(localPath)||StringUtil.isEmpty(hdfsFile))
			return false;
		File file = new File(localPath);
		//如果下载目标路径不存在或目标路径指向文件
		if(!file.exists()||file.isFile())
			return false;
		//如果下载源路径不存在
		if(!existsFiles(hdfsFile))
			return false;
		FileSystem fileSystem = FileSystem.get(ConfigFactory.getInstance().getCoreSiteConf());
		Path path1 = new Path(localPath);
		Path path2 = new Path(hdfsFile);
		fileSystem.copyToLocalFile(path2, path1);
		fileSystem.close();
		return true;
	}

	/**
	 * 删除文件或目录
	 * 文件和目录都可删除
	 * 如果目录不为空则递归删除
	 * @param hdfsPath 文件完整路径
	 * @return 删除成功返回true，否则返回false
	 */
	@Override
	public boolean deletePath(String hdfsPath) throws IOException {
		if(StringUtil.isEmpty(hdfsPath))
			return false;
		FileSystem fileSystem = FileSystem.get(ConfigFactory.getInstance().getCoreSiteConf());
		Path path = new Path(hdfsPath);
		boolean flag = fileSystem.delete(path, true);
		fileSystem.close();
		return flag;
	}


}
