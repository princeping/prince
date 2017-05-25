package com.prince.guide.util.hdfs;

import java.io.IOException;
import java.util.List;

/**
 * hdfs工具类接口
 * @author princeping
 */
public interface HdfsUtil {
	
	/**
	 * 查看一个目录或是文件是否存在
	 * @param hdfsPath 目录或文件完整路径
	 * @return 存在返回true，不存在返回false
	 * @throws IOException
	 */
	boolean existsFiles(String hdfsPath) throws IOException;
	
	/**
	 * 创建文件夹
	 * 可以递归创建
	 * 如果文件夹存在，则直接返回true
	 * @param hdfsPath HDFS文件夹路径
	 * @return 创建成功返回true，失败返回false
	 * @throws IOException
	 */
	boolean createFolder(String hdfsPath) throws IOException;
	
	/**
	 * 创建文件
	 * 可以递归创建
	 * 如果已存在，则覆盖，不管是否有内容
	 * @param hdfsPath HDFS文件完整路径
	 * @return 创建成功返回true，失败返回false
	 * @throws IOException
	 */
	boolean createFile(String hdfsPath) throws IOException;
	
	/**
     * 创建文件
     * 可以递归创建
     * 如果已存在，则覆盖，不管是否有内容
     * @param hdfsPath HDFS文件完整路径
     * @param message 要写入的内容
     * @return 创建成功，返回true，否则返回false
     */
    boolean createFile(String hdfsPath, String message) throws IOException;
	
//	/**
//	 * 写文件
//	 * 不存在则返回false，存在则续写
//	 * @param hdfsPath HDFS文件完整路径
//	 * @param value 一行要写入的内容
//	 * @return 写入成功返回true，否则返回false
//	 * @throws IOException
//	 */
//	boolean writeFile(String hdfsPath, String value) throws IOException;
//	
//	/**
//	 * 写文件
//	 * 不存在则返回false，存在则续写
//	 * @param hdfsPath HDFS文件完整路径
//	 * @param value 一行要写入的内容
//	 * @return 写入成功返回true，否则返回false
//	 * @throws IOException
//	 */
//	boolean writeFile(String hdfsPath, List<String> value) throws IOException;
	
    /**
     * 移动文件到另一个文件夹
     * @param sourcepath 原始文件夹
     * @param targetpath 目标文件夹
     * @return 移动成功返回true，否则返回false
     * @throws IOException
     */
    boolean copyFiles(String sourcepath, String targetpath) throws IOException;
    
	/**
	 * 读文件
	 * 如果文件不存在？
	 * @param hdfsPath 文件完整路径
	 * @return 读取的内容，文件不存在或读取失败，返回null
	 */
	List<String> readFile(String hdfsPath) throws IOException;

	/**
	 * 上传本地文件到HDFS
	 * 如果文件已存在，则删除
	 * @param localFile 本地文件完整路径
	 * @param hdfsPath HDFS文件夹路径
	 * @return 上传成功，返回true，否则返回false
	 */
	boolean localFileUploadHDFS(String localFile, String hdfsPath) throws IOException;
	
	/**
	 * 上传本地文件夹到HDFS
	 * 如果文件夹已存在，则删除
	 * @param localFile 本地文件夹完整路径
	 * @param hdfsPath HDFS文件夹路径
	 * @return 上传成功，返回true，否则返回false
	 */
	boolean localFolderUploadHDFS(String localFolder, String hdfsPath) throws IOException;

	/**
	 * 下载HDFS文件到本地
	 * 如果已存在，则删除
	 * @param localPath 本地文件完整路径
	 * @param hdfsFile HDFS文件夹路径
	 * @return 下载成功，返回true，否则返回false
	 */
	boolean hdfsDownLocal(String localPath, String hdfsFile) throws IOException;

	/**
	 * 删除文件或目录
	 * 是否都能删除？如果目录不为空呢？如果不存在呢？
	 * @param hdfsPath 文件完整路径
	 * @return 删除成功，返回true，否则返回false
	 */
	boolean deletePath(String hdfsPath) throws IOException;

}
