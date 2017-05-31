package org.apache.flume.source.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;

import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SouceHelper {
	private static final Logger LOG = LoggerFactory.getLogger(SouceHelper.class);
	
	private File file,directory;
	private Long currentIndex;
	private String statusFilePath, statusFileName;
	private Map<String,Long> statusFileJsonMap = new LinkedHashMap<String,Long>();

	private static final String LAST_INDEX_STATUS_FILE = "LastIndex";
	

	/**
	 * Builds an SouceHelper containing the configuration parameters and
	 * usefull utils for SQL Source
	 * @param context Flume source context, contains the properties from configuration file
	 */
	public SouceHelper(String path, String fileName){
		statusFilePath = path;
		statusFileName = fileName;
		directory = new File(statusFilePath);
		statusFileJsonMap = new LinkedHashMap<String, Long>();
        
//		LOG.info(">=statusFilePath:" + statusFilePath + "/" + statusFileName);
		
		if (!(isStatusDirectoryCreated())) {
			createDirectory();
		}
		
//		LOG.info(">=statusFilePath:" + statusFilePath + "/" + statusFileName);
		file = new File(statusFilePath + "/" + statusFileName);
		if (file == null || !isStatusFileCreated()){
			currentIndex = 0L;
			createStatusFile();
		}
		else
			currentIndex = getStatusFileLastIndex();
	}
	
	private boolean isStatusFileCreated(){
		return file.exists() && !file.isDirectory() ? true: false;
	}
	
	private boolean isStatusDirectoryCreated() {
		return directory.exists() && !directory.isFile() ? true: false;
	}
	
	/**
	 * 文件重命名
	 * @param reName
	 * @return
	 */
	public boolean rename(String reName) {
		File dest = new File(statusFilePath + "/" + reName);
		return file.renameTo(dest);
	}
	/**
	 * Create status file
	 */
	public void createStatusFile(){		
		statusFileJsonMap.put(LAST_INDEX_STATUS_FILE, currentIndex);
		
		try {
			if (file.createNewFile() ) {
				Writer fileWriter = new FileWriter(file,false);
				JSONValue.writeJSONString(statusFileJsonMap, fileWriter);
				fileWriter.close();
			}
		} catch (IOException e) {
			LOG.error("Error creating value to status file!!!",e);
		}
	}
	
    /**
     * Update status file with last read row index    
     */
	public void updateStatusFile(Long currentIndex) {
		this.currentIndex = currentIndex;
		statusFileJsonMap.put(LAST_INDEX_STATUS_FILE, currentIndex);
		try {
			Writer fileWriter = new FileWriter(file,false);
			JSONValue.writeJSONString(statusFileJsonMap, fileWriter);
			fileWriter.close();
		} catch (IOException e) {
			LOG.error("Error writing incremental value to status file!!!",e);
		}	
	}
	
	public Long getStatusFileLastIndex() {
		if (!isStatusFileCreated()) {
			LOG.info("Status file not created, using start value from config file and creating file");
			return 0L;
		}
		else{
			try {
				FileReader fileReader = new FileReader(file);
				JSONParser jsonParser = new JSONParser();
				statusFileJsonMap = (Map)jsonParser.parse(fileReader);
				return statusFileJsonMap.get(LAST_INDEX_STATUS_FILE);
				
			} catch (Exception e) {
				LOG.error("Exception reading status file, doing back up and creating new status file", e);
				backupStatusFile();
				return 0L;
			}
		}
	}

	private void backupStatusFile() {
		file.renameTo(new File(statusFilePath + "/" + statusFileName + ".bak." + System.currentTimeMillis()));		
	}
	
	/*
	 * @return boolean pathname into directory
	 */
	private boolean createDirectory() {
		return directory.mkdir();
	}
}
