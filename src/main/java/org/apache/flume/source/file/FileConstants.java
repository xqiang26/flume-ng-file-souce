package org.apache.flume.source.file;

public class FileConstants {
	
	  public static final String CONFIG_TAILING_THROTTLE  = "tailing";
	  
	  public static final String CONFIG_READINTERVAL_THROTTLE = "readinterval";
	  
	  public static final String CONFIG_STARTATBEGINNING_THROTTLE = "start_at_beginning";
	  
	  public static final String CONFIG_STATUS_DIRECTORY = "status.file.path";
	  
	  public static final String CONFIG_SERIALIZER = "serializer";
	  
	  public static final String CONFIG_FILENAME_REGEXP = "filenameRegExp";
	  
	  public static final String CONFIG_FILEPATH = "filepath";
	  
	  public static final String DEFAULT_STATUS_DIRECTORY = "F:\\log\\flume";
	  
	  public static final Boolean DEFAULT_ISTAILING_TRUE = true; 
	  
	  public static final Integer DEFAULT_READINTERVAL = 500;
	  
	  public static final Boolean DEFAULT_STARTATBEGINNING = false;
	  
	  public static final String DEFAULT_SERIALIZER_CLASS = "org.apache.flume.source.file.FileSerializer";
	  
}
