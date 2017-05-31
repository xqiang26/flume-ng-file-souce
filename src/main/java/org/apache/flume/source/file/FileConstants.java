package org.apache.flume.source.file;

import java.text.SimpleDateFormat;

public class FileConstants {
	// 标记名称, 用于标记这个应用名称
	public static final String CONFIG_TAG_NAME = "tag_name";

	public static final String CONFIG_TAILING_THROTTLE = "tailing";

	public static final String CONFIG_READINTERVAL_THROTTLE = "readinterval";

	public static final String CONFIG_STARTATBEGINNING_THROTTLE = "start_at_beginning";

	public static final String CONFIG_STATUS_DIRECTORY = "status.file.path";

	public static final String CONFIG_SERIALIZER = "serializer";

	public static final String CONFIG_FILENAME_REGEXP = "filenameRegExp";

	public static final String CONFIG_FILEPATH = "filepath";

	public static final String CONFIG_FLUSH_TIMEOUT = "flush_timeout";

	// logname 的配置信息需要用 通过解析
	public static final String CONFIG_LOG_STRUCT_LOGNAME = "log_name";

	// logname 默认为log-parse 都需要解析
	public static final String DEFAULT_LOG_STRUCT_LOGNAME = "log-parse";

	public static final Long DEFAULT_FLUSH_TIMEOUT = 5000L;

	public static final String DEFAULT_STATUS_DIRECTORY = "F:\\log\\flume";

	public static final Boolean DEFAULT_ISTAILING_TRUE = true;

	public static final Integer DEFAULT_READINTERVAL = 500;

	public static final Boolean DEFAULT_STARTATBEGINNING = false;

	public static final String DEFAULT_SERIALIZER_CLASS = "org.apache.flume.source.file.LoggerFileSerializer";

	public static final String DEFAULT_READ_FILE_ENCODE = "iso8859-1";

	public static final SimpleDateFormat LOG_SOURCE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

	public static final SimpleDateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
}
