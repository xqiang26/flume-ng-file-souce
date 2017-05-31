package org.apache.flume.source.file;

import java.util.Map;

import org.apache.flume.Context;
import org.junit.Test;

import com.google.common.collect.Maps;

public class LoggerFileSerializerTest {

	@Test
	public void testGetContentBuilder() {
	}

	@Test
	public void testConfigure() {
		LoggerFileSerializer logFileSerializer = new LoggerFileSerializer();
		Map<String,String> parameters = Maps.newHashMap();
		parameters.put(FileConstants.CONFIG_LOG_STRUCT_LOGNAME,
				FileConstants.DEFAULT_LOG_STRUCT_LOGNAME);
		logFileSerializer.configure(new Context(parameters));
		String line = "2017-05-26 12:49:36,390 INFO log-parse name=邮件接收程序|type=job|code=0";
		Map<String,Object> lineResult = logFileSerializer.getContentBuilder(line);
		System.out.println(lineResult);
	}

}
