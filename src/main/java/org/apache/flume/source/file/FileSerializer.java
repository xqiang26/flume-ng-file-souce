package org.apache.flume.source.file;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;

import com.google.common.collect.Maps;

public class FileSerializer implements AbstractFileSerializer {

	public static final String PATTERN = "(^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})(.*)";
	 // 创建 Pattern 对象
    Pattern DATATIME_PATTERN = Pattern.compile(PATTERN);
    
	@Override
	public Map<String,Object> getContentBuilder(String line) {
		Map<String,Object> result = Maps.newHashMap();
		String[] arraySplit = line.split("\\|");
		for (String split : arraySplit) {
			String[] one = split.split("=");
			if (one.length == 2) {
				result.put(one[0], one[1]);
			}
		}
		return result;
	}

	@Override
	public void configure(Context arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(ComponentConfiguration arg0) {
		// TODO Auto-generated method stub
		
	}
}
