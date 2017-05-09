package org.apache.flume.source.file;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;

import com.google.common.collect.Maps;

public class LoggerFileSerializer implements AbstractFileSerializer {

	public static final String PATTERN = "(^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})(.*)";
	 // 创建 Pattern 对象
    Pattern DATATIME_PATTERN = Pattern.compile(PATTERN);
    
    
	@Override
	public void configure(Context arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void configure(ComponentConfiguration arg0) {
		// TODO Auto-generated method stub

	}
	   
	@Override
	public Map<String, Object> getContentBuilder(String line) {
		Map<String,Object> result = Maps.newHashMap();
	    // 现在创建 matcher 对象
	    Matcher m = DATATIME_PATTERN.matcher(line);
		result.put("insert_date", m.group(1));
		String temp = m.group(2).trim();
        int firstIndex = temp.indexOf(" ");
        String level = temp.substring(0, firstIndex);
		result.put("level", level);
		
		String extendsParam = temp.substring(temp.indexOf(" ", firstIndex + 1) + 1);
		
		String[] arraySplit = extendsParam.split("\\|");
		for (String split : arraySplit) {
			String[] one = split.split("=");
			if (one.length == 2) {
				result.put(one[0], one[1]);
			}
		}
		return result;
	}

}
