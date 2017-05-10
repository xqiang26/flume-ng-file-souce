package org.apache.flume.source.file;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;

import com.google.common.collect.Maps;

public class LoggerFileSerializer  extends RandomFileSerializer {
    
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
		Map<String,Object> result = super.getContentBuilder(line);
		if (result.containsKey("message")) {
			String message = result.get("message").toString();
			String[] arraySplit = message.split("\\|");
			for (String split : arraySplit) {
				String[] one = split.split("=");
				if (one.length == 2) {
					result.put(one[0], one[1]);
				}
			}
		}
		return result;
	}

}
