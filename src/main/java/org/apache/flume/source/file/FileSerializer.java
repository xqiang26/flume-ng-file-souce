package org.apache.flume.source.file;

import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;

import com.google.common.collect.Maps;

public class FileSerializer implements AbstractFileSerializer {

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
