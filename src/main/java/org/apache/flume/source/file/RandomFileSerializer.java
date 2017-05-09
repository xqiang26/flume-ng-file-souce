package org.apache.flume.source.file;

import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;

import com.google.common.collect.Maps;

public class RandomFileSerializer implements AbstractFileSerializer {

	@Override
	public void configure(Context line) {

	}

	@Override
	public void configure(ComponentConfiguration arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public Map<String, Object> getContentBuilder(String line) {
		Map<String,Object> result = Maps.newHashMap();
		result.put("message", line);
		return result;
	}

}
