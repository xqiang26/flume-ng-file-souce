package org.apache.flume.source.file;

import java.text.ParseException;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;

import com.google.common.collect.Maps;

public class RandomFileSerializer extends FileSerializer {

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
	    // 现在创建 matcher 对象
	    Matcher m = DATATIME_PATTERN.matcher(line);
	    if (m.find()) {
			try {
				String insertDate = FileConstants.DEFAULT_DATE_FORMAT.format(FileConstants.LOG_SOURCE_DATE_FORMAT.parse(m.group(1)));
				result.put("insert_date", insertDate);
				String temp = m.group(2).trim();
				int firstIndex = temp.indexOf(" ");
				String level = temp.substring(0, firstIndex);
				result.put("level", level);
				String extendsParam = temp.substring(temp.indexOf(" ", firstIndex + 1) + 1);
				result.put("message", extendsParam);
			} catch (ParseException e) {
			}
	    } else {
	    	result.put("message", line);
	    }
		return result;
	}

}
