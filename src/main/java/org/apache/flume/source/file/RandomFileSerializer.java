package org.apache.flume.source.file;

import java.util.Map;
import java.util.regex.Matcher;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class RandomFileSerializer extends FileSerializer {
	private static final Logger logger = LoggerFactory.getLogger(ExecTailSource.class);

	@Override
	public void configure(Context line) {

	}


	@Override
	public Map<String, Object> getContentBuilder(String line) {
		Map<String, Object> result = Maps.newHashMap();
		// 现在创建 matcher 对象
		Matcher m = DATATIME_PATTERN.matcher(line);
		if (m.find()) {
			result.put("insert_date", m.group(1).replace(" ", "T").replace(",", "."));
			String temp = m.group(2).trim();
			int firstIndex = temp.indexOf(" ");
			String level = temp.substring(0, firstIndex);
			result.put("level", level);
			String logName = temp.substring(firstIndex + 1, temp.indexOf(" ", firstIndex + 1));
			result.put("logname", logName);
			String extendsParam = temp.substring(temp.indexOf(" ", firstIndex + 1) + 1);
			result.put("message", extendsParam);
		} else {
			result.put("message", line);
		}
		return result;
	}

}
