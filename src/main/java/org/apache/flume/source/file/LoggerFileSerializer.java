package org.apache.flume.source.file;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;

public class LoggerFileSerializer extends RandomFileSerializer {
	private String logNameTag;

	@Override
	public void configure(Context context) {
		logNameTag = context.getString(FileConstants.CONFIG_LOG_STRUCT_LOGNAME,
				FileConstants.DEFAULT_LOG_STRUCT_LOGNAME);
	}

	@Override
	public Map<String, Object> getContentBuilder(String line) {
		Map<String, Object> result = super.getContentBuilder(line);
		if (result.containsKey("message") && result.containsKey("logname") && StringUtils.isNotBlank(logNameTag)
				&& logNameTag.compareToIgnoreCase(result.get("logname").toString()) == 0) {
			String message = result.get("message").toString();
			String[] arraySplit = message.split("\\|");
			for (String split : arraySplit) {
				String[] one = split.split("=");
				if (one.length == 2) {
					result.put(one[0], one[1]);
				}
			}
			result.remove("message");
		}
		return result;
	}

}
