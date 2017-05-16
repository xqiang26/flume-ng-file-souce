package org.apache.flume.source.file;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class RandomFileSerializer extends FileSerializer {
	private static final Logger logger = LoggerFactory
		      .getLogger(ExecTailSource.class);

	@Override
	public void configure(Context line) {

	}

	@Override
	public void configure(ComponentConfiguration arg0) {
		// TODO Auto-generated method stub
	}

//	public static void main(String[] args) {
//		String line  = "2017-05-05 18:12:03,028";
//		Matcher m = Pattern.compile(PATTERN).matcher(line);
//		if (m.find()) {
//			System.out.println(m.group(1));
//			try {
//				String temp = m.group(1).replace(" ", "T").replace(",", ".");
//				Date logDate = FileConstants.LOG_SOURCE_DATE_FORMAT.parse(m.group(1));
//				String insertDate = FileConstants.DEFAULT_DATE_FORMAT.format(FileConstants.LOG_SOURCE_DATE_FORMAT.parse(m.group(1)));
//				System.out.println(temp);
//			} catch (ParseException e) {
//			}
//		}
//	}
	
	@Override
	public Map<String, Object> getContentBuilder(String line) {
		Map<String,Object> result = Maps.newHashMap();
	    // 现在创建 matcher 对象
	    Matcher m = DATATIME_PATTERN.matcher(line);
	    if (m.find()) {
			result.put("insert_date", m.group(1).replace(" ", "T").replace(",", "."));
			String temp = m.group(2).trim();
			int firstIndex = temp.indexOf(" ");
			String level = temp.substring(0, firstIndex);
			result.put("level", level);
			String extendsParam = temp.substring(temp.indexOf(" ", firstIndex + 1) + 1);
			result.put("message", extendsParam);
	    } else {
	    	result.put("message", line);
	    }
		return result;
	}

}
