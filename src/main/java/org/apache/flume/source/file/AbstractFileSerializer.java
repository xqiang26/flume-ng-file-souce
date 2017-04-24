package org.apache.flume.source.file;

import java.util.Map;

import org.apache.flume.conf.Configurable;

public interface AbstractFileSerializer extends Configurable{

  /**
   * @param line 从文件中读取一行解析输出结构
   * @return
   */
  abstract Map<String,Object> getContentBuilder(String line);
}