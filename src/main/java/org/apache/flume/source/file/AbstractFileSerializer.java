package org.apache.flume.source.file;

import java.util.Map;

import org.apache.flume.conf.Configurable;

public interface AbstractFileSerializer extends Configurable{

  /**
   * @param line ���ļ��ж�ȡһ�н�������ṹ
   * @return
   */
  abstract Map<String,Object> getContentBuilder(String line);
}