package org.apache.flume.source.file;

import java.util.Map;
import java.util.UUID;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class ExecTailSourceTest {

	  private ExecTailSource fixture;

	  @Before
	  public void init() throws Exception {
	    fixture = new ExecTailSource();
	    fixture.setName("ExecTailSource-" + UUID.randomUUID().toString());
	  }
	  

	  @Test
	  public void shouldIndexOneEvent() throws Exception {
		Map<String,String> parameters = Maps.newHashMap();
		parameters.put("filepath", "F:\\log");
		parameters.put("filenameRegExp", "log");
	    Configurables.configure(fixture, new Context(parameters));
	    fixture.start();
	  }
}
