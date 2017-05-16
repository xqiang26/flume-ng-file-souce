package org.apache.flume.source.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSourceConfigurationConstants;
import org.elasticsearch.common.collect.Maps;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;


public class ExecTailSource  extends AbstractSource implements EventDrivenSource, Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(ExecTailSource.class);

  private SourceCounter sourceCounter;
  private ExecutorService executor;
  private Map<String, ExecRunnable> mapRuners;
  private Map<String, Future<?>> mapFuture;
  private long restartThrottle;
  private boolean restart = true;
  private boolean logStderr;
  private Integer bufferCount;
  private long batchTimeout;
  private Charset charset;
  private String filepath;
  private String filenameRegExp;
  private String statusPath;
  private String tagName; //标记名称
  private boolean tailing;
  private Integer readinterval;
  private boolean startAtBeginning;
  private AbstractFileSerializer serializer;
  private List<String> listFiles; 
  private ScheduledExecutorService timedFlushService;
  private ScheduledFuture<?> timedFuture;
  private long flushtimeout;
  
	public static String getYesterday() {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) - 1);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		return String.format("%04d-%02d-%02d", year, month, day);
	}
	
  /**
   * 到了，晚上
   */
	public void process() {
		try {
		    List<String> tmpListFiles = getFileList(filepath);
		    if(tmpListFiles==null || tmpListFiles.isEmpty()){
		      Preconditions.checkState(tmpListFiles != null && !tmpListFiles.isEmpty(),
		              "The filepath's file not have fiels with filenameRegExp");
		    }
		    
		    /**
		     * 如果日志文件数量发送变化，说明日志有可能是人为的操作或者是新的日志文件生成；则需求更新日志的状态文件；
		     */
		    if (tmpListFiles.size() != listFiles.size()) {
				stop();
				logger.info("开始检查日志文件发生变更, 并处理有效的日志文件");
				String dateTime = getYesterday();
				for (String fileName : tmpListFiles) {
					File logFile = new File(fileName);
					String logFileName = logFile.getName();
					File file = new File(statusPath + "/" + logFileName);
					/**
					 * 如果状态文件不存在，并且该文件的日期是昨天；则认为是系统将info.log 这类的文件更新成info.log-xxxx-xx-xx
					 * 这里只针对，晚上0点时，日志文件发生变更
					 */
					if (logFileName.endsWith(dateTime) && !file.exists() && logFile.length() > 0) {
						String[] arrayLogFileName = logFileName.split(".log.");
						switch (arrayLogFileName[0]) {
						case "info": {
								logger.info("开始检查日志文件发生变更,info.log ==> " + logFileName);
								SouceHelper sourceHelper = new SouceHelper(statusPath, "info.log");
								sourceHelper.rename(logFileName);
							}
							break;
						case "error": {
							logger.info("开始检查日志文件发生变更,error.log ==> " + logFileName);
								SouceHelper sourceHelper = new SouceHelper(statusPath, "error.log");
								sourceHelper.rename(logFileName);
							}
							break;
						case "debug": {
								logger.info("开始检查日志文件发生变更,debug.log ==> " + logFileName);
								SouceHelper sourceHelper = new SouceHelper(statusPath, "debug.log");
								sourceHelper.rename(logFileName);
							}
							break;
						}
					}
				}
				start();
		    }
		} catch (Exception e) {
			logger.error("Error procesing row", e);
		}
	}
	
	@Override
	public void configure(Context context) throws FlumeException {
		String serializerClazz = context.getString(FileConstants.CONFIG_SERIALIZER, FileConstants.DEFAULT_SERIALIZER_CLASS);
	    try {
	      @SuppressWarnings("unchecked")
		Class<? extends Configurable> clazz = (Class<? extends Configurable>) Class
	          .forName(serializerClazz);
	      Configurable serializerTmp = clazz.newInstance();

	      if (serializerTmp instanceof FileSerializer) {
	    	  serializer = (FileSerializer) serializerTmp;
	      } else {
	        throw new IllegalArgumentException(serializerClazz
	            + " is not an FileSerializer");
	      }
	    } catch (Exception e) {
	      logger.error("Could not instantiate serializer.", e);
	      Throwables.propagate(e);
	    }

	    filepath = context.getString(FileConstants.CONFIG_FILEPATH);
	    Preconditions.checkState(filepath != null,
	        "The parameter filepath must be specified");
	    logger.info("The parameter filepath is {}" ,filepath);

	    filenameRegExp = context.getString(FileConstants.CONFIG_FILENAME_REGEXP);
	    Preconditions.checkState(filenameRegExp != null,
	            "The parameter filenameRegExp must be specified");
	    logger.info("The parameter filenameRegExp is {}" ,filenameRegExp);

	    statusPath = context.getString(FileConstants.CONFIG_STATUS_DIRECTORY, 
	    		FileConstants.DEFAULT_STATUS_DIRECTORY);
	    
	    flushtimeout = context.getLong(FileConstants.CONFIG_FLUSH_TIMEOUT, 
	    		FileConstants.DEFAULT_FLUSH_TIMEOUT);
	    
	    restartThrottle = context.getLong(ExecSourceConfigurationConstants.CONFIG_RESTART_THROTTLE,
	        ExecSourceConfigurationConstants.DEFAULT_RESTART_THROTTLE);

	    tailing = context.getBoolean(FileConstants.CONFIG_TAILING_THROTTLE,
	    		FileConstants.DEFAULT_ISTAILING_TRUE);

	    readinterval=context.getInteger(FileConstants.CONFIG_READINTERVAL_THROTTLE,
	    		FileConstants.DEFAULT_READINTERVAL);

	    tagName = context.getString(FileConstants.CONFIG_TAG_NAME);
	    
	    startAtBeginning=context.getBoolean(FileConstants.CONFIG_STARTATBEGINNING_THROTTLE,
	    		FileConstants.DEFAULT_STARTATBEGINNING);

	    restart = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_RESTART,
	        ExecSourceConfigurationConstants.DEFAULT_RESTART);

	    logStderr = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_LOG_STDERR,
	        ExecSourceConfigurationConstants.DEFAULT_LOG_STDERR);

	    bufferCount = context.getInteger(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE,
	        ExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE);

	    batchTimeout = context.getLong(ExecSourceConfigurationConstants.CONFIG_BATCH_TIME_OUT,
	        ExecSourceConfigurationConstants.DEFAULT_BATCH_TIME_OUT);

	    charset = Charset.forName(context.getString(ExecSourceConfigurationConstants.CHARSET,
	        ExecSourceConfigurationConstants.DEFAULT_CHARSET));
	   
	    if (sourceCounter == null) {
	      sourceCounter = new SourceCounter(getName());
	    }
	}
	
	@Override
	public void start() throws FlumeException {
	    logger.info("=start=> flume tail source start begin time:"+new Date().toString());
	    logger.info("ExecTail source starting with filepath:{}", filepath);
	    
	    mapRuners = Maps.newHashMap();
	    mapFuture = Maps.newHashMap();
	    startTmp();
	    /*
	     * NB: This comes at the end rather than the beginning of the method because
	     * it sets our state to running. We want to make sure the executor is alive
	     * and well first.
	     */
	    timedFlushService = Executors.newSingleThreadScheduledExecutor(
					      new ThreadFactoryBuilder().setNameFormat(
					      "timedFlushExecService" +
					      Thread.currentThread().getId() + "-%d").build());
	    timedFuture = timedFlushService.scheduleWithFixedDelay(new Runnable() {
	        @Override
	        public void run() {
	        	process();
	        }
	      },flushtimeout, flushtimeout, TimeUnit.MILLISECONDS);
        
	    sourceCounter.start();
	    super.start();
	    logger.info("=start=> flume tail source start end time:"+new Date().toString());
	    logger.debug("ExecTail source started");
	}
	
	public void startTmp() {
	    listFiles = getFileList(filepath);
	    if(listFiles==null || listFiles.isEmpty()){
	      Preconditions.checkState(listFiles != null && !listFiles.isEmpty(),
	              "The filepath's file not have fiels with filenameRegExp");
	    }
	    executor = Executors.newFixedThreadPool(listFiles.size());
	    logger.info("files size is {} ", listFiles.size());
	    for(String oneFilePath : listFiles){
	      File logFile = new File(oneFilePath);
	      SouceHelper sourceHelper = new SouceHelper(statusPath, logFile.getName());
	      logger.info("logFilePath:" + logFile.getName() + ":logFile.length()," +  logFile.length() + " getStatusFileLastIndex(): " + sourceHelper.getStatusFileLastIndex());
	      if (logFile.getName().endsWith(".log") || 
	    		 logFile.length() > sourceHelper.getStatusFileLastIndex()) {
	    	  ExecRunnable runner = 
	    			  new ExecRunnable(getChannelProcessor(), 
	    					  sourceCounter,
	    					  restart, 
	    					  restartThrottle, 
	    					  logStderr, 
	    					  bufferCount, 
	    					  batchTimeout, 
	    					  charset,
	    					  oneFilePath,
	    					  tailing,
	    					  readinterval,
	    					  startAtBeginning,
	    					  serializer,
	    					  sourceHelper,
	    					  tagName
	    					  );
	    	  mapRuners.put(oneFilePath, runner);
	    	  Future<?> runnerFuture = executor.submit(runner);
	    	  mapFuture.put(oneFilePath, runnerFuture);
	    	  logger.info("{} is begin running",oneFilePath);
	      }
	    }
	}
	
	public void stopTmp() {
		logger.info("=stopTmp=> flume tail source stop begin time:"+new Date().toString());
	    if(mapRuners !=null && !mapRuners.isEmpty()){
	      for(ExecRunnable oneRunner : mapRuners.values()){
	        if(oneRunner != null) {
	          oneRunner.setRestart(false);
	          oneRunner.kill();
	        }
	      }
	    }
	    mapRuners.clear();
	    if(mapFuture !=null && !mapFuture.isEmpty()){
	      for(Future<?> oneFuture : mapFuture.values()){
	        if (oneFuture != null) {
	          logger.debug("Stopping ExecTail runner");
	          oneFuture.cancel(true);
	          logger.debug("ExecTail runner stopped");
	        }
	      }
	    }
	    mapFuture.clear();
	    
	    executor.shutdown();
	    while (!executor.isTerminated()) {
	      logger.debug("Waiting for ExecTail executor service to stop");
	      try {
	        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
	      } catch (InterruptedException e) {
	        logger.debug("Interrupted while waiting for ExecTail executor service "
	            + "to stop. Just exiting.");
	        Thread.currentThread().interrupt();
	      }
	    }
	}
	
	@Override
	public void stop() throws FlumeException {
		stopTmp();

        try {
            // Stop the Thread that flushes periodically
            if (timedFuture != null) {
            	timedFuture.cancel(true);
            }

            if (timedFlushService != null) {
              timedFlushService.shutdown();
              while (!timedFlushService.isTerminated()) {
                try {
                  timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                  logger.debug("Interrupted while waiting for ExecTail executor service "
                          + "to stop. Just exiting.");
                  Thread.currentThread().interrupt();
                }
              }
            }
            logger.info("=kill=> flume tail source kill end time:" + new Date().toString());
          } catch (Exception ex) {
            logger.error("=kill=>", ex);
            Thread.currentThread().interrupt();
          }

	    sourceCounter.stop();
	    super.stop();
	    logger.info("=stop=> flume tail source stop end time:"+new Date().toString());
	}

  /**
   * 获取指定路径下的所有文件列表
   *
   * @param dir 要查找的目录
   * @return
   */
  public  List<String> getFileList(String dir) {
    List<String> listFile = new ArrayList<String>();
    File dirFile = new File(dir);
    //如果不是目录文件，则直接返回
    if (dirFile.isDirectory()) {
      //获得文件夹下的文件列表，然后根据文件类型分别处理
      File[] files = dirFile.listFiles();
      if (null != files && files.length > 0) {
        //根据时间排序
        Arrays.sort(files, new Comparator<File>() {
          public int compare(File f1, File f2) {
            return (int) (f1.lastModified() - f2.lastModified());
          }

          public boolean equals(Object obj) {
            return true;
          }
        });
        for (File file : files) {
          //如果不是目录，直接添加
          if (!file.isDirectory()) {
            String oneFileName = file.getName();
            if(match(filenameRegExp,oneFileName)){
              listFile.add(file.getAbsolutePath());
            }
          } else {
            //对于目录文件，递归调用
            listFile.addAll(getFileList(file.getAbsolutePath()));
          }
        }
      }
    }else{
      logger.info("FilePath:{} is not Directory",dir);
    }
    return listFile;
  }

  /**
   * @param regex
   * 正则表达式字符串
   * @param str
   * 要匹配的字符串
   * @return 如果str 符合 regex的正则表达式格式,返回true, 否则返回 false;
   */
  private boolean match(String regex, String str) {
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(str);
   return matcher.find();
  }


  private static class ExecRunnable implements Runnable {

    public ExecRunnable( ChannelProcessor channelProcessor,
        SourceCounter sourceCounter, 
        boolean restart, 
        long restartThrottle,
        boolean logStderr, 
        int bufferCount, 
        long batchTimeout, 
        Charset charset,
        String filepath,
        boolean tailing,
        Integer readinterval,
        boolean startAtBeginning,
        AbstractFileSerializer serializerClazz,
        SouceHelper sourceHelper,
        String tagName) {
    	
      this.serializer = serializerClazz;
      this.channelProcessor = channelProcessor;
      this.sourceCounter = sourceCounter;
      this.restartThrottle = restartThrottle;
      this.bufferCount = bufferCount;
      this.batchTimeout = batchTimeout;
      this.restart = restart;
      this.logStderr = logStderr;
      this.charset = charset;
      this.filepath=filepath;
      this.logfile=new File(filepath);
      this.tailing=tailing;
      this.readinterval=readinterval;
      this.startAtBeginning=startAtBeginning;
      this.sourceHelper = sourceHelper;
      this.tagName = tagName;
    }

    private final SouceHelper sourceHelper; 
    private final AbstractFileSerializer serializer;
    private final ChannelProcessor channelProcessor;
    private final SourceCounter sourceCounter;
    private volatile boolean restart;
    private final long restartThrottle;
    private final int bufferCount;
    private long batchTimeout;
    private final boolean logStderr;
    private final Charset charset;
    private SystemClock systemClock = new SystemClock();
    private Long lastPushToChannel = systemClock.currentTimeMillis();
    private String filepath;
    private String tagName;

    /**
     * 当读到文件结尾后暂停的时间间隔
     */
    private long readinterval = 500;

    /**
     * 设置日志文件
     */
    private File logfile;

    /**
     * 设置是否从头开始读
     */
    private boolean startAtBeginning = false;

    /**
     * 设置tail运行标记
     */
    private boolean tailing = false;

    @Override
    public void run() {
      do {
        logger.info("=run=> " + logfile.getName() + " flume tail source run start time:"+new Date().toString());

        long filePointer = 0;
        if (this.startAtBeginning) { //判断是否从头开始读文件
          filePointer = 0;
        } else {
        	filePointer = sourceHelper.getStatusFileLastIndex();
        }
        final List<Event> eventList = new ArrayList<Event>();
        RandomAccessFile randomAccessFile = null;
        try {
          randomAccessFile= new RandomAccessFile(logfile, "r"); //创建随机读写文件
          while (this.tailing) {
            long fileLength = this.logfile.length();
            if (fileLength < filePointer) {
              logger.info("=run=> " + logfile.getName() + "fileLength < filePointer 状态当前长度 < 文件最大长度 ");
              break;
            }
            if (fileLength > filePointer) {
              randomAccessFile.seek(filePointer);
              String line = nextLine(randomAccessFile);
              Object lastDateTime = null;
              Object lastLevel = "info";
              while (line != null) {
            	if (StringUtils.isNotBlank(line)) {
	                //送channal
                  sourceCounter.incrementEventReceivedCount();
                  HashMap<String, Object> body = new HashMap<String, Object>();
                  if (StringUtils.isNotBlank(tagName)) {
                	  body.put("@tagname", tagName);
                  }
                  body.put("@filepath", filepath);
                  body.put("@createdate", FileConstants.DEFAULT_DATE_FORMAT.format(new Date()));
                  body.put("@timestamp", System.currentTimeMillis());
                  body.put("@localHostIp", HostUtils.getLocalHostIp());
                  body.put("@localHostName", HostUtils.getLocalHostName());
                  body.putAll(serializer.getContentBuilder(line));
                  if (!body.containsKey("insert_date")) {
                	  body.put("insert_date", lastDateTime);
                  } else {
                	  lastDateTime = body.get("insert_date");
                  }
                  
                  if (!body.containsKey("level")) {
                	  body.put("level", lastLevel);
                  } else {
                	  lastLevel = body.get("level");
                  }
                  String bodyjson = JSONValue.toJSONString(body);
                  Event oneEvent = EventBuilder.withBody(bodyjson.getBytes(charset));
                  eventList.add(oneEvent);
                  flushEventBatch(eventList,randomAccessFile.getFilePointer());
            	}
                //读下一行
                line = nextLine(randomAccessFile);
              }
              filePointer = randomAccessFile.getFilePointer();
            }
            flushEventBatch(eventList,randomAccessFile.getFilePointer());
            Thread.sleep(this.readinterval);
          }
          
          flushEventBatch(eventList,randomAccessFile.getFilePointer());
        } catch (Exception e) {
          logger.error("Failed while running filpath: " + filepath, e);
          if(e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
        } finally {

          if(randomAccessFile!=null){
            try {
              randomAccessFile.close();
            } catch (IOException ex) {
              logger.error("Failed to close reader for ExecTail source", ex);
            }
          }
        }
        logger.info("=run=> flume tail source run restart:"+restart);
        if(restart) {
          logger.info("=run=> flume tail source run restart time:"+new Date().toString());
          logger.info("Restarting in {}ms", restartThrottle);
          try {
            Thread.sleep(restartThrottle);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        } else {
          logger.info("filepath [" + filepath + "] exited with restart[" + restart+"]");
        }
      } while(restart);
    }
    
    /**
     * 读取文件,并转码utf8
     * 特别要说明一下,iso8859-1 是读取文件的编码
     * @param file
     * @return
     */
    private String nextLine(RandomAccessFile randomAccessFile) {
    	String line = null;
    	try {
			line = randomAccessFile.readLine();
			if (StringUtils.isNotBlank(line))
				return new String(line.getBytes(FileConstants.DEFAULT_READ_FILE_ENCODE),
		         		ExecSourceConfigurationConstants.DEFAULT_CHARSET);
		} catch (IOException e) {
			logger.debug("读取行 异常readLine", e);
		}
    	return line;
    }
    
    /**
     * 批量刷新，并更新状态； 
     * @param eventList
     * @param filePointer
     */
    private  void flushEventBatch(List<Event> eventList, Long filePointer){
//      synchronized(eventList)
      if(eventList.size() >= bufferCount || timeout()) {
	      if ( channelProcessor != null )  
	    	  channelProcessor.processEventBatch(eventList);
	      sourceCounter.addToEventAcceptedCount(eventList.size());
	      eventList.clear();
	      lastPushToChannel = systemClock.currentTimeMillis();
	      sourceHelper.updateStatusFile(filePointer);
	      logger.info("logfilePath:" + logfile.getName() + ":flushEventBatch date sourceCounter.getEventAcceptedCount():" + sourceCounter.getEventAcceptedCount());
      }
    }

    private boolean timeout(){
      return (systemClock.currentTimeMillis() - lastPushToChannel) >= batchTimeout;
    }

    private static String[] formulateShellCommand(String shell, String command) {
      String[] shellArgs = shell.split("\\s+");
      String[] result = new String[shellArgs.length + 1];
      System.arraycopy(shellArgs, 0, result, 0, shellArgs.length);
      result[shellArgs.length] = command;
      return result;
    }

    public int kill() {
      logger.info("=kill=> flume tail source kill start time:"+new Date().toString());
      this.tailing=false;
//        synchronized (this.getClass()) {
//          try {
//            // Stop the Thread that flushes periodically
//            if (future != null) {
//              future.cancel(true);
//            }
//
//            if (timedFlushService != null) {
//              timedFlushService.shutdown();
//              while (!timedFlushService.isTerminated()) {
//                try {
//                  timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
//                } catch (InterruptedException e) {
//                  logger.debug("Interrupted while waiting for ExecTail executor service "
//                          + "to stop. Just exiting.");
//                  Thread.currentThread().interrupt();
//                }
//              }
//            }
//            logger.info("=kill=> flume tail source kill end time:" + new Date().toString());
//            return Integer.MIN_VALUE;
//          } catch (Exception ex) {
//            logger.error("=kill=>", ex);
//            Thread.currentThread().interrupt();
//          }
//        }
      logger.info("=kill=> flume tail source kill end time:"+new Date().toString());
      return Integer.MIN_VALUE / 2;
    }
    
    public void setRestart(boolean restart) {
      this.restart = restart;
    }
  }

}