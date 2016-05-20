using Sharpen;

namespace org.apache.hadoop.http
{
	/// <summary>RequestLog object for use with Http</summary>
	public class HttpRequestLog
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.HttpRequestLog
			)));

		private static readonly System.Collections.Generic.Dictionary<string, string> serverToComponent;

		static HttpRequestLog()
		{
			serverToComponent = new System.Collections.Generic.Dictionary<string, string>();
			serverToComponent["cluster"] = "resourcemanager";
			serverToComponent["hdfs"] = "namenode";
			serverToComponent["node"] = "nodemanager";
		}

		public static org.mortbay.jetty.RequestLog getRequestLog(string name)
		{
			string lookup = serverToComponent[name];
			if (lookup != null)
			{
				name = lookup;
			}
			string loggerName = "http.requests." + name;
			string appenderName = name + "requestlog";
			org.apache.commons.logging.Log logger = org.apache.commons.logging.LogFactory.getLog
				(loggerName);
			bool isLog4JLogger;
			try
			{
				isLog4JLogger = logger is org.apache.commons.logging.impl.Log4JLogger;
			}
			catch (java.lang.NoClassDefFoundError err)
			{
				// In some dependent projects, log4j may not even be on the classpath at
				// runtime, in which case the above instanceof check will throw
				// NoClassDefFoundError.
				LOG.debug("Could not load Log4JLogger class", err);
				isLog4JLogger = false;
			}
			if (isLog4JLogger)
			{
				org.apache.commons.logging.impl.Log4JLogger httpLog4JLog = (org.apache.commons.logging.impl.Log4JLogger
					)logger;
				org.apache.log4j.Logger httpLogger = httpLog4JLog.getLogger();
				org.apache.log4j.Appender appender = null;
				try
				{
					appender = httpLogger.getAppender(appenderName);
				}
				catch (org.apache.commons.logging.LogConfigurationException e)
				{
					LOG.warn("Http request log for " + loggerName + " could not be created");
					throw;
				}
				if (appender == null)
				{
					LOG.info("Http request log for " + loggerName + " is not defined");
					return null;
				}
				if (appender is org.apache.hadoop.http.HttpRequestLogAppender)
				{
					org.apache.hadoop.http.HttpRequestLogAppender requestLogAppender = (org.apache.hadoop.http.HttpRequestLogAppender
						)appender;
					org.mortbay.jetty.NCSARequestLog requestLog = new org.mortbay.jetty.NCSARequestLog
						();
					requestLog.setFilename(requestLogAppender.getFilename());
					requestLog.setRetainDays(requestLogAppender.getRetainDays());
					return requestLog;
				}
				else
				{
					LOG.warn("Jetty request log for " + loggerName + " was of the wrong class");
					return null;
				}
			}
			else
			{
				LOG.warn("Jetty request log can only be enabled using Log4j");
				return null;
			}
		}
	}
}
