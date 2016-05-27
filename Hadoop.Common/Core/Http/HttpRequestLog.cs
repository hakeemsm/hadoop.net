using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Log4j;
using Org.Mortbay.Jetty;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	/// <summary>RequestLog object for use with Http</summary>
	public class HttpRequestLog
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(HttpRequestLog));

		private static readonly Dictionary<string, string> serverToComponent;

		static HttpRequestLog()
		{
			serverToComponent = new Dictionary<string, string>();
			serverToComponent["cluster"] = "resourcemanager";
			serverToComponent["hdfs"] = "namenode";
			serverToComponent["node"] = "nodemanager";
		}

		public static RequestLog GetRequestLog(string name)
		{
			string lookup = serverToComponent[name];
			if (lookup != null)
			{
				name = lookup;
			}
			string loggerName = "http.requests." + name;
			string appenderName = name + "requestlog";
			Log logger = LogFactory.GetLog(loggerName);
			bool isLog4JLogger;
			try
			{
				isLog4JLogger = logger is Log4JLogger;
			}
			catch (NoClassDefFoundError err)
			{
				// In some dependent projects, log4j may not even be on the classpath at
				// runtime, in which case the above instanceof check will throw
				// NoClassDefFoundError.
				Log.Debug("Could not load Log4JLogger class", err);
				isLog4JLogger = false;
			}
			if (isLog4JLogger)
			{
				Log4JLogger httpLog4JLog = (Log4JLogger)logger;
				Logger httpLogger = httpLog4JLog.GetLogger();
				Appender appender = null;
				try
				{
					appender = httpLogger.GetAppender(appenderName);
				}
				catch (LogConfigurationException e)
				{
					Log.Warn("Http request log for " + loggerName + " could not be created");
					throw;
				}
				if (appender == null)
				{
					Log.Info("Http request log for " + loggerName + " is not defined");
					return null;
				}
				if (appender is HttpRequestLogAppender)
				{
					HttpRequestLogAppender requestLogAppender = (HttpRequestLogAppender)appender;
					NCSARequestLog requestLog = new NCSARequestLog();
					requestLog.SetFilename(requestLogAppender.GetFilename());
					requestLog.SetRetainDays(requestLogAppender.GetRetainDays());
					return requestLog;
				}
				else
				{
					Log.Warn("Jetty request log for " + loggerName + " was of the wrong class");
					return null;
				}
			}
			else
			{
				Log.Warn("Jetty request log can only be enabled using Log4j");
				return null;
			}
		}
	}
}
