using System;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Log4j;
using Org.Apache.Log4j.Spi;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>Event log used by the fair scheduler for machine-readable debug info.</summary>
	/// <remarks>
	/// Event log used by the fair scheduler for machine-readable debug info.
	/// This class uses a log4j rolling file appender to write the log, but uses
	/// a custom tab-separated event format of the form:
	/// <pre>
	/// DATE    EVENT_TYPE   PARAM_1   PARAM_2   ...
	/// </pre>
	/// Various event types are used by the fair scheduler. The purpose of logging
	/// in this format is to enable tools to parse the history log easily and read
	/// internal scheduler variables, rather than trying to make the log human
	/// readable. The fair scheduler also logs human readable messages in the
	/// JobTracker's main log.
	/// Constructing this class creates a disabled log. It must be initialized
	/// using
	/// <see cref="Init(FairSchedulerConfiguration)"/>
	/// to begin
	/// writing to the file.
	/// </remarks>
	internal class FairSchedulerEventLog
	{
		private static readonly Org.Apache.Commons.Logging.Log Log = LogFactory.GetLog(typeof(
			FairSchedulerEventLog).FullName);

		/// <summary>Set to true if logging is disabled due to an error.</summary>
		private bool logDisabled = true;

		/// <summary>
		/// Log directory, set by mapred.fairscheduler.eventlog.location in conf file;
		/// defaults to {hadoop.log.dir}/fairscheduler.
		/// </summary>
		private string logDir;

		/// <summary>Active log file, which is {LOG_DIR}/hadoop-{user}-fairscheduler.log.</summary>
		/// <remarks>
		/// Active log file, which is {LOG_DIR}/hadoop-{user}-fairscheduler.log.
		/// Older files are also stored as {LOG_FILE}.date (date format YYYY-MM-DD).
		/// </remarks>
		private string logFile;

		/// <summary>Log4j appender used to write to the log file</summary>
		private DailyRollingFileAppender appender;

		internal virtual bool Init(FairSchedulerConfiguration conf)
		{
			if (conf.IsEventLogEnabled())
			{
				try
				{
					logDir = conf.GetEventlogDir();
					FilePath logDirFile = new FilePath(logDir);
					if (!logDirFile.Exists())
					{
						if (!logDirFile.Mkdirs())
						{
							throw new IOException("Mkdirs failed to create " + logDirFile.ToString());
						}
					}
					string username = Runtime.GetProperty("user.name");
					logFile = string.Format("%s%shadoop-%s-fairscheduler.log", logDir, FilePath.separator
						, username);
					logDisabled = false;
					PatternLayout layout = new PatternLayout("%d{ISO8601}\t%m%n");
					appender = new DailyRollingFileAppender(layout, logFile, "'.'yyyy-MM-dd");
					appender.ActivateOptions();
					Log.Info("Initialized fair scheduler event log, logging to " + logFile);
				}
				catch (IOException e)
				{
					Log.Error("Failed to initialize fair scheduler event log. Disabling it.", e);
					logDisabled = true;
				}
			}
			else
			{
				logDisabled = true;
			}
			return !(logDisabled);
		}

		/// <summary>
		/// Log an event, writing a line in the log file of the form
		/// <pre>
		/// DATE    EVENT_TYPE   PARAM_1   PARAM_2   ...
		/// </summary>
		/// <remarks>
		/// Log an event, writing a line in the log file of the form
		/// <pre>
		/// DATE    EVENT_TYPE   PARAM_1   PARAM_2   ...
		/// </pre>
		/// </remarks>
		internal virtual void Log(string eventType, params object[] @params)
		{
			lock (this)
			{
				try
				{
					if (logDisabled)
					{
						return;
					}
					StringBuilder buffer = new StringBuilder();
					buffer.Append(eventType);
					foreach (object param in @params)
					{
						buffer.Append("\t");
						buffer.Append(param);
					}
					string message = buffer.ToString();
					Logger logger = Logger.GetLogger(GetType());
					appender.Append(new LoggingEvent(string.Empty, logger, Level.Info, message, null)
						);
				}
				catch (Exception e)
				{
					Log.Error("Failed to append to fair scheduler event log", e);
					logDisabled = true;
				}
			}
		}

		/// <summary>Flush and close the log.</summary>
		internal virtual void Shutdown()
		{
			lock (this)
			{
				try
				{
					if (appender != null)
					{
						appender.Close();
					}
				}
				catch (Exception e)
				{
					Log.Error("Failed to close fair scheduler event log", e);
					logDisabled = true;
				}
			}
		}

		internal virtual bool IsEnabled()
		{
			lock (this)
			{
				return !logDisabled;
			}
		}

		public virtual string GetLogFile()
		{
			return logFile;
		}
	}
}
