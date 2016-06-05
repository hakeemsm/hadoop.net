using System;
using Org.Apache.Commons.Logging;
using Org.Slf4j;


namespace Org.Apache.Hadoop.Util
{
	internal class LogAdapter
	{
		private Log Log;

		private Logger Logger;

		private LogAdapter(Log Log)
		{
			this.Log = Log;
		}

		private LogAdapter(Logger Logger)
		{
			this.Logger = Logger;
		}

		public static Org.Apache.Hadoop.Util.LogAdapter Create(Log Log)
		{
			return new Org.Apache.Hadoop.Util.LogAdapter(Log);
		}

		public static Org.Apache.Hadoop.Util.LogAdapter Create(Logger Logger)
		{
			return new Org.Apache.Hadoop.Util.LogAdapter(Logger);
		}

		public virtual void Info(string msg)
		{
			if (Log != null)
			{
				Log.Info(msg);
			}
			else
			{
				if (Logger != null)
				{
					Logger.Info(msg);
				}
			}
		}

		public virtual void Warn(string msg, Exception t)
		{
			if (Log != null)
			{
				Log.Warn(msg, t);
			}
			else
			{
				if (Logger != null)
				{
					Logger.Warn(msg, t);
				}
			}
		}

		public virtual void Debug(Exception t)
		{
			if (Log != null)
			{
				Log.Debug(t);
			}
			else
			{
				if (Logger != null)
				{
					Logger.Debug(string.Empty, t);
				}
			}
		}

		public virtual void Error(string msg)
		{
			if (Log != null)
			{
				Log.Error(msg);
			}
			else
			{
				if (Logger != null)
				{
					Logger.Error(msg);
				}
			}
		}
	}
}
