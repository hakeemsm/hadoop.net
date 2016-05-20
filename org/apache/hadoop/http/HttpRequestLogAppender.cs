using Sharpen;

namespace org.apache.hadoop.http
{
	/// <summary>Log4j Appender adapter for HttpRequestLog</summary>
	public class HttpRequestLogAppender : org.apache.log4j.AppenderSkeleton
	{
		private string filename;

		private int retainDays;

		public HttpRequestLogAppender()
		{
		}

		public virtual void setRetainDays(int retainDays)
		{
			this.retainDays = retainDays;
		}

		public virtual int getRetainDays()
		{
			return retainDays;
		}

		public virtual void setFilename(string filename)
		{
			this.filename = filename;
		}

		public virtual string getFilename()
		{
			return filename;
		}

		protected override void append(org.apache.log4j.spi.LoggingEvent @event)
		{
		}

		public override void close()
		{
		}

		public override bool requiresLayout()
		{
			return false;
		}
	}
}
