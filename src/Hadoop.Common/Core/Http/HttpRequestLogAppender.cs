using Org.Apache.Log4j;
using Org.Apache.Log4j.Spi;


namespace Org.Apache.Hadoop.Http
{
	/// <summary>Log4j Appender adapter for HttpRequestLog</summary>
	public class HttpRequestLogAppender : AppenderSkeleton
	{
		private string filename;

		private int retainDays;

		public HttpRequestLogAppender()
		{
		}

		public virtual void SetRetainDays(int retainDays)
		{
			this.retainDays = retainDays;
		}

		public virtual int GetRetainDays()
		{
			return retainDays;
		}

		public virtual void SetFilename(string filename)
		{
			this.filename = filename;
		}

		public virtual string GetFilename()
		{
			return filename;
		}

		protected override void Append(LoggingEvent @event)
		{
		}

		public override void Close()
		{
		}

		public override bool RequiresLayout()
		{
			return false;
		}
	}
}
