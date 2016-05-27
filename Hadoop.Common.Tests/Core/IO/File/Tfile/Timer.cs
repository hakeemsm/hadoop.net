using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>
	/// this class is a time class to
	/// measure to measure the time
	/// taken for some event.
	/// </summary>
	public class Timer
	{
		internal long startTimeEpoch;

		internal long finishTimeEpoch;

		private DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartTime()
		{
			startTimeEpoch = Time.Now();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StopTime()
		{
			finishTimeEpoch = Time.Now();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetIntervalMillis()
		{
			return finishTimeEpoch - startTimeEpoch;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void PrintlnWithTimestamp(string message)
		{
			System.Console.Out.WriteLine(FormatCurrentTime() + "  " + message);
		}

		public virtual string FormatTime(long millis)
		{
			return formatter.Format(millis);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string GetIntervalString()
		{
			long time = GetIntervalMillis();
			return FormatTime(time);
		}

		public virtual string FormatCurrentTime()
		{
			return FormatTime(Time.Now());
		}
	}
}
