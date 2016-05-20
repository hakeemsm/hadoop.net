using Sharpen;

namespace org.apache.hadoop.io.file.tfile
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

		private java.text.DateFormat formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss"
			);

		/// <exception cref="System.IO.IOException"/>
		public virtual void startTime()
		{
			startTimeEpoch = org.apache.hadoop.util.Time.now();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void stopTime()
		{
			finishTimeEpoch = org.apache.hadoop.util.Time.now();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long getIntervalMillis()
		{
			return finishTimeEpoch - startTimeEpoch;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void printlnWithTimestamp(string message)
		{
			System.Console.Out.WriteLine(formatCurrentTime() + "  " + message);
		}

		public virtual string formatTime(long millis)
		{
			return formatter.format(millis);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string getIntervalString()
		{
			long time = getIntervalMillis();
			return formatTime(time);
		}

		public virtual string formatCurrentTime()
		{
			return formatTime(org.apache.hadoop.util.Time.now());
		}
	}
}
