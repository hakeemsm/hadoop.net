using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>
	/// Implementation of
	/// <see cref="Clock"/>
	/// that gives the current time from the system
	/// clock in milliseconds.
	/// </summary>
	public class SystemClock : Clock
	{
		public virtual long GetTime()
		{
			return Runtime.CurrentTimeMillis();
		}
	}
}
