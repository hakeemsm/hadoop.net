using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A clock class - can be mocked out for testing.</summary>
	internal class Clock
	{
		internal virtual long GetTime()
		{
			return Runtime.CurrentTimeMillis();
		}
	}
}
