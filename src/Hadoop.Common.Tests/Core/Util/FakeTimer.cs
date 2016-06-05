

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// FakeTimer can be used for test purposes to control the return values
	/// from {
	/// <see cref="Timer"/>
	/// }.
	/// </summary>
	public class FakeTimer : Timer
	{
		private long nowMillis;

		/// <summary>Constructs a FakeTimer with a non-zero value</summary>
		public FakeTimer()
		{
			nowMillis = 1000;
		}

		// Initialize with a non-trivial value.
		public override long Now()
		{
			return nowMillis;
		}

		public override long MonotonicNow()
		{
			return nowMillis;
		}

		/// <summary>Increases the time by milliseconds</summary>
		public virtual void Advance(long advMillis)
		{
			nowMillis += advMillis;
		}
	}
}
