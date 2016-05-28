using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary><code>JobTracker</code> is no longer used since M/R 2.x.</summary>
	/// <remarks>
	/// <code>JobTracker</code> is no longer used since M/R 2.x. This is a dummy
	/// JobTracker class, which is used to be compatible with M/R 1.x applications.
	/// </remarks>
	public class JobTracker
	{
		/// <summary><code>State</code> is no longer used since M/R 2.x.</summary>
		/// <remarks>
		/// <code>State</code> is no longer used since M/R 2.x. It is kept in case
		/// that M/R 1.x applications may still use it.
		/// </remarks>
		public enum State
		{
			Initializing,
			Running
		}
	}
}
