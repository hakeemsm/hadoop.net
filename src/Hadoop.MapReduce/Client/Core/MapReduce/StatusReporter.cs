using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public abstract class StatusReporter
	{
		public abstract Counter GetCounter<_T0>(Enum<_T0> name)
			where _T0 : Enum<E>;

		public abstract Counter GetCounter(string group, string name);

		public abstract void Progress();

		/// <summary>Get the current progress.</summary>
		/// <returns>
		/// a number between 0.0 and 1.0 (inclusive) indicating the attempt's
		/// progress.
		/// </returns>
		public abstract float GetProgress();

		public abstract void SetStatus(string status);
	}
}
