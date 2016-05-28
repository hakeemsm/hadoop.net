using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress
{
	/// <summary>
	/// Internal data structure used to track progress of a
	/// <see cref="Step"/>
	/// .
	/// </summary>
	internal sealed class StepTracking : AbstractTracking
	{
		internal AtomicLong count = new AtomicLong();

		internal long total = long.MinValue;

		public StepTracking Clone()
		{
			StepTracking clone = new StepTracking();
			base.Copy(clone);
			clone.count = new AtomicLong(count.Get());
			clone.total = total;
			return clone;
		}
	}
}
