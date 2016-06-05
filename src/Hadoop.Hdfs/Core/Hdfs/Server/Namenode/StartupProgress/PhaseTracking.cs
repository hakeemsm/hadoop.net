using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress
{
	/// <summary>
	/// Internal data structure used to track progress of a
	/// <see cref="Phase"/>
	/// .
	/// </summary>
	internal sealed class PhaseTracking : AbstractTracking
	{
		internal string file;

		internal long size = long.MinValue;

		internal readonly ConcurrentMap<Step, StepTracking> steps = new ConcurrentHashMap
			<Step, StepTracking>();

		public PhaseTracking Clone()
		{
			PhaseTracking clone = new PhaseTracking();
			base.Copy(clone);
			clone.file = file;
			clone.size = size;
			foreach (KeyValuePair<Step, StepTracking> entry in steps)
			{
				clone.steps[entry.Key] = entry.Value.Clone();
			}
			return clone;
		}
	}
}
