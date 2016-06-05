using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <p><code>YarnClusterMetrics</code> represents cluster metrics.</p>
	/// <p>Currently only number of <code>NodeManager</code>s is provided.</p>
	/// </summary>
	public abstract class YarnClusterMetrics
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static YarnClusterMetrics NewInstance(int numNodeManagers)
		{
			YarnClusterMetrics metrics = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<YarnClusterMetrics
				>();
			metrics.SetNumNodeManagers(numNodeManagers);
			return metrics;
		}

		/// <summary>Get the number of <code>NodeManager</code>s in the cluster.</summary>
		/// <returns>number of <code>NodeManager</code>s in the cluster</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetNumNodeManagers();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNumNodeManagers(int numNodeManagers);
	}
}
