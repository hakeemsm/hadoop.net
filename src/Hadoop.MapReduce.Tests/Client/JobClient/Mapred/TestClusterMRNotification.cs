using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Tests Job end notification in cluster mode.</summary>
	public class TestClusterMRNotification : NotificationTestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public TestClusterMRNotification()
			: base(HadoopTestCase.ClusterMr)
		{
		}
	}
}
