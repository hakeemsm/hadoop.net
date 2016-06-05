using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestClusterStatus
	{
		private ClusterStatus clusterStatus = new ClusterStatus();

		public virtual void TestGraylistedTrackers()
		{
			NUnit.Framework.Assert.AreEqual(0, clusterStatus.GetGraylistedTrackers());
			NUnit.Framework.Assert.IsTrue(clusterStatus.GetGraylistedTrackerNames().IsEmpty()
				);
		}

		public virtual void TestJobTrackerState()
		{
			NUnit.Framework.Assert.AreEqual(JobTracker.State.Running, clusterStatus.GetJobTrackerState
				());
		}
	}
}
