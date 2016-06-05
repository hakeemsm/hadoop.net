using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestLeaseManager
	{
		internal readonly Configuration conf = new HdfsConfiguration();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveLeaseWithPrefixPath()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			cluster.WaitActive();
			LeaseManager lm = NameNodeAdapter.GetLeaseManager(cluster.GetNamesystem());
			lm.AddLease("holder1", "/a/b");
			lm.AddLease("holder2", "/a/c");
			NUnit.Framework.Assert.IsNotNull(lm.GetLeaseByPath("/a/b"));
			NUnit.Framework.Assert.IsNotNull(lm.GetLeaseByPath("/a/c"));
			lm.RemoveLeaseWithPrefixPath("/a");
			NUnit.Framework.Assert.IsNull(lm.GetLeaseByPath("/a/b"));
			NUnit.Framework.Assert.IsNull(lm.GetLeaseByPath("/a/c"));
			lm.AddLease("holder1", "/a/b");
			lm.AddLease("holder2", "/a/c");
			lm.RemoveLeaseWithPrefixPath("/a/");
			NUnit.Framework.Assert.IsNull(lm.GetLeaseByPath("/a/b"));
			NUnit.Framework.Assert.IsNull(lm.GetLeaseByPath("/a/c"));
		}

		/// <summary>
		/// Check that even if LeaseManager.checkLease is not able to relinquish
		/// leases, the Namenode does't enter an infinite loop while holding the FSN
		/// write lock and thus become unresponsive
		/// </summary>
		public virtual void TestCheckLeaseNotInfiniteLoop()
		{
			FSDirectory dir = Org.Mockito.Mockito.Mock<FSDirectory>();
			FSNamesystem fsn = Org.Mockito.Mockito.Mock<FSNamesystem>();
			Org.Mockito.Mockito.When(fsn.IsRunning()).ThenReturn(true);
			Org.Mockito.Mockito.When(fsn.HasWriteLock()).ThenReturn(true);
			Org.Mockito.Mockito.When(fsn.GetFSDirectory()).ThenReturn(dir);
			LeaseManager lm = new LeaseManager(fsn);
			//Make sure the leases we are going to add exceed the hard limit
			lm.SetLeasePeriod(0, 0);
			//Add some leases to the LeaseManager
			lm.AddLease("holder1", "src1");
			lm.AddLease("holder2", "src2");
			lm.AddLease("holder3", "src3");
			NUnit.Framework.Assert.AreEqual(lm.GetNumSortedLeases(), 3);
			//Initiate a call to checkLease. This should exit within the test timeout
			lm.CheckLeases();
		}
	}
}
