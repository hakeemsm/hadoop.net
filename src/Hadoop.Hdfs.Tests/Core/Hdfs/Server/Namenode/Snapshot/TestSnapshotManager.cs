using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Testing snapshot manager functionality.</summary>
	public class TestSnapshotManager
	{
		private const int testMaxSnapshotLimit = 7;

		/// <summary>Test that the global limit on snapshots is honored.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSnapshotLimits()
		{
			// Setup mock objects for SnapshotManager.createSnapshot.
			//
			INodeDirectory ids = Org.Mockito.Mockito.Mock<INodeDirectory>();
			FSDirectory fsdir = Org.Mockito.Mockito.Mock<FSDirectory>();
			INodesInPath iip = Org.Mockito.Mockito.Mock<INodesInPath>();
			SnapshotManager sm = Org.Mockito.Mockito.Spy(new SnapshotManager(fsdir));
			Org.Mockito.Mockito.DoReturn(ids).When(sm).GetSnapshottableRoot((INodesInPath)Matchers.AnyObject
				());
			Org.Mockito.Mockito.DoReturn(testMaxSnapshotLimit).When(sm).GetMaxSnapshotID();
			// Create testMaxSnapshotLimit snapshots. These should all succeed.
			//
			for (int i = 0; i < testMaxSnapshotLimit; ++i)
			{
				sm.CreateSnapshot(iip, "dummy", i.ToString());
			}
			// Attempt to create one more snapshot. This should fail due to snapshot
			// ID rollover.
			//
			try
			{
				sm.CreateSnapshot(iip, "dummy", "shouldFailSnapshot");
				NUnit.Framework.Assert.Fail("Expected SnapshotException not thrown");
			}
			catch (SnapshotException se)
			{
				NUnit.Framework.Assert.IsTrue(StringUtils.ToLowerCase(se.Message).Contains("rollover"
					));
			}
			// Delete a snapshot to free up a slot.
			//
			sm.DeleteSnapshot(iip, string.Empty, Org.Mockito.Mockito.Mock<INode.BlocksMapUpdateInfo
				>(), new AList<INode>());
			// Attempt to create a snapshot again. It should still fail due
			// to snapshot ID rollover.
			//
			try
			{
				sm.CreateSnapshot(iip, "dummy", "shouldFailSnapshot2");
				NUnit.Framework.Assert.Fail("Expected SnapshotException not thrown");
			}
			catch (SnapshotException se)
			{
				NUnit.Framework.Assert.IsTrue(StringUtils.ToLowerCase(se.Message).Contains("rollover"
					));
			}
		}
	}
}
