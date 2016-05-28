using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>Tests to verify the behavior of failing to fully start transition HA states.
	/// 	</summary>
	public class TestStateTransitionFailure
	{
		/// <summary>
		/// Ensure that a failure to fully transition to the active state causes a
		/// shutdown of the NameNode.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureToTransitionCausesShutdown()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				// Set an illegal value for the trash emptier interval. This will cause
				// the NN to fail to transition to the active state.
				conf.SetLong(CommonConfigurationKeys.FsTrashIntervalKey, -1);
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
					()).NumDataNodes(0).CheckExitOnShutdown(false).Build();
				cluster.WaitActive();
				try
				{
					cluster.TransitionToActive(0);
					NUnit.Framework.Assert.Fail("Transitioned to active but should not have been able to."
						);
				}
				catch (ExitUtil.ExitException ee)
				{
					GenericTestUtils.AssertExceptionContains("Cannot start trash emptier with negative interval"
						, ee);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
