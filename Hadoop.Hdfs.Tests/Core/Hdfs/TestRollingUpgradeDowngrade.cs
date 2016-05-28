using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestRollingUpgradeDowngrade
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestDowngrade()
		{
			Configuration conf = new HdfsConfiguration();
			MiniQJMHACluster cluster = null;
			Path foo = new Path("/foo");
			Path bar = new Path("/bar");
			try
			{
				cluster = new MiniQJMHACluster.Builder(conf).Build();
				MiniDFSCluster dfsCluster = cluster.GetDfsCluster();
				dfsCluster.WaitActive();
				// let NN1 tail editlog every 1s
				dfsCluster.GetConfiguration(1).SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
				dfsCluster.RestartNameNode(1);
				dfsCluster.TransitionToActive(0);
				DistributedFileSystem dfs = dfsCluster.GetFileSystem(0);
				dfs.Mkdirs(foo);
				// start rolling upgrade
				RollingUpgradeInfo info = dfs.RollingUpgrade(HdfsConstants.RollingUpgradeAction.Prepare
					);
				NUnit.Framework.Assert.IsTrue(info.IsStarted());
				dfs.Mkdirs(bar);
				TestRollingUpgrade.QueryForPreparation(dfs);
				dfs.Close();
				dfsCluster.RestartNameNode(0, true, "-rollingUpgrade", "downgrade");
				// Once downgraded, there should be no more fsimage for rollbacks.
				NUnit.Framework.Assert.IsFalse(dfsCluster.GetNamesystem(0).GetFSImage().HasRollbackFSImage
					());
				// shutdown NN1
				dfsCluster.ShutdownNameNode(1);
				dfsCluster.TransitionToActive(0);
				dfs = dfsCluster.GetFileSystem(0);
				NUnit.Framework.Assert.IsTrue(dfs.Exists(foo));
				NUnit.Framework.Assert.IsTrue(dfs.Exists(bar));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Ensure that during downgrade the NN fails to load a fsimage with newer
		/// format.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRejectNewFsImage()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				DistributedFileSystem fs = cluster.GetFileSystem();
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				fs.SaveNamespace();
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
				NNStorage storage = Org.Mockito.Mockito.Spy(cluster.GetNameNode().GetFSImage().GetStorage
					());
				int futureVersion = NameNodeLayoutVersion.CurrentLayoutVersion - 1;
				Org.Mockito.Mockito.DoReturn(futureVersion).When(storage).GetServiceLayoutVersion
					();
				storage.WriteAll();
				cluster.RestartNameNode(0, true, "-rollingUpgrade", "downgrade");
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
