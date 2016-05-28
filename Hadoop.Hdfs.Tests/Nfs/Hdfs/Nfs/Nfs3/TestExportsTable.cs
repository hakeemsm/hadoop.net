using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Hdfs.Nfs.Mount;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	public class TestExportsTable
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestExportPoint()
		{
			NfsConfiguration config = new NfsConfiguration();
			MiniDFSCluster cluster = null;
			string exportPoint = "/myexport1";
			config.SetStrings(NfsConfigKeys.DfsNfsExportPointKey, exportPoint);
			// Use emphral port in case tests are running in parallel
			config.SetInt("nfs3.mountd.port", 0);
			config.SetInt("nfs3.server.port", 0);
			try
			{
				cluster = new MiniDFSCluster.Builder(config).NumDataNodes(1).Build();
				cluster.WaitActive();
				// Start nfs
				Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3 nfsServer = new Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3
					(config);
				nfsServer.StartServiceInternal(false);
				Mountd mountd = nfsServer.GetMountd();
				RpcProgramMountd rpcMount = (RpcProgramMountd)mountd.GetRpcProgram();
				NUnit.Framework.Assert.IsTrue(rpcMount.GetExports().Count == 1);
				string exportInMountd = rpcMount.GetExports()[0];
				NUnit.Framework.Assert.IsTrue(exportInMountd.Equals(exportPoint));
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
