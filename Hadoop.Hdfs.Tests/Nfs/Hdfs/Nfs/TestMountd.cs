using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Hdfs.Nfs.Mount;
using Org.Apache.Hadoop.Hdfs.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs
{
	public class TestMountd
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestMountd));

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStart()
		{
			// Start minicluster
			NfsConfiguration config = new NfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(config).NumDataNodes(1).Build
				();
			cluster.WaitActive();
			// Use emphral port in case tests are running in parallel
			config.SetInt("nfs3.mountd.port", 0);
			config.SetInt("nfs3.server.port", 0);
			// Start nfs
			Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3 nfs3 = new Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3
				(config);
			nfs3.StartServiceInternal(false);
			RpcProgramMountd mountd = (RpcProgramMountd)nfs3.GetMountd().GetRpcProgram();
			mountd.NullOp(new XDR(), 1234, Sharpen.Extensions.GetAddressByName("localhost"));
			RpcProgramNfs3 nfsd = (RpcProgramNfs3)nfs3.GetRpcProgram();
			nfsd.NullProcedure();
			cluster.Shutdown();
		}
	}
}
