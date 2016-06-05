using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Nfs.Nfs3.Response;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	public class TestClientAccessPrivilege
	{
		internal static MiniDFSCluster cluster = null;

		internal static NfsConfiguration config = new NfsConfiguration();

		internal static DistributedFileSystem hdfs;

		internal static NameNode nn;

		internal static string testdir = "/tmp";

		internal static SecurityHandler securityHandler;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			string currentUser = Runtime.GetProperty("user.name");
			config.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(currentUser), "*");
			config.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(currentUser), "*");
			ProxyUsers.RefreshSuperUserGroupsConfiguration(config);
			cluster = new MiniDFSCluster.Builder(config).NumDataNodes(1).Build();
			cluster.WaitActive();
			hdfs = cluster.GetFileSystem();
			nn = cluster.GetNameNode();
			// Use ephemeral port in case tests are running in parallel
			config.SetInt("nfs3.mountd.port", 0);
			config.SetInt("nfs3.server.port", 0);
			securityHandler = Org.Mockito.Mockito.Mock<SecurityHandler>();
			Org.Mockito.Mockito.When(securityHandler.GetUser()).ThenReturn(Runtime.GetProperty
				("user.name"));
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Shutdown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.ArgumentException"/>
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void CreateFiles()
		{
			hdfs.Delete(new Path(testdir), true);
			hdfs.Mkdirs(new Path(testdir));
			DFSTestUtil.CreateFile(hdfs, new Path(testdir + "/f1"), 0, (short)1, 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestClientAccessPrivilegeForRemove()
		{
			// Configure ro access for nfs1 service
			config.Set("dfs.nfs.exports.allowed.hosts", "* ro");
			// Start nfs
			Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3 nfs = new Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3
				(config);
			nfs.StartServiceInternal(false);
			RpcProgramNfs3 nfsd = (RpcProgramNfs3)nfs.GetRpcProgram();
			// Create a remove request
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			XDR xdr_req = new XDR();
			FileHandle handle = new FileHandle(dirId);
			handle.Serialize(xdr_req);
			xdr_req.WriteString("f1");
			// Remove operation
			REMOVE3Response response = nfsd.Remove(xdr_req.AsReadOnlyWrap(), securityHandler, 
				new IPEndPoint("localhost", 1234));
			// Assert on return code
			NUnit.Framework.Assert.AreEqual("Incorrect return code", Nfs3Status.Nfs3errAcces, 
				response.GetStatus());
		}
	}
}
