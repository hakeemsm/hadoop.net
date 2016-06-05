using System.Collections.Generic;
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
	/// <summary>Test READDIR and READDIRPLUS request with zero, nonzero cookies</summary>
	public class TestReaddir
	{
		internal static NfsConfiguration config = new NfsConfiguration();

		internal static MiniDFSCluster cluster = null;

		internal static DistributedFileSystem hdfs;

		internal static NameNode nn;

		internal static RpcProgramNfs3 nfsd;

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
			// Use emphral port in case tests are running in parallel
			config.SetInt("nfs3.mountd.port", 0);
			config.SetInt("nfs3.server.port", 0);
			// Start nfs
			Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3 nfs3 = new Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3
				(config);
			nfs3.StartServiceInternal(false);
			nfsd = (RpcProgramNfs3)nfs3.GetRpcProgram();
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
			DFSTestUtil.CreateFile(hdfs, new Path(testdir + "/f2"), 0, (short)1, 0);
			DFSTestUtil.CreateFile(hdfs, new Path(testdir + "/f3"), 0, (short)1, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReaddirBasic()
		{
			// Get inodeId of /tmp
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			// Create related part of the XDR request
			XDR xdr_req = new XDR();
			FileHandle handle = new FileHandle(dirId);
			handle.Serialize(xdr_req);
			xdr_req.WriteLongAsHyper(0);
			// cookie
			xdr_req.WriteLongAsHyper(0);
			// verifier
			xdr_req.WriteInt(100);
			// count
			READDIR3Response response = nfsd.Readdir(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			IList<READDIR3Response.Entry3> dirents = response.GetDirList().GetEntries();
			NUnit.Framework.Assert.IsTrue(dirents.Count == 5);
			// inculding dot, dotdot
			// Test start listing from f2
			status = nn.GetRpcServer().GetFileInfo(testdir + "/f2");
			long f2Id = status.GetFileId();
			// Create related part of the XDR request
			xdr_req = new XDR();
			handle = new FileHandle(dirId);
			handle.Serialize(xdr_req);
			xdr_req.WriteLongAsHyper(f2Id);
			// cookie
			xdr_req.WriteLongAsHyper(0);
			// verifier
			xdr_req.WriteInt(100);
			// count
			response = nfsd.Readdir(xdr_req.AsReadOnlyWrap(), securityHandler, new IPEndPoint
				("localhost", 1234));
			dirents = response.GetDirList().GetEntries();
			NUnit.Framework.Assert.IsTrue(dirents.Count == 1);
			READDIR3Response.Entry3 entry = dirents[0];
			NUnit.Framework.Assert.IsTrue(entry.GetName().Equals("f3"));
			// When the cookie is deleted, list starts over no including dot, dotdot
			hdfs.Delete(new Path(testdir + "/f2"), false);
			response = nfsd.Readdir(xdr_req.AsReadOnlyWrap(), securityHandler, new IPEndPoint
				("localhost", 1234));
			dirents = response.GetDirList().GetEntries();
			NUnit.Framework.Assert.IsTrue(dirents.Count == 2);
		}

		// No dot, dotdot
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReaddirPlus()
		{
			// Test readdirplus
			// Get inodeId of /tmp
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			// Create related part of the XDR request
			XDR xdr_req = new XDR();
			FileHandle handle = new FileHandle(dirId);
			handle.Serialize(xdr_req);
			xdr_req.WriteLongAsHyper(0);
			// cookie
			xdr_req.WriteLongAsHyper(0);
			// verifier
			xdr_req.WriteInt(100);
			// dirCount
			xdr_req.WriteInt(1000);
			// maxCount
			READDIRPLUS3Response responsePlus = nfsd.Readdirplus(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			IList<READDIRPLUS3Response.EntryPlus3> direntPlus = responsePlus.GetDirListPlus()
				.GetEntries();
			NUnit.Framework.Assert.IsTrue(direntPlus.Count == 5);
			// including dot, dotdot
			// Test start listing from f2
			status = nn.GetRpcServer().GetFileInfo(testdir + "/f2");
			long f2Id = status.GetFileId();
			// Create related part of the XDR request
			xdr_req = new XDR();
			handle = new FileHandle(dirId);
			handle.Serialize(xdr_req);
			xdr_req.WriteLongAsHyper(f2Id);
			// cookie
			xdr_req.WriteLongAsHyper(0);
			// verifier
			xdr_req.WriteInt(100);
			// dirCount
			xdr_req.WriteInt(1000);
			// maxCount
			responsePlus = nfsd.Readdirplus(xdr_req.AsReadOnlyWrap(), securityHandler, new IPEndPoint
				("localhost", 1234));
			direntPlus = responsePlus.GetDirListPlus().GetEntries();
			NUnit.Framework.Assert.IsTrue(direntPlus.Count == 1);
			READDIRPLUS3Response.EntryPlus3 entryPlus = direntPlus[0];
			NUnit.Framework.Assert.IsTrue(entryPlus.GetName().Equals("f3"));
			// When the cookie is deleted, list starts over no including dot, dotdot
			hdfs.Delete(new Path(testdir + "/f2"), false);
			responsePlus = nfsd.Readdirplus(xdr_req.AsReadOnlyWrap(), securityHandler, new IPEndPoint
				("localhost", 1234));
			direntPlus = responsePlus.GetDirListPlus().GetEntries();
			NUnit.Framework.Assert.IsTrue(direntPlus.Count == 2);
		}
		// No dot, dotdot
	}
}
