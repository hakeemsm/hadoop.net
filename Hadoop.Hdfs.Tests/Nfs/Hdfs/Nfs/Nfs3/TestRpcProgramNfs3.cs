using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Nfs.Nfs3.Request;
using Org.Apache.Hadoop.Nfs.Nfs3.Response;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>
	/// Tests for
	/// <see cref="RpcProgramNfs3"/>
	/// </summary>
	public class TestRpcProgramNfs3
	{
		internal static DistributedFileSystem hdfs;

		internal static MiniDFSCluster cluster = null;

		internal static NfsConfiguration config = new NfsConfiguration();

		internal static HdfsAdmin dfsAdmin;

		internal static NameNode nn;

		internal static Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3 nfs;

		internal static RpcProgramNfs3 nfsd;

		internal static SecurityHandler securityHandler;

		internal static SecurityHandler securityHandlerUnpriviledged;

		internal static string testdir = "/tmp";

		private const string TestKey = "test_key";

		private static FileSystemTestHelper fsHelper;

		private static FilePath testRootDir;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			string currentUser = Runtime.GetProperty("user.name");
			config.Set("fs.permissions.umask-mode", "u=rwx,g=,o=");
			config.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(currentUser), "*");
			config.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(currentUser), "*");
			fsHelper = new FileSystemTestHelper();
			// Set up java key store
			string testRoot = fsHelper.GetTestRootDir();
			testRootDir = new FilePath(testRoot).GetAbsoluteFile();
			Path jksPath = new Path(testRootDir.ToString(), "test.jks");
			config.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, JavaKeyStoreProvider.SchemeName
				 + "://file" + jksPath.ToUri());
			ProxyUsers.RefreshSuperUserGroupsConfiguration(config);
			cluster = new MiniDFSCluster.Builder(config).NumDataNodes(1).Build();
			cluster.WaitActive();
			hdfs = cluster.GetFileSystem();
			nn = cluster.GetNameNode();
			dfsAdmin = new HdfsAdmin(cluster.GetURI(), config);
			// Use ephemeral ports in case tests are running in parallel
			config.SetInt("nfs3.mountd.port", 0);
			config.SetInt("nfs3.server.port", 0);
			// Start NFS with allowed.hosts set to "* rw"
			config.Set("dfs.nfs.exports.allowed.hosts", "* rw");
			nfs = new Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3(config);
			nfs.StartServiceInternal(false);
			nfsd = (RpcProgramNfs3)nfs.GetRpcProgram();
			hdfs.GetClient().SetKeyProvider(nn.GetNamesystem().GetProvider());
			DFSTestUtil.CreateKey(TestKey, cluster, config);
			// Mock SecurityHandler which returns system user.name
			securityHandler = Org.Mockito.Mockito.Mock<SecurityHandler>();
			Org.Mockito.Mockito.When(securityHandler.GetUser()).ThenReturn(currentUser);
			// Mock SecurityHandler which returns a dummy username "harry"
			securityHandlerUnpriviledged = Org.Mockito.Mockito.Mock<SecurityHandler>();
			Org.Mockito.Mockito.When(securityHandlerUnpriviledged.GetUser()).ThenReturn("harry"
				);
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
			hdfs.Mkdirs(new Path(testdir + "/foo"));
			DFSTestUtil.CreateFile(hdfs, new Path(testdir + "/bar"), 0, (short)1, 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetattr()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo("/tmp/bar");
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			XDR xdr_req = new XDR();
			GETATTR3Request req = new GETATTR3Request(handle);
			req.Serialize(xdr_req);
			// Attempt by an unpriviledged user should fail.
			GETATTR3Response response1 = nfsd.Getattr(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code", Nfs3Status.Nfs3errAcces, 
				response1.GetStatus());
			// Attempt by a priviledged user should pass.
			GETATTR3Response response2 = nfsd.Getattr(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetattr()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			XDR xdr_req = new XDR();
			FileHandle handle = new FileHandle(dirId);
			SetAttr3 symAttr = new SetAttr3(0, 1, 0, 0, null, null, EnumSet.Of(SetAttr3.SetAttrField
				.Uid));
			SETATTR3Request req = new SETATTR3Request(handle, symAttr, false, null);
			req.Serialize(xdr_req);
			// Attempt by an unprivileged user should fail.
			SETATTR3Response response1 = nfsd.Setattr(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code", Nfs3Status.Nfs3errAcces, 
				response1.GetStatus());
			// Attempt by a priviledged user should pass.
			SETATTR3Response response2 = nfsd.Setattr(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLookup()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			LOOKUP3Request lookupReq = new LOOKUP3Request(handle, "bar");
			XDR xdr_req = new XDR();
			lookupReq.Serialize(xdr_req);
			// Attempt by an unpriviledged user should fail.
			LOOKUP3Response response1 = nfsd.Lookup(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code", Nfs3Status.Nfs3errAcces, 
				response1.GetStatus());
			// Attempt by a priviledged user should pass.
			LOOKUP3Response response2 = nfsd.Lookup(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAccess()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo("/tmp/bar");
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			XDR xdr_req = new XDR();
			ACCESS3Request req = new ACCESS3Request(handle);
			req.Serialize(xdr_req);
			// Attempt by an unpriviledged user should fail.
			ACCESS3Response response1 = nfsd.Access(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code", Nfs3Status.Nfs3errAcces, 
				response1.GetStatus());
			// Attempt by a priviledged user should pass.
			ACCESS3Response response2 = nfsd.Access(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReadlink()
		{
			// Create a symlink first.
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			XDR xdr_req = new XDR();
			FileHandle handle = new FileHandle(dirId);
			SYMLINK3Request req = new SYMLINK3Request(handle, "fubar", new SetAttr3(), "bar");
			req.Serialize(xdr_req);
			SYMLINK3Response response = nfsd.Symlink(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response
				.GetStatus());
			// Now perform readlink operations.
			FileHandle handle2 = response.GetObjFileHandle();
			XDR xdr_req2 = new XDR();
			READLINK3Request req2 = new READLINK3Request(handle2);
			req2.Serialize(xdr_req2);
			// Attempt by an unpriviledged user should fail.
			READLINK3Response response1 = nfsd.Readlink(xdr_req2.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a priviledged user should pass.
			READLINK3Response response2 = nfsd.Readlink(xdr_req2.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRead()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo("/tmp/bar");
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			READ3Request readReq = new READ3Request(handle, 0, 5);
			XDR xdr_req = new XDR();
			readReq.Serialize(xdr_req);
			// Attempt by an unpriviledged user should fail.
			READ3Response response1 = nfsd.Read(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a priviledged user should pass.
			READ3Response response2 = nfsd.Read(xdr_req.AsReadOnlyWrap(), securityHandler, new 
				IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEncryptedReadWrite()
		{
			int len = 8192;
			Path zone = new Path("/zone");
			hdfs.Mkdirs(zone);
			dfsAdmin.CreateEncryptionZone(zone, TestKey);
			byte[] buffer = new byte[len];
			for (int i = 0; i < len; i++)
			{
				buffer[i] = unchecked((byte)i);
			}
			string encFile1 = "/zone/myfile";
			CreateFileUsingNfs(encFile1, buffer);
			Commit(encFile1, len);
			Assert.AssertArrayEquals("encFile1 not equal", GetFileContentsUsingNfs(encFile1, 
				len), GetFileContentsUsingDfs(encFile1, len));
			/*
			* Same thing except this time create the encrypted file using DFS.
			*/
			string encFile2 = "/zone/myfile2";
			Path encFile2Path = new Path(encFile2);
			DFSTestUtil.CreateFile(hdfs, encFile2Path, len, (short)1, unchecked((int)(0xFEED)
				));
			Assert.AssertArrayEquals("encFile2 not equal", GetFileContentsUsingNfs(encFile2, 
				len), GetFileContentsUsingDfs(encFile2, len));
		}

		/// <exception cref="System.Exception"/>
		private void CreateFileUsingNfs(string fileName, byte[] buffer)
		{
			DFSTestUtil.CreateFile(hdfs, new Path(fileName), 0, (short)1, 0);
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(fileName);
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			WRITE3Request writeReq = new WRITE3Request(handle, 0, buffer.Length, Nfs3Constant.WriteStableHow
				.DataSync, ByteBuffer.Wrap(buffer));
			XDR xdr_req = new XDR();
			writeReq.Serialize(xdr_req);
			WRITE3Response response = nfsd.Write(xdr_req.AsReadOnlyWrap(), null, 1, securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect response: ", null, response);
		}

		/// <exception cref="System.Exception"/>
		private byte[] GetFileContentsUsingNfs(string fileName, int len)
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(fileName);
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			READ3Request readReq = new READ3Request(handle, 0, len);
			XDR xdr_req = new XDR();
			readReq.Serialize(xdr_req);
			READ3Response response = nfsd.Read(xdr_req.AsReadOnlyWrap(), securityHandler, new 
				IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code: ", Nfs3Status.Nfs3Ok, response
				.GetStatus());
			NUnit.Framework.Assert.IsTrue("expected full read", response.IsEof());
			return ((byte[])response.GetData().Array());
		}

		/// <exception cref="System.Exception"/>
		private byte[] GetFileContentsUsingDfs(string fileName, int len)
		{
			FSDataInputStream @in = hdfs.Open(new Path(fileName));
			byte[] ret = new byte[len];
			@in.ReadFully(ret);
			try
			{
				@in.ReadByte();
				NUnit.Framework.Assert.Fail("expected end of file");
			}
			catch (EOFException)
			{
			}
			// expected. Unfortunately there is no associated message to check
			@in.Close();
			return ret;
		}

		/// <exception cref="System.Exception"/>
		private void Commit(string fileName, int len)
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(fileName);
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			XDR xdr_req = new XDR();
			COMMIT3Request req = new COMMIT3Request(handle, 0, len);
			req.Serialize(xdr_req);
			Org.Jboss.Netty.Channel.Channel ch = Org.Mockito.Mockito.Mock<Org.Jboss.Netty.Channel.Channel
				>();
			COMMIT3Response response2 = nfsd.Commit(xdr_req.AsReadOnlyWrap(), ch, 1, securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect COMMIT3Response:", null, response2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWrite()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo("/tmp/bar");
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			byte[] buffer = new byte[10];
			for (int i = 0; i < 10; i++)
			{
				buffer[i] = unchecked((byte)i);
			}
			WRITE3Request writeReq = new WRITE3Request(handle, 0, 10, Nfs3Constant.WriteStableHow
				.DataSync, ByteBuffer.Wrap(buffer));
			XDR xdr_req = new XDR();
			writeReq.Serialize(xdr_req);
			// Attempt by an unpriviledged user should fail.
			WRITE3Response response1 = nfsd.Write(xdr_req.AsReadOnlyWrap(), null, 1, securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a priviledged user should pass.
			WRITE3Response response2 = nfsd.Write(xdr_req.AsReadOnlyWrap(), null, 1, securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect response:", null, response2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCreate()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			XDR xdr_req = new XDR();
			FileHandle handle = new FileHandle(dirId);
			CREATE3Request req = new CREATE3Request(handle, "fubar", Nfs3Constant.CreateUnchecked
				, new SetAttr3(), 0);
			req.Serialize(xdr_req);
			// Attempt by an unpriviledged user should fail.
			CREATE3Response response1 = nfsd.Create(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a priviledged user should pass.
			CREATE3Response response2 = nfsd.Create(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMkdir()
		{
			//FixME
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			XDR xdr_req = new XDR();
			FileHandle handle = new FileHandle(dirId);
			MKDIR3Request req = new MKDIR3Request(handle, "fubar1", new SetAttr3());
			req.Serialize(xdr_req);
			// Attempt to mkdir by an unprivileged user should fail.
			MKDIR3Response response1 = nfsd.Mkdir(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			XDR xdr_req2 = new XDR();
			MKDIR3Request req2 = new MKDIR3Request(handle, "fubar2", new SetAttr3());
			req2.Serialize(xdr_req2);
			// Attempt to mkdir by a privileged user should pass.
			MKDIR3Response response2 = nfsd.Mkdir(xdr_req2.AsReadOnlyWrap(), securityHandler, 
				new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSymlink()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			XDR xdr_req = new XDR();
			FileHandle handle = new FileHandle(dirId);
			SYMLINK3Request req = new SYMLINK3Request(handle, "fubar", new SetAttr3(), "bar");
			req.Serialize(xdr_req);
			// Attempt by an unprivileged user should fail.
			SYMLINK3Response response1 = nfsd.Symlink(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a privileged user should pass.
			SYMLINK3Response response2 = nfsd.Symlink(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRemove()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			XDR xdr_req = new XDR();
			FileHandle handle = new FileHandle(dirId);
			REMOVE3Request req = new REMOVE3Request(handle, "bar");
			req.Serialize(xdr_req);
			// Attempt by an unpriviledged user should fail.
			REMOVE3Response response1 = nfsd.Remove(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a priviledged user should pass.
			REMOVE3Response response2 = nfsd.Remove(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRmdir()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			XDR xdr_req = new XDR();
			FileHandle handle = new FileHandle(dirId);
			RMDIR3Request req = new RMDIR3Request(handle, "foo");
			req.Serialize(xdr_req);
			// Attempt by an unprivileged user should fail.
			RMDIR3Response response1 = nfsd.Rmdir(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a privileged user should pass.
			RMDIR3Response response2 = nfsd.Rmdir(xdr_req.AsReadOnlyWrap(), securityHandler, 
				new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRename()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			XDR xdr_req = new XDR();
			FileHandle handle = new FileHandle(dirId);
			RENAME3Request req = new RENAME3Request(handle, "bar", handle, "fubar");
			req.Serialize(xdr_req);
			// Attempt by an unprivileged user should fail.
			RENAME3Response response1 = nfsd.Rename(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a privileged user should pass.
			RENAME3Response response2 = nfsd.Rename(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReaddir()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			XDR xdr_req = new XDR();
			READDIR3Request req = new READDIR3Request(handle, 0, 0, 100);
			req.Serialize(xdr_req);
			// Attempt by an unpriviledged user should fail.
			READDIR3Response response1 = nfsd.Readdir(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a priviledged user should pass.
			READDIR3Response response2 = nfsd.Readdir(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReaddirplus()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo(testdir);
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			XDR xdr_req = new XDR();
			READDIRPLUS3Request req = new READDIRPLUS3Request(handle, 0, 0, 3, 2);
			req.Serialize(xdr_req);
			// Attempt by an unprivileged user should fail.
			READDIRPLUS3Response response1 = nfsd.Readdirplus(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a privileged user should pass.
			READDIRPLUS3Response response2 = nfsd.Readdirplus(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFsstat()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo("/tmp/bar");
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			XDR xdr_req = new XDR();
			FSSTAT3Request req = new FSSTAT3Request(handle);
			req.Serialize(xdr_req);
			// Attempt by an unpriviledged user should fail.
			FSSTAT3Response response1 = nfsd.Fsstat(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a priviledged user should pass.
			FSSTAT3Response response2 = nfsd.Fsstat(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFsinfo()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo("/tmp/bar");
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			XDR xdr_req = new XDR();
			FSINFO3Request req = new FSINFO3Request(handle);
			req.Serialize(xdr_req);
			// Attempt by an unpriviledged user should fail.
			FSINFO3Response response1 = nfsd.Fsinfo(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a priviledged user should pass.
			FSINFO3Response response2 = nfsd.Fsinfo(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPathconf()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo("/tmp/bar");
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			XDR xdr_req = new XDR();
			PATHCONF3Request req = new PATHCONF3Request(handle);
			req.Serialize(xdr_req);
			// Attempt by an unpriviledged user should fail.
			PATHCONF3Response response1 = nfsd.Pathconf(xdr_req.AsReadOnlyWrap(), securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a priviledged user should pass.
			PATHCONF3Response response2 = nfsd.Pathconf(xdr_req.AsReadOnlyWrap(), securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3Ok, response2
				.GetStatus());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCommit()
		{
			HdfsFileStatus status = nn.GetRpcServer().GetFileInfo("/tmp/bar");
			long dirId = status.GetFileId();
			FileHandle handle = new FileHandle(dirId);
			XDR xdr_req = new XDR();
			COMMIT3Request req = new COMMIT3Request(handle, 0, 5);
			req.Serialize(xdr_req);
			Org.Jboss.Netty.Channel.Channel ch = Org.Mockito.Mockito.Mock<Org.Jboss.Netty.Channel.Channel
				>();
			// Attempt by an unpriviledged user should fail.
			COMMIT3Response response1 = nfsd.Commit(xdr_req.AsReadOnlyWrap(), ch, 1, securityHandlerUnpriviledged
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect return code:", Nfs3Status.Nfs3errAcces
				, response1.GetStatus());
			// Attempt by a priviledged user should pass.
			COMMIT3Response response2 = nfsd.Commit(xdr_req.AsReadOnlyWrap(), ch, 1, securityHandler
				, new IPEndPoint("localhost", 1234));
			NUnit.Framework.Assert.AreEqual("Incorrect COMMIT3Response:", null, response2);
		}

		public virtual void TestIdempotent()
		{
			object[][] procedures = new object[][] { new object[] { Nfs3Constant.NFSPROC3.Null
				, 1 }, new object[] { Nfs3Constant.NFSPROC3.Getattr, 1 }, new object[] { Nfs3Constant.NFSPROC3
				.Setattr, 1 }, new object[] { Nfs3Constant.NFSPROC3.Lookup, 1 }, new object[] { 
				Nfs3Constant.NFSPROC3.Access, 1 }, new object[] { Nfs3Constant.NFSPROC3.Readlink
				, 1 }, new object[] { Nfs3Constant.NFSPROC3.Read, 1 }, new object[] { Nfs3Constant.NFSPROC3
				.Write, 1 }, new object[] { Nfs3Constant.NFSPROC3.Create, 0 }, new object[] { Nfs3Constant.NFSPROC3
				.Mkdir, 0 }, new object[] { Nfs3Constant.NFSPROC3.Symlink, 0 }, new object[] { Nfs3Constant.NFSPROC3
				.Mknod, 0 }, new object[] { Nfs3Constant.NFSPROC3.Remove, 0 }, new object[] { Nfs3Constant.NFSPROC3
				.Rmdir, 0 }, new object[] { Nfs3Constant.NFSPROC3.Rename, 0 }, new object[] { Nfs3Constant.NFSPROC3
				.Link, 0 }, new object[] { Nfs3Constant.NFSPROC3.Readdir, 1 }, new object[] { Nfs3Constant.NFSPROC3
				.Readdirplus, 1 }, new object[] { Nfs3Constant.NFSPROC3.Fsstat, 1 }, new object[
				] { Nfs3Constant.NFSPROC3.Fsinfo, 1 }, new object[] { Nfs3Constant.NFSPROC3.Pathconf
				, 1 }, new object[] { Nfs3Constant.NFSPROC3.Commit, 1 } };
			foreach (object[] procedure in procedures)
			{
				bool idempotent = procedure[1].Equals(Sharpen.Extensions.ValueOf(1));
				Nfs3Constant.NFSPROC3 proc = (Nfs3Constant.NFSPROC3)procedure[0];
				if (idempotent)
				{
					NUnit.Framework.Assert.IsTrue(("Procedure " + proc + " should be idempotent"), proc
						.IsIdempotent());
				}
				else
				{
					NUnit.Framework.Assert.IsFalse(("Procedure " + proc + " should be non-idempotent"
						), proc.IsIdempotent());
				}
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestDeprecatedKeys()
		{
			NfsConfiguration conf = new NfsConfiguration();
			conf.SetInt("nfs3.server.port", 998);
			NUnit.Framework.Assert.IsTrue(conf.GetInt(NfsConfigKeys.DfsNfsServerPortKey, 0) ==
				 998);
			conf.SetInt("nfs3.mountd.port", 999);
			NUnit.Framework.Assert.IsTrue(conf.GetInt(NfsConfigKeys.DfsNfsMountdPortKey, 0) ==
				 999);
			conf.Set("dfs.nfs.exports.allowed.hosts", "host1");
			NUnit.Framework.Assert.IsTrue(conf.Get(CommonConfigurationKeys.NfsExportsAllowedHostsKey
				).Equals("host1"));
			conf.SetInt("dfs.nfs.exports.cache.expirytime.millis", 1000);
			NUnit.Framework.Assert.IsTrue(conf.GetInt(Nfs3Constant.NfsExportsCacheExpirytimeMillisKey
				, 0) == 1000);
			conf.SetInt("hadoop.nfs.userupdate.milly", 10);
			NUnit.Framework.Assert.IsTrue(conf.GetInt(IdMappingConstant.UsergroupidUpdateMillisKey
				, 0) == 10);
			conf.Set("dfs.nfs3.dump.dir", "/nfs/tmp");
			NUnit.Framework.Assert.IsTrue(conf.Get(NfsConfigKeys.DfsNfsFileDumpDirKey).Equals
				("/nfs/tmp"));
			conf.SetBoolean("dfs.nfs3.enableDump", false);
			NUnit.Framework.Assert.IsTrue(conf.GetBoolean(NfsConfigKeys.DfsNfsFileDumpKey, true
				) == false);
			conf.SetInt("dfs.nfs3.max.open.files", 500);
			NUnit.Framework.Assert.IsTrue(conf.GetInt(NfsConfigKeys.DfsNfsMaxOpenFilesKey, 0)
				 == 500);
			conf.SetInt("dfs.nfs3.stream.timeout", 6000);
			NUnit.Framework.Assert.IsTrue(conf.GetInt(NfsConfigKeys.DfsNfsStreamTimeoutKey, 0
				) == 6000);
			conf.Set("dfs.nfs3.export.point", "/dir1");
			NUnit.Framework.Assert.IsTrue(conf.Get(NfsConfigKeys.DfsNfsExportPointKey).Equals
				("/dir1"));
		}
	}
}
