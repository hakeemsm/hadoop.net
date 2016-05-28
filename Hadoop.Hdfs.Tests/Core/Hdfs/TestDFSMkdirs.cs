using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests that the DFS command mkdirs only creates valid
	/// directories, and generally behaves as expected.
	/// </summary>
	public class TestDFSMkdirs
	{
		private readonly Configuration conf = new HdfsConfiguration();

		private static readonly string[] NonCanonicalPaths = new string[] { "//test1", "/test2/.."
			, "/test2//bar", "/test2/../test4", "/test5/." };

		/// <summary>
		/// Tests mkdirs can create a directory that does not exist and will
		/// not create a subdirectory off a file.
		/// </summary>
		/// <remarks>
		/// Tests mkdirs can create a directory that does not exist and will
		/// not create a subdirectory off a file. Regression test for HADOOP-281.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDFSMkdirs()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			FileSystem fileSys = cluster.GetFileSystem();
			try
			{
				// First create a new directory with mkdirs
				Path myPath = new Path("/test/mkdirs");
				NUnit.Framework.Assert.IsTrue(fileSys.Mkdirs(myPath));
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(myPath));
				NUnit.Framework.Assert.IsTrue(fileSys.Mkdirs(myPath));
				// Second, create a file in that directory.
				Path myFile = new Path("/test/mkdirs/myFile");
				DFSTestUtil.WriteFile(fileSys, myFile, "hello world");
				// Third, use mkdir to create a subdirectory off of that file,
				// and check that it fails.
				Path myIllegalPath = new Path("/test/mkdirs/myFile/subdir");
				bool exist = true;
				try
				{
					fileSys.Mkdirs(myIllegalPath);
				}
				catch (IOException)
				{
					exist = false;
				}
				NUnit.Framework.Assert.IsFalse(exist);
				NUnit.Framework.Assert.IsFalse(fileSys.Exists(myIllegalPath));
				fileSys.Delete(myFile, true);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Tests mkdir will not create directory when parent is missing.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdir()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			try
			{
				// Create a dir in root dir, should succeed
				NUnit.Framework.Assert.IsTrue(dfs.Mkdir(new Path("/mkdir-" + Time.Now()), FsPermission
					.GetDefault()));
				// Create a dir when parent dir exists as a file, should fail
				IOException expectedException = null;
				string filePath = "/mkdir-file-" + Time.Now();
				DFSTestUtil.WriteFile(dfs, new Path(filePath), "hello world");
				try
				{
					dfs.Mkdir(new Path(filePath + "/mkdir"), FsPermission.GetDefault());
				}
				catch (IOException e)
				{
					expectedException = e;
				}
				NUnit.Framework.Assert.IsTrue("Create a directory when parent dir exists as file using"
					 + " mkdir() should throw ParentNotDirectoryException ", expectedException != null
					 && expectedException is ParentNotDirectoryException);
				// Create a dir in a non-exist directory, should fail
				expectedException = null;
				try
				{
					dfs.Mkdir(new Path("/non-exist/mkdir-" + Time.Now()), FsPermission.GetDefault());
				}
				catch (IOException e)
				{
					expectedException = e;
				}
				NUnit.Framework.Assert.IsTrue("Create a directory in a non-exist parent dir using"
					 + " mkdir() should throw FileNotFoundException ", expectedException != null && 
					expectedException is FileNotFoundException);
			}
			finally
			{
				dfs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Regression test for HDFS-3626.</summary>
		/// <remarks>
		/// Regression test for HDFS-3626. Creates a file using a non-canonical path
		/// (i.e. with extra slashes between components) and makes sure that the NN
		/// rejects it.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirRpcNonCanonicalPath()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
			try
			{
				NamenodeProtocols nnrpc = cluster.GetNameNodeRpc();
				foreach (string pathStr in NonCanonicalPaths)
				{
					try
					{
						nnrpc.Mkdirs(pathStr, new FsPermission((short)0x1ed), true);
						NUnit.Framework.Assert.Fail("Did not fail when called with a non-canonicalized path: "
							 + pathStr);
					}
					catch (InvalidPathException)
					{
					}
				}
			}
			finally
			{
				// expected
				cluster.Shutdown();
			}
		}
	}
}
