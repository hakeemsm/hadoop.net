using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Tests the CreateEditsLog utility.</summary>
	public class TestCreateEditsLog
	{
		private static readonly FilePath HdfsDir = new FilePath(MiniDFSCluster.GetBaseDirectory
			()).GetAbsoluteFile();

		private static readonly FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "build/test/data"), "TestCreateEditsLog").GetAbsoluteFile();

		private MiniDFSCluster cluster;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			DeleteIfExists(HdfsDir);
			DeleteIfExists(TestDir);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
				cluster = null;
			}
			DeleteIfExists(HdfsDir);
			DeleteIfExists(TestDir);
		}

		/// <summary>
		/// Tests that an edits log created using CreateEditsLog is valid and can be
		/// loaded successfully by a namenode.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCanLoadCreatedEditsLog()
		{
			// Format namenode.
			HdfsConfiguration conf = new HdfsConfiguration();
			FilePath nameDir = new FilePath(HdfsDir, "name");
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Util.FileAsURI(nameDir).ToString());
			DFSTestUtil.FormatNameNode(conf);
			// Call CreateEditsLog and move the resulting edits to the name dir.
			CreateEditsLog.Main(new string[] { "-f", "1000", "0", "1", "-d", TestDir.GetAbsolutePath
				() });
			Path editsWildcard = new Path(TestDir.GetAbsolutePath(), "*");
			FileContext localFc = FileContext.GetLocalFSFileContext();
			foreach (FileStatus edits in localFc.Util().GlobStatus(editsWildcard))
			{
				Path src = edits.GetPath();
				Path dst = new Path(new FilePath(nameDir, "current").GetAbsolutePath(), src.GetName
					());
				localFc.Rename(src, dst);
			}
			// Start a namenode to try to load the edits.
			cluster = new MiniDFSCluster.Builder(conf).Format(false).ManageNameDfsDirs(false)
				.WaitSafeMode(false).Build();
			cluster.WaitClusterUp();
		}

		// Test successful, because no exception thrown.
		/// <summary>Fully delete the given directory if it exists.</summary>
		/// <param name="file">File to delete</param>
		private static void DeleteIfExists(FilePath file)
		{
			if (file.Exists() && !FileUtil.FullyDelete(file))
			{
				NUnit.Framework.Assert.Fail("Could not delete  '" + file + "'");
			}
		}
	}
}
