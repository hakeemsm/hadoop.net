using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests the FileStatus API.</summary>
	public class TestListFilesInFileContext
	{
		internal const long seed = unchecked((long)(0xDEADBEEFL));

		private static readonly Configuration conf = new Configuration();

		private static MiniDFSCluster cluster;

		private static FileContext fc;

		private static readonly Path TestDir = new Path("/main_");

		private const int FileLen = 10;

		private static readonly Path File1 = new Path(TestDir, "file1");

		private static readonly Path Dir1 = new Path(TestDir, "dir1");

		private static readonly Path File2 = new Path(Dir1, "file2");

		private static readonly Path File3 = new Path(Dir1, "file3");

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void TestSetUp()
		{
			cluster = new MiniDFSCluster.Builder(conf).Build();
			fc = FileContext.GetFileContext(cluster.GetConfiguration(0));
			fc.Delete(TestDir, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteFile(FileContext fc, Path name, int fileSize)
		{
			// Create and write a file that contains three blocks of data
			FSDataOutputStream stm = fc.Create(name, EnumSet.Of(CreateFlag.Create), Options.CreateOpts
				.CreateParent());
			byte[] buffer = new byte[fileSize];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TestShutdown()
		{
			cluster.Shutdown();
		}

		/// <summary>Test when input path is a file</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFile()
		{
			fc.Mkdir(TestDir, FsPermission.GetDefault(), true);
			WriteFile(fc, File1, FileLen);
			RemoteIterator<LocatedFileStatus> itor = fc.Util().ListFiles(File1, true);
			LocatedFileStatus stat = itor.Next();
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			NUnit.Framework.Assert.IsTrue(stat.IsFile());
			NUnit.Framework.Assert.AreEqual(FileLen, stat.GetLen());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(File1), stat.GetPath());
			NUnit.Framework.Assert.AreEqual(1, stat.GetBlockLocations().Length);
			itor = fc.Util().ListFiles(File1, false);
			stat = itor.Next();
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			NUnit.Framework.Assert.IsTrue(stat.IsFile());
			NUnit.Framework.Assert.AreEqual(FileLen, stat.GetLen());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(File1), stat.GetPath());
			NUnit.Framework.Assert.AreEqual(1, stat.GetBlockLocations().Length);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void CleanDir()
		{
			fc.Delete(TestDir, true);
		}

		/// <summary>Test when input path is a directory</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDirectory()
		{
			fc.Mkdir(Dir1, FsPermission.GetDefault(), true);
			// test empty directory
			RemoteIterator<LocatedFileStatus> itor = fc.Util().ListFiles(Dir1, true);
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			itor = fc.Util().ListFiles(Dir1, false);
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			// testing directory with 1 file
			WriteFile(fc, File2, FileLen);
			itor = fc.Util().ListFiles(Dir1, true);
			LocatedFileStatus stat = itor.Next();
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			NUnit.Framework.Assert.IsTrue(stat.IsFile());
			NUnit.Framework.Assert.AreEqual(FileLen, stat.GetLen());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(File2), stat.GetPath());
			NUnit.Framework.Assert.AreEqual(1, stat.GetBlockLocations().Length);
			itor = fc.Util().ListFiles(Dir1, false);
			stat = itor.Next();
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			NUnit.Framework.Assert.IsTrue(stat.IsFile());
			NUnit.Framework.Assert.AreEqual(FileLen, stat.GetLen());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(File2), stat.GetPath());
			NUnit.Framework.Assert.AreEqual(1, stat.GetBlockLocations().Length);
			// test more complicated directory
			WriteFile(fc, File1, FileLen);
			WriteFile(fc, File3, FileLen);
			itor = fc.Util().ListFiles(TestDir, true);
			stat = itor.Next();
			NUnit.Framework.Assert.IsTrue(stat.IsFile());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(File2), stat.GetPath());
			stat = itor.Next();
			NUnit.Framework.Assert.IsTrue(stat.IsFile());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(File3), stat.GetPath());
			stat = itor.Next();
			NUnit.Framework.Assert.IsTrue(stat.IsFile());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(File1), stat.GetPath());
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			itor = fc.Util().ListFiles(TestDir, false);
			stat = itor.Next();
			NUnit.Framework.Assert.IsTrue(stat.IsFile());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(File1), stat.GetPath());
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
		}

		/// <summary>Test when input patch has a symbolic links as its children</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSymbolicLinks()
		{
			WriteFile(fc, File1, FileLen);
			WriteFile(fc, File2, FileLen);
			WriteFile(fc, File3, FileLen);
			Path dir4 = new Path(TestDir, "dir4");
			Path dir5 = new Path(dir4, "dir5");
			Path file4 = new Path(dir4, "file4");
			fc.CreateSymlink(Dir1, dir5, true);
			fc.CreateSymlink(File1, file4, true);
			RemoteIterator<LocatedFileStatus> itor = fc.Util().ListFiles(dir4, true);
			LocatedFileStatus stat = itor.Next();
			NUnit.Framework.Assert.IsTrue(stat.IsFile());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(File2), stat.GetPath());
			stat = itor.Next();
			NUnit.Framework.Assert.IsTrue(stat.IsFile());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(File3), stat.GetPath());
			stat = itor.Next();
			NUnit.Framework.Assert.IsTrue(stat.IsFile());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(File1), stat.GetPath());
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			itor = fc.Util().ListFiles(dir4, false);
			stat = itor.Next();
			NUnit.Framework.Assert.IsTrue(stat.IsFile());
			NUnit.Framework.Assert.AreEqual(fc.MakeQualified(File1), stat.GetPath());
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
		}

		public TestListFilesInFileContext()
		{
			{
				((Log4JLogger)FileSystem.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
