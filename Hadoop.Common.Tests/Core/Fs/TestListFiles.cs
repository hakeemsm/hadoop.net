using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>This class tests the FileStatus API.</summary>
	public class TestListFiles
	{
		internal const long seed = unchecked((long)(0xDEADBEEFL));

		protected internal static readonly Configuration conf = new Configuration();

		protected internal static FileSystem fs;

		protected internal static Path TestDir;

		private const int FileLen = 10;

		private static Path File1;

		private static Path Dir1;

		private static Path File2;

		private static Path File3;

		static TestListFiles()
		{
			SetTestPaths(new Path(Runtime.GetProperty("test.build.data", "build/test/data/work-dir/localfs"
				), "main_"));
		}

		protected internal static Path GetTestDir()
		{
			return TestDir;
		}

		/// <summary>
		/// Sets the root testing directory and reinitializes any additional test paths
		/// that are under the root.
		/// </summary>
		/// <remarks>
		/// Sets the root testing directory and reinitializes any additional test paths
		/// that are under the root.  This method is intended to be called from a
		/// subclass's @BeforeClass method if there is a need to override the testing
		/// directory.
		/// </remarks>
		/// <param name="testDir">Path root testing directory</param>
		protected internal static void SetTestPaths(Path testDir)
		{
			TestDir = testDir;
			File1 = new Path(TestDir, "file1");
			Dir1 = new Path(TestDir, "dir1");
			File2 = new Path(Dir1, "file2");
			File3 = new Path(Dir1, "file3");
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void TestSetUp()
		{
			fs = FileSystem.GetLocal(conf);
			fs.Delete(TestDir, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteFile(FileSystem fileSys, Path name, int fileSize)
		{
			// Create and write a file that contains three blocks of data
			FSDataOutputStream stm = fileSys.Create(name);
			byte[] buffer = new byte[fileSize];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
		}

		/// <summary>Test when input path is a file</summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestFile()
		{
			fs.Mkdirs(TestDir);
			WriteFile(fs, File1, FileLen);
			RemoteIterator<LocatedFileStatus> itor = fs.ListFiles(File1, true);
			LocatedFileStatus stat = itor.Next();
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			Assert.True(stat.IsFile());
			Assert.Equal(FileLen, stat.GetLen());
			Assert.Equal(fs.MakeQualified(File1), stat.GetPath());
			Assert.Equal(1, stat.GetBlockLocations().Length);
			itor = fs.ListFiles(File1, false);
			stat = itor.Next();
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			Assert.True(stat.IsFile());
			Assert.Equal(FileLen, stat.GetLen());
			Assert.Equal(fs.MakeQualified(File1), stat.GetPath());
			Assert.Equal(1, stat.GetBlockLocations().Length);
			fs.Delete(File1, true);
		}

		/// <summary>Test when input path is a directory</summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestDirectory()
		{
			fs.Mkdirs(Dir1);
			// test empty directory
			RemoteIterator<LocatedFileStatus> itor = fs.ListFiles(Dir1, true);
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			itor = fs.ListFiles(Dir1, false);
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			// testing directory with 1 file
			WriteFile(fs, File2, FileLen);
			itor = fs.ListFiles(Dir1, true);
			LocatedFileStatus stat = itor.Next();
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			Assert.True(stat.IsFile());
			Assert.Equal(FileLen, stat.GetLen());
			Assert.Equal(fs.MakeQualified(File2), stat.GetPath());
			Assert.Equal(1, stat.GetBlockLocations().Length);
			itor = fs.ListFiles(Dir1, false);
			stat = itor.Next();
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			Assert.True(stat.IsFile());
			Assert.Equal(FileLen, stat.GetLen());
			Assert.Equal(fs.MakeQualified(File2), stat.GetPath());
			Assert.Equal(1, stat.GetBlockLocations().Length);
			// test more complicated directory
			WriteFile(fs, File1, FileLen);
			WriteFile(fs, File3, FileLen);
			ICollection<Path> filesToFind = new HashSet<Path>();
			filesToFind.AddItem(fs.MakeQualified(File1));
			filesToFind.AddItem(fs.MakeQualified(File2));
			filesToFind.AddItem(fs.MakeQualified(File3));
			itor = fs.ListFiles(TestDir, true);
			stat = itor.Next();
			Assert.True(stat.IsFile());
			Assert.True("Path " + stat.GetPath() + " unexpected", filesToFind
				.Remove(stat.GetPath()));
			stat = itor.Next();
			Assert.True(stat.IsFile());
			Assert.True("Path " + stat.GetPath() + " unexpected", filesToFind
				.Remove(stat.GetPath()));
			stat = itor.Next();
			Assert.True(stat.IsFile());
			Assert.True("Path " + stat.GetPath() + " unexpected", filesToFind
				.Remove(stat.GetPath()));
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			Assert.True(filesToFind.IsEmpty());
			itor = fs.ListFiles(TestDir, false);
			stat = itor.Next();
			Assert.True(stat.IsFile());
			Assert.Equal(fs.MakeQualified(File1), stat.GetPath());
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			fs.Delete(TestDir, true);
		}
	}
}
