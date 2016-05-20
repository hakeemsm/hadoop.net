using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>This class tests the FileStatus API.</summary>
	public class TestListFiles
	{
		internal const long seed = unchecked((long)(0xDEADBEEFL));

		protected internal static readonly org.apache.hadoop.conf.Configuration conf = new 
			org.apache.hadoop.conf.Configuration();

		protected internal static org.apache.hadoop.fs.FileSystem fs;

		protected internal static org.apache.hadoop.fs.Path TEST_DIR;

		private const int FILE_LEN = 10;

		private static org.apache.hadoop.fs.Path FILE1;

		private static org.apache.hadoop.fs.Path DIR1;

		private static org.apache.hadoop.fs.Path FILE2;

		private static org.apache.hadoop.fs.Path FILE3;

		static TestListFiles()
		{
			setTestPaths(new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty("test.build.data"
				, "build/test/data/work-dir/localfs"), "main_"));
		}

		protected internal static org.apache.hadoop.fs.Path getTestDir()
		{
			return TEST_DIR;
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
		protected internal static void setTestPaths(org.apache.hadoop.fs.Path testDir)
		{
			TEST_DIR = testDir;
			FILE1 = new org.apache.hadoop.fs.Path(TEST_DIR, "file1");
			DIR1 = new org.apache.hadoop.fs.Path(TEST_DIR, "dir1");
			FILE2 = new org.apache.hadoop.fs.Path(DIR1, "file2");
			FILE3 = new org.apache.hadoop.fs.Path(DIR1, "file3");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void testSetUp()
		{
			fs = org.apache.hadoop.fs.FileSystem.getLocal(conf);
			fs.delete(TEST_DIR, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void writeFile(org.apache.hadoop.fs.FileSystem fileSys, org.apache.hadoop.fs.Path
			 name, int fileSize)
		{
			// Create and write a file that contains three blocks of data
			org.apache.hadoop.fs.FSDataOutputStream stm = fileSys.create(name);
			byte[] buffer = new byte[fileSize];
			java.util.Random rand = new java.util.Random(seed);
			rand.nextBytes(buffer);
			stm.write(buffer);
			stm.close();
		}

		/// <summary>Test when input path is a file</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFile()
		{
			fs.mkdirs(TEST_DIR);
			writeFile(fs, FILE1, FILE_LEN);
			org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> itor = 
				fs.listFiles(FILE1, true);
			org.apache.hadoop.fs.LocatedFileStatus stat = itor.next();
			NUnit.Framework.Assert.IsFalse(itor.hasNext());
			NUnit.Framework.Assert.IsTrue(stat.isFile());
			NUnit.Framework.Assert.AreEqual(FILE_LEN, stat.getLen());
			NUnit.Framework.Assert.AreEqual(fs.makeQualified(FILE1), stat.getPath());
			NUnit.Framework.Assert.AreEqual(1, stat.getBlockLocations().Length);
			itor = fs.listFiles(FILE1, false);
			stat = itor.next();
			NUnit.Framework.Assert.IsFalse(itor.hasNext());
			NUnit.Framework.Assert.IsTrue(stat.isFile());
			NUnit.Framework.Assert.AreEqual(FILE_LEN, stat.getLen());
			NUnit.Framework.Assert.AreEqual(fs.makeQualified(FILE1), stat.getPath());
			NUnit.Framework.Assert.AreEqual(1, stat.getBlockLocations().Length);
			fs.delete(FILE1, true);
		}

		/// <summary>Test when input path is a directory</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testDirectory()
		{
			fs.mkdirs(DIR1);
			// test empty directory
			org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> itor = 
				fs.listFiles(DIR1, true);
			NUnit.Framework.Assert.IsFalse(itor.hasNext());
			itor = fs.listFiles(DIR1, false);
			NUnit.Framework.Assert.IsFalse(itor.hasNext());
			// testing directory with 1 file
			writeFile(fs, FILE2, FILE_LEN);
			itor = fs.listFiles(DIR1, true);
			org.apache.hadoop.fs.LocatedFileStatus stat = itor.next();
			NUnit.Framework.Assert.IsFalse(itor.hasNext());
			NUnit.Framework.Assert.IsTrue(stat.isFile());
			NUnit.Framework.Assert.AreEqual(FILE_LEN, stat.getLen());
			NUnit.Framework.Assert.AreEqual(fs.makeQualified(FILE2), stat.getPath());
			NUnit.Framework.Assert.AreEqual(1, stat.getBlockLocations().Length);
			itor = fs.listFiles(DIR1, false);
			stat = itor.next();
			NUnit.Framework.Assert.IsFalse(itor.hasNext());
			NUnit.Framework.Assert.IsTrue(stat.isFile());
			NUnit.Framework.Assert.AreEqual(FILE_LEN, stat.getLen());
			NUnit.Framework.Assert.AreEqual(fs.makeQualified(FILE2), stat.getPath());
			NUnit.Framework.Assert.AreEqual(1, stat.getBlockLocations().Length);
			// test more complicated directory
			writeFile(fs, FILE1, FILE_LEN);
			writeFile(fs, FILE3, FILE_LEN);
			System.Collections.Generic.ICollection<org.apache.hadoop.fs.Path> filesToFind = new 
				java.util.HashSet<org.apache.hadoop.fs.Path>();
			filesToFind.add(fs.makeQualified(FILE1));
			filesToFind.add(fs.makeQualified(FILE2));
			filesToFind.add(fs.makeQualified(FILE3));
			itor = fs.listFiles(TEST_DIR, true);
			stat = itor.next();
			NUnit.Framework.Assert.IsTrue(stat.isFile());
			NUnit.Framework.Assert.IsTrue("Path " + stat.getPath() + " unexpected", filesToFind
				.remove(stat.getPath()));
			stat = itor.next();
			NUnit.Framework.Assert.IsTrue(stat.isFile());
			NUnit.Framework.Assert.IsTrue("Path " + stat.getPath() + " unexpected", filesToFind
				.remove(stat.getPath()));
			stat = itor.next();
			NUnit.Framework.Assert.IsTrue(stat.isFile());
			NUnit.Framework.Assert.IsTrue("Path " + stat.getPath() + " unexpected", filesToFind
				.remove(stat.getPath()));
			NUnit.Framework.Assert.IsFalse(itor.hasNext());
			NUnit.Framework.Assert.IsTrue(filesToFind.isEmpty());
			itor = fs.listFiles(TEST_DIR, false);
			stat = itor.next();
			NUnit.Framework.Assert.IsTrue(stat.isFile());
			NUnit.Framework.Assert.AreEqual(fs.makeQualified(FILE1), stat.getPath());
			NUnit.Framework.Assert.IsFalse(itor.hasNext());
			fs.delete(TEST_DIR, true);
		}
	}
}
