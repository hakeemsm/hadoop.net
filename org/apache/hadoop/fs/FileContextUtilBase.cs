using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// <p>
	/// A collection of Util tests for the
	/// <see cref="FileContext.util()"/>
	/// .
	/// This test should be used for testing an instance of
	/// <see cref="FileContext.util()"/>
	/// that has been initialized to a specific default FileSystem such a
	/// LocalFileSystem, HDFS,S3, etc.
	/// </p>
	/// <p>
	/// To test a given
	/// <see cref="FileSystem"/>
	/// implementation create a subclass of this
	/// test and override
	/// <see cref="setUp()"/>
	/// to initialize the <code>fc</code>
	/// <see cref="FileContext"/>
	/// instance variable.
	/// </p>
	/// </summary>
	public abstract class FileContextUtilBase
	{
		protected internal readonly org.apache.hadoop.fs.FileContextTestHelper fileContextTestHelper
			 = new org.apache.hadoop.fs.FileContextTestHelper();

		protected internal org.apache.hadoop.fs.FileContext fc;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			fc.mkdir(fileContextTestHelper.getTestRootPath(fc), org.apache.hadoop.fs.FileContext
				.DEFAULT_PERM, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			fc.delete(fileContextTestHelper.getTestRootPath(fc), true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFcCopy()
		{
			string ts = "some random text";
			org.apache.hadoop.fs.Path file1 = fileContextTestHelper.getTestRootPath(fc, "file1"
				);
			org.apache.hadoop.fs.Path file2 = fileContextTestHelper.getTestRootPath(fc, "file2"
				);
			org.apache.hadoop.fs.FileContextTestHelper.writeFile(fc, file1, Sharpen.Runtime.getBytesForString
				(ts));
			NUnit.Framework.Assert.IsTrue(fc.util().exists(file1));
			fc.util().copy(file1, file2);
			// verify that newly copied file2 exists
			NUnit.Framework.Assert.IsTrue("Failed to copy file2  ", fc.util().exists(file2));
			// verify that file2 contains test string
			NUnit.Framework.Assert.IsTrue("Copied files does not match ", java.util.Arrays.equals
				(Sharpen.Runtime.getBytesForString(ts), org.apache.hadoop.fs.FileContextTestHelper.readFile
				(fc, file2, Sharpen.Runtime.getBytesForString(ts).Length)));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRecursiveFcCopy()
		{
			string ts = "some random text";
			org.apache.hadoop.fs.Path dir1 = fileContextTestHelper.getTestRootPath(fc, "dir1"
				);
			org.apache.hadoop.fs.Path dir2 = fileContextTestHelper.getTestRootPath(fc, "dir2"
				);
			org.apache.hadoop.fs.Path file1 = new org.apache.hadoop.fs.Path(dir1, "file1");
			fc.mkdir(dir1, null, false);
			org.apache.hadoop.fs.FileContextTestHelper.writeFile(fc, file1, Sharpen.Runtime.getBytesForString
				(ts));
			NUnit.Framework.Assert.IsTrue(fc.util().exists(file1));
			org.apache.hadoop.fs.Path file2 = new org.apache.hadoop.fs.Path(dir2, "file1");
			fc.util().copy(dir1, dir2);
			// verify that newly copied file2 exists
			NUnit.Framework.Assert.IsTrue("Failed to copy file2  ", fc.util().exists(file2));
			// verify that file2 contains test string
			NUnit.Framework.Assert.IsTrue("Copied files does not match ", java.util.Arrays.equals
				(Sharpen.Runtime.getBytesForString(ts), org.apache.hadoop.fs.FileContextTestHelper.readFile
				(fc, file2, Sharpen.Runtime.getBytesForString(ts).Length)));
		}

		public FileContextUtilBase()
		{
			{
				try
				{
					((org.apache.commons.logging.impl.Log4JLogger)org.apache.hadoop.fs.FileSystem.LOG
						).getLogger().setLevel(org.apache.log4j.Level.DEBUG);
				}
				catch (System.Exception e)
				{
					System.Console.Out.WriteLine("Cannot change log level\n" + org.apache.hadoop.util.StringUtils
						.stringifyException(e));
				}
			}
		}
	}
}
