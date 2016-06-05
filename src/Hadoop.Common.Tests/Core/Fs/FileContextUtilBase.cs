using System;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// <p>
	/// A collection of Util tests for the
	/// <see cref="FileContext.Util()"/>
	/// .
	/// This test should be used for testing an instance of
	/// <see cref="FileContext.Util()"/>
	/// that has been initialized to a specific default FileSystem such a
	/// LocalFileSystem, HDFS,S3, etc.
	/// </p>
	/// <p>
	/// To test a given
	/// <see cref="FileSystem"/>
	/// implementation create a subclass of this
	/// test and override
	/// <see cref="SetUp()"/>
	/// to initialize the <code>fc</code>
	/// <see cref="FileContext"/>
	/// instance variable.
	/// </p>
	/// </summary>
	public abstract class FileContextUtilBase
	{
		protected internal readonly FileContextTestHelper fileContextTestHelper = new FileContextTestHelper
			();

		protected internal FileContext fc;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			fc.Mkdir(fileContextTestHelper.GetTestRootPath(fc), FileContext.DefaultPerm, true
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fc.Delete(fileContextTestHelper.GetTestRootPath(fc), true);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFcCopy()
		{
			string ts = "some random text";
			Path file1 = fileContextTestHelper.GetTestRootPath(fc, "file1");
			Path file2 = fileContextTestHelper.GetTestRootPath(fc, "file2");
			FileContextTestHelper.WriteFile(fc, file1, Runtime.GetBytesForString(ts));
			Assert.True(fc.Util().Exists(file1));
			fc.Util().Copy(file1, file2);
			// verify that newly copied file2 exists
			Assert.True("Failed to copy file2  ", fc.Util().Exists(file2));
			// verify that file2 contains test string
			Assert.True("Copied files does not match ", Arrays.Equals(Runtime.GetBytesForString
				(ts), FileContextTestHelper.ReadFile(fc, file2, Runtime.GetBytesForString
				(ts).Length)));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRecursiveFcCopy()
		{
			string ts = "some random text";
			Path dir1 = fileContextTestHelper.GetTestRootPath(fc, "dir1");
			Path dir2 = fileContextTestHelper.GetTestRootPath(fc, "dir2");
			Path file1 = new Path(dir1, "file1");
			fc.Mkdir(dir1, null, false);
			FileContextTestHelper.WriteFile(fc, file1, Runtime.GetBytesForString(ts));
			Assert.True(fc.Util().Exists(file1));
			Path file2 = new Path(dir2, "file1");
			fc.Util().Copy(dir1, dir2);
			// verify that newly copied file2 exists
			Assert.True("Failed to copy file2  ", fc.Util().Exists(file2));
			// verify that file2 contains test string
			Assert.True("Copied files does not match ", Arrays.Equals(Runtime.GetBytesForString
				(ts), FileContextTestHelper.ReadFile(fc, file2, Runtime.GetBytesForString
				(ts).Length)));
		}

		public FileContextUtilBase()
		{
			{
				try
				{
					((Log4JLogger)FileSystem.Log).GetLogger().SetLevel(Level.Debug);
				}
				catch (Exception e)
				{
					System.Console.Out.WriteLine("Cannot change log level\n" + StringUtils.StringifyException
						(e));
				}
			}
		}
	}
}
