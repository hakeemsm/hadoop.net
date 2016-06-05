using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// <p>
	/// A collection of tests for the
	/// <see cref="FileContext"/>
	/// , create method
	/// This test should be used for testing an instance of FileContext
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
	/// Since this a junit 4 you can also do a single setup before
	/// the start of any tests.
	/// E.g.
	/// </summary>
	/// <BeforeClass>public static void clusterSetupAtBegining()</BeforeClass>
	/// <AfterClass>
	/// public static void ClusterShutdownAtEnd()
	/// </p>
	/// </AfterClass>
	public abstract class FileContextCreateMkdirBaseTest
	{
		protected internal readonly FileContextTestHelper fileContextTestHelper;

		protected internal static FileContext fc;

		static FileContextCreateMkdirBaseTest()
		{
			GenericTestUtils.SetLogLevel(FileSystem.Log, Level.Debug);
		}

		public FileContextCreateMkdirBaseTest()
		{
			fileContextTestHelper = CreateFileContextHelper();
		}

		protected internal virtual FileContextTestHelper CreateFileContextHelper()
		{
			return new FileContextTestHelper();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			fc.Mkdir(GetTestRootPath(fc), FileContext.DefaultPerm, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			fc.Delete(GetTestRootPath(fc), true);
		}

		///////////////////////
		//      Test Mkdir
		////////////////////////
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestMkdirNonRecursiveWithExistingDir()
		{
			Path f = GetTestRootPath(fc, "aDir");
			fc.Mkdir(f, FileContext.DefaultPerm, false);
			Assert.True(FileContextTestHelper.IsDir(fc, f));
		}

		[Fact]
		public virtual void TestMkdirNonRecursiveWithNonExistingDir()
		{
			try
			{
				fc.Mkdir(GetTestRootPath(fc, "NonExistant/aDir"), FileContext.DefaultPerm, false);
				NUnit.Framework.Assert.Fail("Mkdir with non existing parent dir should have failed"
					);
			}
			catch (IOException)
			{
			}
		}

		// failed As expected
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestMkdirRecursiveWithExistingDir()
		{
			Path f = GetTestRootPath(fc, "aDir");
			fc.Mkdir(f, FileContext.DefaultPerm, true);
			Assert.True(FileContextTestHelper.IsDir(fc, f));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestMkdirRecursiveWithNonExistingDir()
		{
			Path f = GetTestRootPath(fc, "NonExistant2/aDir");
			fc.Mkdir(f, FileContext.DefaultPerm, true);
			Assert.True(FileContextTestHelper.IsDir(fc, f));
		}

		///////////////////////
		//      Test Create
		////////////////////////
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateNonRecursiveWithExistingDir()
		{
			Path f = GetTestRootPath(fc, "foo");
			FileContextTestHelper.CreateFile(fc, f);
			Assert.True(FileContextTestHelper.IsFile(fc, f));
		}

		[Fact]
		public virtual void TestCreateNonRecursiveWithNonExistingDir()
		{
			try
			{
				FileContextTestHelper.CreateFileNonRecursive(fc, GetTestRootPath(fc, "NonExisting/foo"
					));
				NUnit.Framework.Assert.Fail("Create with non existing parent dir should have failed"
					);
			}
			catch (IOException)
			{
			}
		}

		// As expected
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateRecursiveWithExistingDir()
		{
			Path f = GetTestRootPath(fc, "foo");
			FileContextTestHelper.CreateFile(fc, f);
			Assert.True(FileContextTestHelper.IsFile(fc, f));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCreateRecursiveWithNonExistingDir()
		{
			Path f = GetTestRootPath(fc, "NonExisting/foo");
			FileContextTestHelper.CreateFile(fc, f);
			Assert.True(FileContextTestHelper.IsFile(fc, f));
		}

		private Path GetTestRootPath(FileContext fc)
		{
			return fileContextTestHelper.GetTestRootPath(fc);
		}

		private Path GetTestRootPath(FileContext fc, string pathString)
		{
			return fileContextTestHelper.GetTestRootPath(fc, pathString);
		}
	}
}
