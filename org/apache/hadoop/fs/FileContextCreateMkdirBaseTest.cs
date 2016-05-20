using Sharpen;

namespace org.apache.hadoop.fs
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
	/// <see cref="setUp()"/>
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
		protected internal readonly org.apache.hadoop.fs.FileContextTestHelper fileContextTestHelper;

		protected internal static org.apache.hadoop.fs.FileContext fc;

		static FileContextCreateMkdirBaseTest()
		{
			org.apache.hadoop.test.GenericTestUtils.setLogLevel(org.apache.hadoop.fs.FileSystem
				.LOG, org.apache.log4j.Level.DEBUG);
		}

		public FileContextCreateMkdirBaseTest()
		{
			fileContextTestHelper = createFileContextHelper();
		}

		protected internal virtual org.apache.hadoop.fs.FileContextTestHelper createFileContextHelper
			()
		{
			return new org.apache.hadoop.fs.FileContextTestHelper();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			fc.mkdir(getTestRootPath(fc), org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			fc.delete(getTestRootPath(fc), true);
		}

		///////////////////////
		//      Test Mkdir
		////////////////////////
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirNonRecursiveWithExistingDir()
		{
			org.apache.hadoop.fs.Path f = getTestRootPath(fc, "aDir");
			fc.mkdir(f, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, false);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc
				, f));
		}

		[NUnit.Framework.Test]
		public virtual void testMkdirNonRecursiveWithNonExistingDir()
		{
			try
			{
				fc.mkdir(getTestRootPath(fc, "NonExistant/aDir"), org.apache.hadoop.fs.FileContext
					.DEFAULT_PERM, false);
				NUnit.Framework.Assert.Fail("Mkdir with non existing parent dir should have failed"
					);
			}
			catch (System.IO.IOException)
			{
			}
		}

		// failed As expected
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirRecursiveWithExistingDir()
		{
			org.apache.hadoop.fs.Path f = getTestRootPath(fc, "aDir");
			fc.mkdir(f, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc
				, f));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirRecursiveWithNonExistingDir()
		{
			org.apache.hadoop.fs.Path f = getTestRootPath(fc, "NonExistant2/aDir");
			fc.mkdir(f, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isDir(fc
				, f));
		}

		///////////////////////
		//      Test Create
		////////////////////////
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateNonRecursiveWithExistingDir()
		{
			org.apache.hadoop.fs.Path f = getTestRootPath(fc, "foo");
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc, f);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fc
				, f));
		}

		[NUnit.Framework.Test]
		public virtual void testCreateNonRecursiveWithNonExistingDir()
		{
			try
			{
				org.apache.hadoop.fs.FileContextTestHelper.createFileNonRecursive(fc, getTestRootPath
					(fc, "NonExisting/foo"));
				NUnit.Framework.Assert.Fail("Create with non existing parent dir should have failed"
					);
			}
			catch (System.IO.IOException)
			{
			}
		}

		// As expected
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateRecursiveWithExistingDir()
		{
			org.apache.hadoop.fs.Path f = getTestRootPath(fc, "foo");
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc, f);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fc
				, f));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCreateRecursiveWithNonExistingDir()
		{
			org.apache.hadoop.fs.Path f = getTestRootPath(fc, "NonExisting/foo");
			org.apache.hadoop.fs.FileContextTestHelper.createFile(fc, f);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.FileContextTestHelper.isFile(fc
				, f));
		}

		private org.apache.hadoop.fs.Path getTestRootPath(org.apache.hadoop.fs.FileContext
			 fc)
		{
			return fileContextTestHelper.getTestRootPath(fc);
		}

		private org.apache.hadoop.fs.Path getTestRootPath(org.apache.hadoop.fs.FileContext
			 fc, string pathString)
		{
			return fileContextTestHelper.getTestRootPath(fc, pathString);
		}
	}
}
