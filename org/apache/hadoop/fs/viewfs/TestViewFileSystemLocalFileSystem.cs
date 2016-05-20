using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>
	/// Test the ViewFileSystemBaseTest using a viewfs with authority:
	/// viewfs://mountTableName/
	/// ie the authority is used to load a mount table.
	/// </summary>
	/// <remarks>
	/// Test the ViewFileSystemBaseTest using a viewfs with authority:
	/// viewfs://mountTableName/
	/// ie the authority is used to load a mount table.
	/// The authority name used is "default"
	/// </remarks>
	public class TestViewFileSystemLocalFileSystem : org.apache.hadoop.fs.viewfs.ViewFileSystemBaseTest
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			// create the test root on local_fs
			fsTarget = org.apache.hadoop.fs.FileSystem.getLocal(new org.apache.hadoop.conf.Configuration
				());
			base.setUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void tearDown()
		{
			fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
			base.tearDown();
		}
	}
}
