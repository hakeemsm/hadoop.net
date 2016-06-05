using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.FS.Viewfs
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
	public class TestViewFileSystemLocalFileSystem : ViewFileSystemBaseTest
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			// create the test root on local_fs
			fsTarget = FileSystem.GetLocal(new Configuration());
			base.SetUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			fsTarget.Delete(fileSystemTestHelper.GetTestRootPath(fsTarget), true);
			base.TearDown();
		}
	}
}
