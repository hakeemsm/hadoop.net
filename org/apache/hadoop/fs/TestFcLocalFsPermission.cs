using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Test permissions for localFs using FileContext API.</summary>
	public class TestFcLocalFsPermission : org.apache.hadoop.fs.FileContextPermissionBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			base.setUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void tearDown()
		{
			base.tearDown();
		}

		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		protected internal override org.apache.hadoop.fs.FileContext getFileContext()
		{
			return org.apache.hadoop.fs.FileContext.getLocalFSFileContext();
		}
	}
}
