

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Test permissions for localFs using FileContext API.</summary>
	public class TestFcLocalFsPermission : FileContextPermissionBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			base.TearDown();
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		protected internal override FileContext GetFileContext()
		{
			return FileContext.GetLocalFSFileContext();
		}
	}
}
