

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Test Util for localFs using FileContext API.</summary>
	public class TestFcLocalFsUtil : FileContextUtilBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			fc = FileContext.GetLocalFSFileContext();
			base.SetUp();
		}
	}
}
