

namespace Org.Apache.Hadoop.FS
{
	public class TestLocalFSFileContextCreateMkdir : FileContextCreateMkdirBaseTest
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
