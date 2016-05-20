using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Test Util for localFs using FileContext API.</summary>
	public class TestFcLocalFsUtil : org.apache.hadoop.fs.FileContextUtilBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			fc = org.apache.hadoop.fs.FileContext.getLocalFSFileContext();
			base.setUp();
		}
	}
}
