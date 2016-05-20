using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestLocalFSFileContextCreateMkdir : org.apache.hadoop.fs.FileContextCreateMkdirBaseTest
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
