using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
{
	public class TestPrint
	{
		private FileSystem mockFs;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void ResetMock()
		{
			mockFs = MockFileSystem.Setup();
		}

		// test the full path is printed to stdout
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPrint()
		{
			Print print = new Print();
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			FindOptions options = new FindOptions();
			options.SetOut(@out);
			print.SetOptions(options);
			string filename = "/one/two/test";
			PathData item = new PathData(filename, mockFs.GetConf());
			Assert.Equal(Result.Pass, print.Apply(item, -1));
			Org.Mockito.Mockito.Verify(@out).Write(filename + '\n');
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
		}
	}
}
