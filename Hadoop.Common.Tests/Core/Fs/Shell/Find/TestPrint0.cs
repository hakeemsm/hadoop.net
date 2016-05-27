using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
{
	public class TestPrint0
	{
		private FileSystem mockFs;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void ResetMock()
		{
			mockFs = MockFileSystem.Setup();
		}

		// test the full path is printed to stdout with a '\0'
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPrint()
		{
			Print.Print0 print = new Print.Print0();
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			FindOptions options = new FindOptions();
			options.SetOut(@out);
			print.SetOptions(options);
			string filename = "/one/two/test";
			PathData item = new PathData(filename, mockFs.GetConf());
			NUnit.Framework.Assert.AreEqual(Result.Pass, print.Apply(item, -1));
			Org.Mockito.Mockito.Verify(@out).Write(filename + '\0');
			Org.Mockito.Mockito.VerifyNoMoreInteractions(@out);
		}
	}
}
