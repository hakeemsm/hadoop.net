using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	public class TestPrint0
	{
		private org.apache.hadoop.fs.FileSystem mockFs;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void resetMock()
		{
			mockFs = org.apache.hadoop.fs.shell.find.MockFileSystem.setup();
		}

		// test the full path is printed to stdout with a '\0'
		/// <exception cref="System.IO.IOException"/>
		public virtual void testPrint()
		{
			org.apache.hadoop.fs.shell.find.Print.Print0 print = new org.apache.hadoop.fs.shell.find.Print.Print0
				();
			System.IO.TextWriter @out = org.mockito.Mockito.mock<System.IO.TextWriter>();
			org.apache.hadoop.fs.shell.find.FindOptions options = new org.apache.hadoop.fs.shell.find.FindOptions
				();
			options.setOut(@out);
			print.setOptions(options);
			string filename = "/one/two/test";
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				(filename, mockFs.getConf());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.PASS, print
				.apply(item, -1));
			org.mockito.Mockito.verify(@out).Write(filename + '\0');
			org.mockito.Mockito.verifyNoMoreInteractions(@out);
		}
	}
}
