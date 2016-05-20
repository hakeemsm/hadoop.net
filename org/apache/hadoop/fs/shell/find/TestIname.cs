using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	public class TestIname
	{
		private org.apache.hadoop.fs.FileSystem mockFs;

		private org.apache.hadoop.fs.shell.find.Name.Iname name;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void resetMock()
		{
			mockFs = org.apache.hadoop.fs.shell.find.MockFileSystem.setup();
		}

		/// <exception cref="System.IO.IOException"/>
		private void setup(string arg)
		{
			name = new org.apache.hadoop.fs.shell.find.Name.Iname();
			org.apache.hadoop.fs.shell.find.TestHelper.addArgument(name, arg);
			name.setOptions(new org.apache.hadoop.fs.shell.find.FindOptions());
			name.prepare();
		}

		// test a matching name (same case)
		/// <exception cref="System.IO.IOException"/>
		public virtual void applyMatch()
		{
			setup("name");
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				("/directory/path/name", mockFs.getConf());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.PASS, name
				.apply(item, -1));
		}

		// test a non-matching name
		/// <exception cref="System.IO.IOException"/>
		public virtual void applyNotMatch()
		{
			setup("name");
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				("/directory/path/notname", mockFs.getConf());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.FAIL, name
				.apply(item, -1));
		}

		// test a matching name (different case)
		/// <exception cref="System.IO.IOException"/>
		public virtual void applyMixedCase()
		{
			setup("name");
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				("/directory/path/NaMe", mockFs.getConf());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.PASS, name
				.apply(item, -1));
		}

		// test a matching glob pattern (same case)
		/// <exception cref="System.IO.IOException"/>
		public virtual void applyGlob()
		{
			setup("n*e");
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				("/directory/path/name", mockFs.getConf());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.PASS, name
				.apply(item, -1));
		}

		// test a matching glob pattern (different case)
		/// <exception cref="System.IO.IOException"/>
		public virtual void applyGlobMixedCase()
		{
			setup("n*e");
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				("/directory/path/NaMe", mockFs.getConf());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.PASS, name
				.apply(item, -1));
		}

		// test a non-matching glob pattern
		/// <exception cref="System.IO.IOException"/>
		public virtual void applyGlobNotMatch()
		{
			setup("n*e");
			org.apache.hadoop.fs.shell.PathData item = new org.apache.hadoop.fs.shell.PathData
				("/directory/path/notmatch", mockFs.getConf());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.fs.shell.find.Result.FAIL, name
				.apply(item, -1));
		}
	}
}
