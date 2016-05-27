using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
{
	public class TestIname
	{
		private FileSystem mockFs;

		private Name.Iname name;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void ResetMock()
		{
			mockFs = MockFileSystem.Setup();
		}

		/// <exception cref="System.IO.IOException"/>
		private void Setup(string arg)
		{
			name = new Name.Iname();
			TestHelper.AddArgument(name, arg);
			name.SetOptions(new FindOptions());
			name.Prepare();
		}

		// test a matching name (same case)
		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplyMatch()
		{
			Setup("name");
			PathData item = new PathData("/directory/path/name", mockFs.GetConf());
			NUnit.Framework.Assert.AreEqual(Result.Pass, name.Apply(item, -1));
		}

		// test a non-matching name
		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplyNotMatch()
		{
			Setup("name");
			PathData item = new PathData("/directory/path/notname", mockFs.GetConf());
			NUnit.Framework.Assert.AreEqual(Result.Fail, name.Apply(item, -1));
		}

		// test a matching name (different case)
		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplyMixedCase()
		{
			Setup("name");
			PathData item = new PathData("/directory/path/NaMe", mockFs.GetConf());
			NUnit.Framework.Assert.AreEqual(Result.Pass, name.Apply(item, -1));
		}

		// test a matching glob pattern (same case)
		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplyGlob()
		{
			Setup("n*e");
			PathData item = new PathData("/directory/path/name", mockFs.GetConf());
			NUnit.Framework.Assert.AreEqual(Result.Pass, name.Apply(item, -1));
		}

		// test a matching glob pattern (different case)
		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplyGlobMixedCase()
		{
			Setup("n*e");
			PathData item = new PathData("/directory/path/NaMe", mockFs.GetConf());
			NUnit.Framework.Assert.AreEqual(Result.Pass, name.Apply(item, -1));
		}

		// test a non-matching glob pattern
		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplyGlobNotMatch()
		{
			Setup("n*e");
			PathData item = new PathData("/directory/path/notmatch", mockFs.GetConf());
			NUnit.Framework.Assert.AreEqual(Result.Fail, name.Apply(item, -1));
		}
	}
}
