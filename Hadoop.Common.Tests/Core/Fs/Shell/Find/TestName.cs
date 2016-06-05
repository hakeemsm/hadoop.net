using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;


namespace Org.Apache.Hadoop.FS.Shell.Find
{
	public class TestName
	{
		private FileSystem mockFs;

		private Name name;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void ResetMock()
		{
			mockFs = MockFileSystem.Setup();
		}

		/// <exception cref="System.IO.IOException"/>
		private void Setup(string arg)
		{
			name = new Name();
			TestHelper.AddArgument(name, arg);
			name.SetOptions(new FindOptions());
			name.Prepare();
		}

		// test a matching name
		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplyMatch()
		{
			Setup("name");
			PathData item = new PathData("/directory/path/name", mockFs.GetConf());
			Assert.Equal(Result.Pass, name.Apply(item, -1));
		}

		// test a non-matching name
		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplyNotMatch()
		{
			Setup("name");
			PathData item = new PathData("/directory/path/notname", mockFs.GetConf());
			Assert.Equal(Result.Fail, name.Apply(item, -1));
		}

		// test a different case name
		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplyMixedCase()
		{
			Setup("name");
			PathData item = new PathData("/directory/path/NaMe", mockFs.GetConf());
			Assert.Equal(Result.Fail, name.Apply(item, -1));
		}

		// test a matching glob pattern
		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplyGlob()
		{
			Setup("n*e");
			PathData item = new PathData("/directory/path/name", mockFs.GetConf());
			Assert.Equal(Result.Pass, name.Apply(item, -1));
		}

		// test a glob pattern with different case
		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplyGlobMixedCase()
		{
			Setup("n*e");
			PathData item = new PathData("/directory/path/NaMe", mockFs.GetConf());
			Assert.Equal(Result.Fail, name.Apply(item, -1));
		}

		// test a non-matching glob pattern
		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplyGlobNotMatch()
		{
			Setup("n*e");
			PathData item = new PathData("/directory/path/notmatch", mockFs.GetConf());
			Assert.Equal(Result.Fail, name.Apply(item, -1));
		}
	}
}
