using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.FS.Shell.Find
{
	/// <summary>
	/// A mock
	/// <see cref="Org.Apache.Hadoop.FS.FileSystem"/>
	/// for use with the
	/// <see cref="Find"/>
	/// unit tests. Usage:
	/// FileSystem mockFs = MockFileSystem.setup(); Methods in the mockFs can then be
	/// mocked out by the test script. The
	/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
	/// can be accessed by
	/// mockFs.getConf(); The following methods are fixed within the class: -
	/// <see cref="Org.Apache.Hadoop.FS.FileSystem.Initialize(URI, Org.Apache.Hadoop.Conf.Configuration)
	/// 	"/>
	/// blank stub -
	/// <see cref="Org.Apache.Hadoop.FS.FileSystem.MakeQualified(Org.Apache.Hadoop.FS.Path)
	/// 	"/>
	/// returns the passed in
	/// <see cref="Org.Apache.Hadoop.FS.Path"/>
	/// -
	/// <see cref="Org.Apache.Hadoop.FS.FileSystem.GetWorkingDirectory()"/>
	/// returns new Path("/") -
	/// <see cref="Org.Apache.Hadoop.FS.FileSystem.ResolvePath(Org.Apache.Hadoop.FS.Path)
	/// 	"/>
	/// returns the passed in
	/// <see cref="Org.Apache.Hadoop.FS.Path"/>
	/// </summary>
	internal class MockFileSystem : FilterFileSystem
	{
		private static FileSystem mockFs = null;

		/// <summary>
		/// Setup and return the underlying
		/// <see cref="Org.Apache.Hadoop.FS.FileSystem"/>
		/// mock
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal static FileSystem Setup()
		{
			if (mockFs == null)
			{
				mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			}
			Org.Mockito.Mockito.Reset(mockFs);
			Configuration conf = new Configuration();
			conf.Set("fs.defaultFS", "mockfs:///");
			conf.SetClass("fs.mockfs.impl", typeof(Org.Apache.Hadoop.FS.Shell.Find.MockFileSystem
				), typeof(FileSystem));
			Org.Mockito.Mockito.When(mockFs.GetConf()).ThenReturn(conf);
			return mockFs;
		}

		private MockFileSystem()
			: base(mockFs)
		{
		}

		public override void Initialize(URI uri, Configuration conf)
		{
		}

		public override Path MakeQualified(Path path)
		{
			return path;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] GlobStatus(Path pathPattern)
		{
			return fs.GlobStatus(pathPattern);
		}

		public override Path GetWorkingDirectory()
		{
			return new Path("/");
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path ResolvePath(Path p)
		{
			return p;
		}
	}
}
