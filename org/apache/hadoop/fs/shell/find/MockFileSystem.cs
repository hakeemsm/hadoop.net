using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	/// <summary>
	/// A mock
	/// <see cref="org.apache.hadoop.fs.FileSystem"/>
	/// for use with the
	/// <see cref="Find"/>
	/// unit tests. Usage:
	/// FileSystem mockFs = MockFileSystem.setup(); Methods in the mockFs can then be
	/// mocked out by the test script. The
	/// <see cref="org.apache.hadoop.conf.Configuration"/>
	/// can be accessed by
	/// mockFs.getConf(); The following methods are fixed within the class: -
	/// <see cref="org.apache.hadoop.fs.FileSystem.initialize(java.net.URI, org.apache.hadoop.conf.Configuration)
	/// 	"/>
	/// blank stub -
	/// <see cref="org.apache.hadoop.fs.FileSystem.makeQualified(org.apache.hadoop.fs.Path)
	/// 	"/>
	/// returns the passed in
	/// <see cref="org.apache.hadoop.fs.Path"/>
	/// -
	/// <see cref="org.apache.hadoop.fs.FileSystem.getWorkingDirectory()"/>
	/// returns new Path("/") -
	/// <see cref="org.apache.hadoop.fs.FileSystem.resolvePath(org.apache.hadoop.fs.Path)
	/// 	"/>
	/// returns the passed in
	/// <see cref="org.apache.hadoop.fs.Path"/>
	/// </summary>
	internal class MockFileSystem : org.apache.hadoop.fs.FilterFileSystem
	{
		private static org.apache.hadoop.fs.FileSystem mockFs = null;

		/// <summary>
		/// Setup and return the underlying
		/// <see cref="org.apache.hadoop.fs.FileSystem"/>
		/// mock
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal static org.apache.hadoop.fs.FileSystem setup()
		{
			if (mockFs == null)
			{
				mockFs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem>();
			}
			org.mockito.Mockito.reset(mockFs);
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("fs.defaultFS", "mockfs:///");
			conf.setClass("fs.mockfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.MockFileSystem
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			org.mockito.Mockito.when(mockFs.getConf()).thenReturn(conf);
			return mockFs;
		}

		private MockFileSystem()
			: base(mockFs)
		{
		}

		public override void initialize(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
		{
		}

		public override org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path
			 path)
		{
			return path;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path
			 pathPattern)
		{
			return fs.globStatus(pathPattern);
		}

		public override org.apache.hadoop.fs.Path getWorkingDirectory()
		{
			return new org.apache.hadoop.fs.Path("/");
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path resolvePath(org.apache.hadoop.fs.Path p
			)
		{
			return p;
		}
	}
}
