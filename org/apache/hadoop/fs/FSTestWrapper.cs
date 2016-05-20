using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Abstraction of filesystem functionality with additional helper methods
	/// commonly used in tests.
	/// </summary>
	/// <remarks>
	/// Abstraction of filesystem functionality with additional helper methods
	/// commonly used in tests. This allows generic tests to be written which apply
	/// to the two filesystem abstractions in Hadoop:
	/// <see cref="FileSystem"/>
	/// and
	/// <see cref="FileContext"/>
	/// .
	/// </remarks>
	public abstract class FSTestWrapper : org.apache.hadoop.fs.FSWrapper
	{
		protected internal const int DEFAULT_BLOCK_SIZE = 1024;

		protected internal const int DEFAULT_NUM_BLOCKS = 2;

		protected internal string testRootDir = null;

		protected internal string absTestRootDir = null;

		public FSTestWrapper(string testRootDir)
		{
			//
			// Test helper methods taken from FileContextTestHelper
			//
			// Use default test dir if not provided
			if (testRootDir == null || testRootDir.isEmpty())
			{
				testRootDir = Sharpen.Runtime.getProperty("test.build.data", "build/test/data");
			}
			// salt test dir with some random digits for safe parallel runs
			this.testRootDir = testRootDir + "/" + org.apache.commons.lang.RandomStringUtils.
				randomAlphanumeric(10);
		}

		public static byte[] getFileData(int numOfBlocks, long blockSize)
		{
			byte[] data = new byte[(int)(numOfBlocks * blockSize)];
			for (int i = 0; i < data.Length; i++)
			{
				data[i] = unchecked((byte)(i % 10));
			}
			return data;
		}

		public virtual org.apache.hadoop.fs.Path getTestRootPath()
		{
			return makeQualified(new org.apache.hadoop.fs.Path(testRootDir));
		}

		public virtual org.apache.hadoop.fs.Path getTestRootPath(string pathString)
		{
			return makeQualified(new org.apache.hadoop.fs.Path(testRootDir, pathString));
		}

		// the getAbsolutexxx method is needed because the root test dir
		// can be messed up by changing the working dir.
		/// <exception cref="System.IO.IOException"/>
		public virtual string getAbsoluteTestRootDir()
		{
			if (absTestRootDir == null)
			{
				org.apache.hadoop.fs.Path testRootPath = new org.apache.hadoop.fs.Path(testRootDir
					);
				if (testRootPath.isAbsolute())
				{
					absTestRootDir = testRootDir;
				}
				else
				{
					absTestRootDir = getWorkingDirectory().ToString() + "/" + testRootDir;
				}
			}
			return absTestRootDir;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.Path getAbsoluteTestRootPath()
		{
			return makeQualified(new org.apache.hadoop.fs.Path(getAbsoluteTestRootDir()));
		}

		/// <exception cref="org.apache.hadoop.fs.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FSTestWrapper getLocalFSWrapper();

		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.Path getDefaultWorkingDirectory();

		/*
		* Create files with numBlocks blocks each with block size blockSize.
		*/
		/// <exception cref="System.IO.IOException"/>
		public abstract long createFile(org.apache.hadoop.fs.Path path, int numBlocks, params 
			org.apache.hadoop.fs.Options.CreateOpts[] options);

		/// <exception cref="System.IO.IOException"/>
		public abstract long createFile(org.apache.hadoop.fs.Path path, int numBlocks, int
			 blockSize);

		/// <exception cref="System.IO.IOException"/>
		public abstract long createFile(org.apache.hadoop.fs.Path path);

		/// <exception cref="System.IO.IOException"/>
		public abstract long createFile(string name);

		/// <exception cref="System.IO.IOException"/>
		public abstract long createFileNonRecursive(string name);

		/// <exception cref="System.IO.IOException"/>
		public abstract long createFileNonRecursive(org.apache.hadoop.fs.Path path);

		/// <exception cref="System.IO.IOException"/>
		public abstract void appendToFile(org.apache.hadoop.fs.Path path, int numBlocks, 
			params org.apache.hadoop.fs.Options.CreateOpts[] options);

		/// <exception cref="System.IO.IOException"/>
		public abstract bool exists(org.apache.hadoop.fs.Path p);

		/// <exception cref="System.IO.IOException"/>
		public abstract bool isFile(org.apache.hadoop.fs.Path p);

		/// <exception cref="System.IO.IOException"/>
		public abstract bool isDir(org.apache.hadoop.fs.Path p);

		/// <exception cref="System.IO.IOException"/>
		public abstract bool isSymlink(org.apache.hadoop.fs.Path p);

		/// <exception cref="System.IO.IOException"/>
		public abstract void writeFile(org.apache.hadoop.fs.Path path, byte[] b);

		/// <exception cref="System.IO.IOException"/>
		public abstract byte[] readFile(org.apache.hadoop.fs.Path path, int len);

		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FileStatus containsPath(org.apache.hadoop.fs.Path
			 path, org.apache.hadoop.fs.FileStatus[] dirList);

		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.fs.FileStatus containsPath(string path, org.apache.hadoop.fs.FileStatus
			[] dirList);

		internal enum fileType
		{
			isDir,
			isFile,
			isSymlink
		}

		/// <exception cref="System.IO.IOException"/>
		public abstract void checkFileStatus(string path, org.apache.hadoop.fs.FSTestWrapper.fileType
			 expectedType);

		/// <exception cref="System.IO.IOException"/>
		public abstract void checkFileLinkStatus(string path, org.apache.hadoop.fs.FSTestWrapper.fileType
			 expectedType);

		public abstract org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 arg1, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> arg2, org.apache.hadoop.fs.Options.CreateOpts
			[] arg3);

		public abstract void createSymlink(org.apache.hadoop.fs.Path arg1, org.apache.hadoop.fs.Path
			 arg2, bool arg3);

		public abstract bool delete(org.apache.hadoop.fs.Path arg1, bool arg2);

		public abstract org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
			 arg1, long arg2, long arg3);

		public abstract org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
			 arg1);

		public abstract org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 arg1);

		public abstract org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
			 arg1);

		public abstract org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path
			 arg1);

		public abstract org.apache.hadoop.fs.Path getWorkingDirectory();

		public abstract org.apache.hadoop.fs.FileStatus[] globStatus(org.apache.hadoop.fs.Path
			 arg1, org.apache.hadoop.fs.PathFilter arg2);

		public abstract org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 arg1);

		public abstract org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.FileStatus
			> listStatusIterator(org.apache.hadoop.fs.Path arg1);

		public abstract org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path
			 arg1);

		public abstract void mkdir(org.apache.hadoop.fs.Path arg1, org.apache.hadoop.fs.permission.FsPermission
			 arg2, bool arg3);

		public abstract org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 arg1);

		public abstract void rename(org.apache.hadoop.fs.Path arg1, org.apache.hadoop.fs.Path
			 arg2, org.apache.hadoop.fs.Options.Rename[] arg3);

		public abstract void setOwner(org.apache.hadoop.fs.Path arg1, string arg2, string
			 arg3);

		public abstract void setPermission(org.apache.hadoop.fs.Path arg1, org.apache.hadoop.fs.permission.FsPermission
			 arg2);

		public abstract bool setReplication(org.apache.hadoop.fs.Path arg1, short arg2);

		public abstract void setTimes(org.apache.hadoop.fs.Path arg1, long arg2, long arg3
			);

		public abstract void setWorkingDirectory(org.apache.hadoop.fs.Path arg1);
	}
}
