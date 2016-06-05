using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.FS.Permission;


namespace Org.Apache.Hadoop.FS
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
	public abstract class FSTestWrapper : FSWrapper
	{
		protected internal const int DefaultBlockSize = 1024;

		protected internal const int DefaultNumBlocks = 2;

		protected internal string testRootDir = null;

		protected internal string absTestRootDir = null;

		public FSTestWrapper(string testRootDir)
		{
			//
			// Test helper methods taken from FileContextTestHelper
			//
			// Use default test dir if not provided
			if (testRootDir == null || testRootDir.IsEmpty())
			{
				testRootDir = Runtime.GetProperty("test.build.data", "build/test/data");
			}
			// salt test dir with some random digits for safe parallel runs
			this.testRootDir = testRootDir + "/" + RandomStringUtils.RandomAlphanumeric(10);
		}

		public static byte[] GetFileData(int numOfBlocks, long blockSize)
		{
			byte[] data = new byte[(int)(numOfBlocks * blockSize)];
			for (int i = 0; i < data.Length; i++)
			{
				data[i] = unchecked((byte)(i % 10));
			}
			return data;
		}

		public virtual Path GetTestRootPath()
		{
			return MakeQualified(new Path(testRootDir));
		}

		public virtual Path GetTestRootPath(string pathString)
		{
			return MakeQualified(new Path(testRootDir, pathString));
		}

		// the getAbsolutexxx method is needed because the root test dir
		// can be messed up by changing the working dir.
		/// <exception cref="System.IO.IOException"/>
		public virtual string GetAbsoluteTestRootDir()
		{
			if (absTestRootDir == null)
			{
				Path testRootPath = new Path(testRootDir);
				if (testRootPath.IsAbsolute())
				{
					absTestRootDir = testRootDir;
				}
				else
				{
					absTestRootDir = GetWorkingDirectory().ToString() + "/" + testRootDir;
				}
			}
			return absTestRootDir;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetAbsoluteTestRootPath()
		{
			return MakeQualified(new Path(GetAbsoluteTestRootDir()));
		}

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		public abstract Org.Apache.Hadoop.FS.FSTestWrapper GetLocalFSWrapper();

		/// <exception cref="System.IO.IOException"/>
		public abstract Path GetDefaultWorkingDirectory();

		/*
		* Create files with numBlocks blocks each with block size blockSize.
		*/
		/// <exception cref="System.IO.IOException"/>
		public abstract long CreateFile(Path path, int numBlocks, params Options.CreateOpts
			[] options);

		/// <exception cref="System.IO.IOException"/>
		public abstract long CreateFile(Path path, int numBlocks, int blockSize);

		/// <exception cref="System.IO.IOException"/>
		public abstract long CreateFile(Path path);

		/// <exception cref="System.IO.IOException"/>
		public abstract long CreateFile(string name);

		/// <exception cref="System.IO.IOException"/>
		public abstract long CreateFileNonRecursive(string name);

		/// <exception cref="System.IO.IOException"/>
		public abstract long CreateFileNonRecursive(Path path);

		/// <exception cref="System.IO.IOException"/>
		public abstract void AppendToFile(Path path, int numBlocks, params Options.CreateOpts
			[] options);

		/// <exception cref="System.IO.IOException"/>
		public abstract bool Exists(Path p);

		/// <exception cref="System.IO.IOException"/>
		public abstract bool IsFile(Path p);

		/// <exception cref="System.IO.IOException"/>
		public abstract bool IsDir(Path p);

		/// <exception cref="System.IO.IOException"/>
		public abstract bool IsSymlink(Path p);

		/// <exception cref="System.IO.IOException"/>
		public abstract void WriteFile(Path path, byte[] b);

		/// <exception cref="System.IO.IOException"/>
		public abstract byte[] ReadFile(Path path, int len);

		/// <exception cref="System.IO.IOException"/>
		public abstract FileStatus ContainsPath(Path path, FileStatus[] dirList);

		/// <exception cref="System.IO.IOException"/>
		public abstract FileStatus ContainsPath(string path, FileStatus[] dirList);

		internal enum FileType
		{
			isDir,
			isFile,
			isSymlink
		}

		/// <exception cref="System.IO.IOException"/>
		public abstract void CheckFileStatus(string path, FSTestWrapper.FileType expectedType
			);

		/// <exception cref="System.IO.IOException"/>
		public abstract void CheckFileLinkStatus(string path, FSTestWrapper.FileType expectedType
			);

		public abstract FSDataOutputStream Create(Path arg1, EnumSet<CreateFlag> arg2, Options.CreateOpts
			[] arg3);

		public abstract void CreateSymlink(Path arg1, Path arg2, bool arg3);

		public abstract bool Delete(Path arg1, bool arg2);

		public abstract BlockLocation[] GetFileBlockLocations(Path arg1, long arg2, long 
			arg3);

		public abstract FileChecksum GetFileChecksum(Path arg1);

		public abstract FileStatus GetFileLinkStatus(Path arg1);

		public abstract FileStatus GetFileStatus(Path arg1);

		public abstract Path GetLinkTarget(Path arg1);

		public abstract Path GetWorkingDirectory();

		public abstract FileStatus[] GlobStatus(Path arg1, PathFilter arg2);

		public abstract FileStatus[] ListStatus(Path arg1);

		public abstract RemoteIterator<FileStatus> ListStatusIterator(Path arg1);

		public abstract Path MakeQualified(Path arg1);

		public abstract void Mkdir(Path arg1, FsPermission arg2, bool arg3);

		public abstract FSDataInputStream Open(Path arg1);

		public abstract void Rename(Path arg1, Path arg2, Options.Rename[] arg3);

		public abstract void SetOwner(Path arg1, string arg2, string arg3);

		public abstract void SetPermission(Path arg1, FsPermission arg2);

		public abstract bool SetReplication(Path arg1, short arg2);

		public abstract void SetTimes(Path arg1, long arg2, long arg3);

		public abstract void SetWorkingDirectory(Path arg1);
	}
}
