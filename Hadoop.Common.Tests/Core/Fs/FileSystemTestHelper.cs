using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>Helper class for unit tests.</summary>
	public class FileSystemTestHelper
	{
		private const int DefaultBlockSize = 1024;

		private const int DefaultNumBlocks = 2;

		private const short DefaultNumRepl = 1;

		protected internal readonly string testRootDir;

		private string absTestRootDir = null;

		/// <summary>Create helper with test root located at <wd>/build/test/data</summary>
		public FileSystemTestHelper()
			: this(Runtime.GetProperty("test.build.data", "target/test/data") + "/" + RandomStringUtils
				.RandomAlphanumeric(10))
		{
		}

		/// <summary>Create helper with the specified test root dir</summary>
		public FileSystemTestHelper(string testRootDir)
		{
			this.testRootDir = testRootDir;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void AddFileSystemForTesting(URI uri, Configuration conf, FileSystem
			 fs)
		{
			FileSystem.AddFileSystemForTesting(uri, conf, fs);
		}

		public static int GetDefaultBlockSize()
		{
			return DefaultBlockSize;
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

		public virtual string GetTestRootDir()
		{
			return testRootDir;
		}

		/*
		* get testRootPath qualified for fSys
		*/
		public virtual Path GetTestRootPath(FileSystem fSys)
		{
			return fSys.MakeQualified(new Path(testRootDir));
		}

		/*
		* get testRootPath + pathString qualified for fSys
		*/
		public virtual Path GetTestRootPath(FileSystem fSys, string pathString)
		{
			return fSys.MakeQualified(new Path(testRootDir, pathString));
		}

		// the getAbsolutexxx method is needed because the root test dir
		// can be messed up by changing the working dir since the TEST_ROOT_PATH
		// is often relative to the working directory of process
		// running the unit tests.
		/// <exception cref="System.IO.IOException"/>
		internal virtual string GetAbsoluteTestRootDir(FileSystem fSys)
		{
			// NOTE: can't cache because of different filesystems!
			//if (absTestRootDir == null) 
			if (new Path(testRootDir).IsAbsolute())
			{
				absTestRootDir = testRootDir;
			}
			else
			{
				absTestRootDir = fSys.GetWorkingDirectory().ToString() + "/" + testRootDir;
			}
			//}
			return absTestRootDir;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetAbsoluteTestRootPath(FileSystem fSys)
		{
			return fSys.MakeQualified(new Path(GetAbsoluteTestRootDir(fSys)));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetDefaultWorkingDirectory(FileSystem fSys)
		{
			return GetTestRootPath(fSys, "/user/" + Runtime.GetProperty("user.name")).MakeQualified
				(fSys.GetUri(), fSys.GetWorkingDirectory());
		}

		/*
		* Create files with numBlocks blocks each with block size blockSize.
		*/
		/// <exception cref="System.IO.IOException"/>
		public static long CreateFile(FileSystem fSys, Path path, int numBlocks, int blockSize
			, short numRepl, bool createParent)
		{
			return CreateFile(fSys, path, GetFileData(numBlocks, blockSize), blockSize, numRepl
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public static long CreateFile(FileSystem fSys, Path path, byte[] data, int blockSize
			, short numRepl)
		{
			FSDataOutputStream @out = fSys.Create(path, false, 4096, numRepl, blockSize);
			try
			{
				@out.Write(data, 0, data.Length);
			}
			finally
			{
				@out.Close();
			}
			return data.Length;
		}

		/// <exception cref="System.IO.IOException"/>
		public static long CreateFile(FileSystem fSys, Path path, int numBlocks, int blockSize
			, bool createParent)
		{
			return CreateFile(fSys, path, numBlocks, blockSize, fSys.GetDefaultReplication(path
				), true);
		}

		/// <exception cref="System.IO.IOException"/>
		public static long CreateFile(FileSystem fSys, Path path, int numBlocks, int blockSize
			)
		{
			return CreateFile(fSys, path, numBlocks, blockSize, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public static long CreateFile(FileSystem fSys, Path path)
		{
			return CreateFile(fSys, path, DefaultNumBlocks, DefaultBlockSize, DefaultNumRepl, 
				true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long CreateFile(FileSystem fSys, string name)
		{
			Path path = GetTestRootPath(fSys, name);
			return CreateFile(fSys, path);
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool Exists(FileSystem fSys, Path p)
		{
			return fSys.Exists(p);
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool IsFile(FileSystem fSys, Path p)
		{
			try
			{
				return fSys.GetFileStatus(p).IsFile();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool IsDir(FileSystem fSys, Path p)
		{
			try
			{
				return fSys.GetFileStatus(p).IsDirectory();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string WriteFile(FileSystem fileSys, Path name, int fileSize)
		{
			long seed = unchecked((long)(0xDEADBEEFL));
			// Create and write a file that contains three blocks of data
			FSDataOutputStream stm = fileSys.Create(name);
			byte[] buffer = new byte[fileSize];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
			return Runtime.GetStringForBytes(buffer);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string ReadFile(FileSystem fs, Path name, int buflen)
		{
			byte[] b = new byte[buflen];
			int offset = 0;
			FSDataInputStream @in = fs.Open(name);
			for (int remaining; (remaining = b.Length - offset) > 0 && (n = @in.Read(b, offset
				, remaining)) != -1; offset += n)
			{
			}
			Assert.Equal(offset, Math.Min(b.Length, @in.GetPos()));
			@in.Close();
			string s = Runtime.GetStringForBytes(b, 0, offset);
			return s;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual FileStatus ContainsPath(FileSystem fSys, Path path, FileStatus[] dirList
			)
		{
			for (int i = 0; i < dirList.Length; i++)
			{
				if (GetTestRootPath(fSys, path.ToString()).Equals(dirList[i].GetPath()))
				{
					return dirList[i];
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public static FileStatus ContainsPath(Path path, FileStatus[] dirList)
		{
			for (int i = 0; i < dirList.Length; i++)
			{
				if (path.Equals(dirList[i].GetPath()))
				{
					return dirList[i];
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual FileStatus ContainsPath(FileSystem fSys, string path, FileStatus[]
			 dirList)
		{
			return ContainsPath(fSys, new Path(path), dirList);
		}

		public enum FileType
		{
			isDir,
			isFile,
			isSymlink
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CheckFileStatus(FileSystem aFs, string path, FileSystemTestHelper.FileType
			 expectedType)
		{
			FileStatus s = aFs.GetFileStatus(new Path(path));
			NUnit.Framework.Assert.IsNotNull(s);
			if (expectedType == FileSystemTestHelper.FileType.isDir)
			{
				Assert.True(s.IsDirectory());
			}
			else
			{
				if (expectedType == FileSystemTestHelper.FileType.isFile)
				{
					Assert.True(s.IsFile());
				}
				else
				{
					if (expectedType == FileSystemTestHelper.FileType.isSymlink)
					{
						Assert.True(s.IsSymlink());
					}
				}
			}
			Assert.Equal(aFs.MakeQualified(new Path(path)), s.GetPath());
		}

		/// <summary>
		/// Class to enable easier mocking of a FileSystem
		/// Use getRawFileSystem to retrieve the mock
		/// </summary>
		public class MockFileSystem : FilterFileSystem
		{
			public MockFileSystem()
				: base(Org.Mockito.Mockito.Mock<FileSystemTestHelper.MockFileSystem>())
			{
			}

			// it's a bit ackward to mock ourselves, but it allows the visibility
			// of methods to be increased
			public override FileSystem GetRawFileSystem()
			{
				return (FileSystemTestHelper.MockFileSystem)base.GetRawFileSystem();
			}

			// these basic methods need to directly propagate to the mock to be
			// more transparent
			/// <exception cref="System.IO.IOException"/>
			public override void Initialize(URI uri, Configuration conf)
			{
				fs.Initialize(uri, conf);
			}

			public override string GetCanonicalServiceName()
			{
				return fs.GetCanonicalServiceName();
			}

			public override FileSystem[] GetChildFileSystems()
			{
				return fs.GetChildFileSystems();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationToken
				(string renewer)
			{
				// publicly expose for mocking
				return fs.GetDelegationToken(renewer);
			}
		}
	}
}
