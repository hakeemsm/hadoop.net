using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Helper class for unit tests.</summary>
	public class FileSystemTestHelper
	{
		private const int DEFAULT_BLOCK_SIZE = 1024;

		private const int DEFAULT_NUM_BLOCKS = 2;

		private const short DEFAULT_NUM_REPL = 1;

		protected internal readonly string testRootDir;

		private string absTestRootDir = null;

		/// <summary>Create helper with test root located at <wd>/build/test/data</summary>
		public FileSystemTestHelper()
			: this(Sharpen.Runtime.getProperty("test.build.data", "target/test/data") + "/" +
				 org.apache.commons.lang.RandomStringUtils.randomAlphanumeric(10))
		{
		}

		/// <summary>Create helper with the specified test root dir</summary>
		public FileSystemTestHelper(string testRootDir)
		{
			this.testRootDir = testRootDir;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void addFileSystemForTesting(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.fs.FileSystem fs)
		{
			org.apache.hadoop.fs.FileSystem.addFileSystemForTesting(uri, conf, fs);
		}

		public static int getDefaultBlockSize()
		{
			return DEFAULT_BLOCK_SIZE;
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

		public virtual string getTestRootDir()
		{
			return testRootDir;
		}

		/*
		* get testRootPath qualified for fSys
		*/
		public virtual org.apache.hadoop.fs.Path getTestRootPath(org.apache.hadoop.fs.FileSystem
			 fSys)
		{
			return fSys.makeQualified(new org.apache.hadoop.fs.Path(testRootDir));
		}

		/*
		* get testRootPath + pathString qualified for fSys
		*/
		public virtual org.apache.hadoop.fs.Path getTestRootPath(org.apache.hadoop.fs.FileSystem
			 fSys, string pathString)
		{
			return fSys.makeQualified(new org.apache.hadoop.fs.Path(testRootDir, pathString));
		}

		// the getAbsolutexxx method is needed because the root test dir
		// can be messed up by changing the working dir since the TEST_ROOT_PATH
		// is often relative to the working directory of process
		// running the unit tests.
		/// <exception cref="System.IO.IOException"/>
		internal virtual string getAbsoluteTestRootDir(org.apache.hadoop.fs.FileSystem fSys
			)
		{
			// NOTE: can't cache because of different filesystems!
			//if (absTestRootDir == null) 
			if (new org.apache.hadoop.fs.Path(testRootDir).isAbsolute())
			{
				absTestRootDir = testRootDir;
			}
			else
			{
				absTestRootDir = fSys.getWorkingDirectory().ToString() + "/" + testRootDir;
			}
			//}
			return absTestRootDir;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.Path getAbsoluteTestRootPath(org.apache.hadoop.fs.FileSystem
			 fSys)
		{
			return fSys.makeQualified(new org.apache.hadoop.fs.Path(getAbsoluteTestRootDir(fSys
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.Path getDefaultWorkingDirectory(org.apache.hadoop.fs.FileSystem
			 fSys)
		{
			return getTestRootPath(fSys, "/user/" + Sharpen.Runtime.getProperty("user.name"))
				.makeQualified(fSys.getUri(), fSys.getWorkingDirectory());
		}

		/*
		* Create files with numBlocks blocks each with block size blockSize.
		*/
		/// <exception cref="System.IO.IOException"/>
		public static long createFile(org.apache.hadoop.fs.FileSystem fSys, org.apache.hadoop.fs.Path
			 path, int numBlocks, int blockSize, short numRepl, bool createParent)
		{
			return createFile(fSys, path, getFileData(numBlocks, blockSize), blockSize, numRepl
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public static long createFile(org.apache.hadoop.fs.FileSystem fSys, org.apache.hadoop.fs.Path
			 path, byte[] data, int blockSize, short numRepl)
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = fSys.create(path, false, 4096, numRepl
				, blockSize);
			try
			{
				@out.write(data, 0, data.Length);
			}
			finally
			{
				@out.close();
			}
			return data.Length;
		}

		/// <exception cref="System.IO.IOException"/>
		public static long createFile(org.apache.hadoop.fs.FileSystem fSys, org.apache.hadoop.fs.Path
			 path, int numBlocks, int blockSize, bool createParent)
		{
			return createFile(fSys, path, numBlocks, blockSize, fSys.getDefaultReplication(path
				), true);
		}

		/// <exception cref="System.IO.IOException"/>
		public static long createFile(org.apache.hadoop.fs.FileSystem fSys, org.apache.hadoop.fs.Path
			 path, int numBlocks, int blockSize)
		{
			return createFile(fSys, path, numBlocks, blockSize, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public static long createFile(org.apache.hadoop.fs.FileSystem fSys, org.apache.hadoop.fs.Path
			 path)
		{
			return createFile(fSys, path, DEFAULT_NUM_BLOCKS, DEFAULT_BLOCK_SIZE, DEFAULT_NUM_REPL
				, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long createFile(org.apache.hadoop.fs.FileSystem fSys, string name)
		{
			org.apache.hadoop.fs.Path path = getTestRootPath(fSys, name);
			return createFile(fSys, path);
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool exists(org.apache.hadoop.fs.FileSystem fSys, org.apache.hadoop.fs.Path
			 p)
		{
			return fSys.exists(p);
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool isFile(org.apache.hadoop.fs.FileSystem fSys, org.apache.hadoop.fs.Path
			 p)
		{
			try
			{
				return fSys.getFileStatus(p).isFile();
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool isDir(org.apache.hadoop.fs.FileSystem fSys, org.apache.hadoop.fs.Path
			 p)
		{
			try
			{
				return fSys.getFileStatus(p).isDirectory();
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string writeFile(org.apache.hadoop.fs.FileSystem fileSys, org.apache.hadoop.fs.Path
			 name, int fileSize)
		{
			long seed = unchecked((long)(0xDEADBEEFL));
			// Create and write a file that contains three blocks of data
			org.apache.hadoop.fs.FSDataOutputStream stm = fileSys.create(name);
			byte[] buffer = new byte[fileSize];
			java.util.Random rand = new java.util.Random(seed);
			rand.nextBytes(buffer);
			stm.write(buffer);
			stm.close();
			return Sharpen.Runtime.getStringForBytes(buffer);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string readFile(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 name, int buflen)
		{
			byte[] b = new byte[buflen];
			int offset = 0;
			org.apache.hadoop.fs.FSDataInputStream @in = fs.open(name);
			for (int remaining; (remaining = b.Length - offset) > 0 && (n = @in.read(b, offset
				, remaining)) != -1; offset += n)
			{
			}
			NUnit.Framework.Assert.AreEqual(offset, System.Math.min(b.Length, @in.getPos()));
			@in.close();
			string s = Sharpen.Runtime.getStringForBytes(b, 0, offset);
			return s;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileStatus containsPath(org.apache.hadoop.fs.FileSystem
			 fSys, org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.FileStatus[] dirList
			)
		{
			for (int i = 0; i < dirList.Length; i++)
			{
				if (getTestRootPath(fSys, path.ToString()).Equals(dirList[i].getPath()))
				{
					return dirList[i];
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.fs.FileStatus containsPath(org.apache.hadoop.fs.Path
			 path, org.apache.hadoop.fs.FileStatus[] dirList)
		{
			for (int i = 0; i < dirList.Length; i++)
			{
				if (path.Equals(dirList[i].getPath()))
				{
					return dirList[i];
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileStatus containsPath(org.apache.hadoop.fs.FileSystem
			 fSys, string path, org.apache.hadoop.fs.FileStatus[] dirList)
		{
			return containsPath(fSys, new org.apache.hadoop.fs.Path(path), dirList);
		}

		public enum fileType
		{
			isDir,
			isFile,
			isSymlink
		}

		/// <exception cref="System.IO.IOException"/>
		public static void checkFileStatus(org.apache.hadoop.fs.FileSystem aFs, string path
			, org.apache.hadoop.fs.FileSystemTestHelper.fileType expectedType)
		{
			org.apache.hadoop.fs.FileStatus s = aFs.getFileStatus(new org.apache.hadoop.fs.Path
				(path));
			NUnit.Framework.Assert.IsNotNull(s);
			if (expectedType == org.apache.hadoop.fs.FileSystemTestHelper.fileType.isDir)
			{
				NUnit.Framework.Assert.IsTrue(s.isDirectory());
			}
			else
			{
				if (expectedType == org.apache.hadoop.fs.FileSystemTestHelper.fileType.isFile)
				{
					NUnit.Framework.Assert.IsTrue(s.isFile());
				}
				else
				{
					if (expectedType == org.apache.hadoop.fs.FileSystemTestHelper.fileType.isSymlink)
					{
						NUnit.Framework.Assert.IsTrue(s.isSymlink());
					}
				}
			}
			NUnit.Framework.Assert.AreEqual(aFs.makeQualified(new org.apache.hadoop.fs.Path(path
				)), s.getPath());
		}

		/// <summary>
		/// Class to enable easier mocking of a FileSystem
		/// Use getRawFileSystem to retrieve the mock
		/// </summary>
		public class MockFileSystem : org.apache.hadoop.fs.FilterFileSystem
		{
			public MockFileSystem()
				: base(org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem
					>())
			{
			}

			// it's a bit ackward to mock ourselves, but it allows the visibility
			// of methods to be increased
			public override org.apache.hadoop.fs.FileSystem getRawFileSystem()
			{
				return (org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem)base.getRawFileSystem
					();
			}

			// these basic methods need to directly propagate to the mock to be
			// more transparent
			/// <exception cref="System.IO.IOException"/>
			public override void initialize(java.net.URI uri, org.apache.hadoop.conf.Configuration
				 conf)
			{
				fs.initialize(uri, conf);
			}

			public override string getCanonicalServiceName()
			{
				return fs.getCanonicalServiceName();
			}

			public override org.apache.hadoop.fs.FileSystem[] getChildFileSystems()
			{
				return fs.getChildFileSystems();
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.security.token.Token<object> getDelegationToken
				(string renewer)
			{
				// publicly expose for mocking
				return fs.getDelegationToken(renewer);
			}
		}
	}
}
