using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Helper class for unit tests.</summary>
	public sealed class FileContextTestHelper
	{
		private const int DEFAULT_BLOCK_SIZE = 1024;

		private const int DEFAULT_NUM_BLOCKS = 2;

		private readonly string testRootDir;

		private string absTestRootDir = null;

		/// <summary>Create a context with test root relative to the <wd>/build/test/data</summary>
		public FileContextTestHelper()
			: this(Sharpen.Runtime.getProperty("test.build.data", "target/test/data") + "/" +
				 org.apache.commons.lang.RandomStringUtils.randomAlphanumeric(10))
		{
		}

		/// <summary>Create a context with the given test root</summary>
		public FileContextTestHelper(string testRootDir)
		{
			this.testRootDir = testRootDir;
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

		public org.apache.hadoop.fs.Path getTestRootPath(org.apache.hadoop.fs.FileContext
			 fc)
		{
			return fc.makeQualified(new org.apache.hadoop.fs.Path(testRootDir));
		}

		public org.apache.hadoop.fs.Path getTestRootPath(org.apache.hadoop.fs.FileContext
			 fc, string pathString)
		{
			return fc.makeQualified(new org.apache.hadoop.fs.Path(testRootDir, pathString));
		}

		// the getAbsolutexxx method is needed because the root test dir
		// can be messed up by changing the working dir.
		public string getAbsoluteTestRootDir(org.apache.hadoop.fs.FileContext fc)
		{
			if (absTestRootDir == null)
			{
				if (new org.apache.hadoop.fs.Path(testRootDir).isAbsolute())
				{
					absTestRootDir = testRootDir;
				}
				else
				{
					absTestRootDir = fc.getWorkingDirectory().ToString() + "/" + testRootDir;
				}
			}
			return absTestRootDir;
		}

		public org.apache.hadoop.fs.Path getAbsoluteTestRootPath(org.apache.hadoop.fs.FileContext
			 fc)
		{
			return fc.makeQualified(new org.apache.hadoop.fs.Path(getAbsoluteTestRootDir(fc))
				);
		}

		public org.apache.hadoop.fs.Path getDefaultWorkingDirectory(org.apache.hadoop.fs.FileContext
			 fc)
		{
			return getTestRootPath(fc, "/user/" + Sharpen.Runtime.getProperty("user.name")).makeQualified
				(fc.getDefaultFileSystem().getUri(), fc.getWorkingDirectory());
		}

		/*
		* Create files with numBlocks blocks each with block size blockSize.
		*/
		/// <exception cref="System.IO.IOException"/>
		public static long createFile(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 path, int numBlocks, params org.apache.hadoop.fs.Options.CreateOpts[] options)
		{
			org.apache.hadoop.fs.Options.CreateOpts.BlockSize blockSizeOpt = org.apache.hadoop.fs.Options.CreateOpts
				.getOpt<org.apache.hadoop.fs.Options.CreateOpts.BlockSize>(options);
			long blockSize = blockSizeOpt != null ? blockSizeOpt.getValue() : DEFAULT_BLOCK_SIZE;
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(path, java.util.EnumSet.
				of(org.apache.hadoop.fs.CreateFlag.CREATE), options);
			byte[] data = getFileData(numBlocks, blockSize);
			@out.write(data, 0, data.Length);
			@out.close();
			return data.Length;
		}

		/// <exception cref="System.IO.IOException"/>
		public static long createFile(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 path, int numBlocks, int blockSize)
		{
			return createFile(fc, path, numBlocks, org.apache.hadoop.fs.Options.CreateOpts.blockSize
				(blockSize), org.apache.hadoop.fs.Options.CreateOpts.createParent());
		}

		/// <exception cref="System.IO.IOException"/>
		public static long createFile(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 path)
		{
			return createFile(fc, path, DEFAULT_NUM_BLOCKS, org.apache.hadoop.fs.Options.CreateOpts
				.createParent());
		}

		/// <exception cref="System.IO.IOException"/>
		public long createFile(org.apache.hadoop.fs.FileContext fc, string name)
		{
			org.apache.hadoop.fs.Path path = getTestRootPath(fc, name);
			return createFile(fc, path);
		}

		/// <exception cref="System.IO.IOException"/>
		public long createFileNonRecursive(org.apache.hadoop.fs.FileContext fc, string name
			)
		{
			org.apache.hadoop.fs.Path path = getTestRootPath(fc, name);
			return createFileNonRecursive(fc, path);
		}

		/// <exception cref="System.IO.IOException"/>
		public static long createFileNonRecursive(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 path)
		{
			return createFile(fc, path, DEFAULT_NUM_BLOCKS, org.apache.hadoop.fs.Options.CreateOpts
				.donotCreateParent());
		}

		/// <exception cref="System.IO.IOException"/>
		public static void appendToFile(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 path, int numBlocks, params org.apache.hadoop.fs.Options.CreateOpts[] options)
		{
			org.apache.hadoop.fs.Options.CreateOpts.BlockSize blockSizeOpt = org.apache.hadoop.fs.Options.CreateOpts
				.getOpt<org.apache.hadoop.fs.Options.CreateOpts.BlockSize>(options);
			long blockSize = blockSizeOpt != null ? blockSizeOpt.getValue() : DEFAULT_BLOCK_SIZE;
			org.apache.hadoop.fs.FSDataOutputStream @out;
			@out = fc.create(path, java.util.EnumSet.of(org.apache.hadoop.fs.CreateFlag.APPEND
				));
			byte[] data = getFileData(numBlocks, blockSize);
			@out.write(data, 0, data.Length);
			@out.close();
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool exists(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 p)
		{
			return fc.util().exists(p);
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool isFile(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 p)
		{
			try
			{
				return fc.getFileStatus(p).isFile();
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool isDir(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 p)
		{
			try
			{
				return fc.getFileStatus(p).isDirectory();
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool isSymlink(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 p)
		{
			try
			{
				return fc.getFileLinkStatus(p).isSymlink();
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void writeFile(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 path, byte[] b)
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(path, java.util.EnumSet.
				of(org.apache.hadoop.fs.CreateFlag.CREATE), org.apache.hadoop.fs.Options.CreateOpts
				.createParent());
			@out.write(b);
			@out.close();
		}

		/// <exception cref="System.IO.IOException"/>
		public static byte[] readFile(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path
			 path, int len)
		{
			java.io.DataInputStream dis = fc.open(path);
			byte[] buffer = new byte[len];
			org.apache.hadoop.io.IOUtils.readFully(dis, buffer, 0, len);
			dis.close();
			return buffer;
		}

		public org.apache.hadoop.fs.FileStatus containsPath(org.apache.hadoop.fs.FileContext
			 fc, org.apache.hadoop.fs.Path path, org.apache.hadoop.fs.FileStatus[] dirList)
		{
			return containsPath(getTestRootPath(fc, path.ToString()), dirList);
		}

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

		public org.apache.hadoop.fs.FileStatus containsPath(org.apache.hadoop.fs.FileContext
			 fc, string path, org.apache.hadoop.fs.FileStatus[] dirList)
		{
			return containsPath(fc, new org.apache.hadoop.fs.Path(path), dirList);
		}

		public enum fileType
		{
			isDir,
			isFile,
			isSymlink
		}

		/// <exception cref="System.IO.IOException"/>
		public static void checkFileStatus(org.apache.hadoop.fs.FileContext aFc, string path
			, org.apache.hadoop.fs.FileContextTestHelper.fileType expectedType)
		{
			org.apache.hadoop.fs.FileStatus s = aFc.getFileStatus(new org.apache.hadoop.fs.Path
				(path));
			NUnit.Framework.Assert.IsNotNull(s);
			if (expectedType == org.apache.hadoop.fs.FileContextTestHelper.fileType.isDir)
			{
				NUnit.Framework.Assert.IsTrue(s.isDirectory());
			}
			else
			{
				if (expectedType == org.apache.hadoop.fs.FileContextTestHelper.fileType.isFile)
				{
					NUnit.Framework.Assert.IsTrue(s.isFile());
				}
				else
				{
					if (expectedType == org.apache.hadoop.fs.FileContextTestHelper.fileType.isSymlink)
					{
						NUnit.Framework.Assert.IsTrue(s.isSymlink());
					}
				}
			}
			NUnit.Framework.Assert.AreEqual(aFc.makeQualified(new org.apache.hadoop.fs.Path(path
				)), s.getPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public static void checkFileLinkStatus(org.apache.hadoop.fs.FileContext aFc, string
			 path, org.apache.hadoop.fs.FileContextTestHelper.fileType expectedType)
		{
			org.apache.hadoop.fs.FileStatus s = aFc.getFileLinkStatus(new org.apache.hadoop.fs.Path
				(path));
			NUnit.Framework.Assert.IsNotNull(s);
			if (expectedType == org.apache.hadoop.fs.FileContextTestHelper.fileType.isDir)
			{
				NUnit.Framework.Assert.IsTrue(s.isDirectory());
			}
			else
			{
				if (expectedType == org.apache.hadoop.fs.FileContextTestHelper.fileType.isFile)
				{
					NUnit.Framework.Assert.IsTrue(s.isFile());
				}
				else
				{
					if (expectedType == org.apache.hadoop.fs.FileContextTestHelper.fileType.isSymlink)
					{
						NUnit.Framework.Assert.IsTrue(s.isSymlink());
					}
				}
			}
			NUnit.Framework.Assert.AreEqual(aFc.makeQualified(new org.apache.hadoop.fs.Path(path
				)), s.getPath());
		}
	}
}
