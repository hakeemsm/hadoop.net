using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Helper class for unit tests.</summary>
	public sealed class FileContextTestHelper
	{
		private const int DefaultBlockSize = 1024;

		private const int DefaultNumBlocks = 2;

		private readonly string testRootDir;

		private string absTestRootDir = null;

		/// <summary>Create a context with test root relative to the <wd>/build/test/data</summary>
		public FileContextTestHelper()
			: this(Runtime.GetProperty("test.build.data", "target/test/data") + "/" + RandomStringUtils
				.RandomAlphanumeric(10))
		{
		}

		/// <summary>Create a context with the given test root</summary>
		public FileContextTestHelper(string testRootDir)
		{
			this.testRootDir = testRootDir;
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

		public Path GetTestRootPath(FileContext fc)
		{
			return fc.MakeQualified(new Path(testRootDir));
		}

		public Path GetTestRootPath(FileContext fc, string pathString)
		{
			return fc.MakeQualified(new Path(testRootDir, pathString));
		}

		// the getAbsolutexxx method is needed because the root test dir
		// can be messed up by changing the working dir.
		public string GetAbsoluteTestRootDir(FileContext fc)
		{
			if (absTestRootDir == null)
			{
				if (new Path(testRootDir).IsAbsolute())
				{
					absTestRootDir = testRootDir;
				}
				else
				{
					absTestRootDir = fc.GetWorkingDirectory().ToString() + "/" + testRootDir;
				}
			}
			return absTestRootDir;
		}

		public Path GetAbsoluteTestRootPath(FileContext fc)
		{
			return fc.MakeQualified(new Path(GetAbsoluteTestRootDir(fc)));
		}

		public Path GetDefaultWorkingDirectory(FileContext fc)
		{
			return GetTestRootPath(fc, "/user/" + Runtime.GetProperty("user.name")).MakeQualified
				(fc.GetDefaultFileSystem().GetUri(), fc.GetWorkingDirectory());
		}

		/*
		* Create files with numBlocks blocks each with block size blockSize.
		*/
		/// <exception cref="System.IO.IOException"/>
		public static long CreateFile(FileContext fc, Path path, int numBlocks, params Options.CreateOpts
			[] options)
		{
			Options.CreateOpts.BlockSize blockSizeOpt = Options.CreateOpts.GetOpt<Options.CreateOpts.BlockSize
				>(options);
			long blockSize = blockSizeOpt != null ? blockSizeOpt.GetValue() : DefaultBlockSize;
			FSDataOutputStream @out = fc.Create(path, EnumSet.Of(CreateFlag.Create), options);
			byte[] data = GetFileData(numBlocks, blockSize);
			@out.Write(data, 0, data.Length);
			@out.Close();
			return data.Length;
		}

		/// <exception cref="System.IO.IOException"/>
		public static long CreateFile(FileContext fc, Path path, int numBlocks, int blockSize
			)
		{
			return CreateFile(fc, path, numBlocks, Options.CreateOpts.BlockSize(blockSize), Options.CreateOpts
				.CreateParent());
		}

		/// <exception cref="System.IO.IOException"/>
		public static long CreateFile(FileContext fc, Path path)
		{
			return CreateFile(fc, path, DefaultNumBlocks, Options.CreateOpts.CreateParent());
		}

		/// <exception cref="System.IO.IOException"/>
		public long CreateFile(FileContext fc, string name)
		{
			Path path = GetTestRootPath(fc, name);
			return CreateFile(fc, path);
		}

		/// <exception cref="System.IO.IOException"/>
		public long CreateFileNonRecursive(FileContext fc, string name)
		{
			Path path = GetTestRootPath(fc, name);
			return CreateFileNonRecursive(fc, path);
		}

		/// <exception cref="System.IO.IOException"/>
		public static long CreateFileNonRecursive(FileContext fc, Path path)
		{
			return CreateFile(fc, path, DefaultNumBlocks, Options.CreateOpts.DonotCreateParent
				());
		}

		/// <exception cref="System.IO.IOException"/>
		public static void AppendToFile(FileContext fc, Path path, int numBlocks, params 
			Options.CreateOpts[] options)
		{
			Options.CreateOpts.BlockSize blockSizeOpt = Options.CreateOpts.GetOpt<Options.CreateOpts.BlockSize
				>(options);
			long blockSize = blockSizeOpt != null ? blockSizeOpt.GetValue() : DefaultBlockSize;
			FSDataOutputStream @out;
			@out = fc.Create(path, EnumSet.Of(CreateFlag.Append));
			byte[] data = GetFileData(numBlocks, blockSize);
			@out.Write(data, 0, data.Length);
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool Exists(FileContext fc, Path p)
		{
			return fc.Util().Exists(p);
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool IsFile(FileContext fc, Path p)
		{
			try
			{
				return fc.GetFileStatus(p).IsFile();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool IsDir(FileContext fc, Path p)
		{
			try
			{
				return fc.GetFileStatus(p).IsDirectory();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool IsSymlink(FileContext fc, Path p)
		{
			try
			{
				return fc.GetFileLinkStatus(p).IsSymlink();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void WriteFile(FileContext fc, Path path, byte[] b)
		{
			FSDataOutputStream @out = fc.Create(path, EnumSet.Of(CreateFlag.Create), Options.CreateOpts
				.CreateParent());
			@out.Write(b);
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public static byte[] ReadFile(FileContext fc, Path path, int len)
		{
			DataInputStream dis = fc.Open(path);
			byte[] buffer = new byte[len];
			IOUtils.ReadFully(dis, buffer, 0, len);
			dis.Close();
			return buffer;
		}

		public FileStatus ContainsPath(FileContext fc, Path path, FileStatus[] dirList)
		{
			return ContainsPath(GetTestRootPath(fc, path.ToString()), dirList);
		}

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

		public FileStatus ContainsPath(FileContext fc, string path, FileStatus[] dirList)
		{
			return ContainsPath(fc, new Path(path), dirList);
		}

		public enum FileType
		{
			isDir,
			isFile,
			isSymlink
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CheckFileStatus(FileContext aFc, string path, FileContextTestHelper.FileType
			 expectedType)
		{
			FileStatus s = aFc.GetFileStatus(new Path(path));
			NUnit.Framework.Assert.IsNotNull(s);
			if (expectedType == FileContextTestHelper.FileType.isDir)
			{
				Assert.True(s.IsDirectory());
			}
			else
			{
				if (expectedType == FileContextTestHelper.FileType.isFile)
				{
					Assert.True(s.IsFile());
				}
				else
				{
					if (expectedType == FileContextTestHelper.FileType.isSymlink)
					{
						Assert.True(s.IsSymlink());
					}
				}
			}
			Assert.Equal(aFc.MakeQualified(new Path(path)), s.GetPath());
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CheckFileLinkStatus(FileContext aFc, string path, FileContextTestHelper.FileType
			 expectedType)
		{
			FileStatus s = aFc.GetFileLinkStatus(new Path(path));
			NUnit.Framework.Assert.IsNotNull(s);
			if (expectedType == FileContextTestHelper.FileType.isDir)
			{
				Assert.True(s.IsDirectory());
			}
			else
			{
				if (expectedType == FileContextTestHelper.FileType.isFile)
				{
					Assert.True(s.IsFile());
				}
				else
				{
					if (expectedType == FileContextTestHelper.FileType.isSymlink)
					{
						Assert.True(s.IsSymlink());
					}
				}
			}
			Assert.Equal(aFc.MakeQualified(new Path(path)), s.GetPath());
		}
	}
}
