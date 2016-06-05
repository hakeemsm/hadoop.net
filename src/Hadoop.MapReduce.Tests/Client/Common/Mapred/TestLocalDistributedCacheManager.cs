using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestLocalDistributedCacheManager
	{
		private static FileSystem mockfs;

		public class MockFileSystem : FilterFileSystem
		{
			public MockFileSystem()
				: base(mockfs)
			{
			}
		}

		private FilePath localDir;

		/// <exception cref="System.IO.IOException"/>
		private static void Delete(FilePath file)
		{
			if (file.GetAbsolutePath().Length < 5)
			{
				throw new ArgumentException("Path [" + file + "] is too short, not deleting");
			}
			if (file.Exists())
			{
				if (file.IsDirectory())
				{
					FilePath[] children = file.ListFiles();
					if (children != null)
					{
						foreach (FilePath child in children)
						{
							Delete(child);
						}
					}
				}
				if (!file.Delete())
				{
					throw new RuntimeException("Could not delete path [" + file + "]");
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			mockfs = Org.Mockito.Mockito.Mock<FileSystem>();
			localDir = new FilePath(Runtime.GetProperty("test.build.dir", "target/test-dir"), 
				typeof(TestLocalDistributedCacheManager).FullName);
			Delete(localDir);
			localDir.Mkdirs();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Cleanup()
		{
			Delete(localDir);
		}

		/// <summary>
		/// Mock input stream based on a byte array so that it can be used by a
		/// FSDataInputStream.
		/// </summary>
		private class MockInputStream : ByteArrayInputStream, Seekable, PositionedReadable
		{
			public MockInputStream(byte[] buf)
				: base(buf)
			{
			}

			// empty implementation for unused methods
			public virtual int Read(long position, byte[] buffer, int offset, int length)
			{
				return -1;
			}

			public virtual void ReadFully(long position, byte[] buffer, int offset, int length
				)
			{
			}

			public virtual void ReadFully(long position, byte[] buffer)
			{
			}

			public virtual void Seek(long position)
			{
			}

			public virtual long GetPos()
			{
				return 0;
			}

			public virtual bool SeekToNewSource(long targetPos)
			{
				return false;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDownload()
		{
			JobConf conf = new JobConf();
			conf.SetClass("fs.mock.impl", typeof(TestLocalDistributedCacheManager.MockFileSystem
				), typeof(FileSystem));
			URI mockBase = new URI("mock://test-nn1/");
			Org.Mockito.Mockito.When(mockfs.GetUri()).ThenReturn(mockBase);
			Path working = new Path("mock://test-nn1/user/me/");
			Org.Mockito.Mockito.When(mockfs.GetWorkingDirectory()).ThenReturn(working);
			Org.Mockito.Mockito.When(mockfs.ResolvePath(Matchers.Any<Path>())).ThenAnswer(new 
				_Answer_127());
			URI file = new URI("mock://test-nn1/user/me/file.txt#link");
			Path filePath = new Path(file);
			FilePath link = new FilePath("link");
			Org.Mockito.Mockito.When(mockfs.GetFileStatus(Matchers.Any<Path>())).ThenAnswer(new 
				_Answer_138(filePath));
			Org.Mockito.Mockito.When(mockfs.GetConf()).ThenReturn(conf);
			FSDataInputStream @in = new FSDataInputStream(new TestLocalDistributedCacheManager.MockInputStream
				(Sharpen.Runtime.GetBytesForString("This is a test file\n")));
			Org.Mockito.Mockito.When(mockfs.Open(Matchers.Any<Path>(), Matchers.AnyInt())).ThenAnswer
				(new _Answer_154(@in));
			DistributedCache.AddCacheFile(file, conf);
			conf.Set(MRJobConfig.CacheFileTimestamps, "101");
			conf.Set(MRJobConfig.CacheFilesSizes, "201");
			conf.Set(MRJobConfig.CacheFileVisibilities, "false");
			conf.Set(MRConfig.LocalDir, localDir.GetAbsolutePath());
			LocalDistributedCacheManager manager = new LocalDistributedCacheManager();
			try
			{
				manager.Setup(conf);
				NUnit.Framework.Assert.IsTrue(link.Exists());
			}
			finally
			{
				manager.Close();
			}
			NUnit.Framework.Assert.IsFalse(link.Exists());
		}

		private sealed class _Answer_127 : Answer<Path>
		{
			public _Answer_127()
			{
			}

			/// <exception cref="System.Exception"/>
			public Path Answer(InvocationOnMock args)
			{
				return (Path)args.GetArguments()[0];
			}
		}

		private sealed class _Answer_138 : Answer<FileStatus>
		{
			public _Answer_138(Path filePath)
			{
				this.filePath = filePath;
			}

			/// <exception cref="System.Exception"/>
			public FileStatus Answer(InvocationOnMock args)
			{
				Path p = (Path)args.GetArguments()[0];
				if ("file.txt".Equals(p.GetName()))
				{
					return new FileStatus(201, false, 1, 500, 101, 101, FsPermission.GetDefault(), "me"
						, "me", filePath);
				}
				else
				{
					throw new FileNotFoundException(p + " not supported by mocking");
				}
			}

			private readonly Path filePath;
		}

		private sealed class _Answer_154 : Answer<FSDataInputStream>
		{
			public _Answer_154(FSDataInputStream @in)
			{
				this.@in = @in;
			}

			/// <exception cref="System.Exception"/>
			public FSDataInputStream Answer(InvocationOnMock args)
			{
				Path src = (Path)args.GetArguments()[0];
				if ("file.txt".Equals(src.GetName()))
				{
					return @in;
				}
				else
				{
					throw new FileNotFoundException(src + " not supported by mocking");
				}
			}

			private readonly FSDataInputStream @in;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEmptyDownload()
		{
			JobConf conf = new JobConf();
			conf.SetClass("fs.mock.impl", typeof(TestLocalDistributedCacheManager.MockFileSystem
				), typeof(FileSystem));
			URI mockBase = new URI("mock://test-nn1/");
			Org.Mockito.Mockito.When(mockfs.GetUri()).ThenReturn(mockBase);
			Path working = new Path("mock://test-nn1/user/me/");
			Org.Mockito.Mockito.When(mockfs.GetWorkingDirectory()).ThenReturn(working);
			Org.Mockito.Mockito.When(mockfs.ResolvePath(Matchers.Any<Path>())).ThenAnswer(new 
				_Answer_190());
			Org.Mockito.Mockito.When(mockfs.GetFileStatus(Matchers.Any<Path>())).ThenAnswer(new 
				_Answer_197());
			Org.Mockito.Mockito.When(mockfs.GetConf()).ThenReturn(conf);
			Org.Mockito.Mockito.When(mockfs.Open(Matchers.Any<Path>(), Matchers.AnyInt())).ThenAnswer
				(new _Answer_206());
			conf.Set(MRJobConfig.CacheFiles, string.Empty);
			conf.Set(MRConfig.LocalDir, localDir.GetAbsolutePath());
			LocalDistributedCacheManager manager = new LocalDistributedCacheManager();
			try
			{
				manager.Setup(conf);
			}
			finally
			{
				manager.Close();
			}
		}

		private sealed class _Answer_190 : Answer<Path>
		{
			public _Answer_190()
			{
			}

			/// <exception cref="System.Exception"/>
			public Path Answer(InvocationOnMock args)
			{
				return (Path)args.GetArguments()[0];
			}
		}

		private sealed class _Answer_197 : Answer<FileStatus>
		{
			public _Answer_197()
			{
			}

			/// <exception cref="System.Exception"/>
			public FileStatus Answer(InvocationOnMock args)
			{
				Path p = (Path)args.GetArguments()[0];
				throw new FileNotFoundException(p + " not supported by mocking");
			}
		}

		private sealed class _Answer_206 : Answer<FSDataInputStream>
		{
			public _Answer_206()
			{
			}

			/// <exception cref="System.Exception"/>
			public FSDataInputStream Answer(InvocationOnMock args)
			{
				Path src = (Path)args.GetArguments()[0];
				throw new FileNotFoundException(src + " not supported by mocking");
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDuplicateDownload()
		{
			JobConf conf = new JobConf();
			conf.SetClass("fs.mock.impl", typeof(TestLocalDistributedCacheManager.MockFileSystem
				), typeof(FileSystem));
			URI mockBase = new URI("mock://test-nn1/");
			Org.Mockito.Mockito.When(mockfs.GetUri()).ThenReturn(mockBase);
			Path working = new Path("mock://test-nn1/user/me/");
			Org.Mockito.Mockito.When(mockfs.GetWorkingDirectory()).ThenReturn(working);
			Org.Mockito.Mockito.When(mockfs.ResolvePath(Matchers.Any<Path>())).ThenAnswer(new 
				_Answer_234());
			URI file = new URI("mock://test-nn1/user/me/file.txt#link");
			Path filePath = new Path(file);
			FilePath link = new FilePath("link");
			Org.Mockito.Mockito.When(mockfs.GetFileStatus(Matchers.Any<Path>())).ThenAnswer(new 
				_Answer_245(filePath));
			Org.Mockito.Mockito.When(mockfs.GetConf()).ThenReturn(conf);
			FSDataInputStream @in = new FSDataInputStream(new TestLocalDistributedCacheManager.MockInputStream
				(Sharpen.Runtime.GetBytesForString("This is a test file\n")));
			Org.Mockito.Mockito.When(mockfs.Open(Matchers.Any<Path>(), Matchers.AnyInt())).ThenAnswer
				(new _Answer_261(@in));
			DistributedCache.AddCacheFile(file, conf);
			DistributedCache.AddCacheFile(file, conf);
			conf.Set(MRJobConfig.CacheFileTimestamps, "101,101");
			conf.Set(MRJobConfig.CacheFilesSizes, "201,201");
			conf.Set(MRJobConfig.CacheFileVisibilities, "false,false");
			conf.Set(MRConfig.LocalDir, localDir.GetAbsolutePath());
			LocalDistributedCacheManager manager = new LocalDistributedCacheManager();
			try
			{
				manager.Setup(conf);
				NUnit.Framework.Assert.IsTrue(link.Exists());
			}
			finally
			{
				manager.Close();
			}
			NUnit.Framework.Assert.IsFalse(link.Exists());
		}

		private sealed class _Answer_234 : Answer<Path>
		{
			public _Answer_234()
			{
			}

			/// <exception cref="System.Exception"/>
			public Path Answer(InvocationOnMock args)
			{
				return (Path)args.GetArguments()[0];
			}
		}

		private sealed class _Answer_245 : Answer<FileStatus>
		{
			public _Answer_245(Path filePath)
			{
				this.filePath = filePath;
			}

			/// <exception cref="System.Exception"/>
			public FileStatus Answer(InvocationOnMock args)
			{
				Path p = (Path)args.GetArguments()[0];
				if ("file.txt".Equals(p.GetName()))
				{
					return new FileStatus(201, false, 1, 500, 101, 101, FsPermission.GetDefault(), "me"
						, "me", filePath);
				}
				else
				{
					throw new FileNotFoundException(p + " not supported by mocking");
				}
			}

			private readonly Path filePath;
		}

		private sealed class _Answer_261 : Answer<FSDataInputStream>
		{
			public _Answer_261(FSDataInputStream @in)
			{
				this.@in = @in;
			}

			/// <exception cref="System.Exception"/>
			public FSDataInputStream Answer(InvocationOnMock args)
			{
				Path src = (Path)args.GetArguments()[0];
				if ("file.txt".Equals(src.GetName()))
				{
					return @in;
				}
				else
				{
					throw new FileNotFoundException(src + " not supported by mocking");
				}
			}

			private readonly FSDataInputStream @in;
		}
	}
}
