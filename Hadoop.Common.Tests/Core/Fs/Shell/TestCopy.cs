using System;
using System.IO;
using System.Threading;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Org.Mockito.Stubbing;


namespace Org.Apache.Hadoop.FS.Shell
{
	public class TestCopy
	{
		internal static Configuration conf;

		internal static Path path = new Path("mockfs:/file");

		internal static Path tmpPath = new Path("mockfs:/file._COPYING_");

		internal static CopyCommands.Put cmd;

		internal static FileSystem mockFs;

		internal static PathData target;

		internal static FileStatus fileStat;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestCopy.MockFileSystem), typeof(FileSystem
				));
			mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			fileStat = Org.Mockito.Mockito.Mock<FileStatus>();
			Org.Mockito.Mockito.When(fileStat.IsDirectory()).ThenReturn(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void ResetMock()
		{
			Org.Mockito.Mockito.Reset(mockFs);
			target = new PathData(path.ToString(), conf);
			cmd = new CopyCommands.Put();
			cmd.SetConf(conf);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyStreamTarget()
		{
			FSDataOutputStream @out = Org.Mockito.Mockito.Mock<FSDataOutputStream>();
			WhenFsCreate().ThenReturn(@out);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(tmpPath))).ThenReturn(fileStat);
			Org.Mockito.Mockito.When(mockFs.Rename(Eq(tmpPath), Eq(path))).ThenReturn(true);
			FSInputStream @in = Org.Mockito.Mockito.Mock<FSInputStream>();
			Org.Mockito.Mockito.When(@in.Read(Any<byte[]>(), AnyInt(), AnyInt())).ThenReturn(
				-1);
			TryCopyStream(@in, true);
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Delete(Eq(path), 
				AnyBoolean());
			Org.Mockito.Mockito.Verify(mockFs).Rename(Eq(tmpPath), Eq(path));
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Delete(Eq(tmpPath
				), AnyBoolean());
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Close();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyStreamTargetExists()
		{
			FSDataOutputStream @out = Org.Mockito.Mockito.Mock<FSDataOutputStream>();
			WhenFsCreate().ThenReturn(@out);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(path))).ThenReturn(fileStat);
			target.RefreshStatus();
			// so it's updated as existing
			cmd.SetOverwrite(true);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(tmpPath))).ThenReturn(fileStat);
			Org.Mockito.Mockito.When(mockFs.Delete(Eq(path), Eq(false))).ThenReturn(true);
			Org.Mockito.Mockito.When(mockFs.Rename(Eq(tmpPath), Eq(path))).ThenReturn(true);
			FSInputStream @in = Org.Mockito.Mockito.Mock<FSInputStream>();
			Org.Mockito.Mockito.When(@in.Read(Any<byte[]>(), AnyInt(), AnyInt())).ThenReturn(
				-1);
			TryCopyStream(@in, true);
			Org.Mockito.Mockito.Verify(mockFs).Delete(Eq(path), AnyBoolean());
			Org.Mockito.Mockito.Verify(mockFs).Rename(Eq(tmpPath), Eq(path));
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Delete(Eq(tmpPath
				), AnyBoolean());
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Close();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInterruptedCreate()
		{
			WhenFsCreate().ThenThrow(new ThreadInterruptedException());
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(tmpPath))).ThenReturn(fileStat);
			FSDataInputStream @in = Org.Mockito.Mockito.Mock<FSDataInputStream>();
			TryCopyStream(@in, false);
			Org.Mockito.Mockito.Verify(mockFs).Delete(Eq(tmpPath), AnyBoolean());
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Rename(Any<Path>(
				), Any<Path>());
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Delete(Eq(path), 
				AnyBoolean());
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Close();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInterruptedCopyBytes()
		{
			FSDataOutputStream @out = Org.Mockito.Mockito.Mock<FSDataOutputStream>();
			WhenFsCreate().ThenReturn(@out);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(tmpPath))).ThenReturn(fileStat);
			FSInputStream @in = Org.Mockito.Mockito.Mock<FSInputStream>();
			// make IOUtils.copyBytes fail
			Org.Mockito.Mockito.When(@in.Read(Any<byte[]>(), AnyInt(), AnyInt())).ThenThrow(new 
				ThreadInterruptedException());
			TryCopyStream(@in, false);
			Org.Mockito.Mockito.Verify(mockFs).Delete(Eq(tmpPath), AnyBoolean());
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Rename(Any<Path>(
				), Any<Path>());
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Delete(Eq(path), 
				AnyBoolean());
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Close();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInterruptedRename()
		{
			FSDataOutputStream @out = Org.Mockito.Mockito.Mock<FSDataOutputStream>();
			WhenFsCreate().ThenReturn(@out);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Eq(tmpPath))).ThenReturn(fileStat);
			Org.Mockito.Mockito.When(mockFs.Rename(Eq(tmpPath), Eq(path))).ThenThrow(new ThreadInterruptedException
				());
			FSInputStream @in = Org.Mockito.Mockito.Mock<FSInputStream>();
			Org.Mockito.Mockito.When(@in.Read(Any<byte[]>(), AnyInt(), AnyInt())).ThenReturn(
				-1);
			TryCopyStream(@in, false);
			Org.Mockito.Mockito.Verify(mockFs).Delete(Eq(tmpPath), AnyBoolean());
			Org.Mockito.Mockito.Verify(mockFs).Rename(Eq(tmpPath), Eq(path));
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Delete(Eq(path), 
				AnyBoolean());
			Org.Mockito.Mockito.Verify(mockFs, Org.Mockito.Mockito.Never()).Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private OngoingStubbing<FSDataOutputStream> WhenFsCreate()
		{
			return Org.Mockito.Mockito.When(mockFs.Create(Eq(tmpPath), Any<FsPermission>(), AnyBoolean
				(), AnyInt(), AnyShort(), AnyLong(), Any<Progressable>()));
		}

		private void TryCopyStream(InputStream @in, bool shouldPass)
		{
			try
			{
				cmd.CopyStreamToTarget(new FSDataInputStream(@in), target);
			}
			catch (ThreadInterruptedException)
			{
				NUnit.Framework.Assert.IsFalse("copy failed", shouldPass);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsFalse(e.Message, shouldPass);
			}
		}

		internal class MockFileSystem : FilterFileSystem
		{
			internal Configuration conf;

			internal MockFileSystem()
				: base(mockFs)
			{
			}

			public override void Initialize(URI uri, Configuration conf)
			{
				this.conf = conf;
			}

			public override Path MakeQualified(Path path)
			{
				return path;
			}

			public override Configuration GetConf()
			{
				return conf;
			}
		}
	}
}
