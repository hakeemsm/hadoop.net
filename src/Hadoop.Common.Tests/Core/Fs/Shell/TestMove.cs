using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Mockito;


namespace Org.Apache.Hadoop.FS.Shell
{
	public class TestMove
	{
		internal static Configuration conf;

		internal static FileSystem mockFs;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="URISyntaxException"/>
		[BeforeClass]
		public static void Setup()
		{
			mockFs = Org.Mockito.Mockito.Mock<FileSystem>();
			conf = new Configuration();
			conf.SetClass("fs.mockfs.impl", typeof(TestMove.MockFileSystem), typeof(FileSystem
				));
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void ResetMock()
		{
			Org.Mockito.Mockito.Reset(mockFs);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMoveTargetExistsWithoutExplicitRename()
		{
			Path srcPath = new Path("mockfs:/file");
			Path targetPath = new Path("mockfs:/fold0");
			Path dupPath = new Path("mockfs:/fold0/file");
			Path srcPath2 = new Path("mockfs://user/file");
			Path targetPath2 = new Path("mockfs://user/fold0");
			Path dupPath2 = new Path("mockfs://user/fold0/file");
			TestMove.InstrumentedRenameCommand cmd;
			string[] cmdargs = new string[] { "mockfs:/file", "mockfs:/fold0" };
			FileStatus src_fileStat;
			FileStatus target_fileStat;
			FileStatus dup_fileStat;
			URI myuri;
			src_fileStat = Org.Mockito.Mockito.Mock<FileStatus>();
			target_fileStat = Org.Mockito.Mockito.Mock<FileStatus>();
			dup_fileStat = Org.Mockito.Mockito.Mock<FileStatus>();
			myuri = new URI("mockfs://user");
			Org.Mockito.Mockito.When(src_fileStat.IsDirectory()).ThenReturn(false);
			Org.Mockito.Mockito.When(target_fileStat.IsDirectory()).ThenReturn(true);
			Org.Mockito.Mockito.When(dup_fileStat.IsDirectory()).ThenReturn(false);
			Org.Mockito.Mockito.When(src_fileStat.GetPath()).ThenReturn(srcPath2);
			Org.Mockito.Mockito.When(target_fileStat.GetPath()).ThenReturn(targetPath2);
			Org.Mockito.Mockito.When(dup_fileStat.GetPath()).ThenReturn(dupPath2);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Matchers.Eq(srcPath))).ThenReturn(src_fileStat
				);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Matchers.Eq(targetPath))).ThenReturn
				(target_fileStat);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Matchers.Eq(dupPath))).ThenReturn(dup_fileStat
				);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Matchers.Eq(srcPath2))).ThenReturn(
				src_fileStat);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Matchers.Eq(targetPath2))).ThenReturn
				(target_fileStat);
			Org.Mockito.Mockito.When(mockFs.GetFileStatus(Matchers.Eq(dupPath2))).ThenReturn(
				dup_fileStat);
			Org.Mockito.Mockito.When(mockFs.GetUri()).ThenReturn(myuri);
			cmd = new TestMove.InstrumentedRenameCommand();
			cmd.SetConf(conf);
			cmd.SetOverwrite(true);
			cmd.Run(cmdargs);
			// make sure command failed with the proper exception
			Assert.True("Rename should have failed with path exists exception"
				, cmd.error is PathExistsException);
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

		private class InstrumentedRenameCommand : MoveCommands.Rename
		{
			private Exception error = null;

			public override void DisplayError(Exception e)
			{
				error = e;
			}
		}
	}
}
