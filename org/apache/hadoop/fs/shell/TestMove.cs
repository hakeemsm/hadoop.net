using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	public class TestMove
	{
		internal static org.apache.hadoop.conf.Configuration conf;

		internal static org.apache.hadoop.fs.FileSystem mockFs;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		[NUnit.Framework.BeforeClass]
		public static void setup()
		{
			mockFs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem>();
			conf = new org.apache.hadoop.conf.Configuration();
			conf.setClass("fs.mockfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestMove.MockFileSystem
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void resetMock()
		{
			org.mockito.Mockito.reset(mockFs);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMoveTargetExistsWithoutExplicitRename()
		{
			org.apache.hadoop.fs.Path srcPath = new org.apache.hadoop.fs.Path("mockfs:/file");
			org.apache.hadoop.fs.Path targetPath = new org.apache.hadoop.fs.Path("mockfs:/fold0"
				);
			org.apache.hadoop.fs.Path dupPath = new org.apache.hadoop.fs.Path("mockfs:/fold0/file"
				);
			org.apache.hadoop.fs.Path srcPath2 = new org.apache.hadoop.fs.Path("mockfs://user/file"
				);
			org.apache.hadoop.fs.Path targetPath2 = new org.apache.hadoop.fs.Path("mockfs://user/fold0"
				);
			org.apache.hadoop.fs.Path dupPath2 = new org.apache.hadoop.fs.Path("mockfs://user/fold0/file"
				);
			org.apache.hadoop.fs.shell.TestMove.InstrumentedRenameCommand cmd;
			string[] cmdargs = new string[] { "mockfs:/file", "mockfs:/fold0" };
			org.apache.hadoop.fs.FileStatus src_fileStat;
			org.apache.hadoop.fs.FileStatus target_fileStat;
			org.apache.hadoop.fs.FileStatus dup_fileStat;
			java.net.URI myuri;
			src_fileStat = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileStatus>();
			target_fileStat = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileStatus>();
			dup_fileStat = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileStatus>();
			myuri = new java.net.URI("mockfs://user");
			org.mockito.Mockito.when(src_fileStat.isDirectory()).thenReturn(false);
			org.mockito.Mockito.when(target_fileStat.isDirectory()).thenReturn(true);
			org.mockito.Mockito.when(dup_fileStat.isDirectory()).thenReturn(false);
			org.mockito.Mockito.when(src_fileStat.getPath()).thenReturn(srcPath2);
			org.mockito.Mockito.when(target_fileStat.getPath()).thenReturn(targetPath2);
			org.mockito.Mockito.when(dup_fileStat.getPath()).thenReturn(dupPath2);
			org.mockito.Mockito.when(mockFs.getFileStatus(org.mockito.Matchers.eq(srcPath))).
				thenReturn(src_fileStat);
			org.mockito.Mockito.when(mockFs.getFileStatus(org.mockito.Matchers.eq(targetPath)
				)).thenReturn(target_fileStat);
			org.mockito.Mockito.when(mockFs.getFileStatus(org.mockito.Matchers.eq(dupPath))).
				thenReturn(dup_fileStat);
			org.mockito.Mockito.when(mockFs.getFileStatus(org.mockito.Matchers.eq(srcPath2)))
				.thenReturn(src_fileStat);
			org.mockito.Mockito.when(mockFs.getFileStatus(org.mockito.Matchers.eq(targetPath2
				))).thenReturn(target_fileStat);
			org.mockito.Mockito.when(mockFs.getFileStatus(org.mockito.Matchers.eq(dupPath2)))
				.thenReturn(dup_fileStat);
			org.mockito.Mockito.when(mockFs.getUri()).thenReturn(myuri);
			cmd = new org.apache.hadoop.fs.shell.TestMove.InstrumentedRenameCommand();
			cmd.setConf(conf);
			cmd.setOverwrite(true);
			cmd.run(cmdargs);
			// make sure command failed with the proper exception
			NUnit.Framework.Assert.IsTrue("Rename should have failed with path exists exception"
				, cmd.error is org.apache.hadoop.fs.PathExistsException);
		}

		internal class MockFileSystem : org.apache.hadoop.fs.FilterFileSystem
		{
			internal org.apache.hadoop.conf.Configuration conf;

			internal MockFileSystem()
				: base(mockFs)
			{
			}

			public override void initialize(java.net.URI uri, org.apache.hadoop.conf.Configuration
				 conf)
			{
				this.conf = conf;
			}

			public override org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path
				 path)
			{
				return path;
			}

			public override org.apache.hadoop.conf.Configuration getConf()
			{
				return conf;
			}
		}

		private class InstrumentedRenameCommand : org.apache.hadoop.fs.shell.MoveCommands.Rename
		{
			private System.Exception error = null;

			public override void displayError(System.Exception e)
			{
				error = e;
			}
		}
	}
}
