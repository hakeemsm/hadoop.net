using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	public class TestCopy
	{
		internal static org.apache.hadoop.conf.Configuration conf;

		internal static org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("mockfs:/file"
			);

		internal static org.apache.hadoop.fs.Path tmpPath = new org.apache.hadoop.fs.Path
			("mockfs:/file._COPYING_");

		internal static org.apache.hadoop.fs.shell.CopyCommands.Put cmd;

		internal static org.apache.hadoop.fs.FileSystem mockFs;

		internal static org.apache.hadoop.fs.shell.PathData target;

		internal static org.apache.hadoop.fs.FileStatus fileStat;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.BeforeClass]
		public static void setup()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			conf.setClass("fs.mockfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestCopy.MockFileSystem
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			mockFs = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem>();
			fileStat = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileStatus>();
			org.mockito.Mockito.when(fileStat.isDirectory()).thenReturn(false);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void resetMock()
		{
			org.mockito.Mockito.reset(mockFs);
			target = new org.apache.hadoop.fs.shell.PathData(path.ToString(), conf);
			cmd = new org.apache.hadoop.fs.shell.CopyCommands.Put();
			cmd.setConf(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyStreamTarget()
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = org.mockito.Mockito.mock<org.apache.hadoop.fs.FSDataOutputStream
				>();
			whenFsCreate().thenReturn(@out);
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(tmpPath))).thenReturn(fileStat);
			org.mockito.Mockito.when(mockFs.rename(eq(tmpPath), eq(path))).thenReturn(true);
			org.apache.hadoop.fs.FSInputStream @in = org.mockito.Mockito.mock<org.apache.hadoop.fs.FSInputStream
				>();
			org.mockito.Mockito.when(@in.read(any<byte[]>(), anyInt(), anyInt())).thenReturn(
				-1);
			tryCopyStream(@in, true);
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).delete(eq(path), 
				anyBoolean());
			org.mockito.Mockito.verify(mockFs).rename(eq(tmpPath), eq(path));
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).delete(eq(tmpPath
				), anyBoolean());
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyStreamTargetExists()
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = org.mockito.Mockito.mock<org.apache.hadoop.fs.FSDataOutputStream
				>();
			whenFsCreate().thenReturn(@out);
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);
			target.refreshStatus();
			// so it's updated as existing
			cmd.setOverwrite(true);
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(tmpPath))).thenReturn(fileStat);
			org.mockito.Mockito.when(mockFs.delete(eq(path), eq(false))).thenReturn(true);
			org.mockito.Mockito.when(mockFs.rename(eq(tmpPath), eq(path))).thenReturn(true);
			org.apache.hadoop.fs.FSInputStream @in = org.mockito.Mockito.mock<org.apache.hadoop.fs.FSInputStream
				>();
			org.mockito.Mockito.when(@in.read(any<byte[]>(), anyInt(), anyInt())).thenReturn(
				-1);
			tryCopyStream(@in, true);
			org.mockito.Mockito.verify(mockFs).delete(eq(path), anyBoolean());
			org.mockito.Mockito.verify(mockFs).rename(eq(tmpPath), eq(path));
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).delete(eq(tmpPath
				), anyBoolean());
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInterruptedCreate()
		{
			whenFsCreate().thenThrow(new java.io.InterruptedIOException());
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(tmpPath))).thenReturn(fileStat);
			org.apache.hadoop.fs.FSDataInputStream @in = org.mockito.Mockito.mock<org.apache.hadoop.fs.FSDataInputStream
				>();
			tryCopyStream(@in, false);
			org.mockito.Mockito.verify(mockFs).delete(eq(tmpPath), anyBoolean());
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).rename(any<org.apache.hadoop.fs.Path
				>(), any<org.apache.hadoop.fs.Path>());
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).delete(eq(path), 
				anyBoolean());
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInterruptedCopyBytes()
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = org.mockito.Mockito.mock<org.apache.hadoop.fs.FSDataOutputStream
				>();
			whenFsCreate().thenReturn(@out);
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(tmpPath))).thenReturn(fileStat);
			org.apache.hadoop.fs.FSInputStream @in = org.mockito.Mockito.mock<org.apache.hadoop.fs.FSInputStream
				>();
			// make IOUtils.copyBytes fail
			org.mockito.Mockito.when(@in.read(any<byte[]>(), anyInt(), anyInt())).thenThrow(new 
				java.io.InterruptedIOException());
			tryCopyStream(@in, false);
			org.mockito.Mockito.verify(mockFs).delete(eq(tmpPath), anyBoolean());
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).rename(any<org.apache.hadoop.fs.Path
				>(), any<org.apache.hadoop.fs.Path>());
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).delete(eq(path), 
				anyBoolean());
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInterruptedRename()
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = org.mockito.Mockito.mock<org.apache.hadoop.fs.FSDataOutputStream
				>();
			whenFsCreate().thenReturn(@out);
			org.mockito.Mockito.when(mockFs.getFileStatus(eq(tmpPath))).thenReturn(fileStat);
			org.mockito.Mockito.when(mockFs.rename(eq(tmpPath), eq(path))).thenThrow(new java.io.InterruptedIOException
				());
			org.apache.hadoop.fs.FSInputStream @in = org.mockito.Mockito.mock<org.apache.hadoop.fs.FSInputStream
				>();
			org.mockito.Mockito.when(@in.read(any<byte[]>(), anyInt(), anyInt())).thenReturn(
				-1);
			tryCopyStream(@in, false);
			org.mockito.Mockito.verify(mockFs).delete(eq(tmpPath), anyBoolean());
			org.mockito.Mockito.verify(mockFs).rename(eq(tmpPath), eq(path));
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).delete(eq(path), 
				anyBoolean());
			org.mockito.Mockito.verify(mockFs, org.mockito.Mockito.never()).close();
		}

		/// <exception cref="System.IO.IOException"/>
		private org.mockito.stubbing.OngoingStubbing<org.apache.hadoop.fs.FSDataOutputStream
			> whenFsCreate()
		{
			return org.mockito.Mockito.when(mockFs.create(eq(tmpPath), any<org.apache.hadoop.fs.permission.FsPermission
				>(), anyBoolean(), anyInt(), anyShort(), anyLong(), any<org.apache.hadoop.util.Progressable
				>()));
		}

		private void tryCopyStream(java.io.InputStream @in, bool shouldPass)
		{
			try
			{
				cmd.copyStreamToTarget(new org.apache.hadoop.fs.FSDataInputStream(@in), target);
			}
			catch (java.io.InterruptedIOException)
			{
				NUnit.Framework.Assert.IsFalse("copy failed", shouldPass);
			}
			catch (System.Exception e)
			{
				NUnit.Framework.Assert.IsFalse(e.Message, shouldPass);
			}
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
	}
}
