using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	public class TestCopyPreserveFlag
	{
		private const int MODIFICATION_TIME = 12345000;

		private static readonly org.apache.hadoop.fs.Path FROM = new org.apache.hadoop.fs.Path
			("d1", "f1");

		private static readonly org.apache.hadoop.fs.Path TO = new org.apache.hadoop.fs.Path
			("d2", "f2");

		private static readonly org.apache.hadoop.fs.permission.FsPermission PERMISSIONS = 
			new org.apache.hadoop.fs.permission.FsPermission(org.apache.hadoop.fs.permission.FsAction
			.ALL, org.apache.hadoop.fs.permission.FsAction.EXECUTE, org.apache.hadoop.fs.permission.FsAction
			.READ_WRITE);

		private org.apache.hadoop.fs.FileSystem fs;

		private org.apache.hadoop.fs.Path testDir;

		private org.apache.hadoop.conf.Configuration conf;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void initialize()
		{
			conf = new org.apache.hadoop.conf.Configuration(false);
			conf.set("fs.file.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.LocalFileSystem
				)).getName());
			fs = org.apache.hadoop.fs.FileSystem.getLocal(conf);
			testDir = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty("test.build.data"
				, "build/test/data") + "/testStat");
			// don't want scheme on the path, just an absolute path
			testDir = new org.apache.hadoop.fs.Path(fs.makeQualified(testDir).toUri().getPath
				());
			org.apache.hadoop.fs.FileSystem.setDefaultUri(conf, fs.getUri());
			fs.setWorkingDirectory(testDir);
			fs.mkdirs(new org.apache.hadoop.fs.Path("d1"));
			fs.mkdirs(new org.apache.hadoop.fs.Path("d2"));
			fs.createNewFile(FROM);
			org.apache.hadoop.fs.FSDataOutputStream output = fs.create(FROM, true);
			for (int i = 0; i < 100; ++i)
			{
				output.writeInt(i);
				output.writeChar('\n');
			}
			output.close();
			fs.setTimes(FROM, MODIFICATION_TIME, 0);
			fs.setPermission(FROM, PERMISSIONS);
			fs.setTimes(new org.apache.hadoop.fs.Path("d1"), MODIFICATION_TIME, 0);
			fs.setPermission(new org.apache.hadoop.fs.Path("d1"), PERMISSIONS);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void cleanup()
		{
			fs.delete(testDir, true);
			fs.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void assertAttributesPreserved()
		{
			NUnit.Framework.Assert.AreEqual(MODIFICATION_TIME, fs.getFileStatus(TO).getModificationTime
				());
			NUnit.Framework.Assert.AreEqual(PERMISSIONS, fs.getFileStatus(TO).getPermission()
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void assertAttributesChanged()
		{
			NUnit.Framework.Assert.IsTrue(MODIFICATION_TIME != fs.getFileStatus(TO).getModificationTime
				());
			NUnit.Framework.Assert.IsTrue(!PERMISSIONS.Equals(fs.getFileStatus(TO).getPermission
				()));
		}

		private void run(org.apache.hadoop.fs.shell.CommandWithDestination cmd, params string
			[] args)
		{
			cmd.setConf(conf);
			NUnit.Framework.Assert.AreEqual(0, cmd.run(args));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testPutWithP()
		{
			run(new org.apache.hadoop.fs.shell.CopyCommands.Put(), "-p", FROM.ToString(), TO.
				ToString());
			assertAttributesPreserved();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testPutWithoutP()
		{
			run(new org.apache.hadoop.fs.shell.CopyCommands.Put(), FROM.ToString(), TO.ToString
				());
			assertAttributesChanged();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGetWithP()
		{
			run(new org.apache.hadoop.fs.shell.CopyCommands.Get(), "-p", FROM.ToString(), TO.
				ToString());
			assertAttributesPreserved();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGetWithoutP()
		{
			run(new org.apache.hadoop.fs.shell.CopyCommands.Get(), FROM.ToString(), TO.ToString
				());
			assertAttributesChanged();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testCpWithP()
		{
			run(new org.apache.hadoop.fs.shell.CopyCommands.Cp(), "-p", FROM.ToString(), TO.ToString
				());
			assertAttributesPreserved();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testCpWithoutP()
		{
			run(new org.apache.hadoop.fs.shell.CopyCommands.Cp(), FROM.ToString(), TO.ToString
				());
			assertAttributesChanged();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testDirectoryCpWithP()
		{
			run(new org.apache.hadoop.fs.shell.CopyCommands.Cp(), "-p", "d1", "d3");
			NUnit.Framework.Assert.AreEqual(fs.getFileStatus(new org.apache.hadoop.fs.Path("d1"
				)).getModificationTime(), fs.getFileStatus(new org.apache.hadoop.fs.Path("d3")).
				getModificationTime());
			NUnit.Framework.Assert.AreEqual(fs.getFileStatus(new org.apache.hadoop.fs.Path("d1"
				)).getPermission(), fs.getFileStatus(new org.apache.hadoop.fs.Path("d3")).getPermission
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void testDirectoryCpWithoutP()
		{
			run(new org.apache.hadoop.fs.shell.CopyCommands.Cp(), "d1", "d4");
			NUnit.Framework.Assert.IsTrue(fs.getFileStatus(new org.apache.hadoop.fs.Path("d1"
				)).getModificationTime() != fs.getFileStatus(new org.apache.hadoop.fs.Path("d4")
				).getModificationTime());
			NUnit.Framework.Assert.IsTrue(!fs.getFileStatus(new org.apache.hadoop.fs.Path("d1"
				)).getPermission().Equals(fs.getFileStatus(new org.apache.hadoop.fs.Path("d4")).
				getPermission()));
		}
	}
}
