using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	public class TestCopyPreserveFlag
	{
		private const int ModificationTime = 12345000;

		private static readonly Path From = new Path("d1", "f1");

		private static readonly Path To = new Path("d2", "f2");

		private static readonly FsPermission Permissions = new FsPermission(FsAction.All, 
			FsAction.Execute, FsAction.ReadWrite);

		private FileSystem fs;

		private Path testDir;

		private Configuration conf;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Initialize()
		{
			conf = new Configuration(false);
			conf.Set("fs.file.impl", typeof(LocalFileSystem).FullName);
			fs = FileSystem.GetLocal(conf);
			testDir = new Path(Runtime.GetProperty("test.build.data", "build/test/data") + "/testStat"
				);
			// don't want scheme on the path, just an absolute path
			testDir = new Path(fs.MakeQualified(testDir).ToUri().GetPath());
			FileSystem.SetDefaultUri(conf, fs.GetUri());
			fs.SetWorkingDirectory(testDir);
			fs.Mkdirs(new Path("d1"));
			fs.Mkdirs(new Path("d2"));
			fs.CreateNewFile(From);
			FSDataOutputStream output = fs.Create(From, true);
			for (int i = 0; i < 100; ++i)
			{
				output.WriteInt(i);
				output.WriteChar('\n');
			}
			output.Close();
			fs.SetTimes(From, ModificationTime, 0);
			fs.SetPermission(From, Permissions);
			fs.SetTimes(new Path("d1"), ModificationTime, 0);
			fs.SetPermission(new Path("d1"), Permissions);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Cleanup()
		{
			fs.Delete(testDir, true);
			fs.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void AssertAttributesPreserved()
		{
			NUnit.Framework.Assert.AreEqual(ModificationTime, fs.GetFileStatus(To).GetModificationTime
				());
			NUnit.Framework.Assert.AreEqual(Permissions, fs.GetFileStatus(To).GetPermission()
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void AssertAttributesChanged()
		{
			NUnit.Framework.Assert.IsTrue(ModificationTime != fs.GetFileStatus(To).GetModificationTime
				());
			NUnit.Framework.Assert.IsTrue(!Permissions.Equals(fs.GetFileStatus(To).GetPermission
				()));
		}

		private void Run(CommandWithDestination cmd, params string[] args)
		{
			cmd.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, cmd.Run(args));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPutWithP()
		{
			Run(new CopyCommands.Put(), "-p", From.ToString(), To.ToString());
			AssertAttributesPreserved();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPutWithoutP()
		{
			Run(new CopyCommands.Put(), From.ToString(), To.ToString());
			AssertAttributesChanged();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetWithP()
		{
			Run(new CopyCommands.Get(), "-p", From.ToString(), To.ToString());
			AssertAttributesPreserved();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetWithoutP()
		{
			Run(new CopyCommands.Get(), From.ToString(), To.ToString());
			AssertAttributesChanged();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCpWithP()
		{
			Run(new CopyCommands.CP(), "-p", From.ToString(), To.ToString());
			AssertAttributesPreserved();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCpWithoutP()
		{
			Run(new CopyCommands.CP(), From.ToString(), To.ToString());
			AssertAttributesChanged();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDirectoryCpWithP()
		{
			Run(new CopyCommands.CP(), "-p", "d1", "d3");
			NUnit.Framework.Assert.AreEqual(fs.GetFileStatus(new Path("d1")).GetModificationTime
				(), fs.GetFileStatus(new Path("d3")).GetModificationTime());
			NUnit.Framework.Assert.AreEqual(fs.GetFileStatus(new Path("d1")).GetPermission(), 
				fs.GetFileStatus(new Path("d3")).GetPermission());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDirectoryCpWithoutP()
		{
			Run(new CopyCommands.CP(), "d1", "d4");
			NUnit.Framework.Assert.IsTrue(fs.GetFileStatus(new Path("d1")).GetModificationTime
				() != fs.GetFileStatus(new Path("d4")).GetModificationTime());
			NUnit.Framework.Assert.IsTrue(!fs.GetFileStatus(new Path("d1")).GetPermission().Equals
				(fs.GetFileStatus(new Path("d4")).GetPermission()));
		}
	}
}
