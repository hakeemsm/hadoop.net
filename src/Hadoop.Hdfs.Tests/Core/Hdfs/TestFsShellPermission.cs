using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This test covers privilege related aspects of FsShell</summary>
	public class TestFsShellPermission
	{
		private const string TestRoot = "/testroot";

		internal static UserGroupInformation CreateUGI(string ownername, string groupName
			)
		{
			return UserGroupInformation.CreateUserForTesting(ownername, new string[] { groupName
				 });
		}

		private class FileEntry
		{
			private string path;

			private bool isDir;

			private string owner;

			private string group;

			private string permission;

			public FileEntry(TestFsShellPermission _enclosing, string path, bool isDir, string
				 owner, string group, string permission)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.isDir = isDir;
				this.owner = owner;
				this.group = group;
				this.permission = permission;
			}

			internal virtual string GetPath()
			{
				return this.path;
			}

			internal virtual bool IsDirectory()
			{
				return this.isDir;
			}

			internal virtual string GetOwner()
			{
				return this.owner;
			}

			internal virtual string GetGroup()
			{
				return this.group;
			}

			internal virtual string GetPermission()
			{
				return this.permission;
			}

			private readonly TestFsShellPermission _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateFiles(FileSystem fs, string topdir, TestFsShellPermission.FileEntry
			[] entries)
		{
			foreach (TestFsShellPermission.FileEntry entry in entries)
			{
				string newPathStr = topdir + "/" + entry.GetPath();
				Path newPath = new Path(newPathStr);
				if (entry.IsDirectory())
				{
					fs.Mkdirs(newPath);
				}
				else
				{
					FileSystemTestHelper.CreateFile(fs, newPath);
				}
				fs.SetPermission(newPath, new FsPermission(entry.GetPermission()));
				fs.SetOwner(newPath, entry.GetOwner(), entry.GetGroup());
			}
		}

		/// <summary>delete directory and everything underneath it.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static void Deldir(FileSystem fs, string topdir)
		{
			fs.Delete(new Path(topdir), true);
		}

		/// <exception cref="System.Exception"/>
		internal static string ExecCmd(FsShell shell, string[] args)
		{
			ByteArrayOutputStream baout = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(baout, true);
			TextWriter old = System.Console.Out;
			Runtime.SetOut(@out);
			int ret = shell.Run(args);
			@out.Close();
			Runtime.SetOut(old);
			return ret.ToString();
		}

		private class TestDeleteHelper
		{
			private TestFsShellPermission.FileEntry[] fileEntries;

			private TestFsShellPermission.FileEntry deleteEntry;

			private string cmdAndOptions;

			private bool expectedToDelete;

			internal readonly string doAsGroup;

			internal readonly UserGroupInformation userUgi;

			public TestDeleteHelper(TestFsShellPermission _enclosing, TestFsShellPermission.FileEntry
				[] fileEntries, TestFsShellPermission.FileEntry deleteEntry, string cmdAndOptions
				, string doAsUser, bool expectedToDelete)
			{
				this._enclosing = _enclosing;
				/*
				* Each instance of TestDeleteHelper captures one testing scenario.
				*
				* To create all files listed in fileEntries, and then delete as user
				* doAsuser the deleteEntry with command+options specified in cmdAndOptions.
				*
				* When expectedToDelete is true, the deleteEntry is expected to be deleted;
				* otherwise, it's not expected to be deleted. At the end of test,
				* the existence of deleteEntry is checked against expectedToDelete
				* to ensure the command is finished with expected result
				*/
				this.fileEntries = fileEntries;
				this.deleteEntry = deleteEntry;
				this.cmdAndOptions = cmdAndOptions;
				this.expectedToDelete = expectedToDelete;
				this.doAsGroup = doAsUser.Equals("hdfs") ? "supergroup" : "users";
				this.userUgi = TestFsShellPermission.CreateUGI(doAsUser, this.doAsGroup);
			}

			/// <exception cref="System.Exception"/>
			public virtual void Execute(Configuration conf, FileSystem fs)
			{
				fs.Mkdirs(new Path(TestFsShellPermission.TestRoot));
				this._enclosing.CreateFiles(fs, TestFsShellPermission.TestRoot, this.fileEntries);
				FsShell fsShell = new FsShell(conf);
				string deletePath = TestFsShellPermission.TestRoot + "/" + this.deleteEntry.GetPath
					();
				string[] tmpCmdOpts = StringUtils.Split(this.cmdAndOptions);
				AList<string> tmpArray = new AList<string>(Arrays.AsList(tmpCmdOpts));
				tmpArray.AddItem(deletePath);
				string[] cmdOpts = Sharpen.Collections.ToArray(tmpArray, new string[tmpArray.Count
					]);
				this.userUgi.DoAs(new _PrivilegedExceptionAction_153(fsShell, cmdOpts));
				bool deleted = !fs.Exists(new Path(deletePath));
				NUnit.Framework.Assert.AreEqual(this.expectedToDelete, deleted);
				TestFsShellPermission.Deldir(fs, TestFsShellPermission.TestRoot);
			}

			private sealed class _PrivilegedExceptionAction_153 : PrivilegedExceptionAction<string
				>
			{
				public _PrivilegedExceptionAction_153(FsShell fsShell, string[] cmdOpts)
				{
					this.fsShell = fsShell;
					this.cmdOpts = cmdOpts;
				}

				/// <exception cref="System.Exception"/>
				public string Run()
				{
					return TestFsShellPermission.ExecCmd(fsShell, cmdOpts);
				}

				private readonly FsShell fsShell;

				private readonly string[] cmdOpts;
			}

			private readonly TestFsShellPermission _enclosing;
		}

		private TestFsShellPermission.TestDeleteHelper GenDeleteEmptyDirHelper(string cmdOpts
			, string targetPerm, string asUser, bool expectedToDelete)
		{
			TestFsShellPermission.FileEntry[] files = new TestFsShellPermission.FileEntry[] { 
				new TestFsShellPermission.FileEntry(this, "userA", true, "userA", "users", "755"
				), new TestFsShellPermission.FileEntry(this, "userA/userB", true, "userB", "users"
				, targetPerm) };
			TestFsShellPermission.FileEntry deleteEntry = files[1];
			return new TestFsShellPermission.TestDeleteHelper(this, files, deleteEntry, cmdOpts
				, asUser, expectedToDelete);
		}

		// Expect target to be deleted
		private TestFsShellPermission.TestDeleteHelper GenRmrEmptyDirWithReadPerm()
		{
			return GenDeleteEmptyDirHelper("-rm -r", "744", "userA", true);
		}

		// Expect target to be deleted
		private TestFsShellPermission.TestDeleteHelper GenRmrEmptyDirWithNoPerm()
		{
			return GenDeleteEmptyDirHelper("-rm -r", "700", "userA", true);
		}

		// Expect target to be deleted
		private TestFsShellPermission.TestDeleteHelper GenRmrfEmptyDirWithNoPerm()
		{
			return GenDeleteEmptyDirHelper("-rm -r -f", "700", "userA", true);
		}

		private TestFsShellPermission.TestDeleteHelper GenDeleteNonEmptyDirHelper(string 
			cmd, string targetPerm, string asUser, bool expectedToDelete)
		{
			TestFsShellPermission.FileEntry[] files = new TestFsShellPermission.FileEntry[] { 
				new TestFsShellPermission.FileEntry(this, "userA", true, "userA", "users", "755"
				), new TestFsShellPermission.FileEntry(this, "userA/userB", true, "userB", "users"
				, targetPerm), new TestFsShellPermission.FileEntry(this, "userA/userB/xyzfile", 
				false, "userB", "users", targetPerm) };
			TestFsShellPermission.FileEntry deleteEntry = files[1];
			return new TestFsShellPermission.TestDeleteHelper(this, files, deleteEntry, cmd, 
				asUser, expectedToDelete);
		}

		// Expect target not to be deleted
		private TestFsShellPermission.TestDeleteHelper GenRmrNonEmptyDirWithReadPerm()
		{
			return GenDeleteNonEmptyDirHelper("-rm -r", "744", "userA", false);
		}

		// Expect target not to be deleted
		private TestFsShellPermission.TestDeleteHelper GenRmrNonEmptyDirWithNoPerm()
		{
			return GenDeleteNonEmptyDirHelper("-rm -r", "700", "userA", false);
		}

		// Expect target to be deleted
		private TestFsShellPermission.TestDeleteHelper GenRmrNonEmptyDirWithAllPerm()
		{
			return GenDeleteNonEmptyDirHelper("-rm -r", "777", "userA", true);
		}

		// Expect target not to be deleted
		private TestFsShellPermission.TestDeleteHelper GenRmrfNonEmptyDirWithNoPerm()
		{
			return GenDeleteNonEmptyDirHelper("-rm -r -f", "700", "userA", false);
		}

		// Expect target to be deleted
		/// <exception cref="System.Exception"/>
		public virtual TestFsShellPermission.TestDeleteHelper GenDeleteSingleFileNotAsOwner
			()
		{
			TestFsShellPermission.FileEntry[] files = new TestFsShellPermission.FileEntry[] { 
				new TestFsShellPermission.FileEntry(this, "userA", true, "userA", "users", "755"
				), new TestFsShellPermission.FileEntry(this, "userA/userB", false, "userB", "users"
				, "700") };
			TestFsShellPermission.FileEntry deleteEntry = files[1];
			return new TestFsShellPermission.TestDeleteHelper(this, files, deleteEntry, "-rm -r"
				, "userA", true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelete()
		{
			Configuration conf = null;
			MiniDFSCluster cluster = null;
			try
			{
				conf = new Configuration();
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				string nnUri = FileSystem.GetDefaultUri(conf).ToString();
				FileSystem fs = FileSystem.Get(URI.Create(nnUri), conf);
				AList<TestFsShellPermission.TestDeleteHelper> ta = new AList<TestFsShellPermission.TestDeleteHelper
					>();
				// Add empty dir tests
				ta.AddItem(GenRmrEmptyDirWithReadPerm());
				ta.AddItem(GenRmrEmptyDirWithNoPerm());
				ta.AddItem(GenRmrfEmptyDirWithNoPerm());
				// Add non-empty dir tests
				ta.AddItem(GenRmrNonEmptyDirWithReadPerm());
				ta.AddItem(GenRmrNonEmptyDirWithNoPerm());
				ta.AddItem(GenRmrNonEmptyDirWithAllPerm());
				ta.AddItem(GenRmrfNonEmptyDirWithNoPerm());
				// Add single tile test
				ta.AddItem(GenDeleteSingleFileNotAsOwner());
				// Run all tests
				foreach (TestFsShellPermission.TestDeleteHelper t in ta)
				{
					t.Execute(conf, fs);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
