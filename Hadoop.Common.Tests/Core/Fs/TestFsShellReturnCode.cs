using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Hadoop.Common.Core.Fs.Shell;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;
using Org.Apache.Hadoop.IO;
using Path = Org.Apache.Hadoop.FS.Path;

namespace Hadoop.Common.Tests.Core.Fs
{
	/// <summary>This test validates that chmod, chown, chgrp returning correct exit codes
	/// 	</summary>
	public class TestFsShellReturnCode
	{
		private static readonly Org.Apache.Hadoop.Log Log = LogFactory.GetLog("org.apache.hadoop.fs.TestFsShellReturnCode"
			);

		private static readonly Configuration conf = new Configuration();

		private static FileSystem fileSys;

		private static FsShell fsShell;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			conf.SetClass("fs.file.impl", typeof(TestFsShellReturnCode.LocalFileSystemExtn), 
				typeof(LocalFileSystem));
			fileSys = FileSystem.Get(conf);
			fsShell = new FsShell(conf);
		}

		private static string TestRootDir = Runtime.GetProperty("test.build.data", "build/test/data/testCHReturnCode"
			);

		/// <exception cref="System.Exception"/>
		internal static void WriteFile(FileSystem fs, Path name)
		{
			FSDataOutputStream stm = fs.Create(name);
			stm.WriteBytes("42\n");
			stm.Close();
		}

		/// <exception cref="System.Exception"/>
		private void Change(int exit, string owner, string group, params string[] files)
		{
			FileStatus[][] oldStats = new FileStatus[files.Length][];
			for (int i = 0; i < files.Length; i++)
			{
				oldStats[i] = fileSys.GlobStatus(new Path(files[i]));
			}
			IList<string> argv = new List<string>();
			if (owner != null)
			{
				argv.AddItem("-chown");
				string chown = owner;
				if (group != null)
				{
					chown += ":" + group;
					if (group.IsEmpty())
					{
						group = null;
					}
				}
				// avoid testing for it later
				argv.AddItem(chown);
			}
			else
			{
				argv.AddItem("-chgrp");
				argv.AddItem(group);
			}
			Sharpen.Collections.AddAll(argv, files);
			Assert.Equal(exit, fsShell.Run(Sharpen.Collections.ToArray(argv
				, new string[0])));
			for (int i_1 = 0; i_1 < files.Length; i_1++)
			{
				FileStatus[] stats = fileSys.GlobStatus(new Path(files[i_1]));
				if (stats != null)
				{
					for (int j = 0; j < stats.Length; j++)
					{
						Assert.Equal("check owner of " + files[i_1], ((owner != null) ? 
							"STUB-" + owner : oldStats[i_1][j].GetOwner()), stats[j].GetOwner());
						Assert.Equal("check group of " + files[i_1], ((group != null) ? 
							"STUB-" + group : oldStats[i_1][j].GetGroup()), stats[j].GetGroup());
					}
				}
			}
		}

		/// <summary>Test Chmod 1.</summary>
		/// <remarks>
		/// Test Chmod 1. Create and write file on FS 2. Verify that exit code for
		/// chmod on existing file is 0 3. Verify that exit code for chmod on
		/// non-existing file is 1 4. Verify that exit code for chmod with glob input
		/// on non-existing file is 1 5. Verify that exit code for chmod with glob
		/// input on existing file in 0
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestChmod()
		{
			Path p1 = new Path(TestRootDir, "testChmod/fileExists");
			string f1 = p1.ToUri().GetPath();
			string f2 = new Path(TestRootDir, "testChmod/fileDoesNotExist").ToUri().GetPath();
			string f3 = new Path(TestRootDir, "testChmod/nonExistingfiles*").ToUri().GetPath(
				);
			Path p4 = new Path(TestRootDir, "testChmod/file1");
			Path p5 = new Path(TestRootDir, "testChmod/file2");
			Path p6 = new Path(TestRootDir, "testChmod/file3");
			string f7 = new Path(TestRootDir, "testChmod/file*").ToUri().GetPath();
			// create and write test file
			WriteFile(fileSys, p1);
			Assert.True(fileSys.Exists(p1));
			// Test 1: Test 1: exit code for chmod on existing is 0
			string[] argv = new string[] { "-chmod", "777", f1 };
			Assert.Equal(0, fsShell.Run(argv));
			// Test 2: exit code for chmod on non-existing path is 1
			string[] argv2 = new string[] { "-chmod", "777", f2 };
			Assert.Equal(1, fsShell.Run(argv2));
			// Test 3: exit code for chmod on non-existing path with globbed input is 1
			string[] argv3 = new string[] { "-chmod", "777", f3 };
			Assert.Equal(1, fsShell.Run(argv3));
			// create required files
			WriteFile(fileSys, p4);
			Assert.True(fileSys.Exists(p4));
			WriteFile(fileSys, p5);
			Assert.True(fileSys.Exists(p5));
			WriteFile(fileSys, p6);
			Assert.True(fileSys.Exists(p6));
			// Test 4: exit code for chmod on existing path with globbed input is 0
			string[] argv4 = new string[] { "-chmod", "777", f7 };
			Assert.Equal(0, fsShell.Run(argv4));
		}

		/// <summary>Test Chown 1.</summary>
		/// <remarks>
		/// Test Chown 1. Create and write file on FS 2. Verify that exit code for
		/// Chown on existing file is 0 3. Verify that exit code for Chown on
		/// non-existing file is 1 4. Verify that exit code for Chown with glob input
		/// on non-existing file is 1 5. Verify that exit code for Chown with glob
		/// input on existing file in 0
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestChown()
		{
			Path p1 = new Path(TestRootDir, "testChown/fileExists");
			string f1 = p1.ToUri().GetPath();
			string f2 = new Path(TestRootDir, "testChown/fileDoesNotExist").ToUri().GetPath();
			string f3 = new Path(TestRootDir, "testChown/nonExistingfiles*").ToUri().GetPath(
				);
			Path p4 = new Path(TestRootDir, "testChown/file1");
			Path p5 = new Path(TestRootDir, "testChown/file2");
			Path p6 = new Path(TestRootDir, "testChown/file3");
			string f7 = new Path(TestRootDir, "testChown/file*").ToUri().GetPath();
			// create and write test file
			WriteFile(fileSys, p1);
			Assert.True(fileSys.Exists(p1));
			// Test 1: exit code for chown on existing file is 0
			Change(0, "admin", null, f1);
			// Test 2: exit code for chown on non-existing path is 1
			Change(1, "admin", null, f2);
			// Test 3: exit code for chown on non-existing path with globbed input is 1
			Change(1, "admin", null, f3);
			// create required files
			WriteFile(fileSys, p4);
			Assert.True(fileSys.Exists(p4));
			WriteFile(fileSys, p5);
			Assert.True(fileSys.Exists(p5));
			WriteFile(fileSys, p6);
			Assert.True(fileSys.Exists(p6));
			// Test 4: exit code for chown on existing path with globbed input is 0
			Change(0, "admin", null, f7);
			//Test 5: test for setOwner invocation on FS from command handler.
			Change(0, "admin", "Test", f1);
			Change(0, "admin", string.Empty, f1);
		}

		/// <summary>Test Chgrp 1.</summary>
		/// <remarks>
		/// Test Chgrp 1. Create and write file on FS 2. Verify that exit code for
		/// chgrp on existing file is 0 3. Verify that exit code for chgrp on
		/// non-existing file is 1 4. Verify that exit code for chgrp with glob input
		/// on non-existing file is 1 5. Verify that exit code for chgrp with glob
		/// input on existing file in 0
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestChgrp()
		{
			Path p1 = new Path(TestRootDir, "testChgrp/fileExists");
			string f1 = p1.ToUri().GetPath();
			string f2 = new Path(TestRootDir, "testChgrp/fileDoesNotExist").ToUri().GetPath();
			string f3 = new Path(TestRootDir, "testChgrp/nonExistingfiles*").ToUri().GetPath(
				);
			Path p4 = new Path(TestRootDir, "testChgrp/file1");
			Path p5 = new Path(TestRootDir, "testChgrp/file2");
			Path p6 = new Path(TestRootDir, "testChgrp/file3");
			string f7 = new Path(TestRootDir, "testChgrp/file*").ToUri().GetPath();
			// create and write test file
			WriteFile(fileSys, p1);
			Assert.True(fileSys.Exists(p1));
			// Test 1: exit code for chgrp on existing file is 0
			Change(0, null, "admin", f1);
			// Test 2: exit code for chgrp on non existing path is 1
			Change(1, null, "admin", f2);
			Change(1, null, "admin", f2, f1);
			// exit code used to be for last item
			// Test 3: exit code for chgrp on non-existing path with globbed input is 1
			Change(1, null, "admin", f3);
			Change(1, null, "admin", f3, f1);
			// create required files
			WriteFile(fileSys, p4);
			Assert.True(fileSys.Exists(p4));
			WriteFile(fileSys, p5);
			Assert.True(fileSys.Exists(p5));
			WriteFile(fileSys, p6);
			Assert.True(fileSys.Exists(p6));
			// Test 4: exit code for chgrp on existing path with globbed input is 0
			Change(0, null, "admin", f7);
			Change(1, null, "admin", f2, f7);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetWithInvalidSourcePathShouldNotDisplayNullInConsole()
		{
			Configuration conf = new Configuration();
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(bytes);
			TextWriter oldErr = System.Console.Error;
			Runtime.SetErr(@out);
			string results;
			try
			{
				Path tdir = new Path(TestRootDir, "notNullCopy");
				fileSys.Delete(tdir, true);
				fileSys.Mkdirs(tdir);
				string[] args = new string[3];
				args[0] = "-get";
				args[1] = new Path(tdir.ToUri().GetPath(), "/invalidSrc").ToString();
				args[2] = new Path(tdir.ToUri().GetPath(), "/invalidDst").ToString();
				Assert.True("file exists", !fileSys.Exists(new Path(args[1])));
				Assert.True("file exists", !fileSys.Exists(new Path(args[2])));
				int run = shell.Run(args);
				results = bytes.ToString();
				Assert.Equal("Return code should be 1", 1, run);
				Assert.True(" Null is coming when source path is invalid. ", !results
					.Contains("get: null"));
				Assert.True(" Not displaying the intended message ", results.Contains
					("get: `" + args[1] + "': No such file or directory"));
			}
			finally
			{
				IOUtils.CloseStream(@out);
				Runtime.SetErr(oldErr);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRmWithNonexistentGlob()
		{
			Configuration conf = new Configuration();
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			TextWriter err = new TextWriter(bytes);
			TextWriter oldErr = System.Console.Error;
			Runtime.SetErr(err);
			string results;
			try
			{
				int exit = shell.Run(new string[] { "-rm", "nomatch*" });
				Assert.Equal(1, exit);
				results = bytes.ToString();
				Assert.True(results.Contains("rm: `nomatch*': No such file or directory"
					));
			}
			finally
			{
				IOUtils.CloseStream(err);
				Runtime.SetErr(oldErr);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRmForceWithNonexistentGlob()
		{
			Configuration conf = new Configuration();
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			TextWriter err = new TextWriter(bytes);
			TextWriter oldErr = System.Console.Error;
			Runtime.SetErr(err);
			try
			{
				int exit = shell.Run(new string[] { "-rm", "-f", "nomatch*" });
				Assert.Equal(0, exit);
				Assert.True(bytes.ToString().IsEmpty());
			}
			finally
			{
				IOUtils.CloseStream(err);
				Runtime.SetErr(oldErr);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestInvalidDefaultFS()
		{
			// if default fs doesn't exist or is invalid, but the path provided in 
			// arguments is valid - fsshell should work
			FsShell shell = new FsShell();
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, "hhhh://doesnotexist/");
			shell.SetConf(conf);
			string[] args = new string[2];
			args[0] = "-ls";
			args[1] = "file:///";
			// this is valid, so command should run
			int res = shell.Run(args);
			System.Console.Out.WriteLine("res =" + res);
			shell.SetConf(conf);
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(bytes);
			TextWriter oldErr = System.Console.Error;
			Runtime.SetErr(@out);
			string results;
			try
			{
				int run = shell.Run(args);
				results = bytes.ToString();
				Log.Info("result=" + results);
				Assert.True("Return code should be 0", run == 0);
			}
			finally
			{
				IOUtils.CloseStream(@out);
				Runtime.SetErr(oldErr);
			}
		}

		/// <summary>Tests combinations of valid and invalid user and group arguments to chown.
		/// 	</summary>
		[Fact]
		public virtual void TestChownUserAndGroupValidity()
		{
			// This test only covers argument parsing, so override to skip processing.
			FsCommand chown = new _Chown_391();
			chown.SetConf(new Configuration());
			// The following are valid (no exception expected).
			chown.Run("user", "/path");
			chown.Run("user:group", "/path");
			chown.Run(":group", "/path");
			// The following are valid only on Windows.
			AssertValidArgumentsOnWindows(chown, "User With Spaces", "/path");
			AssertValidArgumentsOnWindows(chown, "User With Spaces:group", "/path");
			AssertValidArgumentsOnWindows(chown, "User With Spaces:Group With Spaces", "/path"
				);
			AssertValidArgumentsOnWindows(chown, "user:Group With Spaces", "/path");
			AssertValidArgumentsOnWindows(chown, ":Group With Spaces", "/path");
			// The following are invalid (exception expected).
			AssertIllegalArguments(chown, "us!er", "/path");
			AssertIllegalArguments(chown, "us^er", "/path");
			AssertIllegalArguments(chown, "user:gr#oup", "/path");
			AssertIllegalArguments(chown, "user:gr%oup", "/path");
			AssertIllegalArguments(chown, ":gr#oup", "/path");
			AssertIllegalArguments(chown, ":gr%oup", "/path");
		}

		private sealed class _Chown_391 : FsShellPermissions.Chown
		{
			public _Chown_391()
			{
			}

			protected internal override void ProcessArgument(PathData item)
			{
			}
		}

		/// <summary>Tests valid and invalid group arguments to chgrp.</summary>
		[Fact]
		public virtual void TestChgrpGroupValidity()
		{
			// This test only covers argument parsing, so override to skip processing.
			FsCommand chgrp = new _Chgrp_426();
			chgrp.SetConf(new Configuration());
			// The following are valid (no exception expected).
			chgrp.Run("group", "/path");
			// The following are valid only on Windows.
			AssertValidArgumentsOnWindows(chgrp, "Group With Spaces", "/path");
			// The following are invalid (exception expected).
			AssertIllegalArguments(chgrp, ":gr#oup", "/path");
			AssertIllegalArguments(chgrp, ":gr%oup", "/path");
		}

		private sealed class _Chgrp_426 : FsShellPermissions.Chgrp
		{
			public _Chgrp_426()
			{
			}

			protected internal override void ProcessArgument(PathData item)
			{
			}
		}

		internal class LocalFileSystemExtn : LocalFileSystem
		{
			public LocalFileSystemExtn()
				: base(new TestFsShellReturnCode.RawLocalFileSystemExtn())
			{
			}
		}

		internal class RawLocalFileSystemExtn : RawLocalFileSystem
		{
			protected internal static Dictionary<string, string> owners = new Dictionary<string
				, string>();

			protected internal static Dictionary<string, string> groups = new Dictionary<string
				, string>();

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream Create(Path p)
			{
				//owners.remove(p);
				//groups.remove(p);
				return base.Create(p);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetOwner(Path p, string username, string groupname)
			{
				string f = MakeQualified(p).ToString();
				if (username != null)
				{
					owners[f] = username;
				}
				if (groupname != null)
				{
					groups[f] = groupname;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus GetFileStatus(Path p)
			{
				string f = MakeQualified(p).ToString();
				FileStatus stat = base.GetFileStatus(p);
				stat.GetPermission();
				if (owners.Contains(f))
				{
					stat.SetOwner("STUB-" + owners[f]);
				}
				else
				{
					stat.SetOwner("REAL-" + stat.GetOwner());
				}
				if (groups.Contains(f))
				{
					stat.SetGroup("STUB-" + groups[f]);
				}
				else
				{
					stat.SetGroup("REAL-" + stat.GetGroup());
				}
				return stat;
			}
		}

		/// <summary>
		/// Asserts that for the given command, the given arguments are considered
		/// invalid.
		/// </summary>
		/// <remarks>
		/// Asserts that for the given command, the given arguments are considered
		/// invalid.  The expectation is that the command will throw
		/// IllegalArgumentException.
		/// </remarks>
		/// <param name="cmd">FsCommand to check</param>
		/// <param name="args">String... arguments to check</param>
		private static void AssertIllegalArguments(FsCommand cmd, params string[] args)
		{
			try
			{
				cmd.Run(args);
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException from args: " + Arrays
					.ToString(args));
			}
			catch (ArgumentException)
			{
			}
		}

		/// <summary>
		/// Asserts that for the given command, the given arguments are considered valid
		/// on Windows, but invalid elsewhere.
		/// </summary>
		/// <param name="cmd">FsCommand to check</param>
		/// <param name="args">String... arguments to check</param>
		private static void AssertValidArgumentsOnWindows(FsCommand cmd, params string[] 
			args)
		{
			if (Org.Apache.Hadoop.Util.Shell.Windows)
			{
				cmd.Run(args);
			}
			else
			{
				AssertIllegalArguments(cmd, args);
			}
		}
	}
}
