using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>This test validates that chmod, chown, chgrp returning correct exit codes
	/// 	</summary>
	public class TestFsShellReturnCode
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog("org.apache.hadoop.fs.TestFsShellReturnCode");

		private static readonly org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		private static org.apache.hadoop.fs.FileSystem fileSys;

		private static org.apache.hadoop.fs.FsShell fsShell;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.BeforeClass]
		public static void setup()
		{
			conf.setClass("fs.file.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestFsShellReturnCode.LocalFileSystemExtn
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.LocalFileSystem)
				));
			fileSys = org.apache.hadoop.fs.FileSystem.get(conf);
			fsShell = new org.apache.hadoop.fs.FsShell(conf);
		}

		private static string TEST_ROOT_DIR = Sharpen.Runtime.getProperty("test.build.data"
			, "build/test/data/testCHReturnCode");

		/// <exception cref="System.Exception"/>
		internal static void writeFile(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 name)
		{
			org.apache.hadoop.fs.FSDataOutputStream stm = fs.create(name);
			stm.writeBytes("42\n");
			stm.close();
		}

		/// <exception cref="System.Exception"/>
		private void change(int exit, string owner, string group, params string[] files)
		{
			org.apache.hadoop.fs.FileStatus[][] oldStats = new org.apache.hadoop.fs.FileStatus
				[files.Length][];
			for (int i = 0; i < files.Length; i++)
			{
				oldStats[i] = fileSys.globStatus(new org.apache.hadoop.fs.Path(files[i]));
			}
			System.Collections.Generic.IList<string> argv = new System.Collections.Generic.LinkedList
				<string>();
			if (owner != null)
			{
				argv.add("-chown");
				string chown = owner;
				if (group != null)
				{
					chown += ":" + group;
					if (group.isEmpty())
					{
						group = null;
					}
				}
				// avoid testing for it later
				argv.add(chown);
			}
			else
			{
				argv.add("-chgrp");
				argv.add(group);
			}
			java.util.Collections.addAll(argv, files);
			NUnit.Framework.Assert.AreEqual(exit, fsShell.run(Sharpen.Collections.ToArray(argv
				, new string[0])));
			for (int i_1 = 0; i_1 < files.Length; i_1++)
			{
				org.apache.hadoop.fs.FileStatus[] stats = fileSys.globStatus(new org.apache.hadoop.fs.Path
					(files[i_1]));
				if (stats != null)
				{
					for (int j = 0; j < stats.Length; j++)
					{
						NUnit.Framework.Assert.AreEqual("check owner of " + files[i_1], ((owner != null) ? 
							"STUB-" + owner : oldStats[i_1][j].getOwner()), stats[j].getOwner());
						NUnit.Framework.Assert.AreEqual("check group of " + files[i_1], ((group != null) ? 
							"STUB-" + group : oldStats[i_1][j].getGroup()), stats[j].getGroup());
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
		public virtual void testChmod()
		{
			org.apache.hadoop.fs.Path p1 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChmod/fileExists"
				);
			string f1 = p1.toUri().getPath();
			string f2 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChmod/fileDoesNotExist"
				).toUri().getPath();
			string f3 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChmod/nonExistingfiles*"
				).toUri().getPath();
			org.apache.hadoop.fs.Path p4 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChmod/file1"
				);
			org.apache.hadoop.fs.Path p5 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChmod/file2"
				);
			org.apache.hadoop.fs.Path p6 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChmod/file3"
				);
			string f7 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChmod/file*").toUri
				().getPath();
			// create and write test file
			writeFile(fileSys, p1);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(p1));
			// Test 1: Test 1: exit code for chmod on existing is 0
			string[] argv = new string[] { "-chmod", "777", f1 };
			NUnit.Framework.Assert.AreEqual(0, fsShell.run(argv));
			// Test 2: exit code for chmod on non-existing path is 1
			string[] argv2 = new string[] { "-chmod", "777", f2 };
			NUnit.Framework.Assert.AreEqual(1, fsShell.run(argv2));
			// Test 3: exit code for chmod on non-existing path with globbed input is 1
			string[] argv3 = new string[] { "-chmod", "777", f3 };
			NUnit.Framework.Assert.AreEqual(1, fsShell.run(argv3));
			// create required files
			writeFile(fileSys, p4);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(p4));
			writeFile(fileSys, p5);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(p5));
			writeFile(fileSys, p6);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(p6));
			// Test 4: exit code for chmod on existing path with globbed input is 0
			string[] argv4 = new string[] { "-chmod", "777", f7 };
			NUnit.Framework.Assert.AreEqual(0, fsShell.run(argv4));
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
		public virtual void testChown()
		{
			org.apache.hadoop.fs.Path p1 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChown/fileExists"
				);
			string f1 = p1.toUri().getPath();
			string f2 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChown/fileDoesNotExist"
				).toUri().getPath();
			string f3 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChown/nonExistingfiles*"
				).toUri().getPath();
			org.apache.hadoop.fs.Path p4 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChown/file1"
				);
			org.apache.hadoop.fs.Path p5 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChown/file2"
				);
			org.apache.hadoop.fs.Path p6 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChown/file3"
				);
			string f7 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChown/file*").toUri
				().getPath();
			// create and write test file
			writeFile(fileSys, p1);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(p1));
			// Test 1: exit code for chown on existing file is 0
			change(0, "admin", null, f1);
			// Test 2: exit code for chown on non-existing path is 1
			change(1, "admin", null, f2);
			// Test 3: exit code for chown on non-existing path with globbed input is 1
			change(1, "admin", null, f3);
			// create required files
			writeFile(fileSys, p4);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(p4));
			writeFile(fileSys, p5);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(p5));
			writeFile(fileSys, p6);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(p6));
			// Test 4: exit code for chown on existing path with globbed input is 0
			change(0, "admin", null, f7);
			//Test 5: test for setOwner invocation on FS from command handler.
			change(0, "admin", "Test", f1);
			change(0, "admin", string.Empty, f1);
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
		public virtual void testChgrp()
		{
			org.apache.hadoop.fs.Path p1 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChgrp/fileExists"
				);
			string f1 = p1.toUri().getPath();
			string f2 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChgrp/fileDoesNotExist"
				).toUri().getPath();
			string f3 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChgrp/nonExistingfiles*"
				).toUri().getPath();
			org.apache.hadoop.fs.Path p4 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChgrp/file1"
				);
			org.apache.hadoop.fs.Path p5 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChgrp/file2"
				);
			org.apache.hadoop.fs.Path p6 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChgrp/file3"
				);
			string f7 = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "testChgrp/file*").toUri
				().getPath();
			// create and write test file
			writeFile(fileSys, p1);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(p1));
			// Test 1: exit code for chgrp on existing file is 0
			change(0, null, "admin", f1);
			// Test 2: exit code for chgrp on non existing path is 1
			change(1, null, "admin", f2);
			change(1, null, "admin", f2, f1);
			// exit code used to be for last item
			// Test 3: exit code for chgrp on non-existing path with globbed input is 1
			change(1, null, "admin", f3);
			change(1, null, "admin", f3, f1);
			// create required files
			writeFile(fileSys, p4);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(p4));
			writeFile(fileSys, p5);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(p5));
			writeFile(fileSys, p6);
			NUnit.Framework.Assert.IsTrue(fileSys.exists(p6));
			// Test 4: exit code for chgrp on existing path with globbed input is 0
			change(0, null, "admin", f7);
			change(1, null, "admin", f2, f7);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testGetWithInvalidSourcePathShouldNotDisplayNullInConsole()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FsShell shell = new org.apache.hadoop.fs.FsShell();
			shell.setConf(conf);
			java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
			System.IO.TextWriter @out = new System.IO.TextWriter(bytes);
			System.IO.TextWriter oldErr = System.Console.Error;
			Sharpen.Runtime.setErr(@out);
			string results;
			try
			{
				org.apache.hadoop.fs.Path tdir = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR, "notNullCopy"
					);
				fileSys.delete(tdir, true);
				fileSys.mkdirs(tdir);
				string[] args = new string[3];
				args[0] = "-get";
				args[1] = new org.apache.hadoop.fs.Path(tdir.toUri().getPath(), "/invalidSrc").ToString
					();
				args[2] = new org.apache.hadoop.fs.Path(tdir.toUri().getPath(), "/invalidDst").ToString
					();
				NUnit.Framework.Assert.IsTrue("file exists", !fileSys.exists(new org.apache.hadoop.fs.Path
					(args[1])));
				NUnit.Framework.Assert.IsTrue("file exists", !fileSys.exists(new org.apache.hadoop.fs.Path
					(args[2])));
				int run = shell.run(args);
				results = bytes.ToString();
				NUnit.Framework.Assert.AreEqual("Return code should be 1", 1, run);
				NUnit.Framework.Assert.IsTrue(" Null is coming when source path is invalid. ", !results
					.contains("get: null"));
				NUnit.Framework.Assert.IsTrue(" Not displaying the intended message ", results.contains
					("get: `" + args[1] + "': No such file or directory"));
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.closeStream(@out);
				Sharpen.Runtime.setErr(oldErr);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRmWithNonexistentGlob()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FsShell shell = new org.apache.hadoop.fs.FsShell();
			shell.setConf(conf);
			java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
			System.IO.TextWriter err = new System.IO.TextWriter(bytes);
			System.IO.TextWriter oldErr = System.Console.Error;
			Sharpen.Runtime.setErr(err);
			string results;
			try
			{
				int exit = shell.run(new string[] { "-rm", "nomatch*" });
				NUnit.Framework.Assert.AreEqual(1, exit);
				results = bytes.ToString();
				NUnit.Framework.Assert.IsTrue(results.contains("rm: `nomatch*': No such file or directory"
					));
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.closeStream(err);
				Sharpen.Runtime.setErr(oldErr);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRmForceWithNonexistentGlob()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FsShell shell = new org.apache.hadoop.fs.FsShell();
			shell.setConf(conf);
			java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
			System.IO.TextWriter err = new System.IO.TextWriter(bytes);
			System.IO.TextWriter oldErr = System.Console.Error;
			Sharpen.Runtime.setErr(err);
			try
			{
				int exit = shell.run(new string[] { "-rm", "-f", "nomatch*" });
				NUnit.Framework.Assert.AreEqual(0, exit);
				NUnit.Framework.Assert.IsTrue(bytes.ToString().isEmpty());
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.closeStream(err);
				Sharpen.Runtime.setErr(oldErr);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testInvalidDefaultFS()
		{
			// if default fs doesn't exist or is invalid, but the path provided in 
			// arguments is valid - fsshell should work
			org.apache.hadoop.fs.FsShell shell = new org.apache.hadoop.fs.FsShell();
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, 
				"hhhh://doesnotexist/");
			shell.setConf(conf);
			string[] args = new string[2];
			args[0] = "-ls";
			args[1] = "file:///";
			// this is valid, so command should run
			int res = shell.run(args);
			System.Console.Out.WriteLine("res =" + res);
			shell.setConf(conf);
			java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
			System.IO.TextWriter @out = new System.IO.TextWriter(bytes);
			System.IO.TextWriter oldErr = System.Console.Error;
			Sharpen.Runtime.setErr(@out);
			string results;
			try
			{
				int run = shell.run(args);
				results = bytes.ToString();
				LOG.info("result=" + results);
				NUnit.Framework.Assert.IsTrue("Return code should be 0", run == 0);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.closeStream(@out);
				Sharpen.Runtime.setErr(oldErr);
			}
		}

		/// <summary>Tests combinations of valid and invalid user and group arguments to chown.
		/// 	</summary>
		[NUnit.Framework.Test]
		public virtual void testChownUserAndGroupValidity()
		{
			// This test only covers argument parsing, so override to skip processing.
			org.apache.hadoop.fs.shell.FsCommand chown = new _Chown_391();
			chown.setConf(new org.apache.hadoop.conf.Configuration());
			// The following are valid (no exception expected).
			chown.run("user", "/path");
			chown.run("user:group", "/path");
			chown.run(":group", "/path");
			// The following are valid only on Windows.
			assertValidArgumentsOnWindows(chown, "User With Spaces", "/path");
			assertValidArgumentsOnWindows(chown, "User With Spaces:group", "/path");
			assertValidArgumentsOnWindows(chown, "User With Spaces:Group With Spaces", "/path"
				);
			assertValidArgumentsOnWindows(chown, "user:Group With Spaces", "/path");
			assertValidArgumentsOnWindows(chown, ":Group With Spaces", "/path");
			// The following are invalid (exception expected).
			assertIllegalArguments(chown, "us!er", "/path");
			assertIllegalArguments(chown, "us^er", "/path");
			assertIllegalArguments(chown, "user:gr#oup", "/path");
			assertIllegalArguments(chown, "user:gr%oup", "/path");
			assertIllegalArguments(chown, ":gr#oup", "/path");
			assertIllegalArguments(chown, ":gr%oup", "/path");
		}

		private sealed class _Chown_391 : org.apache.hadoop.fs.FsShellPermissions.Chown
		{
			public _Chown_391()
			{
			}

			protected internal override void processArgument(org.apache.hadoop.fs.shell.PathData
				 item)
			{
			}
		}

		/// <summary>Tests valid and invalid group arguments to chgrp.</summary>
		[NUnit.Framework.Test]
		public virtual void testChgrpGroupValidity()
		{
			// This test only covers argument parsing, so override to skip processing.
			org.apache.hadoop.fs.shell.FsCommand chgrp = new _Chgrp_426();
			chgrp.setConf(new org.apache.hadoop.conf.Configuration());
			// The following are valid (no exception expected).
			chgrp.run("group", "/path");
			// The following are valid only on Windows.
			assertValidArgumentsOnWindows(chgrp, "Group With Spaces", "/path");
			// The following are invalid (exception expected).
			assertIllegalArguments(chgrp, ":gr#oup", "/path");
			assertIllegalArguments(chgrp, ":gr%oup", "/path");
		}

		private sealed class _Chgrp_426 : org.apache.hadoop.fs.FsShellPermissions.Chgrp
		{
			public _Chgrp_426()
			{
			}

			protected internal override void processArgument(org.apache.hadoop.fs.shell.PathData
				 item)
			{
			}
		}

		internal class LocalFileSystemExtn : org.apache.hadoop.fs.LocalFileSystem
		{
			public LocalFileSystemExtn()
				: base(new org.apache.hadoop.fs.TestFsShellReturnCode.RawLocalFileSystemExtn())
			{
			}
		}

		internal class RawLocalFileSystemExtn : org.apache.hadoop.fs.RawLocalFileSystem
		{
			protected internal static System.Collections.Generic.Dictionary<string, string> owners
				 = new System.Collections.Generic.Dictionary<string, string>();

			protected internal static System.Collections.Generic.Dictionary<string, string> groups
				 = new System.Collections.Generic.Dictionary<string, string>();

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 p)
			{
				//owners.remove(p);
				//groups.remove(p);
				return base.create(p);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void setOwner(org.apache.hadoop.fs.Path p, string username, string
				 groupname)
			{
				string f = makeQualified(p).ToString();
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
			public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
				 p)
			{
				string f = makeQualified(p).ToString();
				org.apache.hadoop.fs.FileStatus stat = base.getFileStatus(p);
				stat.getPermission();
				if (owners.Contains(f))
				{
					stat.setOwner("STUB-" + owners[f]);
				}
				else
				{
					stat.setOwner("REAL-" + stat.getOwner());
				}
				if (groups.Contains(f))
				{
					stat.setGroup("STUB-" + groups[f]);
				}
				else
				{
					stat.setGroup("REAL-" + stat.getGroup());
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
		private static void assertIllegalArguments(org.apache.hadoop.fs.shell.FsCommand cmd
			, params string[] args)
		{
			try
			{
				cmd.run(args);
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException from args: " + java.util.Arrays
					.toString(args));
			}
			catch (System.ArgumentException)
			{
			}
		}

		/// <summary>
		/// Asserts that for the given command, the given arguments are considered valid
		/// on Windows, but invalid elsewhere.
		/// </summary>
		/// <param name="cmd">FsCommand to check</param>
		/// <param name="args">String... arguments to check</param>
		private static void assertValidArgumentsOnWindows(org.apache.hadoop.fs.shell.FsCommand
			 cmd, params string[] args)
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				cmd.run(args);
			}
			else
			{
				assertIllegalArguments(cmd, args);
			}
		}
	}
}
