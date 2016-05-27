using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Matchers;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>Test cases for helper Windows winutils.exe utility.</summary>
	public class TestWinUtils
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestWinUtils));

		private static FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"), typeof(TestWinUtils).Name);

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			// Not supported on non-Windows platforms
			Assume.AssumeTrue(Shell.Windows);
			TestDir.Mkdirs();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			FileUtil.FullyDelete(TestDir);
		}

		// Helper routine that writes the given content to the file.
		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FilePath file, string content)
		{
			byte[] data = Sharpen.Runtime.GetBytesForString(content);
			FileOutputStream os = new FileOutputStream(file);
			os.Write(data);
			os.Close();
		}

		// Helper routine that reads the first 100 bytes from the file.
		/// <exception cref="System.IO.IOException"/>
		private string ReadFile(FilePath file)
		{
			FileInputStream fos = new FileInputStream(file);
			byte[] b = new byte[100];
			fos.Read(b);
			return b.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestLs()
		{
			string content = "6bytes";
			int contentSize = content.Length;
			FilePath testFile = new FilePath(TestDir, "file1");
			WriteFile(testFile, content);
			// Verify permissions and file name return tokens
			string output = Shell.ExecCommand(Shell.Winutils, "ls", testFile.GetCanonicalPath
				());
			string[] outputArgs = output.Split("[ \r\n]");
			NUnit.Framework.Assert.IsTrue(outputArgs[0].Equals("-rwx------"));
			NUnit.Framework.Assert.IsTrue(outputArgs[outputArgs.Length - 1].Equals(testFile.GetCanonicalPath
				()));
			// Verify most tokens when using a formatted output (other tokens
			// will be verified with chmod/chown)
			output = Shell.ExecCommand(Shell.Winutils, "ls", "-F", testFile.GetCanonicalPath(
				));
			outputArgs = output.Split("[|\r\n]");
			NUnit.Framework.Assert.AreEqual(9, outputArgs.Length);
			NUnit.Framework.Assert.IsTrue(outputArgs[0].Equals("-rwx------"));
			NUnit.Framework.Assert.AreEqual(contentSize, long.Parse(outputArgs[4]));
			NUnit.Framework.Assert.IsTrue(outputArgs[8].Equals(testFile.GetCanonicalPath()));
			testFile.Delete();
			NUnit.Framework.Assert.IsFalse(testFile.Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGroups()
		{
			string currentUser = Runtime.GetProperty("user.name");
			// Verify that groups command returns information about the current user
			// groups when invoked with no args
			string outputNoArgs = Shell.ExecCommand(Shell.Winutils, "groups").Trim();
			string output = Shell.ExecCommand(Shell.Winutils, "groups", currentUser).Trim();
			NUnit.Framework.Assert.AreEqual(output, outputNoArgs);
			// Verify that groups command with the -F flag returns the same information
			string outputFormat = Shell.ExecCommand(Shell.Winutils, "groups", "-F", currentUser
				).Trim();
			outputFormat = outputFormat.Replace("|", " ");
			NUnit.Framework.Assert.AreEqual(output, outputFormat);
		}

		/// <exception cref="System.IO.IOException"/>
		private void Chmod(string mask, FilePath file)
		{
			Shell.ExecCommand(Shell.Winutils, "chmod", mask, file.GetCanonicalPath());
		}

		/// <exception cref="System.IO.IOException"/>
		private void ChmodR(string mask, FilePath file)
		{
			Shell.ExecCommand(Shell.Winutils, "chmod", "-R", mask, file.GetCanonicalPath());
		}

		/// <exception cref="System.IO.IOException"/>
		private string Ls(FilePath file)
		{
			return Shell.ExecCommand(Shell.Winutils, "ls", file.GetCanonicalPath());
		}

		/// <exception cref="System.IO.IOException"/>
		private string LsF(FilePath file)
		{
			return Shell.ExecCommand(Shell.Winutils, "ls", "-F", file.GetCanonicalPath());
		}

		/// <exception cref="System.IO.IOException"/>
		private void AssertPermissions(FilePath file, string expected)
		{
			string output = Ls(file).Split("[ \r\n]")[0];
			NUnit.Framework.Assert.AreEqual(expected, output);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestChmodInternal(string mode, string expectedPerm)
		{
			FilePath a = new FilePath(TestDir, "file1");
			NUnit.Framework.Assert.IsTrue(a.CreateNewFile());
			// Reset permissions on the file to default
			Chmod("700", a);
			// Apply the mode mask
			Chmod(mode, a);
			// Compare the output
			AssertPermissions(a, expectedPerm);
			a.Delete();
			NUnit.Framework.Assert.IsFalse(a.Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestNewFileChmodInternal(string expectedPerm)
		{
			// Create a new directory
			FilePath dir = new FilePath(TestDir, "dir1");
			NUnit.Framework.Assert.IsTrue(dir.Mkdir());
			// Set permission use chmod
			Chmod("755", dir);
			// Create a child file in the directory
			FilePath child = new FilePath(dir, "file1");
			NUnit.Framework.Assert.IsTrue(child.CreateNewFile());
			// Verify the child file has correct permissions
			AssertPermissions(child, expectedPerm);
			child.Delete();
			dir.Delete();
			NUnit.Framework.Assert.IsFalse(dir.Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestChmodInternalR(string mode, string expectedPerm, string expectedPermx
			)
		{
			// Setup test folder hierarchy
			FilePath a = new FilePath(TestDir, "a");
			NUnit.Framework.Assert.IsTrue(a.Mkdir());
			Chmod("700", a);
			FilePath aa = new FilePath(a, "a");
			NUnit.Framework.Assert.IsTrue(aa.CreateNewFile());
			Chmod("600", aa);
			FilePath ab = new FilePath(a, "b");
			NUnit.Framework.Assert.IsTrue(ab.Mkdir());
			Chmod("700", ab);
			FilePath aba = new FilePath(ab, "a");
			NUnit.Framework.Assert.IsTrue(aba.Mkdir());
			Chmod("700", aba);
			FilePath abb = new FilePath(ab, "b");
			NUnit.Framework.Assert.IsTrue(abb.CreateNewFile());
			Chmod("600", abb);
			FilePath abx = new FilePath(ab, "x");
			NUnit.Framework.Assert.IsTrue(abx.CreateNewFile());
			Chmod("u+x", abx);
			// Run chmod recursive
			ChmodR(mode, a);
			// Verify outcome
			AssertPermissions(a, "d" + expectedPermx);
			AssertPermissions(aa, "-" + expectedPerm);
			AssertPermissions(ab, "d" + expectedPermx);
			AssertPermissions(aba, "d" + expectedPermx);
			AssertPermissions(abb, "-" + expectedPerm);
			AssertPermissions(abx, "-" + expectedPermx);
			NUnit.Framework.Assert.IsTrue(FileUtil.FullyDelete(a));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestBasicChmod()
		{
			// - Create a file.
			// - Change mode to 377 so owner does not have read permission.
			// - Verify the owner truly does not have the permissions to read.
			FilePath a = new FilePath(TestDir, "a");
			a.CreateNewFile();
			Chmod("377", a);
			try
			{
				ReadFile(a);
				NUnit.Framework.Assert.IsFalse("readFile should have failed!", true);
			}
			catch (IOException)
			{
				Log.Info("Expected: Failed read from a file with permissions 377");
			}
			// restore permissions
			Chmod("700", a);
			// - Create a file.
			// - Change mode to 577 so owner does not have write permission.
			// - Verify the owner truly does not have the permissions to write.
			Chmod("577", a);
			try
			{
				WriteFile(a, "test");
				NUnit.Framework.Assert.IsFalse("writeFile should have failed!", true);
			}
			catch (IOException)
			{
				Log.Info("Expected: Failed write to a file with permissions 577");
			}
			// restore permissions
			Chmod("700", a);
			NUnit.Framework.Assert.IsTrue(a.Delete());
			// - Copy WINUTILS to a new executable file, a.exe.
			// - Change mode to 677 so owner does not have execute permission.
			// - Verify the owner truly does not have the permissions to execute the file.
			FilePath winutilsFile = new FilePath(Shell.Winutils);
			FilePath aExe = new FilePath(TestDir, "a.exe");
			FileUtils.CopyFile(winutilsFile, aExe);
			Chmod("677", aExe);
			try
			{
				Shell.ExecCommand(aExe.GetCanonicalPath(), "ls");
				NUnit.Framework.Assert.IsFalse("executing " + aExe + " should have failed!", true
					);
			}
			catch (IOException)
			{
				Log.Info("Expected: Failed to execute a file with permissions 677");
			}
			NUnit.Framework.Assert.IsTrue(aExe.Delete());
		}

		/// <summary>Validate behavior of chmod commands on directories on Windows.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestBasicChmodOnDir()
		{
			// Validate that listing a directory with no read permission fails
			FilePath a = new FilePath(TestDir, "a");
			FilePath b = new FilePath(a, "b");
			a.Mkdirs();
			NUnit.Framework.Assert.IsTrue(b.CreateNewFile());
			// Remove read permissions on directory a
			Chmod("300", a);
			string[] files = a.List();
			NUnit.Framework.Assert.IsTrue("Listing a directory without read permission should fail"
				, null == files);
			// restore permissions
			Chmod("700", a);
			// validate that the directory can be listed now
			files = a.List();
			NUnit.Framework.Assert.AreEqual("b", files[0]);
			// Remove write permissions on the directory and validate the
			// behavior for adding, deleting and renaming files
			Chmod("500", a);
			FilePath c = new FilePath(a, "c");
			try
			{
				// Adding a new file will fail as expected because the
				// FILE_WRITE_DATA/FILE_ADD_FILE privilege is denied on
				// the dir.
				c.CreateNewFile();
				NUnit.Framework.Assert.IsFalse("writeFile should have failed!", true);
			}
			catch (IOException)
			{
				Log.Info("Expected: Failed to create a file when directory " + "permissions are 577"
					);
			}
			// Deleting a file will succeed even if write permissions are not present
			// on the parent dir. Check the following link for additional details:
			// http://support.microsoft.com/kb/238018
			NUnit.Framework.Assert.IsTrue("Special behavior: deleting a file will succeed on Windows "
				 + "even if a user does not have write permissions on the parent dir", b.Delete(
				));
			NUnit.Framework.Assert.IsFalse("Renaming a file should fail on the dir where a user does "
				 + "not have write permissions", b.RenameTo(new FilePath(a, "d")));
			// restore permissions
			Chmod("700", a);
			// Make sure adding new files and rename succeeds now
			NUnit.Framework.Assert.IsTrue(c.CreateNewFile());
			FilePath d = new FilePath(a, "d");
			NUnit.Framework.Assert.IsTrue(c.RenameTo(d));
			// at this point in the test, d is the only remaining file in directory a
			// Removing execute permissions does not have the same behavior on
			// Windows as on Linux. Adding, renaming, deleting and listing files
			// will still succeed. Windows default behavior is to bypass directory
			// traverse checking (BYPASS_TRAVERSE_CHECKING privilege) for all users.
			// See the following link for additional details:
			// http://msdn.microsoft.com/en-us/library/windows/desktop/aa364399(v=vs.85).aspx
			Chmod("600", a);
			// validate directory listing
			files = a.List();
			NUnit.Framework.Assert.AreEqual("d", files[0]);
			// validate delete
			NUnit.Framework.Assert.IsTrue(d.Delete());
			// validate add
			FilePath e = new FilePath(a, "e");
			NUnit.Framework.Assert.IsTrue(e.CreateNewFile());
			// validate rename
			NUnit.Framework.Assert.IsTrue(e.RenameTo(new FilePath(a, "f")));
			// restore permissions
			Chmod("700", a);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestChmod()
		{
			TestChmodInternal("7", "-------rwx");
			TestChmodInternal("70", "----rwx---");
			TestChmodInternal("u-x,g+r,o=g", "-rw-r--r--");
			TestChmodInternal("u-x,g+rw", "-rw-rw----");
			TestChmodInternal("u-x,g+rwx-x,o=u", "-rw-rw-rw-");
			TestChmodInternal("+", "-rwx------");
			// Recursive chmod tests
			TestChmodInternalR("755", "rwxr-xr-x", "rwxr-xr-x");
			TestChmodInternalR("u-x,g+r,o=g", "rw-r--r--", "rw-r--r--");
			TestChmodInternalR("u-x,g+rw", "rw-rw----", "rw-rw----");
			TestChmodInternalR("u-x,g+rwx-x,o=u", "rw-rw-rw-", "rw-rw-rw-");
			TestChmodInternalR("a+rX", "rw-r--r--", "rwxr-xr-x");
			// Test a new file created in a chmod'ed directory has expected permission
			TestNewFileChmodInternal("-rwxr-xr-x");
		}

		/// <exception cref="System.IO.IOException"/>
		private void Chown(string userGroup, FilePath file)
		{
			Shell.ExecCommand(Shell.Winutils, "chown", userGroup, file.GetCanonicalPath());
		}

		/// <exception cref="System.IO.IOException"/>
		private void AssertOwners(FilePath file, string expectedUser, string expectedGroup
			)
		{
			string[] args = LsF(file).Trim().Split("[\\|]");
			NUnit.Framework.Assert.AreEqual(StringUtils.ToLowerCase(expectedUser), StringUtils
				.ToLowerCase(args[2]));
			NUnit.Framework.Assert.AreEqual(StringUtils.ToLowerCase(expectedGroup), StringUtils
				.ToLowerCase(args[3]));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestChown()
		{
			FilePath a = new FilePath(TestDir, "a");
			NUnit.Framework.Assert.IsTrue(a.CreateNewFile());
			string username = Runtime.GetProperty("user.name");
			// username including the domain aka DOMAIN\\user
			string qualifiedUsername = Shell.ExecCommand("whoami").Trim();
			string admins = "Administrators";
			string qualifiedAdmins = "BUILTIN\\Administrators";
			Chown(username + ":" + admins, a);
			AssertOwners(a, qualifiedUsername, qualifiedAdmins);
			Chown(username, a);
			Chown(":" + admins, a);
			AssertOwners(a, qualifiedUsername, qualifiedAdmins);
			Chown(":" + admins, a);
			Chown(username + ":", a);
			AssertOwners(a, qualifiedUsername, qualifiedAdmins);
			NUnit.Framework.Assert.IsTrue(a.Delete());
			NUnit.Framework.Assert.IsFalse(a.Exists());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSymlinkRejectsForwardSlashesInLink()
		{
			FilePath newFile = new FilePath(TestDir, "file");
			NUnit.Framework.Assert.IsTrue(newFile.CreateNewFile());
			string target = newFile.GetPath();
			string link = new FilePath(TestDir, "link").GetPath().ReplaceAll("\\\\", "/");
			try
			{
				Shell.ExecCommand(Shell.Winutils, "symlink", link, target);
				NUnit.Framework.Assert.Fail(string.Format("did not receive expected failure creating symlink "
					 + "with forward slashes in link: link = %s, target = %s", link, target));
			}
			catch (IOException)
			{
				Log.Info("Expected: Failed to create symlink with forward slashes in target");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSymlinkRejectsForwardSlashesInTarget()
		{
			FilePath newFile = new FilePath(TestDir, "file");
			NUnit.Framework.Assert.IsTrue(newFile.CreateNewFile());
			string target = newFile.GetPath().ReplaceAll("\\\\", "/");
			string link = new FilePath(TestDir, "link").GetPath();
			try
			{
				Shell.ExecCommand(Shell.Winutils, "symlink", link, target);
				NUnit.Framework.Assert.Fail(string.Format("did not receive expected failure creating symlink "
					 + "with forward slashes in target: link = %s, target = %s", link, target));
			}
			catch (IOException)
			{
				Log.Info("Expected: Failed to create symlink with forward slashes in target");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReadLink()
		{
			// Create TEST_DIR\dir1\file1.txt
			//
			FilePath dir1 = new FilePath(TestDir, "dir1");
			NUnit.Framework.Assert.IsTrue(dir1.Mkdirs());
			FilePath file1 = new FilePath(dir1, "file1.txt");
			NUnit.Framework.Assert.IsTrue(file1.CreateNewFile());
			FilePath dirLink = new FilePath(TestDir, "dlink");
			FilePath fileLink = new FilePath(TestDir, "flink");
			// Next create a directory symlink to dir1 and a file
			// symlink to file1.txt.
			//
			Shell.ExecCommand(Shell.Winutils, "symlink", dirLink.ToString(), dir1.ToString());
			Shell.ExecCommand(Shell.Winutils, "symlink", fileLink.ToString(), file1.ToString(
				));
			// Read back the two links and ensure we get what we expected.
			//
			string readLinkOutput = Shell.ExecCommand(Shell.Winutils, "readlink", dirLink.ToString
				());
			Assert.AssertThat(readLinkOutput, CoreMatchers.EqualTo(dir1.ToString()));
			readLinkOutput = Shell.ExecCommand(Shell.Winutils, "readlink", fileLink.ToString(
				));
			Assert.AssertThat(readLinkOutput, CoreMatchers.EqualTo(file1.ToString()));
			// Try a few invalid inputs and verify we get an ExitCodeException for each.
			//
			try
			{
				// No link name specified.
				//
				Shell.ExecCommand(Shell.Winutils, "readlink", string.Empty);
				NUnit.Framework.Assert.Fail("Failed to get Shell.ExitCodeException when reading bad symlink"
					);
			}
			catch (Shell.ExitCodeException ece)
			{
				Assert.AssertThat(ece.GetExitCode(), CoreMatchers.Is(1));
			}
			try
			{
				// Bad link name.
				//
				Shell.ExecCommand(Shell.Winutils, "readlink", "ThereIsNoSuchLink");
				NUnit.Framework.Assert.Fail("Failed to get Shell.ExitCodeException when reading bad symlink"
					);
			}
			catch (Shell.ExitCodeException ece)
			{
				Assert.AssertThat(ece.GetExitCode(), CoreMatchers.Is(1));
			}
			try
			{
				// Non-symlink directory target.
				//
				Shell.ExecCommand(Shell.Winutils, "readlink", dir1.ToString());
				NUnit.Framework.Assert.Fail("Failed to get Shell.ExitCodeException when reading bad symlink"
					);
			}
			catch (Shell.ExitCodeException ece)
			{
				Assert.AssertThat(ece.GetExitCode(), CoreMatchers.Is(1));
			}
			try
			{
				// Non-symlink file target.
				//
				Shell.ExecCommand(Shell.Winutils, "readlink", file1.ToString());
				NUnit.Framework.Assert.Fail("Failed to get Shell.ExitCodeException when reading bad symlink"
					);
			}
			catch (Shell.ExitCodeException ece)
			{
				Assert.AssertThat(ece.GetExitCode(), CoreMatchers.Is(1));
			}
			try
			{
				// Too many parameters.
				//
				Shell.ExecCommand(Shell.Winutils, "readlink", "a", "b");
				NUnit.Framework.Assert.Fail("Failed to get Shell.ExitCodeException with bad parameters"
					);
			}
			catch (Shell.ExitCodeException ece)
			{
				Assert.AssertThat(ece.GetExitCode(), CoreMatchers.Is(1));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTaskCreate()
		{
			FilePath batch = new FilePath(TestDir, "testTaskCreate.cmd");
			FilePath proof = new FilePath(TestDir, "testTaskCreate.out");
			FileWriter fw = new FileWriter(batch);
			string testNumber = string.Format("%f", Math.Random());
			fw.Write(string.Format("echo %s > \"%s\"", testNumber, proof.GetAbsolutePath()));
			fw.Close();
			NUnit.Framework.Assert.IsFalse(proof.Exists());
			Shell.ExecCommand(Shell.Winutils, "task", "create", "testTaskCreate" + testNumber
				, batch.GetAbsolutePath());
			NUnit.Framework.Assert.IsTrue(proof.Exists());
			string outNumber = FileUtils.ReadFileToString(proof);
			Assert.AssertThat(outNumber, JUnitMatchers.ContainsString(testNumber));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTaskCreateWithLimits()
		{
			// Generate a unique job id
			string jobId = string.Format("%f", Math.Random());
			// Run a task without any options
			string @out = Shell.ExecCommand(Shell.Winutils, "task", "create", "job" + jobId, 
				"cmd /c echo job" + jobId);
			NUnit.Framework.Assert.IsTrue(@out.Trim().Equals("job" + jobId));
			// Run a task without any limits
			jobId = string.Format("%f", Math.Random());
			@out = Shell.ExecCommand(Shell.Winutils, "task", "create", "-c", "-1", "-m", "-1"
				, "job" + jobId, "cmd /c echo job" + jobId);
			NUnit.Framework.Assert.IsTrue(@out.Trim().Equals("job" + jobId));
			// Run a task with limits (128MB should be enough for a cmd)
			jobId = string.Format("%f", Math.Random());
			@out = Shell.ExecCommand(Shell.Winutils, "task", "create", "-c", "10000", "-m", "128"
				, "job" + jobId, "cmd /c echo job" + jobId);
			NUnit.Framework.Assert.IsTrue(@out.Trim().Equals("job" + jobId));
			// Run a task without enough memory
			try
			{
				jobId = string.Format("%f", Math.Random());
				@out = Shell.ExecCommand(Shell.Winutils, "task", "create", "-m", "128", "job" + jobId
					, "java -Xmx256m -version");
				NUnit.Framework.Assert.Fail("Failed to get Shell.ExitCodeException with insufficient memory"
					);
			}
			catch (Shell.ExitCodeException ece)
			{
				Assert.AssertThat(ece.GetExitCode(), CoreMatchers.Is(1));
			}
			// Run tasks with wrong parameters
			//
			try
			{
				jobId = string.Format("%f", Math.Random());
				Shell.ExecCommand(Shell.Winutils, "task", "create", "-c", "-1", "-m", "-1", "foo"
					, "job" + jobId, "cmd /c echo job" + jobId);
				NUnit.Framework.Assert.Fail("Failed to get Shell.ExitCodeException with bad parameters"
					);
			}
			catch (Shell.ExitCodeException ece)
			{
				Assert.AssertThat(ece.GetExitCode(), CoreMatchers.Is(1639));
			}
			try
			{
				jobId = string.Format("%f", Math.Random());
				Shell.ExecCommand(Shell.Winutils, "task", "create", "-c", "-m", "-1", "job" + jobId
					, "cmd /c echo job" + jobId);
				NUnit.Framework.Assert.Fail("Failed to get Shell.ExitCodeException with bad parameters"
					);
			}
			catch (Shell.ExitCodeException ece)
			{
				Assert.AssertThat(ece.GetExitCode(), CoreMatchers.Is(1639));
			}
			try
			{
				jobId = string.Format("%f", Math.Random());
				Shell.ExecCommand(Shell.Winutils, "task", "create", "-c", "foo", "job" + jobId, "cmd /c echo job"
					 + jobId);
				NUnit.Framework.Assert.Fail("Failed to get Shell.ExitCodeException with bad parameters"
					);
			}
			catch (Shell.ExitCodeException ece)
			{
				Assert.AssertThat(ece.GetExitCode(), CoreMatchers.Is(1639));
			}
		}
	}
}
