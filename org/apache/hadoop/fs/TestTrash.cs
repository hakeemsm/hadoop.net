using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>This class tests commands from Trash.</summary>
	public class TestTrash : NUnit.Framework.TestCase
	{
		private static readonly org.apache.hadoop.fs.Path TEST_DIR = new org.apache.hadoop.fs.Path
			(new java.io.File(Sharpen.Runtime.getProperty("test.build.data", "/tmp")).toURI(
			).ToString().Replace(' ', '+'), "testTrash");

		/// <exception cref="System.IO.IOException"/>
		protected internal static org.apache.hadoop.fs.Path mkdir(org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.fs.Path p)
		{
			NUnit.Framework.Assert.IsTrue(fs.mkdirs(p));
			NUnit.Framework.Assert.IsTrue(fs.exists(p));
			NUnit.Framework.Assert.IsTrue(fs.getFileStatus(p).isDirectory());
			return p;
		}

		// check that the specified file is in Trash
		/// <exception cref="System.IO.IOException"/>
		protected internal static void checkTrash(org.apache.hadoop.fs.FileSystem trashFs
			, org.apache.hadoop.fs.Path trashRoot, org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.fs.Path p = org.apache.hadoop.fs.Path.mergePaths(trashRoot, path
				);
			NUnit.Framework.Assert.IsTrue("Could not find file in trash: " + p, trashFs.exists
				(p));
		}

		// counts how many instances of the file are in the Trash
		// they all are in format fileName*
		/// <exception cref="System.IO.IOException"/>
		protected internal static int countSameDeletedFiles(org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.fs.Path trashDir, org.apache.hadoop.fs.Path fileName)
		{
			string prefix = fileName.getName();
			System.Console.Out.WriteLine("Counting " + fileName + " in " + trashDir.ToString(
				));
			// filter that matches all the files that start with fileName*
			org.apache.hadoop.fs.PathFilter pf = new _PathFilter_71(prefix);
			// run the filter
			org.apache.hadoop.fs.FileStatus[] fss = fs.listStatus(trashDir, pf);
			return fss == null ? 0 : fss.Length;
		}

		private sealed class _PathFilter_71 : org.apache.hadoop.fs.PathFilter
		{
			public _PathFilter_71(string prefix)
			{
				this.prefix = prefix;
			}

			public bool accept(org.apache.hadoop.fs.Path file)
			{
				return file.getName().StartsWith(prefix);
			}

			private readonly string prefix;
		}

		// check that the specified file is not in Trash
		/// <exception cref="System.IO.IOException"/>
		internal static void checkNotInTrash(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 trashRoot, string pathname)
		{
			org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(trashRoot + "/" + new 
				org.apache.hadoop.fs.Path(pathname).getName());
			NUnit.Framework.Assert.IsTrue(!fs.exists(p));
		}

		/// <summary>Test trash for the shell's delete command for the file system fs</summary>
		/// <param name="fs"/>
		/// <param name="base">- the base path where files are created</param>
		/// <exception cref="System.IO.IOException"/>
		public static void trashShell(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 @base)
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("fs.defaultFS", fs.getUri().ToString());
			trashShell(conf, @base, null, null);
		}

		/// <summary>
		/// Test trash for the shell's delete command for the default file system
		/// specified in the paramter conf
		/// </summary>
		/// <param name="conf"></param>
		/// <param name="base">- the base path where files are created</param>
		/// <param name="trashRoot">- the expected place where the trashbin resides</param>
		/// <exception cref="System.IO.IOException"/>
		public static void trashShell(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.Path
			 @base, org.apache.hadoop.fs.FileSystem trashRootFs, org.apache.hadoop.fs.Path trashRoot
			)
		{
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
			conf.setLong(FS_TRASH_INTERVAL_KEY, 0);
			// disabled
			NUnit.Framework.Assert.IsFalse(new org.apache.hadoop.fs.Trash(conf).isEnabled());
			conf.setLong(FS_TRASH_INTERVAL_KEY, 10);
			// 10 minute
			NUnit.Framework.Assert.IsTrue(new org.apache.hadoop.fs.Trash(conf).isEnabled());
			org.apache.hadoop.fs.FsShell shell = new org.apache.hadoop.fs.FsShell();
			shell.setConf(conf);
			if (trashRoot == null)
			{
				trashRoot = shell.getCurrentTrashDir();
			}
			if (trashRootFs == null)
			{
				trashRootFs = fs;
			}
			// First create a new directory with mkdirs
			org.apache.hadoop.fs.Path myPath = new org.apache.hadoop.fs.Path(@base, "test/mkdirs"
				);
			mkdir(fs, myPath);
			// Second, create a file in that directory.
			org.apache.hadoop.fs.Path myFile = new org.apache.hadoop.fs.Path(@base, "test/mkdirs/myFile"
				);
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fs, myFile, 10);
			{
				// Verify that expunge without Trash directory
				// won't throw Exception
				string[] args = new string[1];
				args[0] = "-expunge";
				int val = -1;
				try
				{
					val = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsTrue(val == 0);
			}
			{
				// Verify that we succeed in removing the file we created.
				// This should go into Trash.
				string[] args = new string[2];
				args[0] = "-rm";
				args[1] = myFile.ToString();
				int val = -1;
				try
				{
					val = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsTrue(val == 0);
				checkTrash(trashRootFs, trashRoot, fs.makeQualified(myFile));
			}
			// Verify that we can recreate the file
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fs, myFile, 10);
			{
				// Verify that we succeed in removing the file we re-created
				string[] args = new string[2];
				args[0] = "-rm";
				args[1] = new org.apache.hadoop.fs.Path(@base, "test/mkdirs/myFile").ToString();
				int val = -1;
				try
				{
					val = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsTrue(val == 0);
			}
			// Verify that we can recreate the file
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fs, myFile, 10);
			{
				// Verify that we succeed in removing the whole directory
				// along with the file inside it.
				string[] args = new string[2];
				args[0] = "-rmr";
				args[1] = new org.apache.hadoop.fs.Path(@base, "test/mkdirs").ToString();
				int val = -1;
				try
				{
					val = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsTrue(val == 0);
			}
			// recreate directory
			mkdir(fs, myPath);
			{
				// Verify that we succeed in removing the whole directory
				string[] args = new string[2];
				args[0] = "-rmr";
				args[1] = new org.apache.hadoop.fs.Path(@base, "test/mkdirs").ToString();
				int val = -1;
				try
				{
					val = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsTrue(val == 0);
			}
			{
				// Check that we can delete a file from the trash
				org.apache.hadoop.fs.Path toErase = new org.apache.hadoop.fs.Path(trashRoot, "toErase"
					);
				int retVal = -1;
				org.apache.hadoop.fs.FileSystemTestHelper.writeFile(trashRootFs, toErase, 10);
				try
				{
					retVal = shell.run(new string[] { "-rm", toErase.ToString() });
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsTrue(retVal == 0);
				checkNotInTrash(trashRootFs, trashRoot, toErase.ToString());
				checkNotInTrash(trashRootFs, trashRoot, toErase.ToString() + ".1");
			}
			{
				// simulate Trash removal
				string[] args = new string[1];
				args[0] = "-expunge";
				int val = -1;
				try
				{
					val = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsTrue(val == 0);
			}
			// verify that after expunging the Trash, it really goes away
			checkNotInTrash(trashRootFs, trashRoot, new org.apache.hadoop.fs.Path(@base, "test/mkdirs/myFile"
				).ToString());
			// recreate directory and file
			mkdir(fs, myPath);
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fs, myFile, 10);
			{
				// remove file first, then remove directory
				string[] args = new string[2];
				args[0] = "-rm";
				args[1] = myFile.ToString();
				int val = -1;
				try
				{
					val = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsTrue(val == 0);
				checkTrash(trashRootFs, trashRoot, myFile);
				args = new string[2];
				args[0] = "-rmr";
				args[1] = myPath.ToString();
				val = -1;
				try
				{
					val = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsTrue(val == 0);
				checkTrash(trashRootFs, trashRoot, myPath);
			}
			{
				// attempt to remove parent of trash
				string[] args = new string[2];
				args[0] = "-rmr";
				args[1] = trashRoot.getParent().getParent().ToString();
				int val = -1;
				try
				{
					val = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.AreEqual("exit code", 1, val);
				NUnit.Framework.Assert.IsTrue(trashRootFs.exists(trashRoot));
			}
			// Verify skip trash option really works
			// recreate directory and file
			mkdir(fs, myPath);
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fs, myFile, 10);
			{
				// Verify that skip trash option really skips the trash for files (rm)
				string[] args = new string[3];
				args[0] = "-rm";
				args[1] = "-skipTrash";
				args[2] = myFile.ToString();
				int val = -1;
				try
				{
					// Clear out trash
					NUnit.Framework.Assert.AreEqual("-expunge failed", 0, shell.run(new string[] { "-expunge"
						 }));
					val = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsFalse("Expected TrashRoot (" + trashRoot + ") to exist in file system:"
					 + trashRootFs.getUri(), trashRootFs.exists(trashRoot));
				// No new Current should be created
				NUnit.Framework.Assert.IsFalse(fs.exists(myFile));
				NUnit.Framework.Assert.IsTrue(val == 0);
			}
			// recreate directory and file
			mkdir(fs, myPath);
			org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fs, myFile, 10);
			{
				// Verify that skip trash option really skips the trash for rmr
				string[] args = new string[3];
				args[0] = "-rmr";
				args[1] = "-skipTrash";
				args[2] = myPath.ToString();
				int val = -1;
				try
				{
					// Clear out trash
					NUnit.Framework.Assert.AreEqual(0, shell.run(new string[] { "-expunge" }));
					val = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsFalse(trashRootFs.exists(trashRoot));
				// No new Current should be created
				NUnit.Framework.Assert.IsFalse(fs.exists(myPath));
				NUnit.Framework.Assert.IsFalse(fs.exists(myFile));
				NUnit.Framework.Assert.IsTrue(val == 0);
			}
			{
				// deleting same file multiple times
				int val = -1;
				mkdir(fs, myPath);
				try
				{
					NUnit.Framework.Assert.AreEqual(0, shell.run(new string[] { "-expunge" }));
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from fs expunge " + e.getLocalizedMessage
						());
				}
				// create a file in that directory.
				myFile = new org.apache.hadoop.fs.Path(@base, "test/mkdirs/myFile");
				string[] args = new string[] { "-rm", myFile.ToString() };
				int num_runs = 10;
				for (int i = 0; i < num_runs; i++)
				{
					//create file
					org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fs, myFile, 10);
					// delete file
					try
					{
						val = shell.run(args);
					}
					catch (System.Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
							());
					}
					NUnit.Framework.Assert.IsTrue(val == 0);
				}
				// current trash directory
				org.apache.hadoop.fs.Path trashDir = org.apache.hadoop.fs.Path.mergePaths(new org.apache.hadoop.fs.Path
					(trashRoot.toUri().getPath()), new org.apache.hadoop.fs.Path(myFile.getParent().
					toUri().getPath()));
				System.Console.Out.WriteLine("Deleting same myFile: myFile.parent=" + myFile.getParent
					().toUri().getPath() + "; trashroot=" + trashRoot.toUri().getPath() + "; trashDir="
					 + trashDir.toUri().getPath());
				int count = countSameDeletedFiles(fs, trashDir, myFile);
				System.Console.Out.WriteLine("counted " + count + " files " + myFile.getName() + 
					"* in " + trashDir);
				NUnit.Framework.Assert.IsTrue(count == num_runs);
			}
			{
				//Verify skipTrash option is suggested when rm fails due to its absence
				string[] args = new string[2];
				args[0] = "-rmr";
				args[1] = "/";
				//This always contains trash directory
				System.IO.TextWriter stdout = System.Console.Out;
				System.IO.TextWriter stderr = System.Console.Error;
				java.io.ByteArrayOutputStream byteStream = new java.io.ByteArrayOutputStream();
				System.IO.TextWriter newOut = new System.IO.TextWriter(byteStream);
				Sharpen.Runtime.setOut(newOut);
				Sharpen.Runtime.setErr(newOut);
				try
				{
					shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				string output = byteStream.ToString();
				Sharpen.Runtime.setOut(stdout);
				Sharpen.Runtime.setErr(stderr);
				NUnit.Framework.Assert.IsTrue("skipTrash wasn't suggested as remedy to failed rm command"
					 + " or we deleted / even though we could not get server defaults", output.IndexOf
					("Consider using -skipTrash option") != -1 || output.IndexOf("Failed to determine server trash configuration"
					) != -1);
			}
			{
				// Verify old checkpoint format is recognized
				// emulate two old trash checkpoint directories, one that is old enough
				// to be deleted on the next expunge and one that isn't.
				long trashInterval = conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT
					);
				long now = org.apache.hadoop.util.Time.now();
				java.text.DateFormat oldCheckpointFormat = new java.text.SimpleDateFormat("yyMMddHHmm"
					);
				org.apache.hadoop.fs.Path dirToDelete = new org.apache.hadoop.fs.Path(trashRoot.getParent
					(), oldCheckpointFormat.format(now - (trashInterval * 60 * 1000) - 1));
				org.apache.hadoop.fs.Path dirToKeep = new org.apache.hadoop.fs.Path(trashRoot.getParent
					(), oldCheckpointFormat.format(now));
				mkdir(trashRootFs, dirToDelete);
				mkdir(trashRootFs, dirToKeep);
				// Clear out trash
				int rc = -1;
				try
				{
					rc = shell.run(new string[] { "-expunge" });
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from fs expunge " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.AreEqual(0, rc);
				NUnit.Framework.Assert.IsFalse("old checkpoint format not recognized", trashRootFs
					.exists(dirToDelete));
				NUnit.Framework.Assert.IsTrue("old checkpoint format directory should not be removed"
					, trashRootFs.exists(dirToKeep));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void trashNonDefaultFS(org.apache.hadoop.conf.Configuration conf)
		{
			conf.setLong(FS_TRASH_INTERVAL_KEY, 10);
			{
				// 10 minute
				// attempt non-default FileSystem trash
				org.apache.hadoop.fs.FileSystem lfs = org.apache.hadoop.fs.FileSystem.getLocal(conf
					);
				org.apache.hadoop.fs.Path p = TEST_DIR;
				org.apache.hadoop.fs.Path f = new org.apache.hadoop.fs.Path(p, "foo/bar");
				if (lfs.exists(p))
				{
					lfs.delete(p, true);
				}
				try
				{
					org.apache.hadoop.fs.FileSystemTestHelper.writeFile(lfs, f, 10);
					org.apache.hadoop.fs.FileSystem.closeAll();
					org.apache.hadoop.fs.FileSystem localFs = org.apache.hadoop.fs.FileSystem.get(java.net.URI
						.create("file:///"), conf);
					org.apache.hadoop.fs.Trash lTrash = new org.apache.hadoop.fs.Trash(localFs, conf);
					lTrash.moveToTrash(f.getParent());
					checkTrash(localFs, lTrash.getCurrentTrashDir(), f);
				}
				finally
				{
					if (lfs.exists(p))
					{
						lfs.delete(p, true);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testTrash()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setClass("fs.file.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestTrash.TestLFS
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			trashShell(org.apache.hadoop.fs.FileSystem.getLocal(conf), TEST_DIR);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testNonDefaultFS()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setClass("fs.file.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestTrash.TestLFS
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			conf.set("fs.defaultFS", "invalid://host/bar/foo");
			trashNonDefaultFS(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testPluggableTrash()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			// Test plugged TrashPolicy
			conf.setClass("fs.trash.classname", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestTrash.TestTrashPolicy
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TrashPolicy)));
			org.apache.hadoop.fs.Trash trash = new org.apache.hadoop.fs.Trash(conf);
			NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.getClassForObject(trash.getTrashPolicy
				()).Equals(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestTrash.TestTrashPolicy
				))));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testTrashEmptier()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			// Trash with 12 second deletes and 6 seconds checkpoints
			conf.set(FS_TRASH_INTERVAL_KEY, "0.2");
			// 12 seconds
			conf.setClass("fs.file.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestTrash.TestLFS
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			conf.set(FS_TRASH_CHECKPOINT_INTERVAL_KEY, "0.1");
			// 6 seconds
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			conf.set("fs.default.name", fs.getUri().ToString());
			org.apache.hadoop.fs.Trash trash = new org.apache.hadoop.fs.Trash(conf);
			// Start Emptier in background
			java.lang.Runnable emptier = trash.getEmptier();
			java.lang.Thread emptierThread = new java.lang.Thread(emptier);
			emptierThread.start();
			org.apache.hadoop.fs.FsShell shell = new org.apache.hadoop.fs.FsShell();
			shell.setConf(conf);
			shell.init();
			// First create a new directory with mkdirs
			org.apache.hadoop.fs.Path myPath = new org.apache.hadoop.fs.Path(TEST_DIR, "test/mkdirs"
				);
			mkdir(fs, myPath);
			int fileIndex = 0;
			System.Collections.Generic.ICollection<string> checkpoints = new java.util.HashSet
				<string>();
			while (true)
			{
				// Create a file with a new name
				org.apache.hadoop.fs.Path myFile = new org.apache.hadoop.fs.Path(TEST_DIR, "test/mkdirs/myFile"
					 + fileIndex++);
				org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fs, myFile, 10);
				// Delete the file to trash
				string[] args = new string[2];
				args[0] = "-rm";
				args[1] = myFile.ToString();
				int val = -1;
				try
				{
					val = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsTrue(val == 0);
				org.apache.hadoop.fs.Path trashDir = shell.getCurrentTrashDir();
				org.apache.hadoop.fs.FileStatus[] files = fs.listStatus(trashDir.getParent());
				// Scan files in .Trash and add them to set of checkpoints
				foreach (org.apache.hadoop.fs.FileStatus file in files)
				{
					string fileName = file.getPath().getName();
					checkpoints.add(fileName);
				}
				// If checkpoints has 4 objects it is Current + 3 checkpoint directories
				if (checkpoints.Count == 4)
				{
					// The actual contents should be smaller since the last checkpoint
					// should've been deleted and Current might not have been recreated yet
					NUnit.Framework.Assert.IsTrue(checkpoints.Count > files.Length);
					break;
				}
				java.lang.Thread.sleep(5000);
			}
			emptierThread.interrupt();
			emptierThread.join();
		}

		/// <seealso cref="NUnit.Framework.TestCase.tearDown()"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void tearDown()
		{
			java.io.File trashDir = new java.io.File(TEST_DIR.toUri().getPath());
			if (trashDir.exists() && !org.apache.hadoop.fs.FileUtil.fullyDelete(trashDir))
			{
				throw new System.IO.IOException("Cannot remove data directory: " + trashDir);
			}
		}

		internal class TestLFS : org.apache.hadoop.fs.LocalFileSystem
		{
			internal org.apache.hadoop.fs.Path home;

			internal TestLFS()
				: this(new org.apache.hadoop.fs.Path(TEST_DIR, "user/test"))
			{
			}

			internal TestLFS(org.apache.hadoop.fs.Path home)
				: base()
			{
				this.home = home;
			}

			public override org.apache.hadoop.fs.Path getHomeDirectory()
			{
				return home;
			}
		}

		/// <summary>
		/// test same file deletion - multiple time
		/// this is more of a performance test - shouldn't be run as a unit test
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static void performanceTestDeleteSameFile()
		{
			org.apache.hadoop.fs.Path @base = TEST_DIR;
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setClass("fs.file.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestTrash.TestLFS
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			conf.set("fs.defaultFS", fs.getUri().ToString());
			conf.setLong(FS_TRASH_INTERVAL_KEY, 10);
			//minutes..
			org.apache.hadoop.fs.FsShell shell = new org.apache.hadoop.fs.FsShell();
			shell.setConf(conf);
			//Path trashRoot = null;
			org.apache.hadoop.fs.Path myPath = new org.apache.hadoop.fs.Path(@base, "test/mkdirs"
				);
			mkdir(fs, myPath);
			// create a file in that directory.
			org.apache.hadoop.fs.Path myFile;
			long start;
			long first = 0;
			int retVal = 0;
			int factor = 10;
			// how much slower any of subsequent deletion can be
			myFile = new org.apache.hadoop.fs.Path(@base, "test/mkdirs/myFile");
			string[] args = new string[] { "-rm", myFile.ToString() };
			int iters = 1000;
			for (int i = 0; i < iters; i++)
			{
				org.apache.hadoop.fs.FileSystemTestHelper.writeFile(fs, myFile, 10);
				start = org.apache.hadoop.util.Time.now();
				try
				{
					retVal = shell.run(args);
				}
				catch (System.Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.getLocalizedMessage
						());
					throw new System.IO.IOException(e.Message);
				}
				NUnit.Framework.Assert.IsTrue(retVal == 0);
				long iterTime = org.apache.hadoop.util.Time.now() - start;
				// take median of the first 10 runs
				if (i < 10)
				{
					if (i == 0)
					{
						first = iterTime;
					}
					else
					{
						first = (first + iterTime) / 2;
					}
				}
				// we don't want to print every iteration - let's do every 10th
				int print_freq = iters / 10;
				if (i > 10)
				{
					if ((i % print_freq) == 0)
					{
						System.Console.Out.WriteLine("iteration=" + i + ";res =" + retVal + "; start=" + 
							start + "; iterTime = " + iterTime + " vs. firstTime=" + first);
					}
					long factoredTime = first * factor;
					NUnit.Framework.Assert.IsTrue(iterTime < factoredTime);
				}
			}
		}

		//no more then twice of median first 10
		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] arg)
		{
			// run performance piece as a separate test
			performanceTestDeleteSameFile();
		}

		public class TestTrashPolicy : org.apache.hadoop.fs.TrashPolicy
		{
			public TestTrashPolicy()
			{
			}

			// Test TrashPolicy. Don't care about implementation.
			public override void initialize(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, org.apache.hadoop.fs.Path home)
			{
			}

			public override bool isEnabled()
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool moveToTrash(org.apache.hadoop.fs.Path path)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void createCheckpoint()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void deleteCheckpoint()
			{
			}

			public override org.apache.hadoop.fs.Path getCurrentTrashDir()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override java.lang.Runnable getEmptier()
			{
				return null;
			}
		}
	}
}
