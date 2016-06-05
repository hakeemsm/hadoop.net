using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>This class tests commands from Trash.</summary>
	public class TestTrash : TestCase
	{
		private static readonly Path TestDir = new Path(new FilePath(Runtime.GetProperty(
			"test.build.data", "/tmp")).ToURI().ToString().Replace(' ', '+'), "testTrash");

		/// <exception cref="System.IO.IOException"/>
		protected internal static Path Mkdir(FileSystem fs, Path p)
		{
			Assert.True(fs.Mkdirs(p));
			Assert.True(fs.Exists(p));
			Assert.True(fs.GetFileStatus(p).IsDirectory());
			return p;
		}

		// check that the specified file is in Trash
		/// <exception cref="System.IO.IOException"/>
		protected internal static void CheckTrash(FileSystem trashFs, Path trashRoot, Path
			 path)
		{
			Path p = Path.MergePaths(trashRoot, path);
			Assert.True("Could not find file in trash: " + p, trashFs.Exists
				(p));
		}

		// counts how many instances of the file are in the Trash
		// they all are in format fileName*
		/// <exception cref="System.IO.IOException"/>
		protected internal static int CountSameDeletedFiles(FileSystem fs, Path trashDir, 
			Path fileName)
		{
			string prefix = fileName.GetName();
			System.Console.Out.WriteLine("Counting " + fileName + " in " + trashDir.ToString(
				));
			// filter that matches all the files that start with fileName*
			PathFilter pf = new _PathFilter_71(prefix);
			// run the filter
			FileStatus[] fss = fs.ListStatus(trashDir, pf);
			return fss == null ? 0 : fss.Length;
		}

		private sealed class _PathFilter_71 : PathFilter
		{
			public _PathFilter_71(string prefix)
			{
				this.prefix = prefix;
			}

			public bool Accept(Path file)
			{
				return file.GetName().StartsWith(prefix);
			}

			private readonly string prefix;
		}

		// check that the specified file is not in Trash
		/// <exception cref="System.IO.IOException"/>
		internal static void CheckNotInTrash(FileSystem fs, Path trashRoot, string pathname
			)
		{
			Path p = new Path(trashRoot + "/" + new Path(pathname).GetName());
			Assert.True(!fs.Exists(p));
		}

		/// <summary>Test trash for the shell's delete command for the file system fs</summary>
		/// <param name="fs"/>
		/// <param name="base">- the base path where files are created</param>
		/// <exception cref="System.IO.IOException"/>
		public static void TrashShell(FileSystem fs, Path @base)
		{
			Configuration conf = new Configuration();
			conf.Set("fs.defaultFS", fs.GetUri().ToString());
			TrashShell(conf, @base, null, null);
		}

		/// <summary>
		/// Test trash for the shell's delete command for the default file system
		/// specified in the paramter conf
		/// </summary>
		/// <param name="conf"></param>
		/// <param name="base">- the base path where files are created</param>
		/// <param name="trashRoot">- the expected place where the trashbin resides</param>
		/// <exception cref="System.IO.IOException"/>
		public static void TrashShell(Configuration conf, Path @base, FileSystem trashRootFs
			, Path trashRoot)
		{
			FileSystem fs = FileSystem.Get(conf);
			conf.SetLong(FsTrashIntervalKey, 0);
			// disabled
			NUnit.Framework.Assert.IsFalse(new Trash(conf).IsEnabled());
			conf.SetLong(FsTrashIntervalKey, 10);
			// 10 minute
			Assert.True(new Trash(conf).IsEnabled());
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			if (trashRoot == null)
			{
				trashRoot = shell.GetCurrentTrashDir();
			}
			if (trashRootFs == null)
			{
				trashRootFs = fs;
			}
			// First create a new directory with mkdirs
			Path myPath = new Path(@base, "test/mkdirs");
			Mkdir(fs, myPath);
			// Second, create a file in that directory.
			Path myFile = new Path(@base, "test/mkdirs/myFile");
			FileSystemTestHelper.WriteFile(fs, myFile, 10);
			{
				// Verify that expunge without Trash directory
				// won't throw Exception
				string[] args = new string[1];
				args[0] = "-expunge";
				int val = -1;
				try
				{
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				Assert.True(val == 0);
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
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				Assert.True(val == 0);
				CheckTrash(trashRootFs, trashRoot, fs.MakeQualified(myFile));
			}
			// Verify that we can recreate the file
			FileSystemTestHelper.WriteFile(fs, myFile, 10);
			{
				// Verify that we succeed in removing the file we re-created
				string[] args = new string[2];
				args[0] = "-rm";
				args[1] = new Path(@base, "test/mkdirs/myFile").ToString();
				int val = -1;
				try
				{
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				Assert.True(val == 0);
			}
			// Verify that we can recreate the file
			FileSystemTestHelper.WriteFile(fs, myFile, 10);
			{
				// Verify that we succeed in removing the whole directory
				// along with the file inside it.
				string[] args = new string[2];
				args[0] = "-rmr";
				args[1] = new Path(@base, "test/mkdirs").ToString();
				int val = -1;
				try
				{
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				Assert.True(val == 0);
			}
			// recreate directory
			Mkdir(fs, myPath);
			{
				// Verify that we succeed in removing the whole directory
				string[] args = new string[2];
				args[0] = "-rmr";
				args[1] = new Path(@base, "test/mkdirs").ToString();
				int val = -1;
				try
				{
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				Assert.True(val == 0);
			}
			{
				// Check that we can delete a file from the trash
				Path toErase = new Path(trashRoot, "toErase");
				int retVal = -1;
				FileSystemTestHelper.WriteFile(trashRootFs, toErase, 10);
				try
				{
					retVal = shell.Run(new string[] { "-rm", toErase.ToString() });
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				Assert.True(retVal == 0);
				CheckNotInTrash(trashRootFs, trashRoot, toErase.ToString());
				CheckNotInTrash(trashRootFs, trashRoot, toErase.ToString() + ".1");
			}
			{
				// simulate Trash removal
				string[] args = new string[1];
				args[0] = "-expunge";
				int val = -1;
				try
				{
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				Assert.True(val == 0);
			}
			// verify that after expunging the Trash, it really goes away
			CheckNotInTrash(trashRootFs, trashRoot, new Path(@base, "test/mkdirs/myFile").ToString
				());
			// recreate directory and file
			Mkdir(fs, myPath);
			FileSystemTestHelper.WriteFile(fs, myFile, 10);
			{
				// remove file first, then remove directory
				string[] args = new string[2];
				args[0] = "-rm";
				args[1] = myFile.ToString();
				int val = -1;
				try
				{
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				Assert.True(val == 0);
				CheckTrash(trashRootFs, trashRoot, myFile);
				args = new string[2];
				args[0] = "-rmr";
				args[1] = myPath.ToString();
				val = -1;
				try
				{
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				Assert.True(val == 0);
				CheckTrash(trashRootFs, trashRoot, myPath);
			}
			{
				// attempt to remove parent of trash
				string[] args = new string[2];
				args[0] = "-rmr";
				args[1] = trashRoot.GetParent().GetParent().ToString();
				int val = -1;
				try
				{
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				Assert.Equal("exit code", 1, val);
				Assert.True(trashRootFs.Exists(trashRoot));
			}
			// Verify skip trash option really works
			// recreate directory and file
			Mkdir(fs, myPath);
			FileSystemTestHelper.WriteFile(fs, myFile, 10);
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
					Assert.Equal("-expunge failed", 0, shell.Run(new string[] { "-expunge"
						 }));
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsFalse("Expected TrashRoot (" + trashRoot + ") to exist in file system:"
					 + trashRootFs.GetUri(), trashRootFs.Exists(trashRoot));
				// No new Current should be created
				NUnit.Framework.Assert.IsFalse(fs.Exists(myFile));
				Assert.True(val == 0);
			}
			// recreate directory and file
			Mkdir(fs, myPath);
			FileSystemTestHelper.WriteFile(fs, myFile, 10);
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
					Assert.Equal(0, shell.Run(new string[] { "-expunge" }));
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				NUnit.Framework.Assert.IsFalse(trashRootFs.Exists(trashRoot));
				// No new Current should be created
				NUnit.Framework.Assert.IsFalse(fs.Exists(myPath));
				NUnit.Framework.Assert.IsFalse(fs.Exists(myFile));
				Assert.True(val == 0);
			}
			{
				// deleting same file multiple times
				int val = -1;
				Mkdir(fs, myPath);
				try
				{
					Assert.Equal(0, shell.Run(new string[] { "-expunge" }));
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from fs expunge " + e.GetLocalizedMessage
						());
				}
				// create a file in that directory.
				myFile = new Path(@base, "test/mkdirs/myFile");
				string[] args = new string[] { "-rm", myFile.ToString() };
				int num_runs = 10;
				for (int i = 0; i < num_runs; i++)
				{
					//create file
					FileSystemTestHelper.WriteFile(fs, myFile, 10);
					// delete file
					try
					{
						val = shell.Run(args);
					}
					catch (Exception e)
					{
						System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
							());
					}
					Assert.True(val == 0);
				}
				// current trash directory
				Path trashDir = Path.MergePaths(new Path(trashRoot.ToUri().GetPath()), new Path(myFile
					.GetParent().ToUri().GetPath()));
				System.Console.Out.WriteLine("Deleting same myFile: myFile.parent=" + myFile.GetParent
					().ToUri().GetPath() + "; trashroot=" + trashRoot.ToUri().GetPath() + "; trashDir="
					 + trashDir.ToUri().GetPath());
				int count = CountSameDeletedFiles(fs, trashDir, myFile);
				System.Console.Out.WriteLine("counted " + count + " files " + myFile.GetName() + 
					"* in " + trashDir);
				Assert.True(count == num_runs);
			}
			{
				//Verify skipTrash option is suggested when rm fails due to its absence
				string[] args = new string[2];
				args[0] = "-rmr";
				args[1] = "/";
				//This always contains trash directory
				TextWriter stdout = System.Console.Out;
				TextWriter stderr = System.Console.Error;
				ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
				TextWriter newOut = new TextWriter(byteStream);
				Runtime.SetOut(newOut);
				Runtime.SetErr(newOut);
				try
				{
					shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				string output = byteStream.ToString();
				Runtime.SetOut(stdout);
				Runtime.SetErr(stderr);
				Assert.True("skipTrash wasn't suggested as remedy to failed rm command"
					 + " or we deleted / even though we could not get server defaults", output.IndexOf
					("Consider using -skipTrash option") != -1 || output.IndexOf("Failed to determine server trash configuration"
					) != -1);
			}
			{
				// Verify old checkpoint format is recognized
				// emulate two old trash checkpoint directories, one that is old enough
				// to be deleted on the next expunge and one that isn't.
				long trashInterval = conf.GetLong(FsTrashIntervalKey, FsTrashIntervalDefault);
				long now = Time.Now();
				DateFormat oldCheckpointFormat = new SimpleDateFormat("yyMMddHHmm");
				Path dirToDelete = new Path(trashRoot.GetParent(), oldCheckpointFormat.Format(now
					 - (trashInterval * 60 * 1000) - 1));
				Path dirToKeep = new Path(trashRoot.GetParent(), oldCheckpointFormat.Format(now));
				Mkdir(trashRootFs, dirToDelete);
				Mkdir(trashRootFs, dirToKeep);
				// Clear out trash
				int rc = -1;
				try
				{
					rc = shell.Run(new string[] { "-expunge" });
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from fs expunge " + e.GetLocalizedMessage
						());
				}
				Assert.Equal(0, rc);
				NUnit.Framework.Assert.IsFalse("old checkpoint format not recognized", trashRootFs
					.Exists(dirToDelete));
				Assert.True("old checkpoint format directory should not be removed"
					, trashRootFs.Exists(dirToKeep));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void TrashNonDefaultFS(Configuration conf)
		{
			conf.SetLong(FsTrashIntervalKey, 10);
			{
				// 10 minute
				// attempt non-default FileSystem trash
				FileSystem lfs = FileSystem.GetLocal(conf);
				Path p = TestDir;
				Path f = new Path(p, "foo/bar");
				if (lfs.Exists(p))
				{
					lfs.Delete(p, true);
				}
				try
				{
					FileSystemTestHelper.WriteFile(lfs, f, 10);
					FileSystem.CloseAll();
					FileSystem localFs = FileSystem.Get(URI.Create("file:///"), conf);
					Trash lTrash = new Trash(localFs, conf);
					lTrash.MoveToTrash(f.GetParent());
					CheckTrash(localFs, lTrash.GetCurrentTrashDir(), f);
				}
				finally
				{
					if (lfs.Exists(p))
					{
						lfs.Delete(p, true);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTrash()
		{
			Configuration conf = new Configuration();
			conf.SetClass("fs.file.impl", typeof(TestTrash.TestLFS), typeof(FileSystem));
			TrashShell(FileSystem.GetLocal(conf), TestDir);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNonDefaultFS()
		{
			Configuration conf = new Configuration();
			conf.SetClass("fs.file.impl", typeof(TestTrash.TestLFS), typeof(FileSystem));
			conf.Set("fs.defaultFS", "invalid://host/bar/foo");
			TrashNonDefaultFS(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPluggableTrash()
		{
			Configuration conf = new Configuration();
			// Test plugged TrashPolicy
			conf.SetClass("fs.trash.classname", typeof(TestTrash.TestTrashPolicy), typeof(TrashPolicy
				));
			Trash trash = new Trash(conf);
			Assert.True(trash.GetTrashPolicy().GetType().Equals(typeof(TestTrash.TestTrashPolicy
				)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTrashEmptier()
		{
			Configuration conf = new Configuration();
			// Trash with 12 second deletes and 6 seconds checkpoints
			conf.Set(FsTrashIntervalKey, "0.2");
			// 12 seconds
			conf.SetClass("fs.file.impl", typeof(TestTrash.TestLFS), typeof(FileSystem));
			conf.Set(FsTrashCheckpointIntervalKey, "0.1");
			// 6 seconds
			FileSystem fs = FileSystem.GetLocal(conf);
			conf.Set("fs.default.name", fs.GetUri().ToString());
			Trash trash = new Trash(conf);
			// Start Emptier in background
			Runnable emptier = trash.GetEmptier();
			Sharpen.Thread emptierThread = new Sharpen.Thread(emptier);
			emptierThread.Start();
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			shell.Init();
			// First create a new directory with mkdirs
			Path myPath = new Path(TestDir, "test/mkdirs");
			Mkdir(fs, myPath);
			int fileIndex = 0;
			ICollection<string> checkpoints = new HashSet<string>();
			while (true)
			{
				// Create a file with a new name
				Path myFile = new Path(TestDir, "test/mkdirs/myFile" + fileIndex++);
				FileSystemTestHelper.WriteFile(fs, myFile, 10);
				// Delete the file to trash
				string[] args = new string[2];
				args[0] = "-rm";
				args[1] = myFile.ToString();
				int val = -1;
				try
				{
					val = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
				}
				Assert.True(val == 0);
				Path trashDir = shell.GetCurrentTrashDir();
				FileStatus[] files = fs.ListStatus(trashDir.GetParent());
				// Scan files in .Trash and add them to set of checkpoints
				foreach (FileStatus file in files)
				{
					string fileName = file.GetPath().GetName();
					checkpoints.AddItem(fileName);
				}
				// If checkpoints has 4 objects it is Current + 3 checkpoint directories
				if (checkpoints.Count == 4)
				{
					// The actual contents should be smaller since the last checkpoint
					// should've been deleted and Current might not have been recreated yet
					Assert.True(checkpoints.Count > files.Length);
					break;
				}
				Sharpen.Thread.Sleep(5000);
			}
			emptierThread.Interrupt();
			emptierThread.Join();
		}

		/// <seealso cref="NUnit.Framework.TestCase.TearDown()"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void TearDown()
		{
			FilePath trashDir = new FilePath(TestDir.ToUri().GetPath());
			if (trashDir.Exists() && !FileUtil.FullyDelete(trashDir))
			{
				throw new IOException("Cannot remove data directory: " + trashDir);
			}
		}

		internal class TestLFS : LocalFileSystem
		{
			internal Path home;

			internal TestLFS()
				: this(new Path(TestDir, "user/test"))
			{
			}

			internal TestLFS(Path home)
				: base()
			{
				this.home = home;
			}

			public override Path GetHomeDirectory()
			{
				return home;
			}
		}

		/// <summary>
		/// test same file deletion - multiple time
		/// this is more of a performance test - shouldn't be run as a unit test
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static void PerformanceTestDeleteSameFile()
		{
			Path @base = TestDir;
			Configuration conf = new Configuration();
			conf.SetClass("fs.file.impl", typeof(TestTrash.TestLFS), typeof(FileSystem));
			FileSystem fs = FileSystem.GetLocal(conf);
			conf.Set("fs.defaultFS", fs.GetUri().ToString());
			conf.SetLong(FsTrashIntervalKey, 10);
			//minutes..
			FsShell shell = new FsShell();
			shell.SetConf(conf);
			//Path trashRoot = null;
			Path myPath = new Path(@base, "test/mkdirs");
			Mkdir(fs, myPath);
			// create a file in that directory.
			Path myFile;
			long start;
			long first = 0;
			int retVal = 0;
			int factor = 10;
			// how much slower any of subsequent deletion can be
			myFile = new Path(@base, "test/mkdirs/myFile");
			string[] args = new string[] { "-rm", myFile.ToString() };
			int iters = 1000;
			for (int i = 0; i < iters; i++)
			{
				FileSystemTestHelper.WriteFile(fs, myFile, 10);
				start = Time.Now();
				try
				{
					retVal = shell.Run(args);
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("Exception raised from Trash.run " + e.GetLocalizedMessage
						());
					throw new IOException(e.Message);
				}
				Assert.True(retVal == 0);
				long iterTime = Time.Now() - start;
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
					Assert.True(iterTime < factoredTime);
				}
			}
		}

		//no more then twice of median first 10
		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] arg)
		{
			// run performance piece as a separate test
			PerformanceTestDeleteSameFile();
		}

		public class TestTrashPolicy : TrashPolicy
		{
			public TestTrashPolicy()
			{
			}

			// Test TrashPolicy. Don't care about implementation.
			public override void Initialize(Configuration conf, FileSystem fs, Path home)
			{
			}

			public override bool IsEnabled()
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool MoveToTrash(Path path)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CreateCheckpoint()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DeleteCheckpoint()
			{
			}

			public override Path GetCurrentTrashDir()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Runnable GetEmptier()
			{
				return null;
			}
		}
	}
}
