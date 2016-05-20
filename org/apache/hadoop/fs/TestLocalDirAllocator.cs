using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// This test LocalDirAllocator works correctly;
	/// Every test case uses different buffer dirs to
	/// enforce the AllocatorPerContext initialization.
	/// </summary>
	/// <remarks>
	/// This test LocalDirAllocator works correctly;
	/// Every test case uses different buffer dirs to
	/// enforce the AllocatorPerContext initialization.
	/// This test does not run on Cygwin because under Cygwin
	/// a directory can be created in a read-only directory
	/// which breaks this test.
	/// </remarks>
	public class TestLocalDirAllocator
	{
		private static readonly org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		private const string BUFFER_DIR_ROOT = "build/test/temp";

		private static readonly string ABSOLUTE_DIR_ROOT;

		private static readonly string QUALIFIED_DIR_ROOT;

		private static readonly org.apache.hadoop.fs.Path BUFFER_PATH_ROOT = new org.apache.hadoop.fs.Path
			(BUFFER_DIR_ROOT);

		private static readonly java.io.File BUFFER_ROOT = new java.io.File(BUFFER_DIR_ROOT
			);

		private const string CONTEXT = "mapred.local.dir";

		private const string FILENAME = "block";

		private static readonly org.apache.hadoop.fs.LocalDirAllocator dirAllocator = new 
			org.apache.hadoop.fs.LocalDirAllocator(CONTEXT);

		internal static org.apache.hadoop.fs.LocalFileSystem localFs;

		private static readonly bool isWindows = Sharpen.Runtime.getProperty("os.name").StartsWith
			("Windows");

		internal const int SMALL_FILE_SIZE = 100;

		private const string RELATIVE = "/RELATIVE";

		private const string ABSOLUTE = "/ABSOLUTE";

		private const string QUALIFIED = "/QUALIFIED";

		private readonly string ROOT;

		private readonly string PREFIX;

		static TestLocalDirAllocator()
		{
			try
			{
				localFs = org.apache.hadoop.fs.FileSystem.getLocal(conf);
				rmBufferDirs();
			}
			catch (System.IO.IOException e)
			{
				System.Console.Out.WriteLine(e.Message);
				Sharpen.Runtime.printStackTrace(e);
				System.Environment.Exit(-1);
			}
			// absolute path in test environment
			// /home/testuser/src/hadoop-common-project/hadoop-common/build/test/temp
			ABSOLUTE_DIR_ROOT = new org.apache.hadoop.fs.Path(localFs.getWorkingDirectory(), 
				BUFFER_DIR_ROOT).toUri().getPath();
			// file:/home/testuser/src/hadoop-common-project/hadoop-common/build/test/temp
			QUALIFIED_DIR_ROOT = new org.apache.hadoop.fs.Path(localFs.getWorkingDirectory(), 
				BUFFER_DIR_ROOT).toUri().ToString();
		}

		public TestLocalDirAllocator(string root, string prefix)
		{
			ROOT = root;
			PREFIX = prefix;
		}

		[NUnit.Framework.runners.Parameterized.Parameters]
		public static System.Collections.Generic.ICollection<object[]> @params()
		{
			object[][] data = new object[][] { new object[] { BUFFER_DIR_ROOT, RELATIVE }, new 
				object[] { ABSOLUTE_DIR_ROOT, ABSOLUTE }, new object[] { QUALIFIED_DIR_ROOT, QUALIFIED
				 } };
			return java.util.Arrays.asList(data);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void rmBufferDirs()
		{
			NUnit.Framework.Assert.IsTrue(!localFs.exists(BUFFER_PATH_ROOT) || localFs.delete
				(BUFFER_PATH_ROOT, true));
		}

		/// <exception cref="System.IO.IOException"/>
		private static void validateTempDirCreation(string dir)
		{
			java.io.File result = createTempFile(SMALL_FILE_SIZE);
			NUnit.Framework.Assert.IsTrue("Checking for " + dir + " in " + result + " - FAILED!"
				, result.getPath().StartsWith(new org.apache.hadoop.fs.Path(dir, FILENAME).toUri
				().getPath()));
		}

		/// <exception cref="System.IO.IOException"/>
		private static java.io.File createTempFile()
		{
			return createTempFile(-1);
		}

		/// <exception cref="System.IO.IOException"/>
		private static java.io.File createTempFile(long size)
		{
			java.io.File result = dirAllocator.createTmpFileForWrite(FILENAME, size, conf);
			result.delete();
			return result;
		}

		private string buildBufferDir(string dir, int i)
		{
			return dir + PREFIX + i;
		}

		/// <summary>Two buffer dirs.</summary>
		/// <remarks>
		/// Two buffer dirs. The first dir does not exist & is on a read-only disk;
		/// The second dir exists & is RW
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void test0()
		{
			if (isWindows)
			{
				return;
			}
			string dir0 = buildBufferDir(ROOT, 0);
			string dir1 = buildBufferDir(ROOT, 1);
			try
			{
				conf.set(CONTEXT, dir0 + "," + dir1);
				NUnit.Framework.Assert.IsTrue(localFs.mkdirs(new org.apache.hadoop.fs.Path(dir1))
					);
				BUFFER_ROOT.setReadOnly();
				validateTempDirCreation(dir1);
				validateTempDirCreation(dir1);
			}
			finally
			{
				org.apache.hadoop.util.Shell.execCommand(org.apache.hadoop.util.Shell.getSetPermissionCommand
					("u+w", false, BUFFER_DIR_ROOT));
				rmBufferDirs();
			}
		}

		/// <summary>Two buffer dirs.</summary>
		/// <remarks>
		/// Two buffer dirs. The first dir exists & is on a read-only disk;
		/// The second dir exists & is RW
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void testROBufferDirAndRWBufferDir()
		{
			if (isWindows)
			{
				return;
			}
			string dir1 = buildBufferDir(ROOT, 1);
			string dir2 = buildBufferDir(ROOT, 2);
			try
			{
				conf.set(CONTEXT, dir1 + "," + dir2);
				NUnit.Framework.Assert.IsTrue(localFs.mkdirs(new org.apache.hadoop.fs.Path(dir2))
					);
				BUFFER_ROOT.setReadOnly();
				validateTempDirCreation(dir2);
				validateTempDirCreation(dir2);
			}
			finally
			{
				org.apache.hadoop.util.Shell.execCommand(org.apache.hadoop.util.Shell.getSetPermissionCommand
					("u+w", false, BUFFER_DIR_ROOT));
				rmBufferDirs();
			}
		}

		/// <summary>Two buffer dirs.</summary>
		/// <remarks>
		/// Two buffer dirs. Both do not exist but on a RW disk.
		/// Check if tmp dirs are allocated in a round-robin
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void testDirsNotExist()
		{
			if (isWindows)
			{
				return;
			}
			string dir2 = buildBufferDir(ROOT, 2);
			string dir3 = buildBufferDir(ROOT, 3);
			try
			{
				conf.set(CONTEXT, dir2 + "," + dir3);
				// create the first file, and then figure the round-robin sequence
				createTempFile(SMALL_FILE_SIZE);
				int firstDirIdx = (dirAllocator.getCurrentDirectoryIndex() == 0) ? 2 : 3;
				int secondDirIdx = (firstDirIdx == 2) ? 3 : 2;
				// check if tmp dirs are allocated in a round-robin manner
				validateTempDirCreation(buildBufferDir(ROOT, firstDirIdx));
				validateTempDirCreation(buildBufferDir(ROOT, secondDirIdx));
				validateTempDirCreation(buildBufferDir(ROOT, firstDirIdx));
			}
			finally
			{
				rmBufferDirs();
			}
		}

		/// <summary>Two buffer dirs.</summary>
		/// <remarks>
		/// Two buffer dirs. Both exists and on a R/W disk.
		/// Later disk1 becomes read-only.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void testRWBufferDirBecomesRO()
		{
			if (isWindows)
			{
				return;
			}
			string dir3 = buildBufferDir(ROOT, 3);
			string dir4 = buildBufferDir(ROOT, 4);
			try
			{
				conf.set(CONTEXT, dir3 + "," + dir4);
				NUnit.Framework.Assert.IsTrue(localFs.mkdirs(new org.apache.hadoop.fs.Path(dir3))
					);
				NUnit.Framework.Assert.IsTrue(localFs.mkdirs(new org.apache.hadoop.fs.Path(dir4))
					);
				// Create the first small file
				createTempFile(SMALL_FILE_SIZE);
				// Determine the round-robin sequence
				int nextDirIdx = (dirAllocator.getCurrentDirectoryIndex() == 0) ? 3 : 4;
				validateTempDirCreation(buildBufferDir(ROOT, nextDirIdx));
				// change buffer directory 2 to be read only
				new java.io.File(new org.apache.hadoop.fs.Path(dir4).toUri().getPath()).setReadOnly
					();
				validateTempDirCreation(dir3);
				validateTempDirCreation(dir3);
			}
			finally
			{
				rmBufferDirs();
			}
		}

		/// <summary>Two buffer dirs, on read-write disk.</summary>
		/// <remarks>
		/// Two buffer dirs, on read-write disk.
		/// Try to create a whole bunch of files.
		/// Verify that they do indeed all get created where they should.
		/// Would ideally check statistical properties of distribution, but
		/// we don't have the nerve to risk false-positives here.
		/// </remarks>
		/// <exception cref="Exception"/>
		internal const int TRIALS = 100;

		/// <exception cref="System.Exception"/>
		public virtual void testCreateManyFiles()
		{
			if (isWindows)
			{
				return;
			}
			string dir5 = buildBufferDir(ROOT, 5);
			string dir6 = buildBufferDir(ROOT, 6);
			try
			{
				conf.set(CONTEXT, dir5 + "," + dir6);
				NUnit.Framework.Assert.IsTrue(localFs.mkdirs(new org.apache.hadoop.fs.Path(dir5))
					);
				NUnit.Framework.Assert.IsTrue(localFs.mkdirs(new org.apache.hadoop.fs.Path(dir6))
					);
				int inDir5 = 0;
				int inDir6 = 0;
				for (int i = 0; i < TRIALS; ++i)
				{
					java.io.File result = createTempFile();
					if (result.getPath().StartsWith(new org.apache.hadoop.fs.Path(dir5, FILENAME).toUri
						().getPath()))
					{
						inDir5++;
					}
					else
					{
						if (result.getPath().StartsWith(new org.apache.hadoop.fs.Path(dir6, FILENAME).toUri
							().getPath()))
						{
							inDir6++;
						}
					}
					result.delete();
				}
				NUnit.Framework.Assert.IsTrue(inDir5 + inDir6 == TRIALS);
			}
			finally
			{
				rmBufferDirs();
			}
		}

		/// <summary>Two buffer dirs.</summary>
		/// <remarks>
		/// Two buffer dirs. The first dir does not exist & is on a read-only disk;
		/// The second dir exists & is RW
		/// getLocalPathForWrite with checkAccess set to false should create a parent
		/// directory. With checkAccess true, the directory should not be created.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testLocalPathForWriteDirCreation()
		{
			string dir0 = buildBufferDir(ROOT, 0);
			string dir1 = buildBufferDir(ROOT, 1);
			try
			{
				conf.set(CONTEXT, dir0 + "," + dir1);
				NUnit.Framework.Assert.IsTrue(localFs.mkdirs(new org.apache.hadoop.fs.Path(dir1))
					);
				BUFFER_ROOT.setReadOnly();
				org.apache.hadoop.fs.Path p1 = dirAllocator.getLocalPathForWrite("p1/x", SMALL_FILE_SIZE
					, conf);
				NUnit.Framework.Assert.IsTrue(localFs.getFileStatus(p1.getParent()).isDirectory()
					);
				org.apache.hadoop.fs.Path p2 = dirAllocator.getLocalPathForWrite("p2/x", SMALL_FILE_SIZE
					, conf, false);
				try
				{
					localFs.getFileStatus(p2.getParent());
				}
				catch (System.Exception e)
				{
					NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForObject(e), Sharpen.Runtime.getClassForType
						(typeof(java.io.FileNotFoundException)));
				}
			}
			finally
			{
				org.apache.hadoop.util.Shell.execCommand(org.apache.hadoop.util.Shell.getSetPermissionCommand
					("u+w", false, BUFFER_DIR_ROOT));
				rmBufferDirs();
			}
		}

		/// <summary>Test no side effect files are left over.</summary>
		/// <remarks>
		/// Test no side effect files are left over. After creating a temp
		/// temp file, remove both the temp file and its parent. Verify that
		/// no files or directories are left over as can happen when File objects
		/// are mistakenly created from fully qualified path strings.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testNoSideEffects()
		{
			NUnit.Framework.Assume.assumeTrue(!isWindows);
			string dir = buildBufferDir(ROOT, 0);
			try
			{
				conf.set(CONTEXT, dir);
				java.io.File result = dirAllocator.createTmpFileForWrite(FILENAME, -1, conf);
				NUnit.Framework.Assert.IsTrue(result.delete());
				NUnit.Framework.Assert.IsTrue(result.getParentFile().delete());
				NUnit.Framework.Assert.IsFalse(new java.io.File(dir).exists());
			}
			finally
			{
				org.apache.hadoop.util.Shell.execCommand(org.apache.hadoop.util.Shell.getSetPermissionCommand
					("u+w", false, BUFFER_DIR_ROOT));
				rmBufferDirs();
			}
		}

		/// <summary>Test getLocalPathToRead() returns correct filename and "file" schema.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetLocalPathToRead()
		{
			NUnit.Framework.Assume.assumeTrue(!isWindows);
			string dir = buildBufferDir(ROOT, 0);
			try
			{
				conf.set(CONTEXT, dir);
				NUnit.Framework.Assert.IsTrue(localFs.mkdirs(new org.apache.hadoop.fs.Path(dir)));
				java.io.File f1 = dirAllocator.createTmpFileForWrite(FILENAME, SMALL_FILE_SIZE, conf
					);
				org.apache.hadoop.fs.Path p1 = dirAllocator.getLocalPathToRead(f1.getName(), conf
					);
				NUnit.Framework.Assert.AreEqual(f1.getName(), p1.getName());
				NUnit.Framework.Assert.AreEqual("file", p1.getFileSystem(conf).getUri().getScheme
					());
			}
			finally
			{
				org.apache.hadoop.util.Shell.execCommand(org.apache.hadoop.util.Shell.getSetPermissionCommand
					("u+w", false, BUFFER_DIR_ROOT));
				rmBufferDirs();
			}
		}

		/// <summary>
		/// Test that
		/// <see cref="LocalDirAllocator.getAllLocalPathsToRead(string, org.apache.hadoop.conf.Configuration)
		/// 	"/>
		/// 
		/// returns correct filenames and "file" schema.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testGetAllLocalPathsToRead()
		{
			NUnit.Framework.Assume.assumeTrue(!isWindows);
			string dir0 = buildBufferDir(ROOT, 0);
			string dir1 = buildBufferDir(ROOT, 1);
			try
			{
				conf.set(CONTEXT, dir0 + "," + dir1);
				NUnit.Framework.Assert.IsTrue(localFs.mkdirs(new org.apache.hadoop.fs.Path(dir0))
					);
				NUnit.Framework.Assert.IsTrue(localFs.mkdirs(new org.apache.hadoop.fs.Path(dir1))
					);
				localFs.create(new org.apache.hadoop.fs.Path(dir0 + org.apache.hadoop.fs.Path.SEPARATOR
					 + FILENAME));
				localFs.create(new org.apache.hadoop.fs.Path(dir1 + org.apache.hadoop.fs.Path.SEPARATOR
					 + FILENAME));
				// check both the paths are returned as paths to read:  
				System.Collections.Generic.IEnumerable<org.apache.hadoop.fs.Path> pathIterable = 
					dirAllocator.getAllLocalPathsToRead(FILENAME, conf);
				int count = 0;
				foreach (org.apache.hadoop.fs.Path p in pathIterable)
				{
					count++;
					NUnit.Framework.Assert.AreEqual(FILENAME, p.getName());
					NUnit.Framework.Assert.AreEqual("file", p.getFileSystem(conf).getUri().getScheme(
						));
				}
				NUnit.Framework.Assert.AreEqual(2, count);
				// test #next() while no element to iterate any more: 
				try
				{
					org.apache.hadoop.fs.Path p_1 = pathIterable.GetEnumerator().Current;
					NUnit.Framework.Assert.IsFalse("NoSuchElementException must be thrown, but returned ["
						 + p_1 + "] instead.", true);
				}
				catch (java.util.NoSuchElementException)
				{
				}
				// exception expected
				// okay
				// test modification not allowed:
				System.Collections.Generic.IEnumerable<org.apache.hadoop.fs.Path> pathIterable2 = 
					dirAllocator.getAllLocalPathsToRead(FILENAME, conf);
				System.Collections.Generic.IEnumerator<org.apache.hadoop.fs.Path> it = pathIterable2
					.GetEnumerator();
				try
				{
					it.remove();
					NUnit.Framework.Assert.IsFalse(true);
				}
				catch (System.NotSupportedException)
				{
				}
			}
			finally
			{
				// exception expected
				// okay
				org.apache.hadoop.util.Shell.execCommand(new string[] { "chmod", "u+w", BUFFER_DIR_ROOT
					 });
				rmBufferDirs();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRemoveContext()
		{
			string dir = buildBufferDir(ROOT, 0);
			try
			{
				string contextCfgItemName = "application_1340842292563_0004.app.cache.dirs";
				conf.set(contextCfgItemName, dir);
				org.apache.hadoop.fs.LocalDirAllocator localDirAllocator = new org.apache.hadoop.fs.LocalDirAllocator
					(contextCfgItemName);
				localDirAllocator.getLocalPathForWrite("p1/x", SMALL_FILE_SIZE, conf);
				NUnit.Framework.Assert.IsTrue(org.apache.hadoop.fs.LocalDirAllocator.isContextValid
					(contextCfgItemName));
				org.apache.hadoop.fs.LocalDirAllocator.removeContext(contextCfgItemName);
				NUnit.Framework.Assert.IsFalse(org.apache.hadoop.fs.LocalDirAllocator.isContextValid
					(contextCfgItemName));
			}
			finally
			{
				rmBufferDirs();
			}
		}
	}
}
