using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
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
		private static readonly Configuration conf = new Configuration();

		private const string BufferDirRoot = "build/test/temp";

		private static readonly string AbsoluteDirRoot;

		private static readonly string QualifiedDirRoot;

		private static readonly Path BufferPathRoot = new Path(BufferDirRoot);

		private static readonly FilePath BufferRoot = new FilePath(BufferDirRoot);

		private const string Context = "mapred.local.dir";

		private const string Filename = "block";

		private static readonly LocalDirAllocator dirAllocator = new LocalDirAllocator(Context
			);

		internal static LocalFileSystem localFs;

		private static readonly bool isWindows = Runtime.GetProperty("os.name").StartsWith
			("Windows");

		internal const int SmallFileSize = 100;

		private const string Relative = "/RELATIVE";

		private const string Absolute = "/ABSOLUTE";

		private const string Qualified = "/QUALIFIED";

		private readonly string Root;

		private readonly string Prefix;

		static TestLocalDirAllocator()
		{
			try
			{
				localFs = FileSystem.GetLocal(conf);
				RmBufferDirs();
			}
			catch (IOException e)
			{
				System.Console.Out.WriteLine(e.Message);
				Sharpen.Runtime.PrintStackTrace(e);
				System.Environment.Exit(-1);
			}
			// absolute path in test environment
			// /home/testuser/src/hadoop-common-project/hadoop-common/build/test/temp
			AbsoluteDirRoot = new Path(localFs.GetWorkingDirectory(), BufferDirRoot).ToUri().
				GetPath();
			// file:/home/testuser/src/hadoop-common-project/hadoop-common/build/test/temp
			QualifiedDirRoot = new Path(localFs.GetWorkingDirectory(), BufferDirRoot).ToUri()
				.ToString();
		}

		public TestLocalDirAllocator(string root, string prefix)
		{
			Root = root;
			Prefix = prefix;
		}

		[Parameterized.Parameters]
		public static ICollection<object[]> Params()
		{
			object[][] data = new object[][] { new object[] { BufferDirRoot, Relative }, new 
				object[] { AbsoluteDirRoot, Absolute }, new object[] { QualifiedDirRoot, Qualified
				 } };
			return Arrays.AsList(data);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void RmBufferDirs()
		{
			Assert.True(!localFs.Exists(BufferPathRoot) || localFs.Delete(BufferPathRoot
				, true));
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ValidateTempDirCreation(string dir)
		{
			FilePath result = CreateTempFile(SmallFileSize);
			Assert.True("Checking for " + dir + " in " + result + " - FAILED!"
				, result.GetPath().StartsWith(new Path(dir, Filename).ToUri().GetPath()));
		}

		/// <exception cref="System.IO.IOException"/>
		private static FilePath CreateTempFile()
		{
			return CreateTempFile(-1);
		}

		/// <exception cref="System.IO.IOException"/>
		private static FilePath CreateTempFile(long size)
		{
			FilePath result = dirAllocator.CreateTmpFileForWrite(Filename, size, conf);
			result.Delete();
			return result;
		}

		private string BuildBufferDir(string dir, int i)
		{
			return dir + Prefix + i;
		}

		/// <summary>Two buffer dirs.</summary>
		/// <remarks>
		/// Two buffer dirs. The first dir does not exist & is on a read-only disk;
		/// The second dir exists & is RW
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void Test0()
		{
			if (isWindows)
			{
				return;
			}
			string dir0 = BuildBufferDir(Root, 0);
			string dir1 = BuildBufferDir(Root, 1);
			try
			{
				conf.Set(Context, dir0 + "," + dir1);
				Assert.True(localFs.Mkdirs(new Path(dir1)));
				BufferRoot.SetReadOnly();
				ValidateTempDirCreation(dir1);
				ValidateTempDirCreation(dir1);
			}
			finally
			{
				Shell.ExecCommand(Shell.GetSetPermissionCommand("u+w", false, BufferDirRoot));
				RmBufferDirs();
			}
		}

		/// <summary>Two buffer dirs.</summary>
		/// <remarks>
		/// Two buffer dirs. The first dir exists & is on a read-only disk;
		/// The second dir exists & is RW
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestROBufferDirAndRWBufferDir()
		{
			if (isWindows)
			{
				return;
			}
			string dir1 = BuildBufferDir(Root, 1);
			string dir2 = BuildBufferDir(Root, 2);
			try
			{
				conf.Set(Context, dir1 + "," + dir2);
				Assert.True(localFs.Mkdirs(new Path(dir2)));
				BufferRoot.SetReadOnly();
				ValidateTempDirCreation(dir2);
				ValidateTempDirCreation(dir2);
			}
			finally
			{
				Shell.ExecCommand(Shell.GetSetPermissionCommand("u+w", false, BufferDirRoot));
				RmBufferDirs();
			}
		}

		/// <summary>Two buffer dirs.</summary>
		/// <remarks>
		/// Two buffer dirs. Both do not exist but on a RW disk.
		/// Check if tmp dirs are allocated in a round-robin
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDirsNotExist()
		{
			if (isWindows)
			{
				return;
			}
			string dir2 = BuildBufferDir(Root, 2);
			string dir3 = BuildBufferDir(Root, 3);
			try
			{
				conf.Set(Context, dir2 + "," + dir3);
				// create the first file, and then figure the round-robin sequence
				CreateTempFile(SmallFileSize);
				int firstDirIdx = (dirAllocator.GetCurrentDirectoryIndex() == 0) ? 2 : 3;
				int secondDirIdx = (firstDirIdx == 2) ? 3 : 2;
				// check if tmp dirs are allocated in a round-robin manner
				ValidateTempDirCreation(BuildBufferDir(Root, firstDirIdx));
				ValidateTempDirCreation(BuildBufferDir(Root, secondDirIdx));
				ValidateTempDirCreation(BuildBufferDir(Root, firstDirIdx));
			}
			finally
			{
				RmBufferDirs();
			}
		}

		/// <summary>Two buffer dirs.</summary>
		/// <remarks>
		/// Two buffer dirs. Both exists and on a R/W disk.
		/// Later disk1 becomes read-only.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestRWBufferDirBecomesRO()
		{
			if (isWindows)
			{
				return;
			}
			string dir3 = BuildBufferDir(Root, 3);
			string dir4 = BuildBufferDir(Root, 4);
			try
			{
				conf.Set(Context, dir3 + "," + dir4);
				Assert.True(localFs.Mkdirs(new Path(dir3)));
				Assert.True(localFs.Mkdirs(new Path(dir4)));
				// Create the first small file
				CreateTempFile(SmallFileSize);
				// Determine the round-robin sequence
				int nextDirIdx = (dirAllocator.GetCurrentDirectoryIndex() == 0) ? 3 : 4;
				ValidateTempDirCreation(BuildBufferDir(Root, nextDirIdx));
				// change buffer directory 2 to be read only
				new FilePath(new Path(dir4).ToUri().GetPath()).SetReadOnly();
				ValidateTempDirCreation(dir3);
				ValidateTempDirCreation(dir3);
			}
			finally
			{
				RmBufferDirs();
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
		internal const int Trials = 100;

		/// <exception cref="System.Exception"/>
		public virtual void TestCreateManyFiles()
		{
			if (isWindows)
			{
				return;
			}
			string dir5 = BuildBufferDir(Root, 5);
			string dir6 = BuildBufferDir(Root, 6);
			try
			{
				conf.Set(Context, dir5 + "," + dir6);
				Assert.True(localFs.Mkdirs(new Path(dir5)));
				Assert.True(localFs.Mkdirs(new Path(dir6)));
				int inDir5 = 0;
				int inDir6 = 0;
				for (int i = 0; i < Trials; ++i)
				{
					FilePath result = CreateTempFile();
					if (result.GetPath().StartsWith(new Path(dir5, Filename).ToUri().GetPath()))
					{
						inDir5++;
					}
					else
					{
						if (result.GetPath().StartsWith(new Path(dir6, Filename).ToUri().GetPath()))
						{
							inDir6++;
						}
					}
					result.Delete();
				}
				Assert.True(inDir5 + inDir6 == Trials);
			}
			finally
			{
				RmBufferDirs();
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
		public virtual void TestLocalPathForWriteDirCreation()
		{
			string dir0 = BuildBufferDir(Root, 0);
			string dir1 = BuildBufferDir(Root, 1);
			try
			{
				conf.Set(Context, dir0 + "," + dir1);
				Assert.True(localFs.Mkdirs(new Path(dir1)));
				BufferRoot.SetReadOnly();
				Path p1 = dirAllocator.GetLocalPathForWrite("p1/x", SmallFileSize, conf);
				Assert.True(localFs.GetFileStatus(p1.GetParent()).IsDirectory()
					);
				Path p2 = dirAllocator.GetLocalPathForWrite("p2/x", SmallFileSize, conf, false);
				try
				{
					localFs.GetFileStatus(p2.GetParent());
				}
				catch (Exception e)
				{
					Assert.Equal(e.GetType(), typeof(FileNotFoundException));
				}
			}
			finally
			{
				Shell.ExecCommand(Shell.GetSetPermissionCommand("u+w", false, BufferDirRoot));
				RmBufferDirs();
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
		public virtual void TestNoSideEffects()
		{
			Assume.AssumeTrue(!isWindows);
			string dir = BuildBufferDir(Root, 0);
			try
			{
				conf.Set(Context, dir);
				FilePath result = dirAllocator.CreateTmpFileForWrite(Filename, -1, conf);
				Assert.True(result.Delete());
				Assert.True(result.GetParentFile().Delete());
				NUnit.Framework.Assert.IsFalse(new FilePath(dir).Exists());
			}
			finally
			{
				Shell.ExecCommand(Shell.GetSetPermissionCommand("u+w", false, BufferDirRoot));
				RmBufferDirs();
			}
		}

		/// <summary>Test getLocalPathToRead() returns correct filename and "file" schema.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetLocalPathToRead()
		{
			Assume.AssumeTrue(!isWindows);
			string dir = BuildBufferDir(Root, 0);
			try
			{
				conf.Set(Context, dir);
				Assert.True(localFs.Mkdirs(new Path(dir)));
				FilePath f1 = dirAllocator.CreateTmpFileForWrite(Filename, SmallFileSize, conf);
				Path p1 = dirAllocator.GetLocalPathToRead(f1.GetName(), conf);
				Assert.Equal(f1.GetName(), p1.GetName());
				Assert.Equal("file", p1.GetFileSystem(conf).GetUri().GetScheme
					());
			}
			finally
			{
				Shell.ExecCommand(Shell.GetSetPermissionCommand("u+w", false, BufferDirRoot));
				RmBufferDirs();
			}
		}

		/// <summary>
		/// Test that
		/// <see cref="LocalDirAllocator.GetAllLocalPathsToRead(string, Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// 
		/// returns correct filenames and "file" schema.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetAllLocalPathsToRead()
		{
			Assume.AssumeTrue(!isWindows);
			string dir0 = BuildBufferDir(Root, 0);
			string dir1 = BuildBufferDir(Root, 1);
			try
			{
				conf.Set(Context, dir0 + "," + dir1);
				Assert.True(localFs.Mkdirs(new Path(dir0)));
				Assert.True(localFs.Mkdirs(new Path(dir1)));
				localFs.Create(new Path(dir0 + Path.Separator + Filename));
				localFs.Create(new Path(dir1 + Path.Separator + Filename));
				// check both the paths are returned as paths to read:  
				IEnumerable<Path> pathIterable = dirAllocator.GetAllLocalPathsToRead(Filename, conf
					);
				int count = 0;
				foreach (Path p in pathIterable)
				{
					count++;
					Assert.Equal(Filename, p.GetName());
					Assert.Equal("file", p.GetFileSystem(conf).GetUri().GetScheme(
						));
				}
				Assert.Equal(2, count);
				// test #next() while no element to iterate any more: 
				try
				{
					Path p_1 = pathIterable.GetEnumerator().Next();
					NUnit.Framework.Assert.IsFalse("NoSuchElementException must be thrown, but returned ["
						 + p_1 + "] instead.", true);
				}
				catch (NoSuchElementException)
				{
				}
				// exception expected
				// okay
				// test modification not allowed:
				IEnumerable<Path> pathIterable2 = dirAllocator.GetAllLocalPathsToRead(Filename, conf
					);
				IEnumerator<Path> it = pathIterable2.GetEnumerator();
				try
				{
					it.Remove();
					NUnit.Framework.Assert.IsFalse(true);
				}
				catch (NotSupportedException)
				{
				}
			}
			finally
			{
				// exception expected
				// okay
				Shell.ExecCommand(new string[] { "chmod", "u+w", BufferDirRoot });
				RmBufferDirs();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRemoveContext()
		{
			string dir = BuildBufferDir(Root, 0);
			try
			{
				string contextCfgItemName = "application_1340842292563_0004.app.cache.dirs";
				conf.Set(contextCfgItemName, dir);
				LocalDirAllocator localDirAllocator = new LocalDirAllocator(contextCfgItemName);
				localDirAllocator.GetLocalPathForWrite("p1/x", SmallFileSize, conf);
				Assert.True(LocalDirAllocator.IsContextValid(contextCfgItemName
					));
				LocalDirAllocator.RemoveContext(contextCfgItemName);
				NUnit.Framework.Assert.IsFalse(LocalDirAllocator.IsContextValid(contextCfgItemName
					));
			}
			finally
			{
				RmBufferDirs();
			}
		}
	}
}
