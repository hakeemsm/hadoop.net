using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Util
{
	/// <summary>A test for MRAsyncDiskService.</summary>
	public class TestMRAsyncDiskService : TestCase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Util.TestMRAsyncDiskService
			));

		private static string TestRootDir = new Path(Runtime.GetProperty("test.build.data"
			, "/tmp")).ToString();

		protected override void SetUp()
		{
			FileUtil.FullyDelete(new FilePath(TestRootDir));
		}

		/// <summary>Given 'pathname', compute an equivalent path relative to the cwd.</summary>
		/// <param name="pathname">the path to a directory.</param>
		/// <returns>the path to that same directory, relative to ${user.dir}.</returns>
		private string RelativeToWorking(string pathname)
		{
			string cwd = Runtime.GetProperty("user.dir", "/");
			// normalize pathname and cwd into full directory paths.
			pathname = (new Path(pathname)).ToUri().GetPath();
			cwd = (new Path(cwd)).ToUri().GetPath();
			string[] cwdParts = cwd.Split(Path.Separator);
			string[] pathParts = pathname.Split(Path.Separator);
			// There are three possible cases:
			// 1) pathname and cwd are equal. Return '.'
			// 2) pathname is under cwd. Return the components that are under it.
			//     e.g., cwd = /a/b, path = /a/b/c, return 'c'
			// 3) pathname is outside of cwd. Find the common components, if any,
			//    and subtract them from the returned path, then return enough '..'
			//    components to "undo" the non-common components of cwd, then all 
			//    the remaining parts of pathname.
			//    e.g., cwd = /a/b, path = /a/c, return '../c'
			if (cwd.Equals(pathname))
			{
				Log.Info("relative to working: " + pathname + " -> .");
				return ".";
			}
			// They match exactly.
			// Determine how many path components are in common between cwd and path.
			int common = 0;
			for (int i = 0; i < Math.Min(cwdParts.Length, pathParts.Length); i++)
			{
				if (cwdParts[i].Equals(pathParts[i]))
				{
					common++;
				}
				else
				{
					break;
				}
			}
			// output path stringbuilder.
			StringBuilder sb = new StringBuilder();
			// For everything in cwd that isn't in pathname, add a '..' to undo it.
			int parentDirsRequired = cwdParts.Length - common;
			for (int i_1 = 0; i_1 < parentDirsRequired; i_1++)
			{
				sb.Append("..");
				sb.Append(Path.Separator);
			}
			// Then append all non-common parts of 'pathname' itself.
			for (int i_2 = common; i_2 < pathParts.Length; i_2++)
			{
				sb.Append(pathParts[i_2]);
				sb.Append(Path.Separator);
			}
			// Don't end with a '/'.
			string s = sb.ToString();
			if (s.EndsWith(Path.Separator))
			{
				s = Sharpen.Runtime.Substring(s, 0, s.Length - 1);
			}
			Log.Info("relative to working: " + pathname + " -> " + s);
			return s;
		}

		[NUnit.Framework.Test]
		public virtual void TestRelativeToWorking()
		{
			NUnit.Framework.Assert.AreEqual(".", RelativeToWorking(Runtime.GetProperty("user.dir"
				, ".")));
			string cwd = Runtime.GetProperty("user.dir", ".");
			Path cwdPath = new Path(cwd);
			Path subdir = new Path(cwdPath, "foo");
			NUnit.Framework.Assert.AreEqual("foo", RelativeToWorking(subdir.ToUri().GetPath()
				));
			Path subsubdir = new Path(subdir, "bar");
			NUnit.Framework.Assert.AreEqual("foo/bar", RelativeToWorking(subsubdir.ToUri().GetPath
				()));
			Path parent = new Path(cwdPath, "..");
			NUnit.Framework.Assert.AreEqual("..", RelativeToWorking(parent.ToUri().GetPath())
				);
			Path sideways = new Path(parent, "baz");
			NUnit.Framework.Assert.AreEqual("../baz", RelativeToWorking(sideways.ToUri().GetPath
				()));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVolumeNormalization()
		{
			Log.Info("TEST_ROOT_DIR is " + TestRootDir);
			string relativeTestRoot = RelativeToWorking(TestRootDir);
			FileSystem localFileSystem = FileSystem.GetLocal(new Configuration());
			string[] vols = new string[] { relativeTestRoot + "/0", relativeTestRoot + "/1" };
			// Put a file in one of the volumes to be cleared on startup.
			Path delDir = new Path(vols[0], MRAsyncDiskService.Tobedeleted);
			localFileSystem.Mkdirs(delDir);
			localFileSystem.Create(new Path(delDir, "foo")).Close();
			MRAsyncDiskService service = new MRAsyncDiskService(localFileSystem, vols);
			MakeSureCleanedUp(vols, service);
		}

		/// <summary>
		/// This test creates some directories and then removes them through
		/// MRAsyncDiskService.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRAsyncDiskService()
		{
			FileSystem localFileSystem = FileSystem.GetLocal(new Configuration());
			string[] vols = new string[] { TestRootDir + "/0", TestRootDir + "/1" };
			MRAsyncDiskService service = new MRAsyncDiskService(localFileSystem, vols);
			string a = "a";
			string b = "b";
			string c = "b/c";
			string d = "d";
			FilePath fa = new FilePath(vols[0], a);
			FilePath fb = new FilePath(vols[1], b);
			FilePath fc = new FilePath(vols[1], c);
			FilePath fd = new FilePath(vols[1], d);
			// Create the directories
			fa.Mkdirs();
			fb.Mkdirs();
			fc.Mkdirs();
			fd.Mkdirs();
			NUnit.Framework.Assert.IsTrue(fa.Exists());
			NUnit.Framework.Assert.IsTrue(fb.Exists());
			NUnit.Framework.Assert.IsTrue(fc.Exists());
			NUnit.Framework.Assert.IsTrue(fd.Exists());
			// Move and delete them
			service.MoveAndDeleteRelativePath(vols[0], a);
			NUnit.Framework.Assert.IsFalse(fa.Exists());
			service.MoveAndDeleteRelativePath(vols[1], b);
			NUnit.Framework.Assert.IsFalse(fb.Exists());
			NUnit.Framework.Assert.IsFalse(fc.Exists());
			NUnit.Framework.Assert.IsFalse(service.MoveAndDeleteRelativePath(vols[1], "not_exists"
				));
			// asyncDiskService is NOT able to delete files outside all volumes.
			IOException ee = null;
			try
			{
				service.MoveAndDeleteAbsolutePath(TestRootDir + "/2");
			}
			catch (IOException e)
			{
				ee = e;
			}
			NUnit.Framework.Assert.IsNotNull("asyncDiskService should not be able to delete files "
				 + "outside all volumes", ee);
			// asyncDiskService is able to automatically find the file in one
			// of the volumes.
			NUnit.Framework.Assert.IsTrue(service.MoveAndDeleteAbsolutePath(vols[1] + Path.SeparatorChar
				 + d));
			// Make sure everything is cleaned up
			MakeSureCleanedUp(vols, service);
		}

		/// <summary>
		/// This test creates some directories inside the volume roots, and then
		/// call asyncDiskService.MoveAndDeleteAllVolumes.
		/// </summary>
		/// <remarks>
		/// This test creates some directories inside the volume roots, and then
		/// call asyncDiskService.MoveAndDeleteAllVolumes.
		/// We should be able to delete all files/dirs inside the volumes except
		/// the toBeDeleted directory.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRAsyncDiskServiceMoveAndDeleteAllVolumes()
		{
			FileSystem localFileSystem = FileSystem.GetLocal(new Configuration());
			string[] vols = new string[] { TestRootDir + "/0", TestRootDir + "/1" };
			MRAsyncDiskService service = new MRAsyncDiskService(localFileSystem, vols);
			string a = "a";
			string b = "b";
			string c = "b/c";
			string d = "d";
			FilePath fa = new FilePath(vols[0], a);
			FilePath fb = new FilePath(vols[1], b);
			FilePath fc = new FilePath(vols[1], c);
			FilePath fd = new FilePath(vols[1], d);
			// Create the directories
			fa.Mkdirs();
			fb.Mkdirs();
			fc.Mkdirs();
			fd.Mkdirs();
			NUnit.Framework.Assert.IsTrue(fa.Exists());
			NUnit.Framework.Assert.IsTrue(fb.Exists());
			NUnit.Framework.Assert.IsTrue(fc.Exists());
			NUnit.Framework.Assert.IsTrue(fd.Exists());
			// Delete all of them
			service.CleanupAllVolumes();
			NUnit.Framework.Assert.IsFalse(fa.Exists());
			NUnit.Framework.Assert.IsFalse(fb.Exists());
			NUnit.Framework.Assert.IsFalse(fc.Exists());
			NUnit.Framework.Assert.IsFalse(fd.Exists());
			// Make sure everything is cleaned up
			MakeSureCleanedUp(vols, service);
		}

		/// <summary>
		/// This test creates some directories inside the toBeDeleted directory and
		/// then start the asyncDiskService.
		/// </summary>
		/// <remarks>
		/// This test creates some directories inside the toBeDeleted directory and
		/// then start the asyncDiskService.
		/// AsyncDiskService will create tasks to delete the content inside the
		/// toBeDeleted directories.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRAsyncDiskServiceStartupCleaning()
		{
			FileSystem localFileSystem = FileSystem.GetLocal(new Configuration());
			string[] vols = new string[] { TestRootDir + "/0", TestRootDir + "/1" };
			string a = "a";
			string b = "b";
			string c = "b/c";
			string d = "d";
			// Create directories inside SUBDIR
			string suffix = Path.SeparatorChar + MRAsyncDiskService.Tobedeleted;
			FilePath fa = new FilePath(vols[0] + suffix, a);
			FilePath fb = new FilePath(vols[1] + suffix, b);
			FilePath fc = new FilePath(vols[1] + suffix, c);
			FilePath fd = new FilePath(vols[1] + suffix, d);
			// Create the directories
			fa.Mkdirs();
			fb.Mkdirs();
			fc.Mkdirs();
			fd.Mkdirs();
			NUnit.Framework.Assert.IsTrue(fa.Exists());
			NUnit.Framework.Assert.IsTrue(fb.Exists());
			NUnit.Framework.Assert.IsTrue(fc.Exists());
			NUnit.Framework.Assert.IsTrue(fd.Exists());
			// Create the asyncDiskService which will delete all contents inside SUBDIR
			MRAsyncDiskService service = new MRAsyncDiskService(localFileSystem, vols);
			// Make sure everything is cleaned up
			MakeSureCleanedUp(vols, service);
		}

		/// <exception cref="System.Exception"/>
		private void MakeSureCleanedUp(string[] vols, MRAsyncDiskService service)
		{
			// Sleep at most 5 seconds to make sure the deleted items are all gone.
			service.Shutdown();
			if (!service.AwaitTermination(5000))
			{
				Fail("MRAsyncDiskService is still not shutdown in 5 seconds!");
			}
			// All contents should be gone by now.
			for (int i = 0; i < vols.Length; i++)
			{
				FilePath subDir = new FilePath(vols[0]);
				string[] subDirContent = subDir.List();
				NUnit.Framework.Assert.AreEqual("Volume should contain a single child: " + MRAsyncDiskService
					.Tobedeleted, 1, subDirContent.Length);
				FilePath toBeDeletedDir = new FilePath(vols[0], MRAsyncDiskService.Tobedeleted);
				string[] content = toBeDeletedDir.List();
				NUnit.Framework.Assert.IsNotNull("Cannot find " + toBeDeletedDir, content);
				NUnit.Framework.Assert.AreEqual(string.Empty + toBeDeletedDir + " should be empty now."
					, 0, content.Length);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestToleratesSomeUnwritableVolumes()
		{
			FileSystem localFileSystem = FileSystem.GetLocal(new Configuration());
			string[] vols = new string[] { TestRootDir + "/0", TestRootDir + "/1" };
			NUnit.Framework.Assert.IsTrue(new FilePath(vols[0]).Mkdirs());
			NUnit.Framework.Assert.AreEqual(0, FileUtil.Chmod(vols[0], "400"));
			// read only
			try
			{
				new MRAsyncDiskService(localFileSystem, vols);
			}
			finally
			{
				FileUtil.Chmod(vols[0], "755");
			}
		}
		// make writable again
	}
}
