using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>
	/// This class is for  setup and teardown for viewFileSystem so that
	/// it can be tested via the standard FileSystem tests.
	/// </summary>
	/// <remarks>
	/// This class is for  setup and teardown for viewFileSystem so that
	/// it can be tested via the standard FileSystem tests.
	/// If tests launched via ant (build.xml) the test root is absolute path
	/// If tests launched via eclipse, the test root is
	/// is a test dir below the working directory. (see FileContextTestHelper)
	/// We set a viewFileSystems with 3 mount points:
	/// 1) /<firstComponent>" of testdir  pointing to same in  target fs
	/// 2)   /<firstComponent>" of home  pointing to same in  target fs
	/// 3)  /<firstComponent>" of wd  pointing to same in  target fs
	/// (note in many cases the link may be the same - viewFileSytem handles this)
	/// We also set the view file system's wd to point to the wd.
	/// </remarks>
	public class ViewFileSystemTestSetup
	{
		public static string ViewFSTestDir = "/testDir";

		/// <param name="fsTarget">- the target fs of the view fs.</param>
		/// <returns>return the ViewFS File context to be used for tests</returns>
		/// <exception cref="System.Exception"/>
		public static FileSystem SetupForViewFileSystem(Configuration conf, FileSystemTestHelper
			 fileSystemTestHelper, FileSystem fsTarget)
		{
			Path targetOfTests = fileSystemTestHelper.GetTestRootPath(fsTarget);
			// In case previous test was killed before cleanup
			fsTarget.Delete(targetOfTests, true);
			fsTarget.Mkdirs(targetOfTests);
			// Set up viewfs link for test dir as described above
			string testDir = fileSystemTestHelper.GetTestRootPath(fsTarget).ToUri().GetPath();
			LinkUpFirstComponents(conf, testDir, fsTarget, "test dir");
			// Set up viewfs link for home dir as described above
			SetUpHomeDir(conf, fsTarget);
			// the test path may be relative to working dir - we need to make that work:
			// Set up viewfs link for wd as described above
			string wdDir = fsTarget.GetWorkingDirectory().ToUri().GetPath();
			LinkUpFirstComponents(conf, wdDir, fsTarget, "working dir");
			FileSystem fsView = FileSystem.Get(FsConstants.ViewfsUri, conf);
			fsView.SetWorkingDirectory(new Path(wdDir));
			// in case testdir relative to wd.
			Org.Mortbay.Log.Log.Info("Working dir is: " + fsView.GetWorkingDirectory());
			return fsView;
		}

		/// <summary>delete the test directory in the target  fs</summary>
		/// <exception cref="System.Exception"/>
		public static void TearDown(FileSystemTestHelper fileSystemTestHelper, FileSystem
			 fsTarget)
		{
			Path targetOfTests = fileSystemTestHelper.GetTestRootPath(fsTarget);
			fsTarget.Delete(targetOfTests, true);
		}

		public static Configuration CreateConfig()
		{
			return CreateConfig(true);
		}

		public static Configuration CreateConfig(bool disableCache)
		{
			Configuration conf = new Configuration();
			conf.Set("fs.viewfs.impl", typeof(ViewFileSystem).FullName);
			if (disableCache)
			{
				conf.Set("fs.viewfs.impl.disable.cache", "true");
			}
			return conf;
		}

		internal static void SetUpHomeDir(Configuration conf, FileSystem fsTarget)
		{
			string homeDir = fsTarget.GetHomeDirectory().ToUri().GetPath();
			int indexOf2ndSlash = homeDir.IndexOf('/', 1);
			if (indexOf2ndSlash > 0)
			{
				LinkUpFirstComponents(conf, homeDir, fsTarget, "home dir");
			}
			else
			{
				// home dir is at root. Just link the home dir itse
				URI linkTarget = fsTarget.MakeQualified(new Path(homeDir)).ToUri();
				ConfigUtil.AddLink(conf, homeDir, linkTarget);
				Org.Mortbay.Log.Log.Info("Added link for home dir " + homeDir + "->" + linkTarget
					);
			}
			// Now set the root of the home dir for viewfs
			string homeDirRoot = fsTarget.GetHomeDirectory().GetParent().ToUri().GetPath();
			ConfigUtil.SetHomeDirConf(conf, homeDirRoot);
			Org.Mortbay.Log.Log.Info("Home dir base for viewfs" + homeDirRoot);
		}

		/*
		* Set up link in config for first component of path to the same
		* in the target file system.
		*/
		internal static void LinkUpFirstComponents(Configuration conf, string path, FileSystem
			 fsTarget, string info)
		{
			int indexOfEnd = path.IndexOf('/', 1);
			if (Shell.Windows)
			{
				indexOfEnd = path.IndexOf('/', indexOfEnd + 1);
			}
			string firstComponent = Sharpen.Runtime.Substring(path, 0, indexOfEnd);
			URI linkTarget = fsTarget.MakeQualified(new Path(firstComponent)).ToUri();
			ConfigUtil.AddLink(conf, firstComponent, linkTarget);
			Org.Mortbay.Log.Log.Info("Added link for " + info + " " + firstComponent + "->" +
				 linkTarget);
		}
	}
}
