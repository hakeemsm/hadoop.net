using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>
	/// This class is for  setup and teardown for viewFs so that
	/// it can be tested via the standard FileContext tests.
	/// </summary>
	/// <remarks>
	/// This class is for  setup and teardown for viewFs so that
	/// it can be tested via the standard FileContext tests.
	/// If tests launched via ant (build.xml) the test root is absolute path
	/// If tests launched via eclipse, the test root is
	/// is a test dir below the working directory. (see FileContextTestHelper)
	/// We set a viewfs with 3 mount points:
	/// 1) /<firstComponent>" of testdir  pointing to same in  target fs
	/// 2)   /<firstComponent>" of home  pointing to same in  target fs
	/// 3)  /<firstComponent>" of wd  pointing to same in  target fs
	/// (note in many cases the link may be the same - viewfs handles this)
	/// We also set the view file system's wd to point to the wd.
	/// </remarks>
	public class ViewFsTestSetup
	{
		public static string ViewFSTestDir = "/testDir";

		/*
		* return the ViewFS File context to be used for tests
		*/
		/// <exception cref="System.Exception"/>
		public static FileContext SetupForViewFsLocalFs(FileContextTestHelper helper)
		{
			FileContext fsTarget = FileContext.GetLocalFSFileContext();
			Path targetOfTests = helper.GetTestRootPath(fsTarget);
			// In case previous test was killed before cleanup
			fsTarget.Delete(targetOfTests, true);
			fsTarget.Mkdir(targetOfTests, FileContext.DefaultPerm, true);
			Configuration conf = new Configuration();
			// Set up viewfs link for test dir as described above
			string testDir = helper.GetTestRootPath(fsTarget).ToUri().GetPath();
			LinkUpFirstComponents(conf, testDir, fsTarget, "test dir");
			// Set up viewfs link for home dir as described above
			SetUpHomeDir(conf, fsTarget);
			// the test path may be relative to working dir - we need to make that work:
			// Set up viewfs link for wd as described above
			string wdDir = fsTarget.GetWorkingDirectory().ToUri().GetPath();
			LinkUpFirstComponents(conf, wdDir, fsTarget, "working dir");
			FileContext fc = FileContext.GetFileContext(FsConstants.ViewfsUri, conf);
			fc.SetWorkingDirectory(new Path(wdDir));
			// in case testdir relative to wd.
			Org.Mortbay.Log.Log.Info("Working dir is: " + fc.GetWorkingDirectory());
			//System.out.println("SRCOfTests = "+ getTestRootPath(fc, "test"));
			//System.out.println("TargetOfTests = "+ targetOfTests.toUri());
			return fc;
		}

		/// <summary>delete the test directory in the target local fs</summary>
		/// <exception cref="System.Exception"/>
		public static void TearDownForViewFsLocalFs(FileContextTestHelper helper)
		{
			FileContext fclocal = FileContext.GetLocalFSFileContext();
			Path targetOfTests = helper.GetTestRootPath(fclocal);
			fclocal.Delete(targetOfTests, true);
		}

		internal static void SetUpHomeDir(Configuration conf, FileContext fsTarget)
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
		internal static void LinkUpFirstComponents(Configuration conf, string path, FileContext
			 fsTarget, string info)
		{
			int indexOfEnd = path.IndexOf('/', 1);
			if (Shell.Windows)
			{
				indexOfEnd = path.IndexOf('/', indexOfEnd + 1);
			}
			string firstComponent = Runtime.Substring(path, 0, indexOfEnd);
			URI linkTarget = fsTarget.MakeQualified(new Path(firstComponent)).ToUri();
			ConfigUtil.AddLink(conf, firstComponent, linkTarget);
			Org.Mortbay.Log.Log.Info("Added link for " + info + " " + firstComponent + "->" +
				 linkTarget);
		}
	}
}
