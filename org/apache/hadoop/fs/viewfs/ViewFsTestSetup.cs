using Sharpen;

namespace org.apache.hadoop.fs.viewfs
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
		public static org.apache.hadoop.fs.FileContext setupForViewFsLocalFs(org.apache.hadoop.fs.FileContextTestHelper
			 helper)
		{
			org.apache.hadoop.fs.FileContext fsTarget = org.apache.hadoop.fs.FileContext.getLocalFSFileContext
				();
			org.apache.hadoop.fs.Path targetOfTests = helper.getTestRootPath(fsTarget);
			// In case previous test was killed before cleanup
			fsTarget.delete(targetOfTests, true);
			fsTarget.mkdir(targetOfTests, org.apache.hadoop.fs.FileContext.DEFAULT_PERM, true
				);
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			// Set up viewfs link for test dir as described above
			string testDir = helper.getTestRootPath(fsTarget).toUri().getPath();
			linkUpFirstComponents(conf, testDir, fsTarget, "test dir");
			// Set up viewfs link for home dir as described above
			setUpHomeDir(conf, fsTarget);
			// the test path may be relative to working dir - we need to make that work:
			// Set up viewfs link for wd as described above
			string wdDir = fsTarget.getWorkingDirectory().toUri().getPath();
			linkUpFirstComponents(conf, wdDir, fsTarget, "working dir");
			org.apache.hadoop.fs.FileContext fc = org.apache.hadoop.fs.FileContext.getFileContext
				(org.apache.hadoop.fs.FsConstants.VIEWFS_URI, conf);
			fc.setWorkingDirectory(new org.apache.hadoop.fs.Path(wdDir));
			// in case testdir relative to wd.
			org.mortbay.log.Log.info("Working dir is: " + fc.getWorkingDirectory());
			//System.out.println("SRCOfTests = "+ getTestRootPath(fc, "test"));
			//System.out.println("TargetOfTests = "+ targetOfTests.toUri());
			return fc;
		}

		/// <summary>delete the test directory in the target local fs</summary>
		/// <exception cref="System.Exception"/>
		public static void tearDownForViewFsLocalFs(org.apache.hadoop.fs.FileContextTestHelper
			 helper)
		{
			org.apache.hadoop.fs.FileContext fclocal = org.apache.hadoop.fs.FileContext.getLocalFSFileContext
				();
			org.apache.hadoop.fs.Path targetOfTests = helper.getTestRootPath(fclocal);
			fclocal.delete(targetOfTests, true);
		}

		internal static void setUpHomeDir(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileContext
			 fsTarget)
		{
			string homeDir = fsTarget.getHomeDirectory().toUri().getPath();
			int indexOf2ndSlash = homeDir.IndexOf('/', 1);
			if (indexOf2ndSlash > 0)
			{
				linkUpFirstComponents(conf, homeDir, fsTarget, "home dir");
			}
			else
			{
				// home dir is at root. Just link the home dir itse
				java.net.URI linkTarget = fsTarget.makeQualified(new org.apache.hadoop.fs.Path(homeDir
					)).toUri();
				org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, homeDir, linkTarget);
				org.mortbay.log.Log.info("Added link for home dir " + homeDir + "->" + linkTarget
					);
			}
			// Now set the root of the home dir for viewfs
			string homeDirRoot = fsTarget.getHomeDirectory().getParent().toUri().getPath();
			org.apache.hadoop.fs.viewfs.ConfigUtil.setHomeDirConf(conf, homeDirRoot);
			org.mortbay.log.Log.info("Home dir base for viewfs" + homeDirRoot);
		}

		/*
		* Set up link in config for first component of path to the same
		* in the target file system.
		*/
		internal static void linkUpFirstComponents(org.apache.hadoop.conf.Configuration conf
			, string path, org.apache.hadoop.fs.FileContext fsTarget, string info)
		{
			int indexOfEnd = path.IndexOf('/', 1);
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				indexOfEnd = path.IndexOf('/', indexOfEnd + 1);
			}
			string firstComponent = Sharpen.Runtime.substring(path, 0, indexOfEnd);
			java.net.URI linkTarget = fsTarget.makeQualified(new org.apache.hadoop.fs.Path(firstComponent
				)).toUri();
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, firstComponent, linkTarget);
			org.mortbay.log.Log.info("Added link for " + info + " " + firstComponent + "->" +
				 linkTarget);
		}
	}
}
