using Sharpen;

namespace org.apache.hadoop.fs.viewfs
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
		public static org.apache.hadoop.fs.FileSystem setupForViewFileSystem(org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.fs.FileSystemTestHelper fileSystemTestHelper, org.apache.hadoop.fs.FileSystem
			 fsTarget)
		{
			org.apache.hadoop.fs.Path targetOfTests = fileSystemTestHelper.getTestRootPath(fsTarget
				);
			// In case previous test was killed before cleanup
			fsTarget.delete(targetOfTests, true);
			fsTarget.mkdirs(targetOfTests);
			// Set up viewfs link for test dir as described above
			string testDir = fileSystemTestHelper.getTestRootPath(fsTarget).toUri().getPath();
			linkUpFirstComponents(conf, testDir, fsTarget, "test dir");
			// Set up viewfs link for home dir as described above
			setUpHomeDir(conf, fsTarget);
			// the test path may be relative to working dir - we need to make that work:
			// Set up viewfs link for wd as described above
			string wdDir = fsTarget.getWorkingDirectory().toUri().getPath();
			linkUpFirstComponents(conf, wdDir, fsTarget, "working dir");
			org.apache.hadoop.fs.FileSystem fsView = org.apache.hadoop.fs.FileSystem.get(org.apache.hadoop.fs.FsConstants
				.VIEWFS_URI, conf);
			fsView.setWorkingDirectory(new org.apache.hadoop.fs.Path(wdDir));
			// in case testdir relative to wd.
			org.mortbay.log.Log.info("Working dir is: " + fsView.getWorkingDirectory());
			return fsView;
		}

		/// <summary>delete the test directory in the target  fs</summary>
		/// <exception cref="System.Exception"/>
		public static void tearDown(org.apache.hadoop.fs.FileSystemTestHelper fileSystemTestHelper
			, org.apache.hadoop.fs.FileSystem fsTarget)
		{
			org.apache.hadoop.fs.Path targetOfTests = fileSystemTestHelper.getTestRootPath(fsTarget
				);
			fsTarget.delete(targetOfTests, true);
		}

		public static org.apache.hadoop.conf.Configuration createConfig()
		{
			return createConfig(true);
		}

		public static org.apache.hadoop.conf.Configuration createConfig(bool disableCache
			)
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("fs.viewfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.viewfs.ViewFileSystem
				)).getName());
			if (disableCache)
			{
				conf.set("fs.viewfs.impl.disable.cache", "true");
			}
			return conf;
		}

		internal static void setUpHomeDir(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
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
			, string path, org.apache.hadoop.fs.FileSystem fsTarget, string info)
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
