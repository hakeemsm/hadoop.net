using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server
{
	/// <summary>
	/// Verify if NodeManager's in-memory good local dirs list and good log dirs list
	/// get updated properly when disks(nm-local-dirs and nm-log-dirs) fail.
	/// </summary>
	/// <remarks>
	/// Verify if NodeManager's in-memory good local dirs list and good log dirs list
	/// get updated properly when disks(nm-local-dirs and nm-log-dirs) fail. Also
	/// verify if the overall health status of the node gets updated properly when
	/// specified percentage of disks fail.
	/// </remarks>
	public class TestDiskFailures
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDiskFailures));

		private const long DiskHealthCheckInterval = 1000;

		private static FileContext localFS = null;

		private static readonly FilePath testDir = new FilePath("target", typeof(TestDiskFailures
			).FullName).GetAbsoluteFile();

		private static readonly FilePath localFSDirBase = new FilePath(testDir, typeof(TestDiskFailures
			).FullName + "-localDir");

		private const int numLocalDirs = 4;

		private const int numLogDirs = 4;

		private static MiniYARNCluster yarnCluster;

		internal LocalDirsHandlerService dirsHandler;

		//1 sec
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			localFS = FileContext.GetLocalFSFileContext();
			localFS.Delete(new Path(localFSDirBase.GetAbsolutePath()), true);
			localFSDirBase.Mkdirs();
		}

		// Do not start cluster here
		[AfterClass]
		public static void Teardown()
		{
			if (yarnCluster != null)
			{
				yarnCluster.Stop();
				yarnCluster = null;
			}
			FileUtil.FullyDelete(localFSDirBase);
		}

		/// <summary>
		/// Make local-dirs fail/inaccessible and verify if NodeManager can
		/// recognize the disk failures properly and can update the list of
		/// local-dirs accordingly with good disks.
		/// </summary>
		/// <remarks>
		/// Make local-dirs fail/inaccessible and verify if NodeManager can
		/// recognize the disk failures properly and can update the list of
		/// local-dirs accordingly with good disks. Also verify the overall
		/// health status of the node.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalDirsFailures()
		{
			TestDirsFailures(true);
		}

		/// <summary>
		/// Make log-dirs fail/inaccessible and verify if NodeManager can
		/// recognize the disk failures properly and can update the list of
		/// log-dirs accordingly with good disks.
		/// </summary>
		/// <remarks>
		/// Make log-dirs fail/inaccessible and verify if NodeManager can
		/// recognize the disk failures properly and can update the list of
		/// log-dirs accordingly with good disks. Also verify the overall health
		/// status of the node.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLogDirsFailures()
		{
			TestDirsFailures(false);
		}

		/// <summary>
		/// Make a local and log directory inaccessible during initialization
		/// and verify those bad directories are recognized and removed from
		/// the list of available local and log directories.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDirFailuresOnStartup()
		{
			Configuration conf = new YarnConfiguration();
			string localDir1 = new FilePath(testDir, "localDir1").GetPath();
			string localDir2 = new FilePath(testDir, "localDir2").GetPath();
			string logDir1 = new FilePath(testDir, "logDir1").GetPath();
			string logDir2 = new FilePath(testDir, "logDir2").GetPath();
			conf.Set(YarnConfiguration.NmLocalDirs, localDir1 + "," + localDir2);
			conf.Set(YarnConfiguration.NmLogDirs, logDir1 + "," + logDir2);
			PrepareDirToFail(localDir1);
			PrepareDirToFail(logDir2);
			LocalDirsHandlerService dirSvc = new LocalDirsHandlerService();
			dirSvc.Init(conf);
			IList<string> localDirs = dirSvc.GetLocalDirs();
			NUnit.Framework.Assert.AreEqual(1, localDirs.Count);
			NUnit.Framework.Assert.AreEqual(new Path(localDir2).ToString(), localDirs[0]);
			IList<string> logDirs = dirSvc.GetLogDirs();
			NUnit.Framework.Assert.AreEqual(1, logDirs.Count);
			NUnit.Framework.Assert.AreEqual(new Path(logDir1).ToString(), logDirs[0]);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestDirsFailures(bool localORLogDirs)
		{
			string dirType = localORLogDirs ? "local" : "log";
			string dirsProperty = localORLogDirs ? YarnConfiguration.NmLocalDirs : YarnConfiguration
				.NmLogDirs;
			Configuration conf = new Configuration();
			// set disk health check interval to a small value (say 1 sec).
			conf.SetLong(YarnConfiguration.NmDiskHealthCheckIntervalMs, DiskHealthCheckInterval
				);
			// If 2 out of the total 4 local-dirs fail OR if 2 Out of the total 4
			// log-dirs fail, then the node's health status should become unhealthy.
			conf.SetFloat(YarnConfiguration.NmMinHealthyDisksFraction, 0.60F);
			if (yarnCluster != null)
			{
				yarnCluster.Stop();
				FileUtil.FullyDelete(localFSDirBase);
				localFSDirBase.Mkdirs();
			}
			Log.Info("Starting up YARN cluster");
			yarnCluster = new MiniYARNCluster(typeof(TestDiskFailures).FullName, 1, numLocalDirs
				, numLogDirs);
			yarnCluster.Init(conf);
			yarnCluster.Start();
			NodeManager nm = yarnCluster.GetNodeManager(0);
			Log.Info("Configured nm-" + dirType + "-dirs=" + nm.GetConfig().Get(dirsProperty)
				);
			dirsHandler = nm.GetNodeHealthChecker().GetDiskHandler();
			IList<string> list = localORLogDirs ? dirsHandler.GetLocalDirs() : dirsHandler.GetLogDirs
				();
			string[] dirs = Sharpen.Collections.ToArray(list, new string[list.Count]);
			NUnit.Framework.Assert.AreEqual("Number of nm-" + dirType + "-dirs is wrong.", numLocalDirs
				, dirs.Length);
			string expectedDirs = StringUtils.Join(",", list);
			// validate the health of disks initially
			VerifyDisksHealth(localORLogDirs, expectedDirs, true);
			// Make 1 nm-local-dir fail and verify if "the nodemanager can identify
			// the disk failure(s) and can update the list of good nm-local-dirs.
			PrepareDirToFail(dirs[2]);
			expectedDirs = dirs[0] + "," + dirs[1] + "," + dirs[3];
			VerifyDisksHealth(localORLogDirs, expectedDirs, true);
			// Now, make 1 more nm-local-dir/nm-log-dir fail and verify if "the
			// nodemanager can identify the disk failures and can update the list of
			// good nm-local-dirs/nm-log-dirs and can update the overall health status
			// of the node to unhealthy".
			PrepareDirToFail(dirs[0]);
			expectedDirs = dirs[1] + "," + dirs[3];
			VerifyDisksHealth(localORLogDirs, expectedDirs, false);
			// Fail the remaining 2 local-dirs/log-dirs and verify if NM remains with
			// empty list of local-dirs/log-dirs and the overall health status is
			// unhealthy.
			PrepareDirToFail(dirs[1]);
			PrepareDirToFail(dirs[3]);
			expectedDirs = string.Empty;
			VerifyDisksHealth(localORLogDirs, expectedDirs, false);
		}

		/// <summary>Wait for the NodeManger to go for the disk-health-check at least once.</summary>
		private void WaitForDiskHealthCheck()
		{
			long lastDisksCheckTime = dirsHandler.GetLastDisksCheckTime();
			long time = lastDisksCheckTime;
			for (int i = 0; i < 10 && (time <= lastDisksCheckTime); i++)
			{
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
					Log.Error("Interrupted while waiting for NodeManager's disk health check.");
				}
				time = dirsHandler.GetLastDisksCheckTime();
			}
		}

		/// <summary>Verify if the NodeManager could identify disk failures.</summary>
		/// <param name="localORLogDirs">
		/// <em>true</em> represent nm-local-dirs and <em>false
		/// </em> means nm-log-dirs
		/// </param>
		/// <param name="expectedDirs">expected nm-local-dirs/nm-log-dirs as a string</param>
		/// <param name="isHealthy"><em>true</em> if the overall node should be healthy</param>
		private void VerifyDisksHealth(bool localORLogDirs, string expectedDirs, bool isHealthy
			)
		{
			// Wait for the NodeManager to identify disk failures.
			WaitForDiskHealthCheck();
			IList<string> list = localORLogDirs ? dirsHandler.GetLocalDirs() : dirsHandler.GetLogDirs
				();
			string seenDirs = StringUtils.Join(",", list);
			Log.Info("ExpectedDirs=" + expectedDirs);
			Log.Info("SeenDirs=" + seenDirs);
			NUnit.Framework.Assert.IsTrue("NodeManager could not identify disk failure.", expectedDirs
				.Equals(seenDirs));
			NUnit.Framework.Assert.AreEqual("Node's health in terms of disks is wrong", isHealthy
				, dirsHandler.AreDisksHealthy());
			for (int i = 0; i < 10; i++)
			{
				IEnumerator<RMNode> iter = yarnCluster.GetResourceManager().GetRMContext().GetRMNodes
					().Values.GetEnumerator();
				if ((iter.Next().GetState() != NodeState.Unhealthy) == isHealthy)
				{
					break;
				}
				// wait for the node health info to go to RM
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
					Log.Error("Interrupted while waiting for NM->RM heartbeat.");
				}
			}
			IEnumerator<RMNode> iter_1 = yarnCluster.GetResourceManager().GetRMContext().GetRMNodes
				().Values.GetEnumerator();
			NUnit.Framework.Assert.AreEqual("RM is not updated with the health status of a node"
				, isHealthy, iter_1.Next().GetState() != NodeState.Unhealthy);
		}

		/// <summary>
		/// Prepare directory for a failure: Replace the given directory on the
		/// local FileSystem with a regular file with the same name.
		/// </summary>
		/// <remarks>
		/// Prepare directory for a failure: Replace the given directory on the
		/// local FileSystem with a regular file with the same name.
		/// This would cause failure of creation of directory in DiskChecker.checkDir()
		/// with the same name.
		/// </remarks>
		/// <param name="dir">the directory to be failed</param>
		/// <exception cref="System.IO.IOException"></exception>
		private void PrepareDirToFail(string dir)
		{
			FilePath file = new FilePath(dir);
			FileUtil.FullyDelete(file);
			file.CreateNewFile();
			Log.Info("Prepared " + dir + " to fail.");
		}
	}
}
