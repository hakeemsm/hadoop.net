using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util
{
	public class TestCgroupsLCEResourcesHandler
	{
		internal static FilePath cgroupDir = null;

		internal class MockClock : Clock
		{
			internal long time;

			public virtual long GetTime()
			{
				return time;
			}
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUp()
		{
			cgroupDir = new FilePath(Runtime.GetProperty("test.build.data", Runtime.GetProperty
				("java.io.tmpdir", "target")), this.GetType().FullName);
			FileUtils.DeleteQuietly(cgroupDir);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void TearDown()
		{
			FileUtils.DeleteQuietly(cgroupDir);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestcheckAndDeleteCgroup()
		{
			CgroupsLCEResourcesHandler handler = new CgroupsLCEResourcesHandler();
			handler.SetConf(new YarnConfiguration());
			handler.InitConfig();
			FileUtils.DeleteQuietly(cgroupDir);
			// Test 0
			// tasks file not present, should return false
			NUnit.Framework.Assert.IsFalse(handler.CheckAndDeleteCgroup(cgroupDir));
			FilePath tfile = new FilePath(cgroupDir.GetAbsolutePath(), "tasks");
			FileOutputStream fos = FileUtils.OpenOutputStream(tfile);
			FilePath fspy = Org.Mockito.Mockito.Spy(cgroupDir);
			// Test 1, tasks file is empty
			// tasks file has no data, should return true
			Org.Mockito.Mockito.Stub(fspy.Delete()).ToReturn(true);
			NUnit.Framework.Assert.IsTrue(handler.CheckAndDeleteCgroup(fspy));
			// Test 2, tasks file has data
			fos.Write(Sharpen.Runtime.GetBytesForString("1234"));
			fos.Close();
			// tasks has data, would not be able to delete, should return false
			NUnit.Framework.Assert.IsFalse(handler.CheckAndDeleteCgroup(fspy));
			FileUtils.DeleteQuietly(cgroupDir);
		}

		// Verify DeleteCgroup times out if "tasks" file contains data
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteCgroup()
		{
			TestCgroupsLCEResourcesHandler.MockClock clock = new TestCgroupsLCEResourcesHandler.MockClock
				();
			clock.time = Runtime.CurrentTimeMillis();
			CgroupsLCEResourcesHandler handler = new CgroupsLCEResourcesHandler();
			handler.SetConf(new YarnConfiguration());
			handler.InitConfig();
			handler.clock = clock;
			FileUtils.DeleteQuietly(cgroupDir);
			// Create a non-empty tasks file
			FilePath tfile = new FilePath(cgroupDir.GetAbsolutePath(), "tasks");
			FileOutputStream fos = FileUtils.OpenOutputStream(tfile);
			fos.Write(Sharpen.Runtime.GetBytesForString("1234"));
			fos.Close();
			CountDownLatch latch = new CountDownLatch(1);
			new _Thread_112(latch, clock).Start();
			//NOP
			latch.Await();
			NUnit.Framework.Assert.IsFalse(handler.DeleteCgroup(cgroupDir.GetAbsolutePath()));
			FileUtils.DeleteQuietly(cgroupDir);
		}

		private sealed class _Thread_112 : Sharpen.Thread
		{
			public _Thread_112(CountDownLatch latch, TestCgroupsLCEResourcesHandler.MockClock
				 clock)
			{
				this.latch = latch;
				this.clock = clock;
			}

			public override void Run()
			{
				latch.CountDown();
				try
				{
					Sharpen.Thread.Sleep(200);
				}
				catch (Exception)
				{
				}
				clock.time += YarnConfiguration.DefaultNmLinuxContainerCgroupsDeleteTimeout;
			}

			private readonly CountDownLatch latch;

			private readonly TestCgroupsLCEResourcesHandler.MockClock clock;
		}

		internal class MockLinuxContainerExecutor : LinuxContainerExecutor
		{
			public override void MountCgroups(IList<string> x, string y)
			{
			}
		}

		internal class CustomCgroupsLCEResourceHandler : CgroupsLCEResourcesHandler
		{
			internal string mtabFile;

			internal int[] limits = new int[2];

			internal bool generateLimitsMode = false;

			internal override int[] GetOverallLimits(float x)
			{
				if (generateLimitsMode == true)
				{
					return base.GetOverallLimits(x);
				}
				return limits;
			}

			internal virtual void SetMtabFile(string file)
			{
				mtabFile = file;
			}

			internal override string GetMtabFileName()
			{
				return mtabFile;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInit()
		{
			LinuxContainerExecutor mockLCE = new TestCgroupsLCEResourcesHandler.MockLinuxContainerExecutor
				();
			TestCgroupsLCEResourcesHandler.CustomCgroupsLCEResourceHandler handler = new TestCgroupsLCEResourcesHandler.CustomCgroupsLCEResourceHandler
				();
			YarnConfiguration conf = new YarnConfiguration();
			int numProcessors = 4;
			ResourceCalculatorPlugin plugin = Org.Mockito.Mockito.Mock<ResourceCalculatorPlugin
				>();
			Org.Mockito.Mockito.DoReturn(numProcessors).When(plugin).GetNumProcessors();
			handler.SetConf(conf);
			handler.InitConfig();
			// create mock cgroup
			FilePath cgroupMountDir = CreateMockCgroupMount(cgroupDir);
			// create mock mtab
			FilePath mockMtab = CreateMockMTab(cgroupDir);
			// setup our handler and call init()
			handler.SetMtabFile(mockMtab.GetAbsolutePath());
			// check values
			// in this case, we're using all cpu so the files
			// shouldn't exist(because init won't create them
			handler.Init(mockLCE, plugin);
			FilePath periodFile = new FilePath(cgroupMountDir, "cpu.cfs_period_us");
			FilePath quotaFile = new FilePath(cgroupMountDir, "cpu.cfs_quota_us");
			NUnit.Framework.Assert.IsFalse(periodFile.Exists());
			NUnit.Framework.Assert.IsFalse(quotaFile.Exists());
			// subset of cpu being used, files should be created
			conf.SetInt(YarnConfiguration.NmResourcePercentagePhysicalCpuLimit, 75);
			handler.limits[0] = 100 * 1000;
			handler.limits[1] = 1000 * 1000;
			handler.Init(mockLCE, plugin);
			int period = ReadIntFromFile(periodFile);
			int quota = ReadIntFromFile(quotaFile);
			NUnit.Framework.Assert.AreEqual(100 * 1000, period);
			NUnit.Framework.Assert.AreEqual(1000 * 1000, quota);
			// set cpu back to 100, quota should be -1
			conf.SetInt(YarnConfiguration.NmResourcePercentagePhysicalCpuLimit, 100);
			handler.limits[0] = 100 * 1000;
			handler.limits[1] = 1000 * 1000;
			handler.Init(mockLCE, plugin);
			quota = ReadIntFromFile(quotaFile);
			NUnit.Framework.Assert.AreEqual(-1, quota);
			FileUtils.DeleteQuietly(cgroupDir);
		}

		/// <exception cref="System.IO.IOException"/>
		private int ReadIntFromFile(FilePath targetFile)
		{
			Scanner scanner = new Scanner(targetFile);
			try
			{
				return scanner.HasNextInt() ? scanner.NextInt() : -1;
			}
			finally
			{
				scanner.Close();
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestGetOverallLimits()
		{
			int expectedQuota = 1000 * 1000;
			CgroupsLCEResourcesHandler handler = new CgroupsLCEResourcesHandler();
			int[] ret = handler.GetOverallLimits(2);
			NUnit.Framework.Assert.AreEqual(expectedQuota / 2, ret[0]);
			NUnit.Framework.Assert.AreEqual(expectedQuota, ret[1]);
			ret = handler.GetOverallLimits(2000);
			NUnit.Framework.Assert.AreEqual(expectedQuota, ret[0]);
			NUnit.Framework.Assert.AreEqual(-1, ret[1]);
			int[] @params = new int[] { 0, -1 };
			foreach (int cores in @params)
			{
				try
				{
					handler.GetOverallLimits(cores);
					NUnit.Framework.Assert.Fail("Function call should throw error.");
				}
				catch (ArgumentException)
				{
				}
			}
			// expected
			// test minimums
			ret = handler.GetOverallLimits(1000 * 1000);
			NUnit.Framework.Assert.AreEqual(1000 * 1000, ret[0]);
			NUnit.Framework.Assert.AreEqual(-1, ret[1]);
		}

		/// <exception cref="System.IO.IOException"/>
		private FilePath CreateMockCgroupMount(FilePath cgroupDir)
		{
			FilePath cgroupMountDir = new FilePath(cgroupDir.GetAbsolutePath(), "hadoop-yarn"
				);
			FileUtils.DeleteQuietly(cgroupDir);
			if (!cgroupMountDir.Mkdirs())
			{
				string message = "Could not create dir " + cgroupMountDir.GetAbsolutePath();
				throw new IOException(message);
			}
			return cgroupMountDir;
		}

		/// <exception cref="System.IO.IOException"/>
		private FilePath CreateMockMTab(FilePath cgroupDir)
		{
			string mtabContent = "none " + cgroupDir.GetAbsolutePath() + " cgroup rw,relatime,cpu 0 0";
			FilePath mockMtab = new FilePath("target", UUID.RandomUUID().ToString());
			if (!mockMtab.Exists())
			{
				if (!mockMtab.CreateNewFile())
				{
					string message = "Could not create file " + mockMtab.GetAbsolutePath();
					throw new IOException(message);
				}
			}
			FileWriter mtabWriter = new FileWriter(mockMtab.GetAbsoluteFile());
			mtabWriter.Write(mtabContent);
			mtabWriter.Close();
			mockMtab.DeleteOnExit();
			return mockMtab;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLimits()
		{
			LinuxContainerExecutor mockLCE = new TestCgroupsLCEResourcesHandler.MockLinuxContainerExecutor
				();
			TestCgroupsLCEResourcesHandler.CustomCgroupsLCEResourceHandler handler = new TestCgroupsLCEResourcesHandler.CustomCgroupsLCEResourceHandler
				();
			handler.generateLimitsMode = true;
			YarnConfiguration conf = new YarnConfiguration();
			int numProcessors = 4;
			ResourceCalculatorPlugin plugin = Org.Mockito.Mockito.Mock<ResourceCalculatorPlugin
				>();
			Org.Mockito.Mockito.DoReturn(numProcessors).When(plugin).GetNumProcessors();
			handler.SetConf(conf);
			handler.InitConfig();
			// create mock cgroup
			FilePath cgroupMountDir = CreateMockCgroupMount(cgroupDir);
			// create mock mtab
			FilePath mockMtab = CreateMockMTab(cgroupDir);
			// setup our handler and call init()
			handler.SetMtabFile(mockMtab.GetAbsolutePath());
			handler.Init(mockLCE, plugin);
			// check values
			// default case - files shouldn't exist, strict mode off by default
			ContainerId id = ContainerId.FromString("container_1_1_1_1");
			handler.PreExecute(id, Resource.NewInstance(1024, 1));
			FilePath containerDir = new FilePath(cgroupMountDir, id.ToString());
			NUnit.Framework.Assert.IsTrue(containerDir.Exists());
			NUnit.Framework.Assert.IsTrue(containerDir.IsDirectory());
			FilePath periodFile = new FilePath(containerDir, "cpu.cfs_period_us");
			FilePath quotaFile = new FilePath(containerDir, "cpu.cfs_quota_us");
			NUnit.Framework.Assert.IsFalse(periodFile.Exists());
			NUnit.Framework.Assert.IsFalse(quotaFile.Exists());
			// no files created because we're using all cpu
			FileUtils.DeleteQuietly(containerDir);
			conf.SetBoolean(YarnConfiguration.NmLinuxContainerCgroupsStrictResourceUsage, true
				);
			handler.InitConfig();
			handler.PreExecute(id, Resource.NewInstance(1024, YarnConfiguration.DefaultNmVcores
				));
			NUnit.Framework.Assert.IsTrue(containerDir.Exists());
			NUnit.Framework.Assert.IsTrue(containerDir.IsDirectory());
			periodFile = new FilePath(containerDir, "cpu.cfs_period_us");
			quotaFile = new FilePath(containerDir, "cpu.cfs_quota_us");
			NUnit.Framework.Assert.IsFalse(periodFile.Exists());
			NUnit.Framework.Assert.IsFalse(quotaFile.Exists());
			// 50% of CPU
			FileUtils.DeleteQuietly(containerDir);
			conf.SetBoolean(YarnConfiguration.NmLinuxContainerCgroupsStrictResourceUsage, true
				);
			handler.InitConfig();
			handler.PreExecute(id, Resource.NewInstance(1024, YarnConfiguration.DefaultNmVcores
				 / 2));
			NUnit.Framework.Assert.IsTrue(containerDir.Exists());
			NUnit.Framework.Assert.IsTrue(containerDir.IsDirectory());
			periodFile = new FilePath(containerDir, "cpu.cfs_period_us");
			quotaFile = new FilePath(containerDir, "cpu.cfs_quota_us");
			NUnit.Framework.Assert.IsTrue(periodFile.Exists());
			NUnit.Framework.Assert.IsTrue(quotaFile.Exists());
			NUnit.Framework.Assert.AreEqual(500 * 1000, ReadIntFromFile(periodFile));
			NUnit.Framework.Assert.AreEqual(1000 * 1000, ReadIntFromFile(quotaFile));
			// CGroups set to 50% of CPU, container set to 50% of YARN CPU
			FileUtils.DeleteQuietly(containerDir);
			conf.SetBoolean(YarnConfiguration.NmLinuxContainerCgroupsStrictResourceUsage, true
				);
			conf.SetInt(YarnConfiguration.NmResourcePercentagePhysicalCpuLimit, 50);
			handler.InitConfig();
			handler.Init(mockLCE, plugin);
			handler.PreExecute(id, Resource.NewInstance(1024, YarnConfiguration.DefaultNmVcores
				 / 2));
			NUnit.Framework.Assert.IsTrue(containerDir.Exists());
			NUnit.Framework.Assert.IsTrue(containerDir.IsDirectory());
			periodFile = new FilePath(containerDir, "cpu.cfs_period_us");
			quotaFile = new FilePath(containerDir, "cpu.cfs_quota_us");
			NUnit.Framework.Assert.IsTrue(periodFile.Exists());
			NUnit.Framework.Assert.IsTrue(quotaFile.Exists());
			NUnit.Framework.Assert.AreEqual(1000 * 1000, ReadIntFromFile(periodFile));
			NUnit.Framework.Assert.AreEqual(1000 * 1000, ReadIntFromFile(quotaFile));
			FileUtils.DeleteQuietly(cgroupDir);
		}
	}
}
