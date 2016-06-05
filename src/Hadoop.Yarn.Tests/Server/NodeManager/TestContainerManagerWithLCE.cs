using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestContainerManagerWithLCE : TestContainerManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.TestContainerManagerWithLCE
			));

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		public TestContainerManagerWithLCE()
			: base()
		{
		}

		static TestContainerManagerWithLCE()
		{
			localDir = new FilePath("target", typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.TestContainerManagerWithLCE
				).FullName + "-localDir").GetAbsoluteFile();
			tmpDir = new FilePath("target", typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.TestContainerManagerWithLCE
				).FullName + "-tmpDir");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Setup()
		{
			// Don't run the test if the binary is not available.
			if (!ShouldRunTest())
			{
				Log.Info("LCE binary path is not passed. Not running the test");
				return;
			}
			base.Setup();
			localFS.SetPermission(new Path(localDir.GetCanonicalPath()), new FsPermission((short
				)0x1ff));
			localFS.SetPermission(new Path(tmpDir.GetCanonicalPath()), new FsPermission((short
				)0x1ff));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			if (ShouldRunTest())
			{
				base.TearDown();
			}
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void TestContainerSetup()
		{
			// Don't run the test if the binary is not available.
			if (!ShouldRunTest())
			{
				Log.Info("LCE binary path is not passed. Not running the test");
				return;
			}
			Log.Info("Running testContainerSetup");
			base.TestContainerSetup();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void TestContainerManagerInitialization()
		{
			// Don't run the test if the binary is not available.
			if (!ShouldRunTest())
			{
				Log.Info("LCE binary path is not passed. Not running the test");
				return;
			}
			Log.Info("Running testContainerManagerInitialization");
			base.TestContainerManagerInitialization();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void TestContainerLaunchAndStop()
		{
			// Don't run the test if the binary is not available.
			if (!ShouldRunTest())
			{
				Log.Info("LCE binary path is not passed. Not running the test");
				return;
			}
			Log.Info("Running testContainerLaunchAndStop");
			base.TestContainerLaunchAndStop();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void TestContainerLaunchAndExitSuccess()
		{
			// Don't run the test if the binary is not available.
			if (!ShouldRunTest())
			{
				Log.Info("LCE binary path is not passed. Not running the test");
				return;
			}
			Log.Info("Running testContainerLaunchAndExitSuccess");
			base.TestContainerLaunchAndExitSuccess();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void TestContainerLaunchAndExitFailure()
		{
			// Don't run the test if the binary is not available.
			if (!ShouldRunTest())
			{
				Log.Info("LCE binary path is not passed. Not running the test");
				return;
			}
			Log.Info("Running testContainerLaunchAndExitFailure");
			base.TestContainerLaunchAndExitFailure();
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void TestLocalFilesCleanup()
		{
			// Don't run the test if the binary is not available.
			if (!ShouldRunTest())
			{
				Log.Info("LCE binary path is not passed. Not running the test");
				return;
			}
			Log.Info("Running testLocalFilesCleanup");
			base.TestLocalFilesCleanup();
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void TestContainerLaunchFromPreviousRM()
		{
			// Don't run the test if the binary is not available.
			if (!ShouldRunTest())
			{
				Log.Info("LCE binary path is not passed. Not running the test");
				return;
			}
			Log.Info("Running testContainerLaunchFromPreviousRM");
			base.TestContainerLaunchFromPreviousRM();
		}

		/// <exception cref="System.Exception"/>
		public override void TestMultipleContainersLaunch()
		{
			// Don't run the test if the binary is not available.
			if (!ShouldRunTest())
			{
				Log.Info("LCE binary path is not passed. Not running the test");
				return;
			}
			Log.Info("Running testContainerLaunchFromPreviousRM");
			base.TestMultipleContainersLaunch();
		}

		/// <exception cref="System.Exception"/>
		public override void TestMultipleContainersStopAndGetStatus()
		{
			// Don't run the test if the binary is not available.
			if (!ShouldRunTest())
			{
				Log.Info("LCE binary path is not passed. Not running the test");
				return;
			}
			Log.Info("Running testContainerLaunchFromPreviousRM");
			base.TestMultipleContainersStopAndGetStatus();
		}

		/// <exception cref="System.Exception"/>
		public override void TestStartContainerFailureWithUnknownAuxService()
		{
			// Don't run the test if the binary is not available.
			if (!ShouldRunTest())
			{
				Log.Info("LCE binary path is not passed. Not running the test");
				return;
			}
			Log.Info("Running testContainerLaunchFromPreviousRM");
			base.TestStartContainerFailureWithUnknownAuxService();
		}

		private bool ShouldRunTest()
		{
			return Runtime.GetProperty(YarnConfiguration.NmLinuxContainerExecutorPath) != null;
		}

		protected internal override ContainerExecutor CreateContainerExecutor()
		{
			base.conf.Set(YarnConfiguration.NmLinuxContainerExecutorPath, Runtime.GetProperty
				(YarnConfiguration.NmLinuxContainerExecutorPath));
			LinuxContainerExecutor linuxContainerExecutor = new LinuxContainerExecutor();
			linuxContainerExecutor.SetConf(base.conf);
			return linuxContainerExecutor;
		}
	}
}
