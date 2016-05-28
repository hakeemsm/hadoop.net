using System;
using System.IO;
using System.Net;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	/// <summary>Configures and starts the MR-specific components in the YARN cluster.</summary>
	public class MiniMRYarnCluster : MiniYARNCluster
	{
		public static readonly string Appjar = JarFinder.GetJar(typeof(LocalContainerLauncher
			));

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.MiniMRYarnCluster
			));

		private JobHistoryServer historyServer;

		private MiniMRYarnCluster.JobHistoryServerWrapper historyServerWrapper;

		public MiniMRYarnCluster(string testName)
			: this(testName, 1)
		{
		}

		public MiniMRYarnCluster(string testName, int noOfNMs)
			: this(testName, noOfNMs, false)
		{
		}

		public MiniMRYarnCluster(string testName, int noOfNMs, bool enableAHS)
			: base(testName, 1, noOfNMs, 4, 4, enableAHS)
		{
			historyServerWrapper = new MiniMRYarnCluster.JobHistoryServerWrapper(this);
			AddService(historyServerWrapper);
		}

		public static string GetResolvedMRHistoryWebAppURLWithoutScheme(Configuration conf
			, bool isSSLEnabled)
		{
			IPEndPoint address = null;
			if (isSSLEnabled)
			{
				address = conf.GetSocketAddr(JHAdminConfig.MrHistoryWebappHttpsAddress, JHAdminConfig
					.DefaultMrHistoryWebappHttpsAddress, JHAdminConfig.DefaultMrHistoryWebappHttpsPort
					);
			}
			else
			{
				address = conf.GetSocketAddr(JHAdminConfig.MrHistoryWebappAddress, JHAdminConfig.
					DefaultMrHistoryWebappAddress, JHAdminConfig.DefaultMrHistoryWebappPort);
			}
			address = NetUtils.GetConnectAddress(address);
			StringBuilder sb = new StringBuilder();
			IPAddress resolved = address.Address;
			if (resolved == null || resolved.IsAnyLocalAddress() || resolved.IsLoopbackAddress
				())
			{
				string lh = address.GetHostName();
				try
				{
					lh = Sharpen.Runtime.GetLocalHost().ToString();
				}
				catch (UnknownHostException)
				{
				}
				//Ignore and fallback.
				sb.Append(lh);
			}
			else
			{
				sb.Append(address.GetHostName());
			}
			sb.Append(":").Append(address.Port);
			return sb.ToString();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			conf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
			if (conf.Get(MRJobConfig.MrAmStagingDir) == null)
			{
				conf.Set(MRJobConfig.MrAmStagingDir, new FilePath(GetTestWorkDir(), "apps_staging_dir/"
					).GetAbsolutePath());
			}
			// By default, VMEM monitoring disabled, PMEM monitoring enabled.
			if (!conf.GetBoolean(MRConfig.MapreduceMiniclusterControlResourceMonitoring, MRConfig
				.DefaultMapreduceMiniclusterControlResourceMonitoring))
			{
				conf.SetBoolean(YarnConfiguration.NmPmemCheckEnabled, false);
				conf.SetBoolean(YarnConfiguration.NmVmemCheckEnabled, false);
			}
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, "000");
			try
			{
				Path stagingPath = FileContext.GetFileContext(conf).MakeQualified(new Path(conf.Get
					(MRJobConfig.MrAmStagingDir)));
				/*
				* Re-configure the staging path on Windows if the file system is localFs.
				* We need to use a absolute path that contains the drive letter. The unit
				* test could run on a different drive than the AM. We can run into the
				* issue that job files are localized to the drive where the test runs on,
				* while the AM starts on a different drive and fails to find the job
				* metafiles. Using absolute path can avoid this ambiguity.
				*/
				if (Path.Windows)
				{
					if (typeof(LocalFileSystem).IsInstanceOfType(stagingPath.GetFileSystem(conf)))
					{
						conf.Set(MRJobConfig.MrAmStagingDir, new FilePath(conf.Get(MRJobConfig.MrAmStagingDir
							)).GetAbsolutePath());
					}
				}
				FileContext fc = FileContext.GetFileContext(stagingPath.ToUri(), conf);
				if (fc.Util().Exists(stagingPath))
				{
					Log.Info(stagingPath + " exists! deleting...");
					fc.Delete(stagingPath, true);
				}
				Log.Info("mkdir: " + stagingPath);
				//mkdir the staging directory so that right permissions are set while running as proxy user
				fc.Mkdir(stagingPath, null, true);
				//mkdir done directory as well 
				string doneDir = JobHistoryUtils.GetConfiguredHistoryServerDoneDirPrefix(conf);
				Path doneDirPath = fc.MakeQualified(new Path(doneDir));
				fc.Mkdir(doneDirPath, null, true);
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException("Could not create staging directory. ", e);
			}
			conf.Set(MRConfig.MasterAddress, "test");
			// The default is local because of
			// which shuffle doesn't happen
			//configure the shuffle service in NM
			conf.SetStrings(YarnConfiguration.NmAuxServices, new string[] { ShuffleHandler.MapreduceShuffleServiceid
				 });
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, ShuffleHandler.MapreduceShuffleServiceid
				), typeof(ShuffleHandler), typeof(Org.Apache.Hadoop.Service.Service));
			// Non-standard shuffle port
			conf.SetInt(ShuffleHandler.ShufflePortConfigKey, 0);
			conf.SetClass(YarnConfiguration.NmContainerExecutor, typeof(DefaultContainerExecutor
				), typeof(ContainerExecutor));
			// TestMRJobs is for testing non-uberized operation only; see TestUberAM
			// for corresponding uberized tests.
			conf.SetBoolean(MRJobConfig.JobUbertaskEnable, false);
			base.ServiceInit(conf);
		}

		private class JobHistoryServerWrapper : AbstractService
		{
			public JobHistoryServerWrapper(MiniMRYarnCluster _enclosing)
				: base(typeof(MiniMRYarnCluster.JobHistoryServerWrapper).FullName)
			{
				this._enclosing = _enclosing;
			}

			private volatile bool jhsStarted = false;

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				lock (this)
				{
					try
					{
						if (!this.GetConfig().GetBoolean(JHAdminConfig.MrHistoryMiniclusterFixedPorts, JHAdminConfig
							.DefaultMrHistoryMiniclusterFixedPorts))
						{
							string hostname = MiniYARNCluster.GetHostname();
							// pick free random ports.
							this.GetConfig().Set(JHAdminConfig.MrHistoryAddress, hostname + ":0");
							MRWebAppUtil.SetJHSWebappURLWithoutScheme(this.GetConfig(), hostname + ":0");
							this.GetConfig().Set(JHAdminConfig.JhsAdminAddress, hostname + ":0");
						}
						this._enclosing.historyServer = new JobHistoryServer();
						this._enclosing.historyServer.Init(this.GetConfig());
						new _Thread_212(this).Start();
						while (!this.jhsStarted)
						{
							MiniMRYarnCluster.Log.Info("Waiting for HistoryServer to start...");
							Sharpen.Thread.Sleep(1500);
						}
						//TODO Add a timeout. State.STOPPED check ?
						if (this._enclosing.historyServer.GetServiceState() != Service.STATE.Started)
						{
							throw new IOException("HistoryServer failed to start");
						}
						base.ServiceStart();
					}
					catch (Exception t)
					{
						throw new YarnRuntimeException(t);
					}
					//need to do this because historyServer.init creates a new Configuration
					this.GetConfig().Set(JHAdminConfig.MrHistoryAddress, this._enclosing.historyServer
						.GetConfig().Get(JHAdminConfig.MrHistoryAddress));
					MRWebAppUtil.SetJHSWebappURLWithoutScheme(this.GetConfig(), MRWebAppUtil.GetJHSWebappURLWithoutScheme
						(this._enclosing.historyServer.GetConfig()));
					MiniMRYarnCluster.Log.Info("MiniMRYARN ResourceManager address: " + this.GetConfig
						().Get(YarnConfiguration.RmAddress));
					MiniMRYarnCluster.Log.Info("MiniMRYARN ResourceManager web address: " + WebAppUtils
						.GetRMWebAppURLWithoutScheme(this.GetConfig()));
					MiniMRYarnCluster.Log.Info("MiniMRYARN HistoryServer address: " + this.GetConfig(
						).Get(JHAdminConfig.MrHistoryAddress));
					MiniMRYarnCluster.Log.Info("MiniMRYARN HistoryServer web address: " + MiniMRYarnCluster
						.GetResolvedMRHistoryWebAppURLWithoutScheme(this.GetConfig(), MRWebAppUtil.GetJHSHttpPolicy
						() == HttpConfig.Policy.HttpsOnly));
				}
			}

			private sealed class _Thread_212 : Sharpen.Thread
			{
				public _Thread_212(JobHistoryServerWrapper _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public override void Run()
				{
					this._enclosing._enclosing.historyServer.Start();
					this._enclosing.jhsStarted = true;
				}

				private readonly JobHistoryServerWrapper _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				lock (this)
				{
					if (this._enclosing.historyServer != null)
					{
						this._enclosing.historyServer.Stop();
					}
					base.ServiceStop();
				}
			}

			private readonly MiniMRYarnCluster _enclosing;
		}

		public virtual JobHistoryServer GetHistoryServer()
		{
			return this.historyServer;
		}
	}
}
