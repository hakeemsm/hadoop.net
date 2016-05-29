using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http.Lib;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	public class TestApplicationHistoryServer
	{
		// simple test init/start/stop ApplicationHistoryServer. Status should change.
		/// <exception cref="System.Exception"/>
		public virtual void TestStartStopServer()
		{
			ApplicationHistoryServer historyServer = new ApplicationHistoryServer();
			Configuration config = new YarnConfiguration();
			config.SetClass(YarnConfiguration.TimelineServiceStore, typeof(MemoryTimelineStore
				), typeof(TimelineStore));
			config.SetClass(YarnConfiguration.TimelineServiceStateStoreClass, typeof(MemoryTimelineStateStore
				), typeof(TimelineStateStore));
			config.Set(YarnConfiguration.TimelineServiceWebappAddress, "localhost:0");
			try
			{
				try
				{
					historyServer.Init(config);
					config.SetInt(YarnConfiguration.TimelineServiceHandlerThreadCount, 0);
					historyServer.Start();
					NUnit.Framework.Assert.Fail();
				}
				catch (ArgumentException e)
				{
					NUnit.Framework.Assert.IsTrue(e.Message.Contains(YarnConfiguration.TimelineServiceHandlerThreadCount
						));
				}
				config.SetInt(YarnConfiguration.TimelineServiceHandlerThreadCount, YarnConfiguration
					.DefaultTimelineServiceClientThreadCount);
				historyServer = new ApplicationHistoryServer();
				historyServer.Init(config);
				NUnit.Framework.Assert.AreEqual(Service.STATE.Inited, historyServer.GetServiceState
					());
				NUnit.Framework.Assert.AreEqual(5, historyServer.GetServices().Count);
				ApplicationHistoryClientService historyService = historyServer.GetClientService();
				NUnit.Framework.Assert.IsNotNull(historyServer.GetClientService());
				NUnit.Framework.Assert.AreEqual(Service.STATE.Inited, historyService.GetServiceState
					());
				historyServer.Start();
				NUnit.Framework.Assert.AreEqual(Service.STATE.Started, historyServer.GetServiceState
					());
				NUnit.Framework.Assert.AreEqual(Service.STATE.Started, historyService.GetServiceState
					());
				historyServer.Stop();
				NUnit.Framework.Assert.AreEqual(Service.STATE.Stopped, historyServer.GetServiceState
					());
			}
			finally
			{
				historyServer.Stop();
			}
		}

		// test launch method
		/// <exception cref="System.Exception"/>
		public virtual void TestLaunch()
		{
			ExitUtil.DisableSystemExit();
			ApplicationHistoryServer historyServer = null;
			try
			{
				// Not able to modify the config of this test case,
				// but others have been customized to avoid conflicts
				historyServer = ApplicationHistoryServer.LaunchAppHistoryServer(new string[0]);
			}
			catch (ExitUtil.ExitException e)
			{
				NUnit.Framework.Assert.AreEqual(0, e.status);
				ExitUtil.ResetFirstExitException();
				NUnit.Framework.Assert.Fail();
			}
			finally
			{
				if (historyServer != null)
				{
					historyServer.Stop();
				}
			}
		}

		//test launch method with -D arguments
		/// <exception cref="System.Exception"/>
		public virtual void TestLaunchWithArguments()
		{
			ExitUtil.DisableSystemExit();
			ApplicationHistoryServer historyServer = null;
			try
			{
				// Not able to modify the config of this test case,
				// but others have been customized to avoid conflicts
				string[] args = new string[2];
				args[0] = "-D" + YarnConfiguration.TimelineServiceLeveldbTtlIntervalMs + "=4000";
				args[1] = "-D" + YarnConfiguration.TimelineServiceTtlMs + "=200";
				historyServer = ApplicationHistoryServer.LaunchAppHistoryServer(args);
				Configuration conf = historyServer.GetConfig();
				NUnit.Framework.Assert.AreEqual("4000", conf.Get(YarnConfiguration.TimelineServiceLeveldbTtlIntervalMs
					));
				NUnit.Framework.Assert.AreEqual("200", conf.Get(YarnConfiguration.TimelineServiceTtlMs
					));
			}
			catch (ExitUtil.ExitException e)
			{
				NUnit.Framework.Assert.AreEqual(0, e.status);
				ExitUtil.ResetFirstExitException();
				NUnit.Framework.Assert.Fail();
			}
			finally
			{
				if (historyServer != null)
				{
					historyServer.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFilterOverrides()
		{
			Dictionary<string, string> driver = new Dictionary<string, string>();
			driver[string.Empty] = typeof(TimelineAuthenticationFilterInitializer).FullName;
			driver[typeof(StaticUserWebFilter).FullName] = typeof(StaticUserWebFilter).FullName
				 + "," + typeof(TimelineAuthenticationFilterInitializer).FullName;
			driver[typeof(AuthenticationFilterInitializer).FullName] = typeof(TimelineAuthenticationFilterInitializer
				).FullName;
			driver[typeof(TimelineAuthenticationFilterInitializer).FullName] = typeof(TimelineAuthenticationFilterInitializer
				).FullName;
			driver[typeof(AuthenticationFilterInitializer).FullName + "," + typeof(TimelineAuthenticationFilterInitializer
				).FullName] = typeof(TimelineAuthenticationFilterInitializer).FullName;
			driver[typeof(AuthenticationFilterInitializer).FullName + ", " + typeof(TimelineAuthenticationFilterInitializer
				).FullName] = typeof(TimelineAuthenticationFilterInitializer).FullName;
			foreach (KeyValuePair<string, string> entry in driver)
			{
				string filterInitializer = entry.Key;
				string expectedValue = entry.Value;
				ApplicationHistoryServer historyServer = new ApplicationHistoryServer();
				Configuration config = new YarnConfiguration();
				config.SetClass(YarnConfiguration.TimelineServiceStore, typeof(MemoryTimelineStore
					), typeof(TimelineStore));
				config.SetClass(YarnConfiguration.TimelineServiceStateStoreClass, typeof(MemoryTimelineStateStore
					), typeof(TimelineStateStore));
				config.Set(YarnConfiguration.TimelineServiceWebappAddress, "localhost:0");
				try
				{
					config.Set("hadoop.http.filter.initializers", filterInitializer);
					historyServer.Init(config);
					historyServer.Start();
					Configuration tmp = historyServer.GetConfig();
					NUnit.Framework.Assert.AreEqual(expectedValue, tmp.Get("hadoop.http.filter.initializers"
						));
				}
				finally
				{
					historyServer.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHostedUIs()
		{
			ApplicationHistoryServer historyServer = new ApplicationHistoryServer();
			Configuration config = new YarnConfiguration();
			config.SetClass(YarnConfiguration.TimelineServiceStore, typeof(MemoryTimelineStore
				), typeof(TimelineStore));
			config.SetClass(YarnConfiguration.TimelineServiceStateStoreClass, typeof(MemoryTimelineStateStore
				), typeof(TimelineStateStore));
			config.Set(YarnConfiguration.TimelineServiceWebappAddress, "localhost:0");
			string Ui1 = "UI1";
			string connFileStr = string.Empty;
			FilePath diskFile = new FilePath("./pom.xml");
			string diskFileStr = ReadInputStream(new FileInputStream(diskFile));
			try
			{
				config.Set(YarnConfiguration.TimelineServiceUiNames, Ui1);
				config.Set(YarnConfiguration.TimelineServiceUiWebPathPrefix + Ui1, "/" + Ui1);
				config.Set(YarnConfiguration.TimelineServiceUiOnDiskPathPrefix + Ui1, "./");
				historyServer.Init(config);
				historyServer.Start();
				Uri url = new Uri("http://localhost:" + historyServer.GetPort() + "/" + Ui1 + "/pom.xml"
					);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
				connFileStr = ReadInputStream(conn.GetInputStream());
			}
			finally
			{
				historyServer.Stop();
			}
			NUnit.Framework.Assert.AreEqual("Web file contents should be the same as on disk contents"
				, diskFileStr, connFileStr);
		}

		/// <exception cref="System.Exception"/>
		private string ReadInputStream(InputStream input)
		{
			ByteArrayOutputStream data = new ByteArrayOutputStream();
			byte[] buffer = new byte[512];
			int read;
			while ((read = input.Read(buffer)) >= 0)
			{
				data.Write(buffer, 0, read);
			}
			return Sharpen.Runtime.GetStringForBytes(data.ToByteArray(), "UTF-8");
		}
	}
}
