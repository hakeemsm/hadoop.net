using Com.Sun.Jersey.Api.Client;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp
{
	public class TestTimelineWebServicesWithSSL
	{
		private static readonly string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir"
			) + "/" + typeof(TestTimelineWebServicesWithSSL).Name;

		private static string keystoresDir;

		private static string sslConfDir;

		private static ApplicationHistoryServer timelineServer;

		private static TimelineStore store;

		private static Configuration conf;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupServer()
		{
			conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			conf.SetClass(YarnConfiguration.TimelineServiceStore, typeof(MemoryTimelineStore)
				, typeof(TimelineStore));
			conf.Set(YarnConfiguration.YarnHttpPolicyKey, "HTTPS_ONLY");
			FilePath @base = new FilePath(Basedir);
			FileUtil.FullyDelete(@base);
			@base.Mkdirs();
			keystoresDir = new FilePath(Basedir).GetAbsolutePath();
			sslConfDir = KeyStoreTestUtil.GetClasspathDir(typeof(TestTimelineWebServicesWithSSL
				));
			KeyStoreTestUtil.SetupSSLConfig(keystoresDir, sslConfDir, conf, false);
			conf.AddResource("ssl-server.xml");
			conf.AddResource("ssl-client.xml");
			timelineServer = new ApplicationHistoryServer();
			timelineServer.Init(conf);
			timelineServer.Start();
			store = timelineServer.GetTimelineStore();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDownServer()
		{
			if (timelineServer != null)
			{
				timelineServer.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPutEntities()
		{
			TestTimelineWebServicesWithSSL.TestTimelineClient client = new TestTimelineWebServicesWithSSL.TestTimelineClient
				();
			try
			{
				client.Init(conf);
				client.Start();
				TimelineEntity expectedEntity = new TimelineEntity();
				expectedEntity.SetEntityType("test entity type");
				expectedEntity.SetEntityId("test entity id");
				expectedEntity.SetDomainId("test domain id");
				TimelineEvent @event = new TimelineEvent();
				@event.SetEventType("test event type");
				@event.SetTimestamp(0L);
				expectedEntity.AddEvent(@event);
				TimelinePutResponse response = client.PutEntities(expectedEntity);
				NUnit.Framework.Assert.AreEqual(0, response.GetErrors().Count);
				NUnit.Framework.Assert.IsTrue(client.resp.ToString().Contains("https"));
				TimelineEntity actualEntity = store.GetEntity(expectedEntity.GetEntityId(), expectedEntity
					.GetEntityType(), EnumSet.AllOf<TimelineReader.Field>());
				NUnit.Framework.Assert.IsNotNull(actualEntity);
				NUnit.Framework.Assert.AreEqual(expectedEntity.GetEntityId(), actualEntity.GetEntityId
					());
				NUnit.Framework.Assert.AreEqual(expectedEntity.GetEntityType(), actualEntity.GetEntityType
					());
			}
			finally
			{
				client.Stop();
				client.Close();
			}
		}

		private class TestTimelineClient : TimelineClientImpl
		{
			private ClientResponse resp;

			public override ClientResponse DoPostingObject(object obj, string path)
			{
				resp = base.DoPostingObject(obj, path);
				return resp;
			}
		}
	}
}
