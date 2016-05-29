using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords.Impl.PB
{
	public class TestPBLocalizerRPC
	{
		internal static readonly RecordFactory recordFactory = CreatePBRecordFactory();

		internal static RecordFactory CreatePBRecordFactory()
		{
			Configuration conf = new Configuration();
			return RecordFactoryProvider.GetRecordFactory(conf);
		}

		internal class LocalizerService : LocalizationProtocol
		{
			private readonly IPEndPoint locAddr;

			private Org.Apache.Hadoop.Ipc.Server server;

			internal LocalizerService(IPEndPoint locAddr)
			{
				this.locAddr = locAddr;
			}

			public virtual void Start()
			{
				Configuration conf = new Configuration();
				YarnRPC rpc = YarnRPC.Create(conf);
				server = rpc.GetServer(typeof(LocalizationProtocol), this, locAddr, conf, null, 1
					);
				server.Start();
			}

			public virtual void Stop()
			{
				if (server != null)
				{
					server.Stop();
				}
			}

			public virtual LocalizerHeartbeatResponse Heartbeat(LocalizerStatus status)
			{
				return DieHBResponse();
			}
		}

		internal static LocalizerHeartbeatResponse DieHBResponse()
		{
			LocalizerHeartbeatResponse response = recordFactory.NewRecordInstance<LocalizerHeartbeatResponse
				>();
			response.SetLocalizerAction(LocalizerAction.Die);
			return response;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalizerRPC()
		{
			IPEndPoint locAddr = new IPEndPoint("0.0.0.0", 8040);
			TestPBLocalizerRPC.LocalizerService server = new TestPBLocalizerRPC.LocalizerService
				(locAddr);
			try
			{
				server.Start();
				Configuration conf = new Configuration();
				YarnRPC rpc = YarnRPC.Create(conf);
				LocalizationProtocol client = (LocalizationProtocol)rpc.GetProxy(typeof(LocalizationProtocol
					), locAddr, conf);
				LocalizerStatus status = recordFactory.NewRecordInstance<LocalizerStatus>();
				status.SetLocalizerId("localizer0");
				LocalizerHeartbeatResponse response = client.Heartbeat(status);
				NUnit.Framework.Assert.AreEqual(DieHBResponse(), response);
			}
			finally
			{
				server.Stop();
			}
			NUnit.Framework.Assert.IsTrue(true);
		}
	}
}
