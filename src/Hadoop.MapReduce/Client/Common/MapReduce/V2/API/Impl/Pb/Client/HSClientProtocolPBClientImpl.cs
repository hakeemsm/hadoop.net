using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Impl.PB.Client
{
	public class HSClientProtocolPBClientImpl : MRClientProtocolPBClientImpl, HSClientProtocol
	{
		/// <exception cref="System.IO.IOException"/>
		public HSClientProtocolPBClientImpl(long clientVersion, IPEndPoint addr, Configuration
			 conf)
			: base()
		{
			RPC.SetProtocolEngine(conf, typeof(HSClientProtocolPB), typeof(ProtobufRpcEngine)
				);
			proxy = (HSClientProtocolPB)RPC.GetProxy<HSClientProtocolPB>(clientVersion, addr, 
				conf);
		}
	}
}
