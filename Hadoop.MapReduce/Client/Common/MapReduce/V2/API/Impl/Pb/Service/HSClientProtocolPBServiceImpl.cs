using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Impl.PB.Service
{
	public class HSClientProtocolPBServiceImpl : MRClientProtocolPBServiceImpl, HSClientProtocolPB
	{
		public HSClientProtocolPBServiceImpl(HSClientProtocol impl)
			: base(impl)
		{
		}
	}
}
