using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class YarnClientProtocolProvider : ClientProtocolProvider
	{
		/// <exception cref="System.IO.IOException"/>
		public override ClientProtocol Create(Configuration conf)
		{
			if (MRConfig.YarnFrameworkName.Equals(conf.Get(MRConfig.FrameworkName)))
			{
				return new YARNRunner(conf);
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override ClientProtocol Create(IPEndPoint addr, Configuration conf)
		{
			return Create(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close(ClientProtocol clientProtocol)
		{
		}
		// nothing to do
	}
}
