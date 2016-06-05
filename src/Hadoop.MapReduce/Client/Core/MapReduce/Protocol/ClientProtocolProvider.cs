using System.Net;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Protocol
{
	public abstract class ClientProtocolProvider
	{
		/// <exception cref="System.IO.IOException"/>
		public abstract ClientProtocol Create(Configuration conf);

		/// <exception cref="System.IO.IOException"/>
		public abstract ClientProtocol Create(IPEndPoint addr, Configuration conf);

		/// <exception cref="System.IO.IOException"/>
		public abstract void Close(ClientProtocol clientProtocol);
	}
}
