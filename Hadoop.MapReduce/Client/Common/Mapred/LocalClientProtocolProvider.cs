using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class LocalClientProtocolProvider : ClientProtocolProvider
	{
		/// <exception cref="System.IO.IOException"/>
		public override ClientProtocol Create(Configuration conf)
		{
			string framework = conf.Get(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
			if (!MRConfig.LocalFrameworkName.Equals(framework))
			{
				return null;
			}
			conf.SetInt(JobContext.NumMaps, 1);
			return new LocalJobRunner(conf);
		}

		public override ClientProtocol Create(IPEndPoint addr, Configuration conf)
		{
			return null;
		}

		// LocalJobRunner doesn't use a socket
		public override void Close(ClientProtocol clientProtocol)
		{
		}
		// no clean up required
	}
}
