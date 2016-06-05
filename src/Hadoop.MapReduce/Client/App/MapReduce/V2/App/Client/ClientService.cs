using System.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Client
{
	public interface ClientService : Org.Apache.Hadoop.Service.Service
	{
		IPEndPoint GetBindAddress();

		int GetHttpPort();
	}
}
