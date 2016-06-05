using System.IO;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class GetGroupsForTesting : GetGroupsBase
	{
		public GetGroupsForTesting(Configuration conf)
			: base(conf)
		{
		}

		public GetGroupsForTesting(Configuration conf, TextWriter @out)
			: base(conf, @out)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected override IPEndPoint GetProtocolAddress(Configuration conf)
		{
			return conf.GetSocketAddr(YarnConfiguration.RmAdminAddress, YarnConfiguration.DefaultRmAdminAddress
				, YarnConfiguration.DefaultRmAdminPort);
		}

		public override void SetConf(Configuration conf)
		{
			conf = new YarnConfiguration(conf);
			base.SetConf(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override GetUserMappingsProtocol GetUgmProtocol()
		{
			Configuration conf = GetConf();
			IPEndPoint addr = conf.GetSocketAddr(YarnConfiguration.RmAdminAddress, YarnConfiguration
				.DefaultRmAdminAddress, YarnConfiguration.DefaultRmAdminPort);
			YarnRPC rpc = YarnRPC.Create(conf);
			ResourceManagerAdministrationProtocol adminProtocol = (ResourceManagerAdministrationProtocol
				)rpc.GetProxy(typeof(ResourceManagerAdministrationProtocol), addr, GetConf());
			return adminProtocol;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			int res = ToolRunner.Run(new Org.Apache.Hadoop.Yarn.Client.GetGroupsForTesting(new 
				YarnConfiguration()), argv);
			System.Environment.Exit(res);
		}
	}
}
