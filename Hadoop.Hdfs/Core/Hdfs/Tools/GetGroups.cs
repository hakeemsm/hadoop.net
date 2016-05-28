using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>
	/// HDFS implementation of a tool for getting the groups which a given user
	/// belongs to.
	/// </summary>
	public class GetGroups : GetGroupsBase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Tools.GetGroups
			));

		internal const string Usage = "Usage: hdfs groups [username ...]";

		static GetGroups()
		{
			HdfsConfiguration.Init();
		}

		public GetGroups(Configuration conf)
			: base(conf)
		{
		}

		public GetGroups(Configuration conf, TextWriter @out)
			: base(conf, @out)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected override IPEndPoint GetProtocolAddress(Configuration conf)
		{
			return NameNode.GetAddress(conf);
		}

		public override void SetConf(Configuration conf)
		{
			conf = new HdfsConfiguration(conf);
			string nameNodePrincipal = conf.Get(DFSConfigKeys.DfsNamenodeKerberosPrincipalKey
				, string.Empty);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Using NN principal: " + nameNodePrincipal);
			}
			conf.Set(CommonConfigurationKeys.HadoopSecurityServiceUserNameKey, nameNodePrincipal
				);
			base.SetConf(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		protected override GetUserMappingsProtocol GetUgmProtocol()
		{
			return NameNodeProxies.CreateProxy<GetUserMappingsProtocol>(GetConf(), FileSystem
				.GetDefaultUri(GetConf())).GetProxy();
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			if (DFSUtil.ParseHelpArgument(argv, Usage, System.Console.Out, true))
			{
				System.Environment.Exit(0);
			}
			int res = ToolRunner.Run(new Org.Apache.Hadoop.Hdfs.Tools.GetGroups(new HdfsConfiguration
				()), argv);
			System.Environment.Exit(res);
		}
	}
}
