using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class Master
	{
		public enum State
		{
			Initializing,
			Running
		}

		public static string GetMasterUserName(Configuration conf)
		{
			string framework = conf.Get(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
			if (framework.Equals(MRConfig.ClassicFrameworkName))
			{
				return conf.Get(MRConfig.MasterUserName);
			}
			else
			{
				return conf.Get(YarnConfiguration.RmPrincipal);
			}
		}

		public static IPEndPoint GetMasterAddress(Configuration conf)
		{
			string masterAddress;
			string framework = conf.Get(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
			if (framework.Equals(MRConfig.ClassicFrameworkName))
			{
				masterAddress = conf.Get(MRConfig.MasterAddress, "localhost:8012");
				return NetUtils.CreateSocketAddr(masterAddress, 8012, MRConfig.MasterAddress);
			}
			else
			{
				return conf.GetSocketAddr(YarnConfiguration.RmAddress, YarnConfiguration.DefaultRmAddress
					, YarnConfiguration.DefaultRmPort);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static string GetMasterPrincipal(Configuration conf)
		{
			string masterHostname = GetMasterAddress(conf).GetHostName();
			// get kerberos principal for use as delegation token renewer
			return SecurityUtil.GetServerPrincipal(GetMasterUserName(conf), masterHostname);
		}
	}
}
