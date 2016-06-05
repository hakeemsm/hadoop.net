using System.Collections.Generic;
using System.Net;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Util
{
	public class MRWebAppUtil
	{
		private static readonly Splitter AddrSplitter = Splitter.On(':').TrimResults();

		private static readonly Joiner Joiner = Joiner.On(string.Empty);

		private static HttpConfig.Policy httpPolicyInYarn;

		private static HttpConfig.Policy httpPolicyInJHS;

		public static void Initialize(Configuration conf)
		{
			SetHttpPolicyInYARN(conf.Get(YarnConfiguration.YarnHttpPolicyKey, YarnConfiguration
				.YarnHttpPolicyDefault));
			SetHttpPolicyInJHS(conf.Get(JHAdminConfig.MrHsHttpPolicy, JHAdminConfig.DefaultMrHsHttpPolicy
				));
		}

		private static void SetHttpPolicyInJHS(string policy)
		{
			MRWebAppUtil.httpPolicyInJHS = HttpConfig.Policy.FromString(policy);
		}

		private static void SetHttpPolicyInYARN(string policy)
		{
			MRWebAppUtil.httpPolicyInYarn = HttpConfig.Policy.FromString(policy);
		}

		public static HttpConfig.Policy GetJHSHttpPolicy()
		{
			return MRWebAppUtil.httpPolicyInJHS;
		}

		public static HttpConfig.Policy GetYARNHttpPolicy()
		{
			return MRWebAppUtil.httpPolicyInYarn;
		}

		public static string GetYARNWebappScheme()
		{
			return httpPolicyInYarn == HttpConfig.Policy.HttpsOnly ? "https://" : "http://";
		}

		public static string GetJHSWebappScheme()
		{
			return httpPolicyInJHS == HttpConfig.Policy.HttpsOnly ? "https://" : "http://";
		}

		public static void SetJHSWebappURLWithoutScheme(Configuration conf, string hostAddress
			)
		{
			if (httpPolicyInJHS == HttpConfig.Policy.HttpsOnly)
			{
				conf.Set(JHAdminConfig.MrHistoryWebappHttpsAddress, hostAddress);
			}
			else
			{
				conf.Set(JHAdminConfig.MrHistoryWebappAddress, hostAddress);
			}
		}

		public static string GetJHSWebappURLWithoutScheme(Configuration conf)
		{
			if (httpPolicyInJHS == HttpConfig.Policy.HttpsOnly)
			{
				return conf.Get(JHAdminConfig.MrHistoryWebappHttpsAddress, JHAdminConfig.DefaultMrHistoryWebappHttpsAddress
					);
			}
			else
			{
				return conf.Get(JHAdminConfig.MrHistoryWebappAddress, JHAdminConfig.DefaultMrHistoryWebappAddress
					);
			}
		}

		public static string GetJHSWebappURLWithScheme(Configuration conf)
		{
			return GetJHSWebappScheme() + GetJHSWebappURLWithoutScheme(conf);
		}

		public static IPEndPoint GetJHSWebBindAddress(Configuration conf)
		{
			if (httpPolicyInJHS == HttpConfig.Policy.HttpsOnly)
			{
				return conf.GetSocketAddr(JHAdminConfig.MrHistoryBindHost, JHAdminConfig.MrHistoryWebappHttpsAddress
					, JHAdminConfig.DefaultMrHistoryWebappHttpsAddress, JHAdminConfig.DefaultMrHistoryWebappHttpsPort
					);
			}
			else
			{
				return conf.GetSocketAddr(JHAdminConfig.MrHistoryBindHost, JHAdminConfig.MrHistoryWebappAddress
					, JHAdminConfig.DefaultMrHistoryWebappAddress, JHAdminConfig.DefaultMrHistoryWebappPort
					);
			}
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		public static string GetApplicationWebURLOnJHSWithoutScheme(Configuration conf, ApplicationId
			 appId)
		{
			//construct the history url for job
			string addr = GetJHSWebappURLWithoutScheme(conf);
			IEnumerator<string> it = AddrSplitter.Split(addr).GetEnumerator();
			it.Next();
			// ignore the bind host
			string port = it.Next();
			// Use hs address to figure out the host for webapp
			addr = conf.Get(JHAdminConfig.MrHistoryAddress, JHAdminConfig.DefaultMrHistoryAddress
				);
			string host = AddrSplitter.Split(addr).GetEnumerator().Next();
			string hsAddress = Joiner.Join(host, ":", port);
			IPEndPoint address = NetUtils.CreateSocketAddr(hsAddress, GetDefaultJHSWebappPort
				(), GetDefaultJHSWebappURLWithoutScheme());
			StringBuilder sb = new StringBuilder();
			if (address.Address.IsAnyLocalAddress() || address.Address.IsLoopbackAddress())
			{
				sb.Append(Sharpen.Runtime.GetLocalHost().ToString());
			}
			else
			{
				sb.Append(address.GetHostName());
			}
			sb.Append(":").Append(address.Port);
			sb.Append("/jobhistory/job/");
			JobID jobId = TypeConverter.FromYarn(appId);
			sb.Append(jobId.ToString());
			return sb.ToString();
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		public static string GetApplicationWebURLOnJHSWithScheme(Configuration conf, ApplicationId
			 appId)
		{
			return GetJHSWebappScheme() + GetApplicationWebURLOnJHSWithoutScheme(conf, appId);
		}

		private static int GetDefaultJHSWebappPort()
		{
			return httpPolicyInJHS == HttpConfig.Policy.HttpsOnly ? JHAdminConfig.DefaultMrHistoryWebappHttpsPort
				 : JHAdminConfig.DefaultMrHistoryWebappPort;
		}

		private static string GetDefaultJHSWebappURLWithoutScheme()
		{
			return httpPolicyInJHS == HttpConfig.Policy.HttpsOnly ? JHAdminConfig.DefaultMrHistoryWebappHttpsAddress
				 : JHAdminConfig.DefaultMrHistoryWebappAddress;
		}

		public static string GetAMWebappScheme(Configuration conf)
		{
			return "http://";
		}
	}
}
