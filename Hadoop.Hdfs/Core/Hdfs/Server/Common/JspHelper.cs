using System.Collections.Generic;
using System.IO;
using System.Net;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Common
{
	public class JspHelper
	{
		public const string CurrentConf = "current.conf";

		public const string DelegationParameterName = DelegationParam.Name;

		public const string NamenodeAddress = "nnaddr";

		internal const string SetDelegation = "&" + DelegationParameterName + "=";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Common.JspHelper
			));

		/// <summary>Private constructor for preventing creating JspHelper object.</summary>
		private JspHelper()
		{
		}

		private class NodeRecord : DatanodeInfo
		{
			internal int frequency;

			public NodeRecord(DatanodeInfo info, int count)
				: base(info)
			{
				// data structure to count number of blocks on datanodes.
				this.frequency = count;
			}

			public override bool Equals(object obj)
			{
				// Sufficient to use super equality as datanodes are uniquely identified
				// by DatanodeID
				return (this == obj) || base.Equals(obj);
			}

			public override int GetHashCode()
			{
				// Super implementation is sufficient
				return base.GetHashCode();
			}
		}

		private class NodeRecordComparator : IComparer<JspHelper.NodeRecord>
		{
			// compare two records based on their frequency
			public virtual int Compare(JspHelper.NodeRecord o1, JspHelper.NodeRecord o2)
			{
				if (o1.frequency < o2.frequency)
				{
					return -1;
				}
				else
				{
					if (o1.frequency > o2.frequency)
					{
						return 1;
					}
				}
				return 0;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static DatanodeInfo BestNode(LocatedBlocks blks, Configuration conf)
		{
			Dictionary<DatanodeInfo, JspHelper.NodeRecord> map = new Dictionary<DatanodeInfo, 
				JspHelper.NodeRecord>();
			foreach (LocatedBlock block in blks.GetLocatedBlocks())
			{
				DatanodeInfo[] nodes = block.GetLocations();
				foreach (DatanodeInfo node in nodes)
				{
					JspHelper.NodeRecord record = map[node];
					if (record == null)
					{
						map[node] = new JspHelper.NodeRecord(node, 1);
					}
					else
					{
						record.frequency++;
					}
				}
			}
			JspHelper.NodeRecord[] nodes_1 = Sharpen.Collections.ToArray(map.Values, new JspHelper.NodeRecord
				[map.Count]);
			Arrays.Sort(nodes_1, new JspHelper.NodeRecordComparator());
			return BestNode(nodes_1, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private static DatanodeInfo BestNode(DatanodeInfo[] nodes, bool doRandom)
		{
			if (nodes == null || nodes.Length == 0)
			{
				throw new IOException("No nodes contain this block");
			}
			int l = 0;
			while (l < nodes.Length && !nodes[l].IsDecommissioned())
			{
				++l;
			}
			if (l == 0)
			{
				throw new IOException("No active nodes contain this block");
			}
			int index = doRandom ? DFSUtil.GetRandom().Next(l) : 0;
			return nodes[index];
		}

		/// <summary>Validate filename.</summary>
		/// <returns>
		/// null if the filename is invalid.
		/// Otherwise, return the validated filename.
		/// </returns>
		public static string ValidatePath(string p)
		{
			return p == null || p.Length == 0 ? null : new Path(p).ToUri().GetPath();
		}

		/// <summary>Validate a long value.</summary>
		/// <returns>
		/// null if the value is invalid.
		/// Otherwise, return the validated Long object.
		/// </returns>
		public static long ValidateLong(string value)
		{
			return value == null ? null : long.Parse(value);
		}

		/// <exception cref="System.IO.IOException"/>
		public static string GetDefaultWebUserName(Configuration conf)
		{
			string user = conf.Get(CommonConfigurationKeys.HadoopHttpStaticUser, CommonConfigurationKeys
				.DefaultHadoopHttpStaticUser);
			if (user == null || user.Length == 0)
			{
				throw new IOException("Cannot determine UGI from request or conf");
			}
			return user;
		}

		private static IPEndPoint GetNNServiceAddress(ServletContext context, HttpServletRequest
			 request)
		{
			string namenodeAddressInUrl = request.GetParameter(NamenodeAddress);
			IPEndPoint namenodeAddress = null;
			if (namenodeAddressInUrl != null)
			{
				namenodeAddress = NetUtils.CreateSocketAddr(namenodeAddressInUrl);
			}
			else
			{
				if (context != null)
				{
					namenodeAddress = NameNodeHttpServer.GetNameNodeAddressFromContext(context);
				}
			}
			if (namenodeAddress != null)
			{
				return namenodeAddress;
			}
			return null;
		}

		/// <summary>Same as getUGI(null, request, conf).</summary>
		/// <exception cref="System.IO.IOException"/>
		public static UserGroupInformation GetUGI(HttpServletRequest request, Configuration
			 conf)
		{
			return GetUGI(null, request, conf);
		}

		/// <summary>Same as getUGI(context, request, conf, KERBEROS_SSL, true).</summary>
		/// <exception cref="System.IO.IOException"/>
		public static UserGroupInformation GetUGI(ServletContext context, HttpServletRequest
			 request, Configuration conf)
		{
			return GetUGI(context, request, conf, UserGroupInformation.AuthenticationMethod.KerberosSsl
				, true);
		}

		/// <summary>
		/// Get
		/// <see cref="Org.Apache.Hadoop.Security.UserGroupInformation"/>
		/// and possibly the delegation token out of
		/// the request.
		/// </summary>
		/// <param name="context">the ServletContext that is serving this request.</param>
		/// <param name="request">the http request</param>
		/// <param name="conf">configuration</param>
		/// <param name="secureAuthMethod">the AuthenticationMethod used in secure mode.</param>
		/// <param name="tryUgiParameter">Should it try the ugi parameter?</param>
		/// <returns>a new user from the request</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the request has no token
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		public static UserGroupInformation GetUGI(ServletContext context, HttpServletRequest
			 request, Configuration conf, UserGroupInformation.AuthenticationMethod secureAuthMethod
			, bool tryUgiParameter)
		{
			UserGroupInformation ugi = null;
			string usernameFromQuery = GetUsernameFromQuery(request, tryUgiParameter);
			string doAsUserFromQuery = request.GetParameter(DoAsParam.Name);
			string remoteUser;
			if (UserGroupInformation.IsSecurityEnabled())
			{
				remoteUser = request.GetRemoteUser();
				string tokenString = request.GetParameter(DelegationParameterName);
				if (tokenString != null)
				{
					// Token-based connections need only verify the effective user, and
					// disallow proxying to different user.  Proxy authorization checks
					// are not required since the checks apply to issuing a token.
					ugi = GetTokenUGI(context, request, tokenString, conf);
					CheckUsername(ugi.GetShortUserName(), usernameFromQuery);
					CheckUsername(ugi.GetShortUserName(), doAsUserFromQuery);
				}
				else
				{
					if (remoteUser == null)
					{
						throw new IOException("Security enabled but user not authenticated by filter");
					}
				}
			}
			else
			{
				// Security's not on, pull from url or use default web user
				remoteUser = (usernameFromQuery == null) ? GetDefaultWebUserName(conf) : usernameFromQuery;
			}
			// not specified in request
			if (ugi == null)
			{
				// security is off, or there's no token
				ugi = UserGroupInformation.CreateRemoteUser(remoteUser);
				CheckUsername(ugi.GetShortUserName(), usernameFromQuery);
				if (UserGroupInformation.IsSecurityEnabled())
				{
					// This is not necessarily true, could have been auth'ed by user-facing
					// filter
					ugi.SetAuthenticationMethod(secureAuthMethod);
				}
				if (doAsUserFromQuery != null)
				{
					// create and attempt to authorize a proxy user
					ugi = UserGroupInformation.CreateProxyUser(doAsUserFromQuery, ugi);
					ProxyUsers.Authorize(ugi, GetRemoteAddr(request));
				}
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("getUGI is returning: " + ugi.GetShortUserName());
			}
			return ugi;
		}

		/// <exception cref="System.IO.IOException"/>
		private static UserGroupInformation GetTokenUGI(ServletContext context, HttpServletRequest
			 request, string tokenString, Configuration conf)
		{
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>();
			token.DecodeFromUrlString(tokenString);
			IPEndPoint serviceAddress = GetNNServiceAddress(context, request);
			if (serviceAddress != null)
			{
				SecurityUtil.SetTokenService(token, serviceAddress);
				token.SetKind(DelegationTokenIdentifier.HdfsDelegationKind);
			}
			ByteArrayInputStream buf = new ByteArrayInputStream(token.GetIdentifier());
			DataInputStream @in = new DataInputStream(buf);
			DelegationTokenIdentifier id = new DelegationTokenIdentifier();
			id.ReadFields(@in);
			if (context != null)
			{
				NameNode nn = NameNodeHttpServer.GetNameNodeFromContext(context);
				if (nn != null)
				{
					// Verify the token.
					nn.GetNamesystem().VerifyToken(id, token.GetPassword());
				}
			}
			UserGroupInformation ugi = id.GetUser();
			ugi.AddToken(token);
			return ugi;
		}

		// honor the X-Forwarded-For header set by a configured set of trusted
		// proxy servers.  allows audit logging and proxy user checks to work
		// via an http proxy
		public static string GetRemoteAddr(HttpServletRequest request)
		{
			string remoteAddr = request.GetRemoteAddr();
			string proxyHeader = request.GetHeader("X-Forwarded-For");
			if (proxyHeader != null && ProxyServers.IsProxyServer(remoteAddr))
			{
				string clientAddr = proxyHeader.Split(",")[0].Trim();
				if (!clientAddr.IsEmpty())
				{
					remoteAddr = clientAddr;
				}
			}
			return remoteAddr;
		}

		/// <summary>Expected user name should be a short name.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void CheckUsername(string expected, string name)
		{
			if (expected == null && name != null)
			{
				throw new IOException("Usernames not matched: expecting null but name=" + name);
			}
			if (name == null)
			{
				//name is optional, null is okay
				return;
			}
			KerberosName u = new KerberosName(name);
			string shortName = u.GetShortName();
			if (!shortName.Equals(expected))
			{
				throw new IOException("Usernames not matched: name=" + shortName + " != expected="
					 + expected);
			}
		}

		private static string GetUsernameFromQuery(HttpServletRequest request, bool tryUgiParameter
			)
		{
			string username = request.GetParameter(UserParam.Name);
			if (username == null && tryUgiParameter)
			{
				//try ugi parameter
				string ugiStr = request.GetParameter("ugi");
				if (ugiStr != null)
				{
					username = ugiStr.Split(",")[0];
				}
			}
			return username;
		}

		/// <summary>Returns the url parameter for the given token string.</summary>
		/// <param name="tokenString"/>
		/// <returns>url parameter</returns>
		public static string GetDelegationTokenUrlParam(string tokenString)
		{
			if (tokenString == null)
			{
				return string.Empty;
			}
			if (UserGroupInformation.IsSecurityEnabled())
			{
				return SetDelegation + tokenString;
			}
			else
			{
				return string.Empty;
			}
		}

		/// <summary>
		/// Returns the url parameter for the given string, prefixed with
		/// paramSeparator.
		/// </summary>
		/// <param name="name">parameter name</param>
		/// <param name="val">parameter value</param>
		/// <param name="paramSeparator">URL parameter prefix, i.e. either '?' or '&'</param>
		/// <returns>url parameter</returns>
		public static string GetUrlParam(string name, string val, string paramSeparator)
		{
			return val == null ? string.Empty : paramSeparator + name + "=" + val;
		}

		/// <summary>
		/// Returns the url parameter for the given string, prefixed with '?' if
		/// firstParam is true, prefixed with '&' if firstParam is false.
		/// </summary>
		/// <param name="name">parameter name</param>
		/// <param name="val">parameter value</param>
		/// <param name="firstParam">true if this is the first parameter in the list, false otherwise
		/// 	</param>
		/// <returns>url parameter</returns>
		public static string GetUrlParam(string name, string val, bool firstParam)
		{
			return GetUrlParam(name, val, firstParam ? "?" : "&");
		}

		/// <summary>Returns the url parameter for the given string, prefixed with '&'.</summary>
		/// <param name="name">parameter name</param>
		/// <param name="val">parameter value</param>
		/// <returns>url parameter</returns>
		public static string GetUrlParam(string name, string val)
		{
			return GetUrlParam(name, val, false);
		}
	}
}
