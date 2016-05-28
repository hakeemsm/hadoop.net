using System.IO;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Web.Webhdfs
{
	/// <summary>Create UGI from the request for the WebHDFS requests for the DNs.</summary>
	/// <remarks>
	/// Create UGI from the request for the WebHDFS requests for the DNs. Note that
	/// the DN does not authenticate the UGI -- the NN will authenticate them in
	/// subsequent operations.
	/// </remarks>
	internal class DataNodeUGIProvider
	{
		private readonly ParameterParser @params;

		internal DataNodeUGIProvider(ParameterParser @params)
		{
			this.@params = @params;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual UserGroupInformation Ugi()
		{
			if (UserGroupInformation.IsSecurityEnabled())
			{
				return TokenUGI();
			}
			string usernameFromQuery = @params.UserName();
			string doAsUserFromQuery = @params.DoAsUser();
			string remoteUser = usernameFromQuery == null ? JspHelper.GetDefaultWebUserName(@params
				.Conf()) : usernameFromQuery;
			// not specified in
			// request
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(remoteUser);
			JspHelper.CheckUsername(ugi.GetShortUserName(), usernameFromQuery);
			if (doAsUserFromQuery != null)
			{
				// create and attempt to authorize a proxy user
				ugi = UserGroupInformation.CreateProxyUser(doAsUserFromQuery, ugi);
			}
			return ugi;
		}

		/// <exception cref="System.IO.IOException"/>
		private UserGroupInformation TokenUGI()
		{
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = @params
				.DelegationToken();
			ByteArrayInputStream buf = new ByteArrayInputStream(token.GetIdentifier());
			DataInputStream @in = new DataInputStream(buf);
			DelegationTokenIdentifier id = new DelegationTokenIdentifier();
			id.ReadFields(@in);
			UserGroupInformation ugi = id.GetUser();
			ugi.AddToken(token);
			return ugi;
		}
	}
}
