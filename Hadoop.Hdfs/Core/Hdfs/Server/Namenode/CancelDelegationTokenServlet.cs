using System;
using System.IO;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Cancel delegation tokens over http for use in hftp.</summary>
	[System.Serializable]
	public class CancelDelegationTokenServlet : DfsServlet
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(CancelDelegationTokenServlet
			));

		public const string PathSpec = "/cancelDelegationToken";

		public const string Token = "token";

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
		{
			UserGroupInformation ugi;
			ServletContext context = GetServletContext();
			Configuration conf = NameNodeHttpServer.GetConfFromContext(context);
			try
			{
				ugi = GetUGI(req, conf);
			}
			catch (IOException ioe)
			{
				Log.Info("Request for token received with no authentication from " + req.GetRemoteAddr
					(), ioe);
				resp.SendError(HttpServletResponse.ScForbidden, "Unable to identify or authenticate user"
					);
				return;
			}
			NameNode nn = NameNodeHttpServer.GetNameNodeFromContext(context);
			string tokenString = req.GetParameter(Token);
			if (tokenString == null)
			{
				resp.SendError(HttpServletResponse.ScMultipleChoices, "Token to renew not specified"
					);
			}
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>();
			token.DecodeFromUrlString(tokenString);
			try
			{
				ugi.DoAs(new _PrivilegedExceptionAction_70(nn, token));
			}
			catch (Exception e)
			{
				Log.Info("Exception while cancelling token. Re-throwing. ", e);
				resp.SendError(HttpServletResponse.ScInternalServerError, e.Message);
			}
		}

		private sealed class _PrivilegedExceptionAction_70 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_70(NameNode nn, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token)
			{
				this.nn = nn;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				nn.GetRpcServer().CancelDelegationToken(token);
				return null;
			}

			private readonly NameNode nn;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;
		}
	}
}
