using System;
using System.IO;
using Com.Google.Common.Base;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Renew delegation tokens over http for use in hftp.</summary>
	[System.Serializable]
	public class RenewDelegationTokenServlet : DfsServlet
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(RenewDelegationTokenServlet
			));

		public const string PathSpec = "/renewDelegationToken";

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
				long result = ugi.DoAs(new _PrivilegedExceptionAction_73(nn, token));
				PrintWriter os = new PrintWriter(new OutputStreamWriter(resp.GetOutputStream(), Charsets
					.Utf8));
				os.WriteLine(result);
				os.Close();
			}
			catch (Exception e)
			{
				// transfer exception over the http
				string exceptionClass = e.GetType().FullName;
				string exceptionMsg = e.GetLocalizedMessage();
				string strException = exceptionClass + ";" + exceptionMsg;
				Log.Info("Exception while renewing token. Re-throwing. s=" + strException, e);
				resp.SendError(HttpServletResponse.ScInternalServerError, strException);
			}
		}

		private sealed class _PrivilegedExceptionAction_73 : PrivilegedExceptionAction<long
			>
		{
			public _PrivilegedExceptionAction_73(NameNode nn, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token)
			{
				this.nn = nn;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public long Run()
			{
				return nn.GetRpcServer().RenewDelegationToken(token);
			}

			private readonly NameNode nn;

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;
		}
	}
}
