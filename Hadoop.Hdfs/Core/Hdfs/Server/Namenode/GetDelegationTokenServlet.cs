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
	/// <summary>Serve delegation tokens over http for use in hftp.</summary>
	[System.Serializable]
	public class GetDelegationTokenServlet : DfsServlet
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(GetDelegationTokenServlet
			));

		public const string PathSpec = "/getDelegationToken";

		public const string Renewer = "renewer";

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
			Log.Info("Sending token: {" + ugi.GetUserName() + "," + req.GetRemoteAddr() + "}"
				);
			NameNode nn = NameNodeHttpServer.GetNameNodeFromContext(context);
			string renewer = req.GetParameter(Renewer);
			string renewerFinal = (renewer == null) ? req.GetUserPrincipal().GetName() : renewer;
			DataOutputStream dos = null;
			try
			{
				dos = new DataOutputStream(resp.GetOutputStream());
				DataOutputStream dosFinal = dos;
				// for doAs block
				ugi.DoAs(new _PrivilegedExceptionAction_69(nn, ugi, renewerFinal, dosFinal));
			}
			catch (Exception e)
			{
				Log.Info("Exception while sending token. Re-throwing ", e);
				resp.SendError(HttpServletResponse.ScInternalServerError);
			}
			finally
			{
				if (dos != null)
				{
					dos.Close();
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_69 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_69(NameNode nn, UserGroupInformation ugi, string
				 renewerFinal, DataOutputStream dosFinal)
			{
				this.nn = nn;
				this.ugi = ugi;
				this.renewerFinal = renewerFinal;
				this.dosFinal = dosFinal;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Run()
			{
				Credentials ts = DelegationTokenSecretManager.CreateCredentials(nn, ugi, renewerFinal
					);
				ts.Write(dosFinal);
				return null;
			}

			private readonly NameNode nn;

			private readonly UserGroupInformation ugi;

			private readonly string renewerFinal;

			private readonly DataOutputStream dosFinal;
		}
	}
}
