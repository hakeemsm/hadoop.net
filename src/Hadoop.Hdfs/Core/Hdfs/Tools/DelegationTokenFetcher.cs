using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>
	/// Fetch a DelegationToken from the current Namenode and store it in the
	/// specified file.
	/// </summary>
	public class DelegationTokenFetcher
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(DelegationTokenFetcher
			));

		private const string Webservice = "webservice";

		private const string Renewer = "renewer";

		private const string Cancel = "cancel";

		private const string Renew = "renew";

		private const string Print = "print";

		private const string Help = "help";

		private const string HelpShort = "h";

		private static void PrintUsage(TextWriter err)
		{
			err.WriteLine("fetchdt retrieves delegation tokens from the NameNode");
			err.WriteLine();
			err.WriteLine("fetchdt <opts> <token file>");
			err.WriteLine("Options:");
			err.WriteLine("  --webservice <url>  Url to contact NN on");
			err.WriteLine("  --renewer <name>    Name of the delegation token renewer");
			err.WriteLine("  --cancel            Cancel the delegation token");
			err.WriteLine("  --renew             Renew the delegation token.  Delegation " + 
				"token must have been fetched using the --renewer <name> option.");
			err.WriteLine("  --print             Print the delegation token");
			err.WriteLine();
			GenericOptionsParser.PrintGenericCommandUsage(err);
			ExitUtil.Terminate(1);
		}

		/// <exception cref="System.IO.IOException"/>
		private static ICollection<Org.Apache.Hadoop.Security.Token.Token<object>> ReadTokens
			(Path file, Configuration conf)
		{
			Credentials creds = Credentials.ReadTokenStorageFile(file, conf);
			return creds.GetAllTokens();
		}

		/// <summary>Command-line interface</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			Configuration conf = new HdfsConfiguration();
			Options fetcherOptions = new Options();
			fetcherOptions.AddOption(Webservice, true, "HTTP url to reach the NameNode at");
			fetcherOptions.AddOption(Renewer, true, "Name of the delegation token renewer");
			fetcherOptions.AddOption(Cancel, false, "cancel the token");
			fetcherOptions.AddOption(Renew, false, "renew the token");
			fetcherOptions.AddOption(Print, false, "print the token");
			fetcherOptions.AddOption(HelpShort, Help, false, "print out help information");
			GenericOptionsParser parser = new GenericOptionsParser(conf, fetcherOptions, args
				);
			CommandLine cmd = parser.GetCommandLine();
			// get options
			string webUrl = cmd.HasOption(Webservice) ? cmd.GetOptionValue(Webservice) : null;
			string renewer = cmd.HasOption(Renewer) ? cmd.GetOptionValue(Renewer) : null;
			bool cancel = cmd.HasOption(Cancel);
			bool renew = cmd.HasOption(Renew);
			bool print = cmd.HasOption(Print);
			bool help = cmd.HasOption(Help);
			string[] remaining = parser.GetRemainingArgs();
			// check option validity
			if (help)
			{
				PrintUsage(System.Console.Out);
				System.Environment.Exit(0);
			}
			if (cancel && renew || cancel && print || renew && print || cancel && renew && print)
			{
				System.Console.Error.WriteLine("ERROR: Only specify cancel, renew or print.");
				PrintUsage(System.Console.Error);
			}
			if (remaining.Length != 1 || remaining[0][0] == '-')
			{
				System.Console.Error.WriteLine("ERROR: Must specify exacltly one token file");
				PrintUsage(System.Console.Error);
			}
			// default to using the local file system
			FileSystem local = FileSystem.GetLocal(conf);
			Path tokenFile = new Path(local.GetWorkingDirectory(), remaining[0]);
			URLConnectionFactory connectionFactory = URLConnectionFactory.DefaultSystemConnectionFactory;
			// Login the current user
			UserGroupInformation.GetCurrentUser().DoAs(new _PrivilegedExceptionAction_152(print
				, tokenFile, conf, renew, cancel, webUrl, connectionFactory, renewer));
		}

		private sealed class _PrivilegedExceptionAction_152 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_152(bool print, Path tokenFile, Configuration conf
				, bool renew, bool cancel, string webUrl, URLConnectionFactory connectionFactory
				, string renewer)
			{
				this.print = print;
				this.tokenFile = tokenFile;
				this.conf = conf;
				this.renew = renew;
				this.cancel = cancel;
				this.webUrl = webUrl;
				this.connectionFactory = connectionFactory;
				this.renewer = renewer;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				if (print)
				{
					DelegationTokenIdentifier id = new DelegationTokenSecretManager(0, 0, 0, 0, null)
						.CreateIdentifier();
					foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in DelegationTokenFetcher
						.ReadTokens(tokenFile, conf))
					{
						DataInputStream @in = new DataInputStream(new ByteArrayInputStream(token.GetIdentifier
							()));
						id.ReadFields(@in);
						System.Console.Out.WriteLine("Token (" + id + ") for " + token.GetService());
					}
					return null;
				}
				if (renew)
				{
					foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in DelegationTokenFetcher
						.ReadTokens(tokenFile, conf))
					{
						if (token.IsManaged())
						{
							long result = token.Renew(conf);
							if (DelegationTokenFetcher.Log.IsDebugEnabled())
							{
								DelegationTokenFetcher.Log.Debug("Renewed token for " + token.GetService() + " until: "
									 + Sharpen.Extensions.CreateDate(result));
							}
						}
					}
				}
				else
				{
					if (cancel)
					{
						foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in DelegationTokenFetcher
							.ReadTokens(tokenFile, conf))
						{
							if (token.IsManaged())
							{
								token.Cancel(conf);
								if (DelegationTokenFetcher.Log.IsDebugEnabled())
								{
									DelegationTokenFetcher.Log.Debug("Cancelled token for " + token.GetService());
								}
							}
						}
					}
					else
					{
						// otherwise we are fetching
						if (webUrl != null)
						{
							Credentials creds = DelegationTokenFetcher.GetDTfromRemote(connectionFactory, new 
								URI(webUrl), renewer, null);
							creds.WriteTokenStorageFile(tokenFile, conf);
							foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in creds.GetAllTokens
								())
							{
								System.Console.Out.WriteLine("Fetched token via " + webUrl + " for " + token.GetService
									() + " into " + tokenFile);
							}
						}
						else
						{
							FileSystem fs = FileSystem.Get(conf);
							Credentials cred = new Credentials();
							Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = fs.AddDelegationTokens(
								renewer, cred);
							cred.WriteTokenStorageFile(tokenFile, conf);
							foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in tokens)
							{
								System.Console.Out.WriteLine("Fetched token for " + token.GetService() + " into "
									 + tokenFile);
							}
						}
					}
				}
				return null;
			}

			private readonly bool print;

			private readonly Path tokenFile;

			private readonly Configuration conf;

			private readonly bool renew;

			private readonly bool cancel;

			private readonly string webUrl;

			private readonly URLConnectionFactory connectionFactory;

			private readonly string renewer;
		}

		/// <exception cref="System.IO.IOException"/>
		public static Credentials GetDTfromRemote(URLConnectionFactory factory, URI nnUri
			, string renewer, string proxyUser)
		{
			StringBuilder buf = new StringBuilder(nnUri.ToString()).Append(GetDelegationTokenServlet
				.PathSpec);
			string separator = "?";
			if (renewer != null)
			{
				buf.Append("?").Append(GetDelegationTokenServlet.Renewer).Append("=").Append(renewer
					);
				separator = "&";
			}
			if (proxyUser != null)
			{
				buf.Append(separator).Append("doas=").Append(proxyUser);
			}
			bool isHttps = nnUri.GetScheme().Equals("https");
			HttpURLConnection conn = null;
			DataInputStream dis = null;
			IPEndPoint serviceAddr = NetUtils.CreateSocketAddr(nnUri.GetAuthority());
			try
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Retrieving token from: " + buf);
				}
				conn = Run(factory, new Uri(buf.ToString()));
				InputStream @in = conn.GetInputStream();
				Credentials ts = new Credentials();
				dis = new DataInputStream(@in);
				ts.ReadFields(dis);
				foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in ts.GetAllTokens(
					))
				{
					token.SetKind(isHttps ? HsftpFileSystem.TokenKind : HftpFileSystem.TokenKind);
					SecurityUtil.SetTokenService(token, serviceAddr);
				}
				return ts;
			}
			catch (Exception e)
			{
				throw new IOException("Unable to obtain remote token", e);
			}
			finally
			{
				IOUtils.Cleanup(Log, dis);
				if (conn != null)
				{
					conn.Disconnect();
				}
			}
		}

		/// <summary>Cancel a Delegation Token.</summary>
		/// <param name="nnAddr">the NameNode's address</param>
		/// <param name="tok">the token to cancel</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		public static void CancelDelegationToken(URLConnectionFactory factory, URI nnAddr
			, Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> tok)
		{
			StringBuilder buf = new StringBuilder(nnAddr.ToString()).Append(CancelDelegationTokenServlet
				.PathSpec).Append("?").Append(CancelDelegationTokenServlet.Token).Append("=").Append
				(tok.EncodeToUrlString());
			HttpURLConnection conn = Run(factory, new Uri(buf.ToString()));
			conn.Disconnect();
		}

		/// <summary>Renew a Delegation Token.</summary>
		/// <param name="nnAddr">the NameNode's address</param>
		/// <param name="tok">the token to renew</param>
		/// <returns>the Date that the token will expire next.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		public static long RenewDelegationToken(URLConnectionFactory factory, URI nnAddr, 
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> tok)
		{
			StringBuilder buf = new StringBuilder(nnAddr.ToString()).Append(RenewDelegationTokenServlet
				.PathSpec).Append("?").Append(RenewDelegationTokenServlet.Token).Append("=").Append
				(tok.EncodeToUrlString());
			HttpURLConnection connection = null;
			BufferedReader @in = null;
			try
			{
				connection = Run(factory, new Uri(buf.ToString()));
				@in = new BufferedReader(new InputStreamReader(connection.GetInputStream(), Charsets
					.Utf8));
				long result = long.Parse(@in.ReadLine());
				return result;
			}
			catch (IOException ie)
			{
				Log.Info("error in renew over HTTP", ie);
				IOException e = GetExceptionFromResponse(connection);
				if (e != null)
				{
					Log.Info("rethrowing exception from HTTP request: " + e.GetLocalizedMessage());
					throw e;
				}
				throw;
			}
			finally
			{
				IOUtils.Cleanup(Log, @in);
				if (connection != null)
				{
					connection.Disconnect();
				}
			}
		}

		// parse the message and extract the name of the exception and the message
		private static IOException GetExceptionFromResponse(HttpURLConnection con)
		{
			IOException e = null;
			string resp;
			if (con == null)
			{
				return null;
			}
			try
			{
				resp = con.GetResponseMessage();
			}
			catch (IOException)
			{
				return null;
			}
			if (resp == null || resp.IsEmpty())
			{
				return null;
			}
			string exceptionClass = string.Empty;
			string exceptionMsg = string.Empty;
			string[] rs = resp.Split(";");
			if (rs.Length < 2)
			{
				return null;
			}
			exceptionClass = rs[0];
			exceptionMsg = rs[1];
			Log.Info("Error response from HTTP request=" + resp + ";ec=" + exceptionClass + ";em="
				 + exceptionMsg);
			if (exceptionClass == null || exceptionClass.IsEmpty())
			{
				return null;
			}
			// recreate exception objects
			try
			{
				Type ec = Sharpen.Runtime.GetType(exceptionClass).AsSubclass<Exception>();
				// we are interested in constructor with String arguments
				Constructor<Exception> constructor = ec.GetConstructor(new Type[] { typeof(string
					) });
				// create an instance
				e = (IOException)constructor.NewInstance(exceptionMsg);
			}
			catch (Exception ee)
			{
				Log.Warn("failed to create object of this class", ee);
			}
			if (e == null)
			{
				return null;
			}
			e.SetStackTrace(new StackTraceElement[0]);
			// local stack is not relevant
			Log.Info("Exception from HTTP response=" + e.GetLocalizedMessage());
			return e;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		private static HttpURLConnection Run(URLConnectionFactory factory, Uri url)
		{
			HttpURLConnection conn = null;
			try
			{
				conn = (HttpURLConnection)factory.OpenConnection(url, true);
				if (conn.GetResponseCode() != HttpURLConnection.HttpOk)
				{
					string msg = conn.GetResponseMessage();
					throw new IOException("Error when dealing remote token: " + msg);
				}
			}
			catch (IOException ie)
			{
				Log.Info("Error when dealing remote token:", ie);
				IOException e = GetExceptionFromResponse(conn);
				if (e != null)
				{
					Log.Info("rethrowing exception from HTTP request: " + e.GetLocalizedMessage());
					throw e;
				}
				throw;
			}
			return conn;
		}
	}
}
