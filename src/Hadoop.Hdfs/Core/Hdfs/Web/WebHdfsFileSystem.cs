using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Javax.WS.RS.Core;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Util;
using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>A FileSystem for HDFS over the web.</summary>
	public class WebHdfsFileSystem : FileSystem, DelegationTokenRenewer.Renewable, TokenAspect.TokenManagementDelegator
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(WebHdfsFileSystem));

		/// <summary>File System URI: {SCHEME}://namenode:port/path/to/file</summary>
		public const string Scheme = "webhdfs";

		/// <summary>WebHdfs version.</summary>
		public const int Version = 1;

		/// <summary>Http URI: http://namenode:port/{PATH_PREFIX}/path/to/file</summary>
		public const string PathPrefix = "/" + Scheme + "/v" + Version;

		/// <summary>Default connection factory may be overridden in tests to use smaller timeout values
		/// 	</summary>
		protected internal URLConnectionFactory connectionFactory;

		/// <summary>Delegation token kind</summary>
		public static readonly Text TokenKind = new Text("WEBHDFS delegation");

		[VisibleForTesting]
		public const string CantFallbackToInsecureMsg = "The client is configured to only allow connecting to secure cluster";

		private bool canRefreshDelegationToken;

		private UserGroupInformation ugi;

		private URI uri;

		private Org.Apache.Hadoop.Security.Token.Token<object> delegationToken;

		protected internal Text tokenServiceName;

		private RetryPolicy retryPolicy = null;

		private Path workingDir;

		private IPEndPoint[] nnAddrs;

		private int currentNNAddrIndex;

		private bool disallowFallbackToInsecureCluster;

		/// <summary>Return the protocol scheme for the FileSystem.</summary>
		/// <remarks>
		/// Return the protocol scheme for the FileSystem.
		/// <p/>
		/// </remarks>
		/// <returns><code>webhdfs</code></returns>
		public override string GetScheme()
		{
			return Scheme;
		}

		/// <summary>return the underlying transport protocol (http / https).</summary>
		protected internal virtual string GetTransportScheme()
		{
			return "http";
		}

		protected internal virtual Text GetTokenKind()
		{
			return TokenKind;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(URI uri, Configuration conf)
		{
			lock (this)
			{
				base.Initialize(uri, conf);
				SetConf(conf);
				UserParam.SetUserPattern(conf.Get(DFSConfigKeys.DfsWebhdfsUserPatternKey, DFSConfigKeys
					.DfsWebhdfsUserPatternDefault));
				connectionFactory = URLConnectionFactory.NewDefaultURLConnectionFactory(conf);
				ugi = UserGroupInformation.GetCurrentUser();
				this.uri = URI.Create(uri.GetScheme() + "://" + uri.GetAuthority());
				this.nnAddrs = ResolveNNAddr();
				bool isHA = HAUtil.IsClientFailoverConfigured(conf, this.uri);
				bool isLogicalUri = isHA && HAUtil.IsLogicalUri(conf, this.uri);
				// In non-HA or non-logical URI case, the code needs to call
				// getCanonicalUri() in order to handle the case where no port is
				// specified in the URI
				this.tokenServiceName = isLogicalUri ? HAUtil.BuildTokenServiceForLogicalUri(uri, 
					GetScheme()) : SecurityUtil.BuildTokenService(GetCanonicalUri());
				if (!isHA)
				{
					this.retryPolicy = RetryUtils.GetDefaultRetryPolicy(conf, DFSConfigKeys.DfsHttpClientRetryPolicyEnabledKey
						, DFSConfigKeys.DfsHttpClientRetryPolicyEnabledDefault, DFSConfigKeys.DfsHttpClientRetryPolicySpecKey
						, DFSConfigKeys.DfsHttpClientRetryPolicySpecDefault, typeof(SafeModeException));
				}
				else
				{
					int maxFailoverAttempts = conf.GetInt(DFSConfigKeys.DfsHttpClientFailoverMaxAttemptsKey
						, DFSConfigKeys.DfsHttpClientFailoverMaxAttemptsDefault);
					int maxRetryAttempts = conf.GetInt(DFSConfigKeys.DfsHttpClientRetryMaxAttemptsKey
						, DFSConfigKeys.DfsHttpClientRetryMaxAttemptsDefault);
					int failoverSleepBaseMillis = conf.GetInt(DFSConfigKeys.DfsHttpClientFailoverSleeptimeBaseKey
						, DFSConfigKeys.DfsHttpClientFailoverSleeptimeBaseDefault);
					int failoverSleepMaxMillis = conf.GetInt(DFSConfigKeys.DfsHttpClientFailoverSleeptimeMaxKey
						, DFSConfigKeys.DfsHttpClientFailoverSleeptimeMaxDefault);
					this.retryPolicy = RetryPolicies.FailoverOnNetworkException(RetryPolicies.TryOnceThenFail
						, maxFailoverAttempts, maxRetryAttempts, failoverSleepBaseMillis, failoverSleepMaxMillis
						);
				}
				this.workingDir = GetHomeDirectory();
				this.canRefreshDelegationToken = UserGroupInformation.IsSecurityEnabled();
				this.disallowFallbackToInsecureCluster = !conf.GetBoolean(CommonConfigurationKeys
					.IpcClientFallbackToSimpleAuthAllowedKey, CommonConfigurationKeys.IpcClientFallbackToSimpleAuthAllowedDefault
					);
				this.delegationToken = null;
			}
		}

		protected override URI GetCanonicalUri()
		{
			return base.GetCanonicalUri();
		}

		/// <summary>Is WebHDFS enabled in conf?</summary>
		public static bool IsEnabled(Configuration conf, Log log)
		{
			bool b = conf.GetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, DFSConfigKeys.DfsWebhdfsEnabledDefault
				);
			return b;
		}

		private sealed class _AbstractDelegationTokenSelector_221 : AbstractDelegationTokenSelector
			<DelegationTokenIdentifier>
		{
			public _AbstractDelegationTokenSelector_221(Text baseArg1)
				: base(baseArg1)
			{
			}
		}

		internal TokenSelector<DelegationTokenIdentifier> tokenSelector;

		// the first getAuthParams() for a non-token op will either get the
		// internal token from the ugi or lazy fetch one
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationToken
			()
		{
			lock (this)
			{
				if (canRefreshDelegationToken && delegationToken == null)
				{
					Org.Apache.Hadoop.Security.Token.Token<object> token = tokenSelector.SelectToken(
						new Text(GetCanonicalServiceName()), ugi.GetTokens());
					// ugi tokens are usually indicative of a task which can't
					// refetch tokens.  even if ugi has credentials, don't attempt
					// to get another token to match hdfs/rpc behavior
					if (token != null)
					{
						Log.Debug("Using UGI token: " + token);
						canRefreshDelegationToken = false;
					}
					else
					{
						token = ((Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>)GetDelegationToken
							(null));
						if (token != null)
						{
							Log.Debug("Fetched new token: " + token);
						}
						else
						{
							// security is disabled
							canRefreshDelegationToken = false;
						}
					}
					SetDelegationToken(token);
				}
				return delegationToken;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual bool ReplaceExpiredDelegationToken()
		{
			lock (this)
			{
				bool replaced = false;
				if (canRefreshDelegationToken)
				{
					Org.Apache.Hadoop.Security.Token.Token<object> token = ((Org.Apache.Hadoop.Security.Token.Token
						<DelegationTokenIdentifier>)GetDelegationToken(null));
					Log.Debug("Replaced expired token: " + token);
					SetDelegationToken(token);
					replaced = (token != null);
				}
				return replaced;
			}
		}

		[VisibleForTesting]
		protected override int GetDefaultPort()
		{
			return GetConf().GetInt(DFSConfigKeys.DfsNamenodeHttpPortKey, DFSConfigKeys.DfsNamenodeHttpPortDefault
				);
		}

		public override URI GetUri()
		{
			return this.uri;
		}

		protected override URI CanonicalizeUri(URI uri)
		{
			return NetUtils.GetCanonicalUri(uri, GetDefaultPort());
		}

		/// <returns>the home directory.</returns>
		public static string GetHomeDirectoryString(UserGroupInformation ugi)
		{
			return "/user/" + ugi.GetShortUserName();
		}

		public override Path GetHomeDirectory()
		{
			return MakeQualified(new Path(GetHomeDirectoryString(ugi)));
		}

		public override Path GetWorkingDirectory()
		{
			lock (this)
			{
				return workingDir;
			}
		}

		public override void SetWorkingDirectory(Path dir)
		{
			lock (this)
			{
				string result = MakeAbsolute(dir).ToUri().GetPath();
				if (!DFSUtil.IsValidName(result))
				{
					throw new ArgumentException("Invalid DFS directory name " + result);
				}
				workingDir = MakeAbsolute(dir);
			}
		}

		private Path MakeAbsolute(Path f)
		{
			return f.IsAbsolute() ? f : new Path(workingDir, f);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static IDictionary<object, object> JsonParse(HttpURLConnection c, bool useErrorStream
			)
		{
			if (c.GetContentLength() == 0)
			{
				return null;
			}
			InputStream @in = useErrorStream ? c.GetErrorStream() : c.GetInputStream();
			if (@in == null)
			{
				throw new IOException("The " + (useErrorStream ? "error" : "input") + " stream is null."
					);
			}
			try
			{
				string contentType = c.GetContentType();
				if (contentType != null)
				{
					MediaType parsed = MediaType.ValueOf(contentType);
					if (!MediaType.ApplicationJsonType.IsCompatible(parsed))
					{
						throw new IOException("Content-Type \"" + contentType + "\" is incompatible with \""
							 + MediaType.ApplicationJson + "\" (parsed=\"" + parsed + "\")");
					}
				}
				ObjectMapper mapper = new ObjectMapper();
				return mapper.Reader(typeof(IDictionary)).ReadValue(@in);
			}
			finally
			{
				@in.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static IDictionary<object, object> ValidateResponse(HttpOpParam.OP op, HttpURLConnection
			 conn, bool unwrapException)
		{
			int code = conn.GetResponseCode();
			// server is demanding an authentication we don't support
			if (code == HttpURLConnection.HttpUnauthorized)
			{
				// match hdfs/rpc exception
				throw new AccessControlException(conn.GetResponseMessage());
			}
			if (code != op.GetExpectedHttpResponseCode())
			{
				IDictionary<object, object> m;
				try
				{
					m = JsonParse(conn, true);
				}
				catch (Exception e)
				{
					throw new IOException("Unexpected HTTP response: code=" + code + " != " + op.GetExpectedHttpResponseCode
						() + ", " + op.ToQueryString() + ", message=" + conn.GetResponseMessage(), e);
				}
				if (m == null)
				{
					throw new IOException("Unexpected HTTP response: code=" + code + " != " + op.GetExpectedHttpResponseCode
						() + ", " + op.ToQueryString() + ", message=" + conn.GetResponseMessage());
				}
				else
				{
					if (m[typeof(RemoteException).Name] == null)
					{
						return m;
					}
				}
				IOException re = JsonUtil.ToRemoteException(m);
				// extract UGI-related exceptions and unwrap InvalidToken
				// the NN mangles these exceptions but the DN does not and may need
				// to re-fetch a token if either report the token is expired
				if (re.Message != null && re.Message.StartsWith(SecurityUtil.FailedToGetUgiMsgHeader
					))
				{
					string[] parts = re.Message.Split(":\\s+", 3);
					re = new RemoteException(parts[1], parts[2]);
					re = ((RemoteException)re).UnwrapRemoteException(typeof(SecretManager.InvalidToken
						));
				}
				throw unwrapException ? ToIOException(re) : re;
			}
			return null;
		}

		/// <summary>Covert an exception to an IOException.</summary>
		/// <remarks>
		/// Covert an exception to an IOException.
		/// For a non-IOException, wrap it with IOException.
		/// For a RemoteException, unwrap it.
		/// For an IOException which is not a RemoteException, return it.
		/// </remarks>
		private static IOException ToIOException(Exception e)
		{
			if (!(e is IOException))
			{
				return new IOException(e);
			}
			IOException ioe = (IOException)e;
			if (!(ioe is RemoteException))
			{
				return ioe;
			}
			return ((RemoteException)ioe).UnwrapRemoteException();
		}

		private IPEndPoint GetCurrentNNAddr()
		{
			lock (this)
			{
				return nnAddrs[currentNNAddrIndex];
			}
		}

		/// <summary>Reset the appropriate state to gracefully fail over to another name node
		/// 	</summary>
		private void ResetStateToFailOver()
		{
			lock (this)
			{
				currentNNAddrIndex = (currentNNAddrIndex + 1) % nnAddrs.Length;
			}
		}

		/// <summary>Return a URL pointing to given path on the namenode.</summary>
		/// <param name="path">to obtain the URL for</param>
		/// <param name="query">string to append to the path</param>
		/// <returns>namenode URL referring to the given path</returns>
		/// <exception cref="System.IO.IOException">on error constructing the URL</exception>
		private Uri GetNamenodeURL(string path, string query)
		{
			IPEndPoint nnAddr = GetCurrentNNAddr();
			Uri url = new Uri(GetTransportScheme(), nnAddr.GetHostName(), nnAddr.Port, path +
				 '?' + query);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("url=" + url);
			}
			return url;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual Param<object, object>[] GetAuthParameters(HttpOpParam.OP op)
		{
			IList<Param<object, object>> authParams = Lists.NewArrayList();
			// Skip adding delegation token for token operations because these
			// operations require authentication.
			Org.Apache.Hadoop.Security.Token.Token<object> token = null;
			if (!op.GetRequireAuth())
			{
				token = GetDelegationToken();
			}
			if (token != null)
			{
				authParams.AddItem(new DelegationParam(token.EncodeToUrlString()));
			}
			else
			{
				UserGroupInformation userUgi = ugi;
				UserGroupInformation realUgi = userUgi.GetRealUser();
				if (realUgi != null)
				{
					// proxy user
					authParams.AddItem(new DoAsParam(userUgi.GetShortUserName()));
					userUgi = realUgi;
				}
				authParams.AddItem(new UserParam(userUgi.GetShortUserName()));
			}
			return Sharpen.Collections.ToArray(authParams, new Param<object, object>[0]);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual Uri ToUrl(HttpOpParam.OP op, Path fspath, params Param<object, object
			>[] parameters)
		{
			//initialize URI path and query
			string path = PathPrefix + (fspath == null ? "/" : MakeQualified(fspath).ToUri().
				GetRawPath());
			string query = op.ToQueryString() + Param.ToSortedString("&", GetAuthParameters(op
				)) + Param.ToSortedString("&", parameters);
			Uri url = GetNamenodeURL(path, query);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("url=" + url);
			}
			return url;
		}

		/// <summary>
		/// This class is for initialing a HTTP connection, connecting to server,
		/// obtaining a response, and also handling retry on failures.
		/// </summary>
		internal abstract class AbstractRunner<T>
		{
			/// <exception cref="System.IO.IOException"/>
			protected internal abstract Uri GetUrl();

			protected internal readonly HttpOpParam.OP op;

			private readonly bool redirected;

			protected internal ExcludeDatanodesParam excludeDatanodes = new ExcludeDatanodesParam
				(string.Empty);

			private bool checkRetry;

			protected internal AbstractRunner(WebHdfsFileSystem _enclosing, HttpOpParam.OP op
				, bool redirected)
			{
				this._enclosing = _enclosing;
				this.op = op;
				this.redirected = redirected;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual T Run()
			{
				UserGroupInformation connectUgi = this._enclosing.ugi.GetRealUser();
				if (connectUgi == null)
				{
					connectUgi = this._enclosing.ugi;
				}
				if (this.op.GetRequireAuth())
				{
					connectUgi.CheckTGTAndReloginFromKeytab();
				}
				try
				{
					// the entire lifecycle of the connection must be run inside the
					// doAs to ensure authentication is performed correctly
					return connectUgi.DoAs(new _PrivilegedExceptionAction_489(this));
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
			}

			private sealed class _PrivilegedExceptionAction_489 : PrivilegedExceptionAction<T
				>
			{
				public _PrivilegedExceptionAction_489(AbstractRunner<T> _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.IO.IOException"/>
				public T Run()
				{
					return this._enclosing.RunWithRetry();
				}

				private readonly AbstractRunner<T> _enclosing;
			}

			/// <summary>
			/// Two-step requests redirected to a DN
			/// Create/Append:
			/// Step 1) Submit a Http request with neither auto-redirect nor data.
			/// </summary>
			/// <remarks>
			/// Two-step requests redirected to a DN
			/// Create/Append:
			/// Step 1) Submit a Http request with neither auto-redirect nor data.
			/// Step 2) Submit another Http request with the URL from the Location header with data.
			/// The reason of having two-step create/append is for preventing clients to
			/// send out the data before the redirect. This issue is addressed by the
			/// "Expect: 100-continue" header in HTTP/1.1; see RFC 2616, Section 8.2.3.
			/// Unfortunately, there are software library bugs (e.g. Jetty 6 http server
			/// and Java 6 http client), which do not correctly implement "Expect:
			/// 100-continue". The two-step create/append is a temporary workaround for
			/// the software library bugs.
			/// Open/Checksum
			/// Also implements two-step connects for other operations redirected to
			/// a DN such as open and checksum
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			private HttpURLConnection Connect(Uri url)
			{
				//redirect hostname and port
				string redirectHost = null;
				// resolve redirects for a DN operation unless already resolved
				if (this.op.GetRedirect() && !this.redirected)
				{
					HttpOpParam.OP redirectOp = HttpOpParam.TemporaryRedirectOp.ValueOf(this.op);
					HttpURLConnection conn = this.Connect(redirectOp, url);
					// application level proxy like httpfs might not issue a redirect
					if (conn.GetResponseCode() == this.op.GetExpectedHttpResponseCode())
					{
						return conn;
					}
					try
					{
						WebHdfsFileSystem.ValidateResponse(redirectOp, conn, false);
						url = new Uri(conn.GetHeaderField("Location"));
						redirectHost = url.GetHost() + ":" + url.Port;
					}
					finally
					{
						conn.Disconnect();
					}
				}
				try
				{
					return this.Connect(this.op, url);
				}
				catch (IOException ioe)
				{
					if (redirectHost != null)
					{
						if (this.excludeDatanodes.GetValue() != null)
						{
							this.excludeDatanodes = new ExcludeDatanodesParam(redirectHost + "," + this.excludeDatanodes
								.GetValue());
						}
						else
						{
							this.excludeDatanodes = new ExcludeDatanodesParam(redirectHost);
						}
					}
					throw;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private HttpURLConnection Connect(HttpOpParam.OP op, Uri url)
			{
				HttpURLConnection conn = (HttpURLConnection)this._enclosing.connectionFactory.OpenConnection
					(url);
				bool doOutput = op.GetDoOutput();
				conn.SetRequestMethod(op.GetType().ToString());
				conn.SetInstanceFollowRedirects(false);
				switch (op.GetType())
				{
					case HttpOpParam.Type.Post:
					case HttpOpParam.Type.Put:
					{
						// if not sending a message body for a POST or PUT operation, need
						// to ensure the server/proxy knows this 
						conn.SetDoOutput(true);
						if (!doOutput)
						{
							// explicitly setting content-length to 0 won't do spnego!!
							// opening and closing the stream will send "Content-Length: 0"
							conn.GetOutputStream().Close();
						}
						else
						{
							conn.SetRequestProperty("Content-Type", MediaType.ApplicationOctetStream);
							conn.SetChunkedStreamingMode(32 << 10);
						}
						//32kB-chunk
						break;
					}

					default:
					{
						conn.SetDoOutput(doOutput);
						break;
					}
				}
				conn.Connect();
				return conn;
			}

			/// <exception cref="System.IO.IOException"/>
			private T RunWithRetry()
			{
				for (int retry = 0; ; retry++)
				{
					this.checkRetry = !this.redirected;
					Uri url = this.GetUrl();
					try
					{
						HttpURLConnection conn = this.Connect(url);
						// output streams will validate on close
						if (!this.op.GetDoOutput())
						{
							WebHdfsFileSystem.ValidateResponse(this.op, conn, false);
						}
						return this.GetResponse(conn);
					}
					catch (AccessControlException ace)
					{
						// no retries for auth failures
						throw;
					}
					catch (SecretManager.InvalidToken it)
					{
						// try to replace the expired token with a new one.  the attempt
						// to acquire a new token must be outside this operation's retry
						// so if it fails after its own retries, this operation fails too.
						if (this.op.GetRequireAuth() || !this._enclosing.ReplaceExpiredDelegationToken())
						{
							throw;
						}
					}
					catch (IOException ioe)
					{
						this.ShouldRetry(ioe, retry);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void ShouldRetry(IOException ioe, int retry)
			{
				IPEndPoint nnAddr = this._enclosing.GetCurrentNNAddr();
				if (this.checkRetry)
				{
					try
					{
						RetryPolicy.RetryAction a = this._enclosing.retryPolicy.ShouldRetry(ioe, retry, 0
							, true);
						bool isRetry = a.action == RetryPolicy.RetryAction.RetryDecision.Retry;
						bool isFailoverAndRetry = a.action == RetryPolicy.RetryAction.RetryDecision.FailoverAndRetry;
						if (isRetry || isFailoverAndRetry)
						{
							WebHdfsFileSystem.Log.Info("Retrying connect to namenode: " + nnAddr + ". Already tried "
								 + retry + " time(s); retry policy is " + this._enclosing.retryPolicy + ", delay "
								 + a.delayMillis + "ms.");
							if (isFailoverAndRetry)
							{
								this._enclosing.ResetStateToFailOver();
							}
							Sharpen.Thread.Sleep(a.delayMillis);
							return;
						}
					}
					catch (Exception e)
					{
						WebHdfsFileSystem.Log.Warn("Original exception is ", ioe);
						throw WebHdfsFileSystem.ToIOException(e);
					}
				}
				throw WebHdfsFileSystem.ToIOException(ioe);
			}

			/// <exception cref="System.IO.IOException"/>
			internal abstract T GetResponse(HttpURLConnection conn);

			private readonly WebHdfsFileSystem _enclosing;
		}

		/// <summary>Abstract base class to handle path-based operations with params</summary>
		internal abstract class AbstractFsPathRunner<T> : WebHdfsFileSystem.AbstractRunner
			<T>
		{
			private readonly Path fspath;

			private readonly Param<object, object>[] parameters;

			internal AbstractFsPathRunner(WebHdfsFileSystem _enclosing, HttpOpParam.OP op, Path
				 fspath, params Param<object, object>[] parameters)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.fspath = fspath;
				this.parameters = parameters;
			}

			internal AbstractFsPathRunner(WebHdfsFileSystem _enclosing, HttpOpParam.OP op, Param
				<object, object>[] parameters, Path fspath)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.fspath = fspath;
				this.parameters = parameters;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override Uri GetUrl()
			{
				if (this.excludeDatanodes.GetValue() != null)
				{
					Param<object, object>[] tmpParam = new Param<object, object>[this.parameters.Length
						 + 1];
					System.Array.Copy(this.parameters, 0, tmpParam, 0, this.parameters.Length);
					tmpParam[this.parameters.Length] = this.excludeDatanodes;
					return this._enclosing.ToUrl(this.op, this.fspath, tmpParam);
				}
				else
				{
					return this._enclosing.ToUrl(this.op, this.fspath, this.parameters);
				}
			}

			private readonly WebHdfsFileSystem _enclosing;
		}

		/// <summary>Default path-based implementation expects no json response</summary>
		internal class FsPathRunner : WebHdfsFileSystem.AbstractFsPathRunner<Void>
		{
			internal FsPathRunner(WebHdfsFileSystem _enclosing, HttpOpParam.OP op, Path fspath
				, params Param<object, object>[] parameters)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override Void GetResponse(HttpURLConnection conn)
			{
				return null;
			}

			private readonly WebHdfsFileSystem _enclosing;
		}

		/// <summary>Handle path-based operations with a json response</summary>
		internal abstract class FsPathResponseRunner<T> : WebHdfsFileSystem.AbstractFsPathRunner
			<T>
		{
			internal FsPathResponseRunner(WebHdfsFileSystem _enclosing, HttpOpParam.OP op, Path
				 fspath, params Param<object, object>[] parameters)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal FsPathResponseRunner(WebHdfsFileSystem _enclosing, HttpOpParam.OP op, Param
				<object, object>[] parameters, Path fspath)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			internal sealed override T GetResponse(HttpURLConnection conn)
			{
				try
				{
					IDictionary<object, object> json = WebHdfsFileSystem.JsonParse(conn, false);
					if (json == null)
					{
						// match exception class thrown by parser
						throw new InvalidOperationException("Missing response");
					}
					return this.DecodeResponse(json);
				}
				catch (IOException ioe)
				{
					throw;
				}
				catch (Exception e)
				{
					// catch json parser errors
					IOException ioe = new IOException("Response decoding failure: " + e.ToString(), e
						);
					if (WebHdfsFileSystem.Log.IsDebugEnabled())
					{
						WebHdfsFileSystem.Log.Debug(ioe);
					}
					throw ioe;
				}
				finally
				{
					conn.Disconnect();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal abstract T DecodeResponse<_T0>(IDictionary<_T0> json);

			private readonly WebHdfsFileSystem _enclosing;
		}

		/// <summary>Handle path-based operations with json boolean response</summary>
		internal class FsPathBooleanRunner : WebHdfsFileSystem.FsPathResponseRunner<bool>
		{
			internal FsPathBooleanRunner(WebHdfsFileSystem _enclosing, HttpOpParam.OP op, Path
				 fspath, params Param<object, object>[] parameters)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override bool DecodeResponse<_T0>(IDictionary<_T0> json)
			{
				return (bool)json["boolean"];
			}

			private readonly WebHdfsFileSystem _enclosing;
		}

		/// <summary>Handle create/append output streams</summary>
		internal class FsPathOutputStreamRunner : WebHdfsFileSystem.AbstractFsPathRunner<
			FSDataOutputStream>
		{
			private readonly int bufferSize;

			internal FsPathOutputStreamRunner(WebHdfsFileSystem _enclosing, HttpOpParam.OP op
				, Path fspath, int bufferSize, params Param<object, object>[] parameters)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.bufferSize = bufferSize;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override FSDataOutputStream GetResponse(HttpURLConnection conn)
			{
				return new _FSDataOutputStream_785(this, conn, new BufferedOutputStream(conn.GetOutputStream
					(), this.bufferSize), this._enclosing.statistics);
			}

			private sealed class _FSDataOutputStream_785 : FSDataOutputStream
			{
				public _FSDataOutputStream_785(FsPathOutputStreamRunner _enclosing, HttpURLConnection
					 conn, OutputStream baseArg1, FileSystem.Statistics baseArg2)
					: base(baseArg1, baseArg2)
				{
					this._enclosing = _enclosing;
					this.conn = conn;
				}

				/// <exception cref="System.IO.IOException"/>
				public override void Close()
				{
					try
					{
						base.Close();
					}
					finally
					{
						try
						{
							WebHdfsFileSystem.ValidateResponse(this._enclosing._enclosing._enclosing.op, conn
								, true);
						}
						finally
						{
							conn.Disconnect();
						}
					}
				}

				private readonly FsPathOutputStreamRunner _enclosing;

				private readonly HttpURLConnection conn;
			}

			private readonly WebHdfsFileSystem _enclosing;
		}

		internal class FsPathConnectionRunner : WebHdfsFileSystem.AbstractFsPathRunner<HttpURLConnection
			>
		{
			internal FsPathConnectionRunner(WebHdfsFileSystem _enclosing, HttpOpParam.OP op, 
				Path fspath, params Param<object, object>[] parameters)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override HttpURLConnection GetResponse(HttpURLConnection conn)
			{
				return conn;
			}

			private readonly WebHdfsFileSystem _enclosing;
		}

		/// <summary>Used by open() which tracks the resolved url itself</summary>
		internal sealed class URLRunner : WebHdfsFileSystem.AbstractRunner<HttpURLConnection
			>
		{
			private readonly Uri url;

			protected internal override Uri GetUrl()
			{
				return this.url;
			}

			protected internal URLRunner(WebHdfsFileSystem _enclosing, HttpOpParam.OP op, Uri
				 url, bool redirected)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.url = url;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override HttpURLConnection GetResponse(HttpURLConnection conn)
			{
				return conn;
			}

			private readonly WebHdfsFileSystem _enclosing;
		}

		private FsPermission ApplyUMask(FsPermission permission)
		{
			if (permission == null)
			{
				permission = FsPermission.GetDefault();
			}
			return permission.ApplyUMask(FsPermission.GetUMask(GetConf()));
		}

		/// <exception cref="System.IO.IOException"/>
		private HdfsFileStatus GetHdfsFileStatus(Path f)
		{
			HttpOpParam.OP op = GetOpParam.OP.Getfilestatus;
			//HM: 3rd parm being passed in as null to make Sharpen happy.remove
			HdfsFileStatus status = new _FsPathResponseRunner_844(op, f, null).Run();
			if (status == null)
			{
				throw new FileNotFoundException("File does not exist: " + f);
			}
			return status;
		}

		private sealed class _FsPathResponseRunner_844 : WebHdfsFileSystem.FsPathResponseRunner
			<HdfsFileStatus>
		{
			public _FsPathResponseRunner_844(HttpOpParam.OP baseArg1, Path baseArg2, Param<object
				, object>[] baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			internal override HdfsFileStatus DecodeResponse<_T0>(IDictionary<_T0> json)
			{
				return JsonUtil.ToFileStatus(json, true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			statistics.IncrementReadOps(1);
			return MakeQualified(GetHdfsFileStatus(f), f);
		}

		private FileStatus MakeQualified(HdfsFileStatus f, Path parent)
		{
			return new FileStatus(f.GetLen(), f.IsDir(), f.GetReplication(), f.GetBlockSize()
				, f.GetModificationTime(), f.GetAccessTime(), f.GetPermission(), f.GetOwner(), f
				.GetGroup(), f.IsSymlink() ? new Path(f.GetSymlink()) : null, f.GetFullPath(parent
				).MakeQualified(GetUri(), GetWorkingDirectory()));
		}

		/// <exception cref="System.IO.IOException"/>
		public override AclStatus GetAclStatus(Path f)
		{
			HttpOpParam.OP op = GetOpParam.OP.Getaclstatus;
			//HM: 3rd parm being passed in as null to make Sharpen happy.remove
			AclStatus status = new _FsPathResponseRunner_874(op, f, null).Run();
			if (status == null)
			{
				throw new FileNotFoundException("File does not exist: " + f);
			}
			return status;
		}

		private sealed class _FsPathResponseRunner_874 : WebHdfsFileSystem.FsPathResponseRunner
			<AclStatus>
		{
			public _FsPathResponseRunner_874(HttpOpParam.OP baseArg1, Path baseArg2, Param<object
				, object>[] baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			internal override AclStatus DecodeResponse<_T0>(IDictionary<_T0> json)
			{
				return JsonUtil.ToAclStatus(json);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Mkdirs(Path f, FsPermission permission)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Mkdirs;
			return new WebHdfsFileSystem.FsPathBooleanRunner(this, op, f, new PermissionParam
				(ApplyUMask(permission))).Run();
		}

		/// <summary>Create a symlink pointing to the destination path.</summary>
		/// <seealso cref="Org.Apache.Hadoop.FS.Hdfs.CreateSymlink(Org.Apache.Hadoop.FS.Path, Org.Apache.Hadoop.FS.Path, bool)
		/// 	"></seealso>
		/// <exception cref="System.IO.IOException"/>
		public override void CreateSymlink(Path destination, Path f, bool createParent)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Createsymlink;
			new WebHdfsFileSystem.FsPathRunner(this, op, f, new DestinationParam(MakeQualified
				(destination).ToUri().GetPath()), new CreateParentParam(createParent)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Rename(Path src, Path dst)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Rename;
			return new WebHdfsFileSystem.FsPathBooleanRunner(this, op, src, new DestinationParam
				(MakeQualified(dst).ToUri().GetPath())).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void Rename(Path src, Path dst, params Options.Rename[] options
			)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Rename;
			new WebHdfsFileSystem.FsPathRunner(this, op, src, new DestinationParam(MakeQualified
				(dst).ToUri().GetPath()), new RenameOptionSetParam(options)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetXAttr(Path p, string name, byte[] value, EnumSet<XAttrSetFlag
			> flag)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Setxattr;
			if (value != null)
			{
				new WebHdfsFileSystem.FsPathRunner(this, op, p, new XAttrNameParam(name), new XAttrValueParam
					(XAttrCodec.EncodeValue(value, XAttrCodec.Hex)), new XAttrSetFlagParam(flag)).Run
					();
			}
			else
			{
				new WebHdfsFileSystem.FsPathRunner(this, op, p, new XAttrNameParam(name), new XAttrSetFlagParam
					(flag)).Run();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] GetXAttr(Path p, string name)
		{
			HttpOpParam.OP op = GetOpParam.OP.Getxattrs;
			return new _FsPathResponseRunner_949(name, op, p, new XAttrNameParam(name)).Run();
		}

		private sealed class _FsPathResponseRunner_949 : WebHdfsFileSystem.FsPathResponseRunner
			<byte[]>
		{
			public _FsPathResponseRunner_949(string name, HttpOpParam.OP baseArg1, Path baseArg2
				, Param<object, object>[] baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override byte[] DecodeResponse<_T0>(IDictionary<_T0> json)
			{
				return JsonUtil.GetXAttr(json, name);
			}

			private readonly string name;
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path p)
		{
			HttpOpParam.OP op = GetOpParam.OP.Getxattrs;
			return new _FsPathResponseRunner_961(op, p, new XAttrEncodingParam(XAttrCodec.Hex
				)).Run();
		}

		private sealed class _FsPathResponseRunner_961 : WebHdfsFileSystem.FsPathResponseRunner
			<IDictionary<string, byte[]>>
		{
			public _FsPathResponseRunner_961(HttpOpParam.OP baseArg1, Path baseArg2, Param<object
				, object>[] baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override IDictionary<string, byte[]> DecodeResponse<_T0>(IDictionary<_T0
				> json)
			{
				return JsonUtil.ToXAttrs(json);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path p, IList<string> names
			)
		{
			Preconditions.CheckArgument(names != null && !names.IsEmpty(), "XAttr names cannot be null or empty."
				);
			Param<object, object>[] parameters = new Param<object, object>[names.Count + 1];
			for (int i = 0; i < parameters.Length - 1; i++)
			{
				parameters[i] = new XAttrNameParam(names[i]);
			}
			parameters[parameters.Length - 1] = new XAttrEncodingParam(XAttrCodec.Hex);
			HttpOpParam.OP op = GetOpParam.OP.Getxattrs;
			return new _FsPathResponseRunner_981(op, parameters, p).Run();
		}

		private sealed class _FsPathResponseRunner_981 : WebHdfsFileSystem.FsPathResponseRunner
			<IDictionary<string, byte[]>>
		{
			public _FsPathResponseRunner_981(HttpOpParam.OP baseArg1, Param<object, object>[]
				 baseArg2, Path baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override IDictionary<string, byte[]> DecodeResponse<_T0>(IDictionary<_T0
				> json)
			{
				return JsonUtil.ToXAttrs(json);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> ListXAttrs(Path p)
		{
			HttpOpParam.OP op = GetOpParam.OP.Listxattrs;
			//HM: 3rd parm being passed in as null to make Sharpen happy.remove
			return new _FsPathResponseRunner_993(op, p, null).Run();
		}

		private sealed class _FsPathResponseRunner_993 : WebHdfsFileSystem.FsPathResponseRunner
			<IList<string>>
		{
			public _FsPathResponseRunner_993(HttpOpParam.OP baseArg1, Path baseArg2, Param<object
				, object>[] baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override IList<string> DecodeResponse<_T0>(IDictionary<_T0> json)
			{
				return JsonUtil.ToXAttrNames(json);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveXAttr(Path p, string name)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Removexattr;
			new WebHdfsFileSystem.FsPathRunner(this, op, p, new XAttrNameParam(name)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetOwner(Path p, string owner, string group)
		{
			if (owner == null && group == null)
			{
				throw new IOException("owner == null && group == null");
			}
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Setowner;
			new WebHdfsFileSystem.FsPathRunner(this, op, p, new OwnerParam(owner), new GroupParam
				(group)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetPermission(Path p, FsPermission permission)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Setpermission;
			new WebHdfsFileSystem.FsPathRunner(this, op, p, new PermissionParam(permission)).
				Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Modifyaclentries;
			new WebHdfsFileSystem.FsPathRunner(this, op, path, new AclPermissionParam(aclSpec
				)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Removeaclentries;
			new WebHdfsFileSystem.FsPathRunner(this, op, path, new AclPermissionParam(aclSpec
				)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDefaultAcl(Path path)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Removedefaultacl;
			new WebHdfsFileSystem.FsPathRunner(this, op, path).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAcl(Path path)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Removeacl;
			new WebHdfsFileSystem.FsPathRunner(this, op, path).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetAcl(Path p, IList<AclEntry> aclSpec)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Setacl;
			new WebHdfsFileSystem.FsPathRunner(this, op, p, new AclPermissionParam(aclSpec)).
				Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path CreateSnapshot(Path path, string snapshotName)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Createsnapshot;
			Path spath = new _FsPathResponseRunner_1074(op, path, new SnapshotNameParam(snapshotName
				)).Run();
			return spath;
		}

		private sealed class _FsPathResponseRunner_1074 : WebHdfsFileSystem.FsPathResponseRunner
			<Path>
		{
			public _FsPathResponseRunner_1074(HttpOpParam.OP baseArg1, Path baseArg2, Param<object
				, object>[] baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			internal override Path DecodeResponse<_T0>(IDictionary<_T0> json)
			{
				return new Path((string)json[typeof(Path).Name]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteSnapshot(Path path, string snapshotName)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = DeleteOpParam.OP.Deletesnapshot;
			new WebHdfsFileSystem.FsPathRunner(this, op, path, new SnapshotNameParam(snapshotName
				)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RenameSnapshot(Path path, string snapshotOldName, string snapshotNewName
			)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Renamesnapshot;
			new WebHdfsFileSystem.FsPathRunner(this, op, path, new OldSnapshotNameParam(snapshotOldName
				), new SnapshotNameParam(snapshotNewName)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool SetReplication(Path p, short replication)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Setreplication;
			return new WebHdfsFileSystem.FsPathBooleanRunner(this, op, p, new ReplicationParam
				(replication)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetTimes(Path p, long mtime, long atime)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Settimes;
			new WebHdfsFileSystem.FsPathRunner(this, op, p, new ModificationTimeParam(mtime), 
				new AccessTimeParam(atime)).Run();
		}

		public override long GetDefaultBlockSize()
		{
			return GetConf().GetLongBytes(DFSConfigKeys.DfsBlockSizeKey, DFSConfigKeys.DfsBlockSizeDefault
				);
		}

		public override short GetDefaultReplication()
		{
			return (short)GetConf().GetInt(DFSConfigKeys.DfsReplicationKey, DFSConfigKeys.DfsReplicationDefault
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Concat(Path trg, Path[] srcs)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PostOpParam.OP.Concat;
			new WebHdfsFileSystem.FsPathRunner(this, op, trg, new ConcatSourcesParam(srcs)).Run
				();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
			, int bufferSize, short replication, long blockSize, Progressable progress)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PutOpParam.OP.Create;
			return new WebHdfsFileSystem.FsPathOutputStreamRunner(this, op, f, bufferSize, new 
				PermissionParam(ApplyUMask(permission)), new OverwriteParam(overwrite), new BufferSizeParam
				(bufferSize), new ReplicationParam(replication), new BlockSizeParam(blockSize)).
				Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
			)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PostOpParam.OP.Append;
			return new WebHdfsFileSystem.FsPathOutputStreamRunner(this, op, f, bufferSize, new 
				BufferSizeParam(bufferSize)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Truncate(Path f, long newLength)
		{
			statistics.IncrementWriteOps(1);
			HttpOpParam.OP op = PostOpParam.OP.Truncate;
			return new WebHdfsFileSystem.FsPathBooleanRunner(this, op, f, new NewLengthParam(
				newLength)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			HttpOpParam.OP op = DeleteOpParam.OP.Delete;
			return new WebHdfsFileSystem.FsPathBooleanRunner(this, op, f, new RecursiveParam(
				recursive)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f, int buffersize)
		{
			statistics.IncrementReadOps(1);
			HttpOpParam.OP op = GetOpParam.OP.Open;
			// use a runner so the open can recover from an invalid token
			WebHdfsFileSystem.FsPathConnectionRunner runner = new WebHdfsFileSystem.FsPathConnectionRunner
				(this, op, f, new BufferSizeParam(buffersize));
			return new FSDataInputStream(new WebHdfsFileSystem.OffsetUrlInputStream(new WebHdfsFileSystem.UnresolvedUrlOpener
				(this, runner), new WebHdfsFileSystem.OffsetUrlOpener(this, null)));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				try
				{
					if (canRefreshDelegationToken && delegationToken != null)
					{
						CancelDelegationToken(delegationToken);
					}
				}
				catch (IOException ioe)
				{
					Log.Debug("Token cancel failed: " + ioe);
				}
				finally
				{
					base.Close();
				}
			}
		}

		internal class UnresolvedUrlOpener : ByteRangeInputStream.URLOpener
		{
			private readonly WebHdfsFileSystem.FsPathConnectionRunner runner;

			internal UnresolvedUrlOpener(WebHdfsFileSystem _enclosing, WebHdfsFileSystem.FsPathConnectionRunner
				 runner)
				: base(null)
			{
				this._enclosing = _enclosing;
				// use FsPathConnectionRunner to ensure retries for InvalidTokens
				this.runner = runner;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override HttpURLConnection Connect(long offset, bool resolved)
			{
				System.Diagnostics.Debug.Assert(offset == 0);
				HttpURLConnection conn = this.runner.Run();
				this.SetURL(conn.GetURL());
				return conn;
			}

			private readonly WebHdfsFileSystem _enclosing;
		}

		internal class OffsetUrlOpener : ByteRangeInputStream.URLOpener
		{
			internal OffsetUrlOpener(WebHdfsFileSystem _enclosing, Uri url)
				: base(url)
			{
				this._enclosing = _enclosing;
			}

			/// <summary>Setup offset url and connect.</summary>
			/// <exception cref="System.IO.IOException"/>
			protected internal override HttpURLConnection Connect(long offset, bool resolved)
			{
				Uri offsetUrl = offset == 0L ? this.url : new Uri(this.url + "&" + new OffsetParam
					(offset));
				return new WebHdfsFileSystem.URLRunner(this, GetOpParam.OP.Open, offsetUrl, resolved
					).Run();
			}

			private readonly WebHdfsFileSystem _enclosing;
		}

		private const string OffsetParamPrefix = OffsetParam.Name + "=";

		/// <summary>Remove offset parameter, if there is any, from the url</summary>
		/// <exception cref="System.UriFormatException"/>
		internal static Uri RemoveOffsetParam(Uri url)
		{
			string query = url.GetQuery();
			if (query == null)
			{
				return url;
			}
			string lower = StringUtils.ToLowerCase(query);
			if (!lower.StartsWith(OffsetParamPrefix) && !lower.Contains("&" + OffsetParamPrefix
				))
			{
				return url;
			}
			//rebuild query
			StringBuilder b = null;
			for (StringTokenizer st = new StringTokenizer(query, "&"); st.HasMoreTokens(); )
			{
				string token = st.NextToken();
				if (!StringUtils.ToLowerCase(token).StartsWith(OffsetParamPrefix))
				{
					if (b == null)
					{
						b = new StringBuilder("?").Append(token);
					}
					else
					{
						b.Append('&').Append(token);
					}
				}
			}
			query = b == null ? string.Empty : b.ToString();
			string urlStr = url.ToString();
			return new Uri(Sharpen.Runtime.Substring(urlStr, 0, urlStr.IndexOf('?')) + query);
		}

		internal class OffsetUrlInputStream : ByteRangeInputStream
		{
			/// <exception cref="System.IO.IOException"/>
			internal OffsetUrlInputStream(WebHdfsFileSystem.UnresolvedUrlOpener o, WebHdfsFileSystem.OffsetUrlOpener
				 r)
				: base(o, r)
			{
			}

			/// <summary>Remove offset parameter before returning the resolved url.</summary>
			/// <exception cref="System.UriFormatException"/>
			protected internal override Uri GetResolvedUrl(HttpURLConnection connection)
			{
				return RemoveOffsetParam(connection.GetURL());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			statistics.IncrementReadOps(1);
			HttpOpParam.OP op = GetOpParam.OP.Liststatus;
			//HM: 3rd parm being passed in as null to make Sharpen happy.remove
			return new _FsPathResponseRunner_1294(this, f, op, f, null).Run();
		}

		private sealed class _FsPathResponseRunner_1294 : WebHdfsFileSystem.FsPathResponseRunner
			<FileStatus[]>
		{
			public _FsPathResponseRunner_1294(WebHdfsFileSystem _enclosing, Path f, HttpOpParam.OP
				 baseArg1, Path baseArg2, Param<object, object>[] baseArg3)
				: base(_enclosing, baseArg1, baseArg2, baseArg3)
			{
				this._enclosing = _enclosing;
				this.f = f;
			}

			internal override FileStatus[] DecodeResponse<_T0>(IDictionary<_T0> json)
			{
				IDictionary<object, object> rootmap = (IDictionary<object, object>)json[typeof(FileStatus
					).Name + "es"];
				IList<object> array = JsonUtil.GetList(rootmap, typeof(FileStatus).Name);
				//convert FileStatus
				FileStatus[] statuses = new FileStatus[array.Count];
				int i = 0;
				foreach (object @object in array)
				{
					IDictionary<object, object> m = (IDictionary<object, object>)@object;
					statuses[i++] = this._enclosing.MakeQualified(JsonUtil.ToFileStatus(m, false), f);
				}
				return statuses;
			}

			private readonly WebHdfsFileSystem _enclosing;

			private readonly Path f;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationToken
			(string renewer)
		{
			HttpOpParam.OP op = GetOpParam.OP.Getdelegationtoken;
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = new _FsPathResponseRunner_1319
				(op, null, new RenewerParam(renewer)).Run();
			if (token != null)
			{
				token.SetService(tokenServiceName);
			}
			else
			{
				if (disallowFallbackToInsecureCluster)
				{
					throw new AccessControlException(CantFallbackToInsecureMsg);
				}
			}
			return token;
		}

		private sealed class _FsPathResponseRunner_1319 : WebHdfsFileSystem.FsPathResponseRunner
			<Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>>
		{
			public _FsPathResponseRunner_1319(HttpOpParam.OP baseArg1, Path baseArg2, Param<object
				, object>[] baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> DecodeResponse<_T0>(IDictionary<_T0> json)
			{
				return JsonUtil.ToDelegationToken(json);
			}
		}

		public virtual Org.Apache.Hadoop.Security.Token.Token<object> GetRenewToken()
		{
			lock (this)
			{
				return delegationToken;
			}
		}

		public virtual void SetDelegationToken<T>(Org.Apache.Hadoop.Security.Token.Token<
			T> token)
			where T : TokenIdentifier
		{
			lock (this)
			{
				delegationToken = token;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long RenewDelegationToken<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			lock (this)
			{
				HttpOpParam.OP op = PutOpParam.OP.Renewdelegationtoken;
				return new _FsPathResponseRunner_1354(op, null, new TokenArgumentParam(token.EncodeToUrlString
					())).Run();
			}
		}

		private sealed class _FsPathResponseRunner_1354 : WebHdfsFileSystem.FsPathResponseRunner
			<long>
		{
			public _FsPathResponseRunner_1354(HttpOpParam.OP baseArg1, Path baseArg2, Param<object
				, object>[] baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long DecodeResponse<_T0>(IDictionary<_T0> json)
			{
				return ((Number)json["long"]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CancelDelegationToken<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			lock (this)
			{
				HttpOpParam.OP op = PutOpParam.OP.Canceldelegationtoken;
				new WebHdfsFileSystem.FsPathRunner(this, op, null, new TokenArgumentParam(token.EncodeToUrlString
					())).Run();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override BlockLocation[] GetFileBlockLocations(FileStatus status, long offset
			, long length)
		{
			if (status == null)
			{
				return null;
			}
			return GetFileBlockLocations(status.GetPath(), offset, length);
		}

		/// <exception cref="System.IO.IOException"/>
		public override BlockLocation[] GetFileBlockLocations(Path p, long offset, long length
			)
		{
			statistics.IncrementReadOps(1);
			HttpOpParam.OP op = GetOpParam.OP.GetBlockLocations;
			return new _FsPathResponseRunner_1387(op, p, new OffsetParam(offset)).Run();
		}

		private sealed class _FsPathResponseRunner_1387 : WebHdfsFileSystem.FsPathResponseRunner
			<BlockLocation[]>
		{
			public _FsPathResponseRunner_1387(HttpOpParam.OP baseArg1, Path baseArg2, Param<object
				, object>[] baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override BlockLocation[] DecodeResponse<_T0>(IDictionary<_T0> json)
			{
				return DFSUtil.LocatedBlocks2Locations(JsonUtil.ToLocatedBlocks(json));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Access(Path path, FsAction mode)
		{
			HttpOpParam.OP op = GetOpParam.OP.Checkaccess;
			new WebHdfsFileSystem.FsPathRunner(this, op, path, new FsActionParam(mode)).Run();
		}

		/// <exception cref="System.IO.IOException"/>
		public override ContentSummary GetContentSummary(Path p)
		{
			statistics.IncrementReadOps(1);
			HttpOpParam.OP op = GetOpParam.OP.Getcontentsummary;
			//HM: 3rd parm being passed in as null to make Sharpen happy.remove
			return new _FsPathResponseRunner_1408(op, p, null).Run();
		}

		private sealed class _FsPathResponseRunner_1408 : WebHdfsFileSystem.FsPathResponseRunner
			<ContentSummary>
		{
			public _FsPathResponseRunner_1408(HttpOpParam.OP baseArg1, Path baseArg2, Param<object
				, object>[] baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			internal override ContentSummary DecodeResponse<_T0>(IDictionary<_T0> json)
			{
				return JsonUtil.ToContentSummary(json);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path p)
		{
			statistics.IncrementReadOps(1);
			HttpOpParam.OP op = GetOpParam.OP.Getfilechecksum;
			//HM: 3rd parm being passed in as null to make Sharpen happy.remove
			return new _FsPathResponseRunner_1423(op, p, null).Run();
		}

		private sealed class _FsPathResponseRunner_1423 : WebHdfsFileSystem.FsPathResponseRunner
			<MD5MD5CRC32FileChecksum>
		{
			public _FsPathResponseRunner_1423(HttpOpParam.OP baseArg1, Path baseArg2, Param<object
				, object>[] baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal override MD5MD5CRC32FileChecksum DecodeResponse<_T0>(IDictionary<_T0> json
				)
			{
				return JsonUtil.ToMD5MD5CRC32FileChecksum(json);
			}
		}

		/// <summary>Resolve an HDFS URL into real INetSocketAddress.</summary>
		/// <remarks>
		/// Resolve an HDFS URL into real INetSocketAddress. It works like a DNS
		/// resolver when the URL points to an non-HA cluster. When the URL points to
		/// an HA cluster with its logical name, the resolver further resolves the
		/// logical name(i.e., the authority in the URL) into real namenode addresses.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private IPEndPoint[] ResolveNNAddr()
		{
			Configuration conf = GetConf();
			string scheme = uri.GetScheme();
			AList<IPEndPoint> ret = new AList<IPEndPoint>();
			if (!HAUtil.IsLogicalUri(conf, uri))
			{
				IPEndPoint addr = NetUtils.CreateSocketAddr(uri.GetAuthority(), GetDefaultPort());
				ret.AddItem(addr);
			}
			else
			{
				IDictionary<string, IDictionary<string, IPEndPoint>> addresses = DFSUtil.GetHaNnWebHdfsAddresses
					(conf, scheme);
				// Extract the entry corresponding to the logical name.
				IDictionary<string, IPEndPoint> addrs = addresses[uri.GetHost()];
				foreach (IPEndPoint addr in addrs.Values)
				{
					ret.AddItem(addr);
				}
			}
			IPEndPoint[] r = new IPEndPoint[ret.Count];
			return Sharpen.Collections.ToArray(ret, r);
		}

		public override string GetCanonicalServiceName()
		{
			return tokenServiceName == null ? base.GetCanonicalServiceName() : tokenServiceName
				.ToString();
		}

		[VisibleForTesting]
		internal virtual IPEndPoint[] GetResolvedNNAddr()
		{
			return nnAddrs;
		}

		public WebHdfsFileSystem()
		{
			tokenSelector = new _AbstractDelegationTokenSelector_221(GetTokenKind());
		}
	}
}
