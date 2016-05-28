using System;
using System.IO;
using System.Net;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Xml.Sax;
using Org.Xml.Sax.Helpers;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>An implementation of a protocol for accessing filesystems over HTTP.</summary>
	/// <remarks>
	/// An implementation of a protocol for accessing filesystems over HTTP.
	/// The following implementation provides a limited, read-only interface
	/// to a filesystem over HTTP.
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.ListPathsServlet"/>
	/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.FileDataServlet"/>
	public class HftpFileSystem : FileSystem, DelegationTokenRenewer.Renewable, TokenAspect.TokenManagementDelegator
	{
		public const string Scheme = "hftp";

		static HftpFileSystem()
		{
			HttpURLConnection.SetFollowRedirects(true);
		}

		internal URLConnectionFactory connectionFactory;

		public static readonly Text TokenKind = new Text("HFTP delegation");

		protected internal UserGroupInformation ugi;

		private URI hftpURI;

		protected internal URI nnUri;

		public const string HftpTimezone = "UTC";

		public const string HftpDateFormat = "yyyy-MM-dd'T'HH:mm:ssZ";

		protected internal TokenAspect<HftpFileSystem> tokenAspect;

		private Org.Apache.Hadoop.Security.Token.Token<object> delegationToken;

		private Org.Apache.Hadoop.Security.Token.Token<object> renewToken;

		protected internal Text tokenServiceName;

		protected override URI GetCanonicalUri()
		{
			return base.GetCanonicalUri();
		}

		public static SimpleDateFormat GetDateFormat()
		{
			SimpleDateFormat df = new SimpleDateFormat(HftpDateFormat);
			df.SetTimeZone(Sharpen.Extensions.GetTimeZone(HftpTimezone));
			return df;
		}

		private sealed class _ThreadLocal_117 : ThreadLocal<SimpleDateFormat>
		{
			public _ThreadLocal_117()
			{
			}

			protected override SimpleDateFormat InitialValue()
			{
				return HftpFileSystem.GetDateFormat();
			}
		}

		protected internal static readonly ThreadLocal<SimpleDateFormat> df = new _ThreadLocal_117
			();

		protected override int GetDefaultPort()
		{
			return GetConf().GetInt(DFSConfigKeys.DfsNamenodeHttpPortKey, DFSConfigKeys.DfsNamenodeHttpPortDefault
				);
		}

		/// <summary>
		/// We generate the address with one of the following ports, in
		/// order of preference.
		/// </summary>
		/// <remarks>
		/// We generate the address with one of the following ports, in
		/// order of preference.
		/// 1. Port from the hftp URI e.g. hftp://namenode:4000/ will return 4000.
		/// 2. Port configured via DFS_NAMENODE_HTTP_PORT_KEY
		/// 3. DFS_NAMENODE_HTTP_PORT_DEFAULT i.e. 50070.
		/// </remarks>
		/// <param name="uri"/>
		protected internal virtual IPEndPoint GetNamenodeAddr(URI uri)
		{
			// use authority so user supplied uri can override port
			return NetUtils.CreateSocketAddr(uri.GetAuthority(), GetDefaultPort());
		}

		protected internal virtual URI GetNamenodeUri(URI uri)
		{
			return DFSUtil.CreateUri(GetUnderlyingProtocol(), GetNamenodeAddr(uri));
		}

		/// <summary>
		/// See the documentation of
		/// <Link>#getNamenodeAddr(URI)</Link>
		/// for the logic
		/// behind selecting the canonical service name.
		/// </summary>
		/// <returns/>
		public override string GetCanonicalServiceName()
		{
			return SecurityUtil.BuildTokenService(nnUri).ToString();
		}

		protected override URI CanonicalizeUri(URI uri)
		{
			return NetUtils.GetCanonicalUri(uri, GetDefaultPort());
		}

		/// <summary>Return the protocol scheme for the FileSystem.</summary>
		/// <remarks>
		/// Return the protocol scheme for the FileSystem.
		/// <p/>
		/// </remarks>
		/// <returns><code>hftp</code></returns>
		public override string GetScheme()
		{
			return Scheme;
		}

		/// <summary>Initialize connectionFactory and tokenAspect.</summary>
		/// <remarks>
		/// Initialize connectionFactory and tokenAspect. This function is intended to
		/// be overridden by HsFtpFileSystem.
		/// </remarks>
		protected internal virtual void InitTokenAspect()
		{
			tokenAspect = new TokenAspect<HftpFileSystem>(this, tokenServiceName, TokenKind);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(URI name, Configuration conf)
		{
			base.Initialize(name, conf);
			SetConf(conf);
			this.connectionFactory = URLConnectionFactory.NewDefaultURLConnectionFactory(conf
				);
			this.ugi = UserGroupInformation.GetCurrentUser();
			this.nnUri = GetNamenodeUri(name);
			this.tokenServiceName = SecurityUtil.BuildTokenService(nnUri);
			try
			{
				this.hftpURI = new URI(name.GetScheme(), name.GetAuthority(), null, null, null);
			}
			catch (URISyntaxException e)
			{
				throw new ArgumentException(e);
			}
			InitTokenAspect();
			if (UserGroupInformation.IsSecurityEnabled())
			{
				tokenAspect.InitDelegationToken(ugi);
			}
		}

		public virtual Org.Apache.Hadoop.Security.Token.Token<object> GetRenewToken()
		{
			return renewToken;
		}

		/// <summary>Return the underlying protocol that is used to talk to the namenode.</summary>
		protected internal virtual string GetUnderlyingProtocol()
		{
			return "http";
		}

		public virtual void SetDelegationToken<T>(Org.Apache.Hadoop.Security.Token.Token<
			T> token)
			where T : TokenIdentifier
		{
			lock (this)
			{
				renewToken = token;
				delegationToken = new Org.Apache.Hadoop.Security.Token.Token<T>(token);
				delegationToken.SetKind(DelegationTokenIdentifier.HdfsDelegationKind);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationToken
			(string renewer)
		{
			lock (this)
			{
				try
				{
					// Renew TGT if needed
					UserGroupInformation connectUgi = ugi.GetRealUser();
					string proxyUser = connectUgi == null ? null : ugi.GetShortUserName();
					if (connectUgi == null)
					{
						connectUgi = ugi;
					}
					return connectUgi.DoAs(new _PrivilegedExceptionAction_246(this, renewer, proxyUser
						));
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_246 : PrivilegedExceptionAction<Org.Apache.Hadoop.Security.Token.Token
			<object>>
		{
			public _PrivilegedExceptionAction_246(HftpFileSystem _enclosing, string renewer, 
				string proxyUser)
			{
				this._enclosing = _enclosing;
				this.renewer = renewer;
				this.proxyUser = proxyUser;
			}

			/// <exception cref="System.IO.IOException"/>
			public Org.Apache.Hadoop.Security.Token.Token<object> Run()
			{
				Credentials c;
				try
				{
					c = DelegationTokenFetcher.GetDTfromRemote(this._enclosing.connectionFactory, this
						._enclosing.nnUri, renewer, proxyUser);
				}
				catch (IOException e)
				{
					if (e.InnerException is ConnectException)
					{
						FileSystem.Log.Warn("Couldn't connect to " + this._enclosing.nnUri + ", assuming security is disabled"
							);
						return null;
					}
					if (FileSystem.Log.IsDebugEnabled())
					{
						FileSystem.Log.Debug("Exception getting delegation token", e);
					}
					throw;
				}
				foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> t in c.GetAllTokens
					())
				{
					if (FileSystem.Log.IsDebugEnabled())
					{
						FileSystem.Log.Debug("Got dt for " + this._enclosing.GetUri() + ";t.service=" + t
							.GetService());
					}
					return t;
				}
				return null;
			}

			private readonly HftpFileSystem _enclosing;

			private readonly string renewer;

			private readonly string proxyUser;
		}

		public override URI GetUri()
		{
			return hftpURI;
		}

		/// <summary>Return a URL pointing to given path on the namenode.</summary>
		/// <param name="path">to obtain the URL for</param>
		/// <param name="query">string to append to the path</param>
		/// <returns>namenode URL referring to the given path</returns>
		/// <exception cref="System.IO.IOException">on error constructing the URL</exception>
		protected internal virtual Uri GetNamenodeURL(string path, string query)
		{
			Uri url = new Uri(GetUnderlyingProtocol(), nnUri.GetHost(), nnUri.GetPort(), path
				 + '?' + query);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("url=" + url);
			}
			return url;
		}

		/// <summary>Get encoded UGI parameter string for a URL.</summary>
		/// <returns>user_shortname,group1,group2...</returns>
		private string GetEncodedUgiParameter()
		{
			StringBuilder ugiParameter = new StringBuilder(ServletUtil.EncodeQueryValue(ugi.GetShortUserName
				()));
			foreach (string g in ugi.GetGroupNames())
			{
				ugiParameter.Append(",");
				ugiParameter.Append(ServletUtil.EncodeQueryValue(g));
			}
			return ugiParameter.ToString();
		}

		/// <summary>Open an HTTP connection to the namenode to read file data and metadata.</summary>
		/// <param name="path">The path component of the URL</param>
		/// <param name="query">The query component of the URL</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual HttpURLConnection OpenConnection(string path, string query
			)
		{
			query = AddDelegationTokenParam(query);
			Uri url = GetNamenodeURL(path, query);
			HttpURLConnection connection;
			connection = (HttpURLConnection)connectionFactory.OpenConnection(url);
			connection.SetRequestMethod("GET");
			connection.Connect();
			return connection;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual string AddDelegationTokenParam(string query)
		{
			string tokenString = null;
			if (UserGroupInformation.IsSecurityEnabled())
			{
				lock (this)
				{
					tokenAspect.EnsureTokenInitialized();
					if (delegationToken != null)
					{
						tokenString = delegationToken.EncodeToUrlString();
						return (query + JspHelper.GetDelegationTokenUrlParam(tokenString));
					}
				}
			}
			return query;
		}

		internal class RangeHeaderUrlOpener : ByteRangeInputStream.URLOpener
		{
			private readonly URLConnectionFactory connFactory;

			internal RangeHeaderUrlOpener(URLConnectionFactory connFactory, Uri url)
				: base(url)
			{
				this.connFactory = connFactory;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual HttpURLConnection OpenConnection()
			{
				return (HttpURLConnection)connFactory.OpenConnection(url);
			}

			/// <summary>Use HTTP Range header for specifying offset.</summary>
			/// <exception cref="System.IO.IOException"/>
			protected internal override HttpURLConnection Connect(long offset, bool resolved)
			{
				HttpURLConnection conn = OpenConnection();
				conn.SetRequestMethod("GET");
				if (offset != 0L)
				{
					conn.SetRequestProperty("Range", "bytes=" + offset + "-");
				}
				conn.Connect();
				//Expects HTTP_OK or HTTP_PARTIAL response codes.
				int code = conn.GetResponseCode();
				if (offset != 0L && code != HttpURLConnection.HttpPartial)
				{
					throw new IOException("HTTP_PARTIAL expected, received " + code);
				}
				else
				{
					if (offset == 0L && code != HttpURLConnection.HttpOk)
					{
						throw new IOException("HTTP_OK expected, received " + code);
					}
				}
				return conn;
			}
		}

		internal class RangeHeaderInputStream : ByteRangeInputStream
		{
			/// <exception cref="System.IO.IOException"/>
			internal RangeHeaderInputStream(HftpFileSystem.RangeHeaderUrlOpener o, HftpFileSystem.RangeHeaderUrlOpener
				 r)
				: base(o, r)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal RangeHeaderInputStream(URLConnectionFactory connFactory, Uri url)
				: this(new HftpFileSystem.RangeHeaderUrlOpener(connFactory, url), new HftpFileSystem.RangeHeaderUrlOpener
					(connFactory, null))
			{
			}

			protected internal override Uri GetResolvedUrl(HttpURLConnection connection)
			{
				return connection.GetURL();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f, int buffersize)
		{
			f = f.MakeQualified(GetUri(), GetWorkingDirectory());
			string path = "/data" + ServletUtil.EncodePath(f.ToUri().GetPath());
			string query = AddDelegationTokenParam("ugi=" + GetEncodedUgiParameter());
			Uri u = GetNamenodeURL(path, query);
			return new FSDataInputStream(new HftpFileSystem.RangeHeaderInputStream(connectionFactory
				, u));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			base.Close();
			tokenAspect.RemoveRenewAction();
		}

		/// <summary>Class to parse and store a listing reply from the server.</summary>
		internal class LsParser : DefaultHandler
		{
			internal readonly AList<FileStatus> fslist = new AList<FileStatus>();

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void StartElement(string ns, string localname, string qname, Attributes
				 attrs)
			{
				if ("listing".Equals(qname))
				{
					return;
				}
				if (!"file".Equals(qname) && !"directory".Equals(qname))
				{
					if (typeof(RemoteException).Name.Equals(qname))
					{
						throw new SAXException(RemoteException.ValueOf(attrs));
					}
					throw new SAXException("Unrecognized entry: " + qname);
				}
				long modif;
				long atime = 0;
				try
				{
					SimpleDateFormat ldf = HftpFileSystem.df.Get();
					modif = ldf.Parse(attrs.GetValue("modified")).GetTime();
					string astr = attrs.GetValue("accesstime");
					if (astr != null)
					{
						atime = ldf.Parse(astr).GetTime();
					}
				}
				catch (ParseException e)
				{
					throw new SAXException(e);
				}
				FileStatus fs = "file".Equals(qname) ? new FileStatus(long.Parse(attrs.GetValue("size"
					)), false, short.ValueOf(attrs.GetValue("replication")), long.Parse(attrs.GetValue
					("blocksize")), modif, atime, FsPermission.ValueOf(attrs.GetValue("permission"))
					, attrs.GetValue("owner"), attrs.GetValue("group"), this._enclosing.MakeQualified
					(new Path(this._enclosing.GetUri().ToString(), attrs.GetValue("path")))) : new FileStatus
					(0L, true, 0, 0L, modif, atime, FsPermission.ValueOf(attrs.GetValue("permission"
					)), attrs.GetValue("owner"), attrs.GetValue("group"), this._enclosing.MakeQualified
					(new Path(this._enclosing.GetUri().ToString(), attrs.GetValue("path"))));
				this.fslist.AddItem(fs);
			}

			/// <exception cref="System.IO.IOException"/>
			private void FetchList(string path, bool recur)
			{
				try
				{
					XMLReader xr = XMLReaderFactory.CreateXMLReader();
					xr.SetContentHandler(this);
					HttpURLConnection connection = this._enclosing.OpenConnection("/listPaths" + ServletUtil
						.EncodePath(path), "ugi=" + this._enclosing.GetEncodedUgiParameter() + (recur ? 
						"&recursive=yes" : string.Empty));
					InputStream resp = connection.GetInputStream();
					xr.Parse(new InputSource(resp));
				}
				catch (SAXException e)
				{
					Exception embedded = e.GetException();
					if (embedded != null && embedded is IOException)
					{
						throw (IOException)embedded;
					}
					throw new IOException("invalid xml directory content", e);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FileStatus GetFileStatus(Path f)
			{
				this.FetchList(f.ToUri().GetPath(), false);
				if (this.fslist.Count == 0)
				{
					throw new FileNotFoundException("File does not exist: " + f);
				}
				return this.fslist[0];
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FileStatus[] ListStatus(Path f, bool recur)
			{
				this.FetchList(f.ToUri().GetPath(), recur);
				if (this.fslist.Count > 0 && (this.fslist.Count != 1 || this.fslist[0].IsDirectory
					()))
				{
					this.fslist.Remove(0);
				}
				return Sharpen.Collections.ToArray(this.fslist, new FileStatus[0]);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FileStatus[] ListStatus(Path f)
			{
				return this.ListStatus(f, false);
			}

			internal LsParser(HftpFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly HftpFileSystem _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			HftpFileSystem.LsParser lsparser = new HftpFileSystem.LsParser(this);
			return lsparser.ListStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			HftpFileSystem.LsParser lsparser = new HftpFileSystem.LsParser(this);
			return lsparser.GetFileStatus(f);
		}

		private class ChecksumParser : DefaultHandler
		{
			private FileChecksum filechecksum;

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void StartElement(string ns, string localname, string qname, Attributes
				 attrs)
			{
				if (!typeof(MD5MD5CRC32FileChecksum).FullName.Equals(qname))
				{
					if (typeof(RemoteException).Name.Equals(qname))
					{
						throw new SAXException(RemoteException.ValueOf(attrs));
					}
					throw new SAXException("Unrecognized entry: " + qname);
				}
				this.filechecksum = MD5MD5CRC32FileChecksum.ValueOf(attrs);
			}

			/// <exception cref="System.IO.IOException"/>
			private FileChecksum GetFileChecksum(string f)
			{
				HttpURLConnection connection = this._enclosing.OpenConnection("/fileChecksum" + ServletUtil
					.EncodePath(f), "ugi=" + this._enclosing.GetEncodedUgiParameter());
				try
				{
					XMLReader xr = XMLReaderFactory.CreateXMLReader();
					xr.SetContentHandler(this);
					xr.Parse(new InputSource(connection.GetInputStream()));
				}
				catch (SAXException e)
				{
					Exception embedded = e.GetException();
					if (embedded != null && embedded is IOException)
					{
						throw (IOException)embedded;
					}
					throw new IOException("invalid xml directory content", e);
				}
				finally
				{
					connection.Disconnect();
				}
				return this.filechecksum;
			}

			internal ChecksumParser(HftpFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly HftpFileSystem _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			string s = MakeQualified(f).ToUri().GetPath();
			return new HftpFileSystem.ChecksumParser(this).GetFileChecksum(s);
		}

		public override Path GetWorkingDirectory()
		{
			return new Path("/").MakeQualified(GetUri(), null);
		}

		public override void SetWorkingDirectory(Path f)
		{
		}

		/// <summary>This optional operation is not yet supported.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
			)
		{
			throw new IOException("Not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
			, int bufferSize, short replication, long blockSize, Progressable progress)
		{
			throw new IOException("Not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Rename(Path src, Path dst)
		{
			throw new IOException("Not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			throw new IOException("Not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Mkdirs(Path f, FsPermission permission)
		{
			throw new IOException("Not supported");
		}

		/// <summary>
		/// A parser for parsing
		/// <see cref="Org.Apache.Hadoop.FS.ContentSummary"/>
		/// xml.
		/// </summary>
		private class ContentSummaryParser : DefaultHandler
		{
			private ContentSummary contentsummary;

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void StartElement(string ns, string localname, string qname, Attributes
				 attrs)
			{
				if (!typeof(ContentSummary).FullName.Equals(qname))
				{
					if (typeof(RemoteException).Name.Equals(qname))
					{
						throw new SAXException(RemoteException.ValueOf(attrs));
					}
					throw new SAXException("Unrecognized entry: " + qname);
				}
				this.contentsummary = HftpFileSystem.ToContentSummary(attrs);
			}

			/// <summary>Connect to the name node and get content summary.</summary>
			/// <param name="path">The path</param>
			/// <returns>The content summary for the path.</returns>
			/// <exception cref="System.IO.IOException"/>
			private ContentSummary GetContentSummary(string path)
			{
				HttpURLConnection connection = this._enclosing.OpenConnection("/contentSummary" +
					 ServletUtil.EncodePath(path), "ugi=" + this._enclosing.GetEncodedUgiParameter()
					);
				InputStream @in = null;
				try
				{
					@in = connection.GetInputStream();
					XMLReader xr = XMLReaderFactory.CreateXMLReader();
					xr.SetContentHandler(this);
					xr.Parse(new InputSource(@in));
				}
				catch (FileNotFoundException)
				{
					//the server may not support getContentSummary
					return null;
				}
				catch (SAXException saxe)
				{
					Exception embedded = saxe.GetException();
					if (embedded != null && embedded is IOException)
					{
						throw (IOException)embedded;
					}
					throw new IOException("Invalid xml format", saxe);
				}
				finally
				{
					if (@in != null)
					{
						@in.Close();
					}
					connection.Disconnect();
				}
				return this.contentsummary;
			}

			internal ContentSummaryParser(HftpFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly HftpFileSystem _enclosing;
		}

		/// <summary>Return the object represented in the attributes.</summary>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		private static ContentSummary ToContentSummary(Attributes attrs)
		{
			string length = attrs.GetValue("length");
			string fileCount = attrs.GetValue("fileCount");
			string directoryCount = attrs.GetValue("directoryCount");
			string quota = attrs.GetValue("quota");
			string spaceConsumed = attrs.GetValue("spaceConsumed");
			string spaceQuota = attrs.GetValue("spaceQuota");
			if (length == null || fileCount == null || directoryCount == null || quota == null
				 || spaceConsumed == null || spaceQuota == null)
			{
				return null;
			}
			try
			{
				return new ContentSummary(long.Parse(length), long.Parse(fileCount), long.Parse(directoryCount
					), long.Parse(quota), long.Parse(spaceConsumed), long.Parse(spaceQuota));
			}
			catch (Exception e)
			{
				throw new SAXException("Invalid attributes: length=" + length + ", fileCount=" + 
					fileCount + ", directoryCount=" + directoryCount + ", quota=" + quota + ", spaceConsumed="
					 + spaceConsumed + ", spaceQuota=" + spaceQuota, e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override ContentSummary GetContentSummary(Path f)
		{
			string s = MakeQualified(f).ToUri().GetPath();
			ContentSummary cs = new HftpFileSystem.ContentSummaryParser(this).GetContentSummary
				(s);
			return cs != null ? cs : base.GetContentSummary(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long RenewDelegationToken<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			// update the kerberos credentials, if they are coming from a keytab
			UserGroupInformation connectUgi = ugi.GetRealUser();
			if (connectUgi == null)
			{
				connectUgi = ugi;
			}
			try
			{
				return connectUgi.DoAs(new _PrivilegedExceptionAction_694(this, token));
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_694 : PrivilegedExceptionAction<long
			>
		{
			public _PrivilegedExceptionAction_694(HftpFileSystem _enclosing, Org.Apache.Hadoop.Security.Token.Token
				<object> token)
			{
				this._enclosing = _enclosing;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public long Run()
			{
				IPEndPoint serviceAddr = SecurityUtil.GetTokenServiceAddr(token);
				return DelegationTokenFetcher.RenewDelegationToken(this._enclosing.connectionFactory
					, DFSUtil.CreateUri(this._enclosing.GetUnderlyingProtocol(), serviceAddr), (Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>)token);
			}

			private readonly HftpFileSystem _enclosing;

			private readonly Org.Apache.Hadoop.Security.Token.Token<object> token;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CancelDelegationToken<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			UserGroupInformation connectUgi = ugi.GetRealUser();
			if (connectUgi == null)
			{
				connectUgi = ugi;
			}
			try
			{
				connectUgi.DoAs(new _PrivilegedExceptionAction_717(this, token));
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_717 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_717(HftpFileSystem _enclosing, Org.Apache.Hadoop.Security.Token.Token
				<object> token)
			{
				this._enclosing = _enclosing;
				this.token = token;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				IPEndPoint serviceAddr = SecurityUtil.GetTokenServiceAddr(token);
				DelegationTokenFetcher.CancelDelegationToken(this._enclosing.connectionFactory, DFSUtil
					.CreateUri(this._enclosing.GetUnderlyingProtocol(), serviceAddr), (Org.Apache.Hadoop.Security.Token.Token
					<DelegationTokenIdentifier>)token);
				return null;
			}

			private readonly HftpFileSystem _enclosing;

			private readonly Org.Apache.Hadoop.Security.Token.Token<object> token;
		}
	}
}
