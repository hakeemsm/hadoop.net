using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Lib.Wsrs;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Hadoop.Util;
using Org.Json.Simple;
using Org.Json.Simple.Parser;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Client
{
	/// <summary>HttpFSServer implementation of the FileSystemAccess FileSystem.</summary>
	/// <remarks>
	/// HttpFSServer implementation of the FileSystemAccess FileSystem.
	/// <p>
	/// This implementation allows a user to access HDFS over HTTP via a HttpFSServer server.
	/// </remarks>
	public class HttpFSFileSystem : FileSystem, DelegationTokenRenewer.Renewable
	{
		public const string ServiceName = HttpFSUtils.ServiceName;

		public const string ServiceVersion = HttpFSUtils.ServiceVersion;

		public const string Scheme = "webhdfs";

		public const string OpParam = "op";

		public const string DoAsParam = "doas";

		public const string OverwriteParam = "overwrite";

		public const string ReplicationParam = "replication";

		public const string BlocksizeParam = "blocksize";

		public const string PermissionParam = "permission";

		public const string AclspecParam = "aclspec";

		public const string DestinationParam = "destination";

		public const string RecursiveParam = "recursive";

		public const string SourcesParam = "sources";

		public const string OwnerParam = "owner";

		public const string GroupParam = "group";

		public const string ModificationTimeParam = "modificationtime";

		public const string AccessTimeParam = "accesstime";

		public const string XattrNameParam = "xattr.name";

		public const string XattrValueParam = "xattr.value";

		public const string XattrSetFlagParam = "flag";

		public const string XattrEncodingParam = "encoding";

		public const string NewLengthParam = "newlength";

		public static readonly short DefaultPermission = 0x1ed;

		public const string AclspecDefault = string.Empty;

		public const string RenameJson = "boolean";

		public const string TruncateJson = "boolean";

		public const string DeleteJson = "boolean";

		public const string MkdirsJson = "boolean";

		public const string HomeDirJson = "Path";

		public const string SetReplicationJson = "boolean";

		public const string UploadContentType = "application/octet-stream";

		[System.Serializable]
		public sealed class FILE_TYPE
		{
			public static readonly HttpFSFileSystem.FILE_TYPE File = new HttpFSFileSystem.FILE_TYPE
				();

			public static readonly HttpFSFileSystem.FILE_TYPE Directory = new HttpFSFileSystem.FILE_TYPE
				();

			public static readonly HttpFSFileSystem.FILE_TYPE Symlink = new HttpFSFileSystem.FILE_TYPE
				();

			public static HttpFSFileSystem.FILE_TYPE GetType(FileStatus fileStatus)
			{
				if (fileStatus.IsFile())
				{
					return HttpFSFileSystem.FILE_TYPE.File;
				}
				if (fileStatus.IsDirectory())
				{
					return HttpFSFileSystem.FILE_TYPE.Directory;
				}
				if (fileStatus.IsSymlink())
				{
					return HttpFSFileSystem.FILE_TYPE.Symlink;
				}
				throw new ArgumentException("Could not determine filetype for: " + fileStatus.GetPath
					());
			}
		}

		public const string FileStatusesJson = "FileStatuses";

		public const string FileStatusJson = "FileStatus";

		public const string PathSuffixJson = "pathSuffix";

		public const string TypeJson = "type";

		public const string LengthJson = "length";

		public const string OwnerJson = "owner";

		public const string GroupJson = "group";

		public const string PermissionJson = "permission";

		public const string AccessTimeJson = "accessTime";

		public const string ModificationTimeJson = "modificationTime";

		public const string BlockSizeJson = "blockSize";

		public const string ReplicationJson = "replication";

		public const string XattrsJson = "XAttrs";

		public const string XattrNameJson = "name";

		public const string XattrValueJson = "value";

		public const string XattrnamesJson = "XAttrNames";

		public const string FileChecksumJson = "FileChecksum";

		public const string ChecksumAlgorithmJson = "algorithm";

		public const string ChecksumBytesJson = "bytes";

		public const string ChecksumLengthJson = "length";

		public const string ContentSummaryJson = "ContentSummary";

		public const string ContentSummaryDirectoryCountJson = "directoryCount";

		public const string ContentSummaryFileCountJson = "fileCount";

		public const string ContentSummaryLengthJson = "length";

		public const string ContentSummaryQuotaJson = "quota";

		public const string ContentSummarySpaceConsumedJson = "spaceConsumed";

		public const string ContentSummarySpaceQuotaJson = "spaceQuota";

		public const string AclStatusJson = "AclStatus";

		public const string AclStickyBitJson = "stickyBit";

		public const string AclEntriesJson = "entries";

		public const string AclBitJson = "aclBit";

		public const int HttpTemporaryRedirect = 307;

		private const string HttpGet = "GET";

		private const string HttpPut = "PUT";

		private const string HttpPost = "POST";

		private const string HttpDelete = "DELETE";

		[System.Serializable]
		public sealed class Operation
		{
			public static readonly HttpFSFileSystem.Operation Open = new HttpFSFileSystem.Operation
				(HttpGet);

			public static readonly HttpFSFileSystem.Operation Getfilestatus = new HttpFSFileSystem.Operation
				(HttpGet);

			public static readonly HttpFSFileSystem.Operation Liststatus = new HttpFSFileSystem.Operation
				(HttpGet);

			public static readonly HttpFSFileSystem.Operation Gethomedirectory = new HttpFSFileSystem.Operation
				(HttpGet);

			public static readonly HttpFSFileSystem.Operation Getcontentsummary = new HttpFSFileSystem.Operation
				(HttpGet);

			public static readonly HttpFSFileSystem.Operation Getfilechecksum = new HttpFSFileSystem.Operation
				(HttpGet);

			public static readonly HttpFSFileSystem.Operation Getfileblocklocations = new HttpFSFileSystem.Operation
				(HttpGet);

			public static readonly HttpFSFileSystem.Operation Instrumentation = new HttpFSFileSystem.Operation
				(HttpGet);

			public static readonly HttpFSFileSystem.Operation Getaclstatus = new HttpFSFileSystem.Operation
				(HttpGet);

			public static readonly HttpFSFileSystem.Operation Append = new HttpFSFileSystem.Operation
				(HttpPost);

			public static readonly HttpFSFileSystem.Operation Concat = new HttpFSFileSystem.Operation
				(HttpPost);

			public static readonly HttpFSFileSystem.Operation Truncate = new HttpFSFileSystem.Operation
				(HttpPost);

			public static readonly HttpFSFileSystem.Operation Create = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Mkdirs = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Rename = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Setowner = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Setpermission = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Setreplication = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Settimes = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Modifyaclentries = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Removeaclentries = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Removedefaultacl = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Removeacl = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Setacl = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Delete = new HttpFSFileSystem.Operation
				(HttpDelete);

			public static readonly HttpFSFileSystem.Operation Setxattr = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Getxattrs = new HttpFSFileSystem.Operation
				(HttpGet);

			public static readonly HttpFSFileSystem.Operation Removexattr = new HttpFSFileSystem.Operation
				(HttpPut);

			public static readonly HttpFSFileSystem.Operation Listxattrs = new HttpFSFileSystem.Operation
				(HttpGet);

			private string httpMethod;

			internal Operation(string httpMethod)
			{
				this.httpMethod = httpMethod;
			}

			public string GetMethod()
			{
				return HttpFSFileSystem.Operation.httpMethod;
			}
		}

		private DelegationTokenAuthenticatedURL authURL;

		private DelegationTokenAuthenticatedURL.Token authToken = new DelegationTokenAuthenticatedURL.Token
			();

		private URI uri;

		private Path workingDir;

		private UserGroupInformation realUser;

		/// <summary>
		/// Convenience method that creates a <code>HttpURLConnection</code> for the
		/// HttpFSServer file system operations.
		/// </summary>
		/// <remarks>
		/// Convenience method that creates a <code>HttpURLConnection</code> for the
		/// HttpFSServer file system operations.
		/// <p>
		/// This methods performs and injects any needed authentication credentials
		/// via the
		/// <see cref="GetConnection(System.Uri, string)"/>
		/// method
		/// </remarks>
		/// <param name="method">the HTTP method.</param>
		/// <param name="params">the query string parameters.</param>
		/// <param name="path">the file path</param>
		/// <param name="makeQualified">if the path should be 'makeQualified'</param>
		/// <returns>
		/// a <code>HttpURLConnection</code> for the HttpFSServer server,
		/// authenticated and ready to use for the specified path and file system operation.
		/// </returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurrs.</exception>
		private HttpURLConnection GetConnection(string method, IDictionary<string, string
			> @params, Path path, bool makeQualified)
		{
			return GetConnection(method, @params, null, path, makeQualified);
		}

		/// <summary>
		/// Convenience method that creates a <code>HttpURLConnection</code> for the
		/// HttpFSServer file system operations.
		/// </summary>
		/// <remarks>
		/// Convenience method that creates a <code>HttpURLConnection</code> for the
		/// HttpFSServer file system operations.
		/// <p/>
		/// This methods performs and injects any needed authentication credentials
		/// via the
		/// <see cref="GetConnection(System.Uri, string)"/>
		/// method
		/// </remarks>
		/// <param name="method">the HTTP method.</param>
		/// <param name="params">the query string parameters.</param>
		/// <param name="multiValuedParams">multi valued parameters of the query string</param>
		/// <param name="path">the file path</param>
		/// <param name="makeQualified">if the path should be 'makeQualified'</param>
		/// <returns>
		/// HttpURLConnection a <code>HttpURLConnection</code> for the
		/// HttpFSServer server, authenticated and ready to use for the
		/// specified path and file system operation.
		/// </returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurrs.</exception>
		private HttpURLConnection GetConnection(string method, IDictionary<string, string
			> @params, IDictionary<string, IList<string>> multiValuedParams, Path path, bool
			 makeQualified)
		{
			if (makeQualified)
			{
				path = MakeQualified(path);
			}
			Uri url = HttpFSUtils.CreateURL(path, @params, multiValuedParams);
			try
			{
				return UserGroupInformation.GetCurrentUser().DoAs(new _PrivilegedExceptionAction_277
					(this, url, method));
			}
			catch (Exception ex)
			{
				if (ex is IOException)
				{
					throw (IOException)ex;
				}
				else
				{
					throw new IOException(ex);
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_277 : PrivilegedExceptionAction<HttpURLConnection
			>
		{
			public _PrivilegedExceptionAction_277(HttpFSFileSystem _enclosing, Uri url, string
				 method)
			{
				this._enclosing = _enclosing;
				this.url = url;
				this.method = method;
			}

			/// <exception cref="System.Exception"/>
			public HttpURLConnection Run()
			{
				return this._enclosing.GetConnection(url, method);
			}

			private readonly HttpFSFileSystem _enclosing;

			private readonly Uri url;

			private readonly string method;
		}

		/// <summary>Convenience method that creates a <code>HttpURLConnection</code> for the specified URL.
		/// 	</summary>
		/// <remarks>
		/// Convenience method that creates a <code>HttpURLConnection</code> for the specified URL.
		/// <p>
		/// This methods performs and injects any needed authentication credentials.
		/// </remarks>
		/// <param name="url">url to connect to.</param>
		/// <param name="method">the HTTP method.</param>
		/// <returns>
		/// a <code>HttpURLConnection</code> for the HttpFSServer server, authenticated and ready to use for
		/// the specified path and file system operation.
		/// </returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurrs.</exception>
		private HttpURLConnection GetConnection(Uri url, string method)
		{
			try
			{
				HttpURLConnection conn = authURL.OpenConnection(url, authToken);
				conn.SetRequestMethod(method);
				if (method.Equals(HttpPost) || method.Equals(HttpPut))
				{
					conn.SetDoOutput(true);
				}
				return conn;
			}
			catch (Exception ex)
			{
				throw new IOException(ex);
			}
		}

		/// <summary>Called after a new FileSystem instance is constructed.</summary>
		/// <param name="name">a uri whose authority section names the host, port, etc. for this FileSystem
		/// 	</param>
		/// <param name="conf">the configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(URI name, Configuration conf)
		{
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			//the real use is the one that has the Kerberos credentials needed for
			//SPNEGO to work
			realUser = ugi.GetRealUser();
			if (realUser == null)
			{
				realUser = UserGroupInformation.GetLoginUser();
			}
			base.Initialize(name, conf);
			try
			{
				uri = new URI(name.GetScheme() + "://" + name.GetAuthority());
			}
			catch (URISyntaxException ex)
			{
				throw new IOException(ex);
			}
			Type klass = GetConf().GetClass<DelegationTokenAuthenticator>("httpfs.authenticator.class"
				, typeof(KerberosDelegationTokenAuthenticator));
			DelegationTokenAuthenticator authenticator = ReflectionUtils.NewInstance(klass, GetConf
				());
			authURL = new DelegationTokenAuthenticatedURL(authenticator);
		}

		public override string GetScheme()
		{
			return Scheme;
		}

		/// <summary>Returns a URI whose scheme and authority identify this FileSystem.</summary>
		/// <returns>the URI whose scheme and authority identify this FileSystem.</returns>
		public override URI GetUri()
		{
			return uri;
		}

		/// <summary>Get the default port for this file system.</summary>
		/// <returns>the default port or 0 if there isn't one</returns>
		protected override int GetDefaultPort()
		{
			return GetConf().GetInt(DFSConfigKeys.DfsNamenodeHttpPortKey, DFSConfigKeys.DfsNamenodeHttpPortDefault
				);
		}

		/// <summary>HttpFSServer subclass of the <code>FSDataInputStream</code>.</summary>
		/// <remarks>
		/// HttpFSServer subclass of the <code>FSDataInputStream</code>.
		/// <p>
		/// This implementation does not support the
		/// <code>PositionReadable</code> and <code>Seekable</code> methods.
		/// </remarks>
		private class HttpFSDataInputStream : FilterInputStream, Seekable, PositionedReadable
		{
			protected internal HttpFSDataInputStream(InputStream @in, int bufferSize)
				: base(new BufferedInputStream(@in, bufferSize))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Read(long position, byte[] buffer, int offset, int length)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFully(long position, byte[] buffer, int offset, int length
				)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFully(long position, byte[] buffer)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Seek(long pos)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPos()
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool SeekToNewSource(long targetPos)
			{
				throw new NotSupportedException();
			}
		}

		/// <summary>Opens an FSDataInputStream at the indicated Path.</summary>
		/// <remarks>
		/// Opens an FSDataInputStream at the indicated Path.
		/// <p>
		/// IMPORTANT: the returned <code>FSDataInputStream</code> does not support the
		/// <code>PositionReadable</code> and <code>Seekable</code> methods.
		/// </remarks>
		/// <param name="f">the file name to open</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Open.ToString();
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Open.GetMethod(
				), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			return new FSDataInputStream(new HttpFSFileSystem.HttpFSDataInputStream(conn.GetInputStream
				(), bufferSize));
		}

		/// <summary>HttpFSServer subclass of the <code>FSDataOutputStream</code>.</summary>
		/// <remarks>
		/// HttpFSServer subclass of the <code>FSDataOutputStream</code>.
		/// <p>
		/// This implementation closes the underlying HTTP connection validating the Http connection status
		/// at closing time.
		/// </remarks>
		private class HttpFSDataOutputStream : FSDataOutputStream
		{
			private HttpURLConnection conn;

			private int closeStatus;

			/// <exception cref="System.IO.IOException"/>
			public HttpFSDataOutputStream(HttpURLConnection conn, OutputStream @out, int closeStatus
				, FileSystem.Statistics stats)
				: base(@out, stats)
			{
				this.conn = conn;
				this.closeStatus = closeStatus;
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
					HttpExceptionUtils.ValidateResponse(conn, closeStatus);
				}
			}
		}

		/// <summary>Converts a <code>FsPermission</code> to a Unix octal representation.</summary>
		/// <param name="p">the permission.</param>
		/// <returns>the Unix string symbolic reprentation.</returns>
		public static string PermissionToString(FsPermission p)
		{
			return Sharpen.Extensions.ToString((p == null) ? DefaultPermission : p.ToShort(), 
				8);
		}

		/*
		* Common handling for uploading data for create and append operations.
		*/
		/// <exception cref="System.IO.IOException"/>
		private FSDataOutputStream UploadData(string method, Path f, IDictionary<string, 
			string> @params, int bufferSize, int expectedStatus)
		{
			HttpURLConnection conn = GetConnection(method, @params, f, true);
			conn.SetInstanceFollowRedirects(false);
			bool exceptionAlreadyHandled = false;
			try
			{
				if (conn.GetResponseCode() == HttpTemporaryRedirect)
				{
					exceptionAlreadyHandled = true;
					string location = conn.GetHeaderField("Location");
					if (location != null)
					{
						conn = GetConnection(new Uri(location), method);
						conn.SetRequestProperty("Content-Type", UploadContentType);
						try
						{
							OutputStream os = new BufferedOutputStream(conn.GetOutputStream(), bufferSize);
							return new HttpFSFileSystem.HttpFSDataOutputStream(conn, os, expectedStatus, statistics
								);
						}
						catch (IOException ex)
						{
							HttpExceptionUtils.ValidateResponse(conn, expectedStatus);
							throw;
						}
					}
					else
					{
						HttpExceptionUtils.ValidateResponse(conn, HttpTemporaryRedirect);
						throw new IOException("Missing HTTP 'Location' header for [" + conn.GetURL() + "]"
							);
					}
				}
				else
				{
					throw new IOException(MessageFormat.Format("Expected HTTP status was [307], received [{0}]"
						, conn.GetResponseCode()));
				}
			}
			catch (IOException ex)
			{
				if (exceptionAlreadyHandled)
				{
					throw;
				}
				else
				{
					HttpExceptionUtils.ValidateResponse(conn, HttpTemporaryRedirect);
					throw;
				}
			}
		}

		/// <summary>
		/// Opens an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// </summary>
		/// <remarks>
		/// Opens an FSDataOutputStream at the indicated Path with write-progress
		/// reporting.
		/// <p>
		/// IMPORTANT: The <code>Progressable</code> parameter is not used.
		/// </remarks>
		/// <param name="f">the file name to open.</param>
		/// <param name="permission">file permission.</param>
		/// <param name="overwrite">
		/// if a file with this name already exists, then if true,
		/// the file will be overwritten, and if false an error will be thrown.
		/// </param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="replication">required block replication for the file.</param>
		/// <param name="blockSize">block size.</param>
		/// <param name="progress">progressable.</param>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="SetPermission(Org.Apache.Hadoop.FS.Path, Org.Apache.Hadoop.FS.Permission.FsPermission)
		/// 	"/>
		public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
			, int bufferSize, short replication, long blockSize, Progressable progress)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Create.ToString();
			@params[OverwriteParam] = bool.ToString(overwrite);
			@params[ReplicationParam] = short.ToString(replication);
			@params[BlocksizeParam] = System.Convert.ToString(blockSize);
			@params[PermissionParam] = PermissionToString(permission);
			return UploadData(HttpFSFileSystem.Operation.Create.GetMethod(), f, @params, bufferSize
				, HttpURLConnection.HttpCreated);
		}

		/// <summary>Append to an existing file (optional operation).</summary>
		/// <remarks>
		/// Append to an existing file (optional operation).
		/// <p>
		/// IMPORTANT: The <code>Progressable</code> parameter is not used.
		/// </remarks>
		/// <param name="f">the existing file to be appended.</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <param name="progress">for reporting progress if it is not null.</param>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
			)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Append.ToString();
			return UploadData(HttpFSFileSystem.Operation.Append.GetMethod(), f, @params, bufferSize
				, HttpURLConnection.HttpOk);
		}

		/// <summary>Truncate a file.</summary>
		/// <param name="f">the file to be truncated.</param>
		/// <param name="newLength">The size the file is to be truncated to.</param>
		/// <exception cref="System.IO.IOException"/>
		public override bool Truncate(Path f, long newLength)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Truncate.ToString();
			@params[NewLengthParam] = System.Convert.ToString(newLength);
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Truncate.GetMethod
				(), @params, f, true);
			JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
			return (bool)json[TruncateJson];
		}

		/// <summary>Concat existing files together.</summary>
		/// <param name="f">the path to the target destination.</param>
		/// <param name="psrcs">the paths to the sources to use for the concatenation.</param>
		/// <exception cref="System.IO.IOException"/>
		public override void Concat(Path f, Path[] psrcs)
		{
			IList<string> strPaths = new AList<string>(psrcs.Length);
			foreach (Path psrc in psrcs)
			{
				strPaths.AddItem(psrc.ToUri().GetPath());
			}
			string srcs = StringUtils.Join(",", strPaths);
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Concat.ToString();
			@params[SourcesParam] = srcs;
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Concat.GetMethod
				(), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
		}

		/// <summary>Renames Path src to Path dst.</summary>
		/// <remarks>
		/// Renames Path src to Path dst.  Can take place on local fs
		/// or remote DFS.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override bool Rename(Path src, Path dst)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Rename.ToString();
			@params[DestinationParam] = dst.ToString();
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Rename.GetMethod
				(), @params, src, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
			return (bool)json[RenameJson];
		}

		/// <summary>Delete a file.</summary>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use delete(Path, boolean) instead")]
		public override bool Delete(Path f)
		{
			return Delete(f, false);
		}

		/// <summary>Delete a file.</summary>
		/// <param name="f">the path to delete.</param>
		/// <param name="recursive">
		/// if path is a directory and set to
		/// true, the directory is deleted else throws an exception. In
		/// case of a file the recursive can be set to either true or false.
		/// </param>
		/// <returns>true if delete is successful else false.</returns>
		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Delete.ToString();
			@params[RecursiveParam] = bool.ToString(recursive);
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Delete.GetMethod
				(), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
			return (bool)json[DeleteJson];
		}

		/// <summary>
		/// List the statuses of the files/directories in the given path if the path is
		/// a directory.
		/// </summary>
		/// <param name="f">given path</param>
		/// <returns>the statuses of the files/directories in the given patch</returns>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Liststatus.ToString();
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Liststatus.GetMethod
				(), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
			json = (JSONObject)json[FileStatusesJson];
			JSONArray jsonArray = (JSONArray)json[FileStatusJson];
			FileStatus[] array = new FileStatus[jsonArray.Count];
			f = MakeQualified(f);
			for (int i = 0; i < jsonArray.Count; i++)
			{
				array[i] = CreateFileStatus(f, (JSONObject)jsonArray[i]);
			}
			return array;
		}

		/// <summary>Set the current working directory for the given file system.</summary>
		/// <remarks>
		/// Set the current working directory for the given file system. All relative
		/// paths will be resolved relative to it.
		/// </remarks>
		/// <param name="newDir">new directory.</param>
		public override void SetWorkingDirectory(Path newDir)
		{
			workingDir = newDir;
		}

		/// <summary>Get the current working directory for the given file system</summary>
		/// <returns>the directory pathname</returns>
		public override Path GetWorkingDirectory()
		{
			if (workingDir == null)
			{
				workingDir = GetHomeDirectory();
			}
			return workingDir;
		}

		/// <summary>
		/// Make the given file and all non-existent parents into
		/// directories.
		/// </summary>
		/// <remarks>
		/// Make the given file and all non-existent parents into
		/// directories. Has the semantics of Unix 'mkdir -p'.
		/// Existence of the directory hierarchy is not an error.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override bool Mkdirs(Path f, FsPermission permission)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Mkdirs.ToString();
			@params[PermissionParam] = PermissionToString(permission);
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Mkdirs.GetMethod
				(), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
			return (bool)json[MkdirsJson];
		}

		/// <summary>Return a file status object that represents the path.</summary>
		/// <param name="f">The path we want information from</param>
		/// <returns>a FileStatus object</returns>
		/// <exception cref="System.IO.FileNotFoundException">
		/// when the path does not exist;
		/// IOException see specific implementation
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileStatus(Path f)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Getfilestatus.ToString();
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Getfilestatus.GetMethod
				(), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
			json = (JSONObject)json[FileStatusJson];
			f = MakeQualified(f);
			return CreateFileStatus(f, json);
		}

		/// <summary>Return the current user's home directory in this filesystem.</summary>
		/// <remarks>
		/// Return the current user's home directory in this filesystem.
		/// The default implementation returns "/user/$USER/".
		/// </remarks>
		public override Path GetHomeDirectory()
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Gethomedirectory.ToString();
			try
			{
				HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Gethomedirectory
					.GetMethod(), @params, new Path(GetUri().ToString(), "/"), false);
				HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
				JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
				return new Path((string)json[HomeDirJson]);
			}
			catch (IOException ex)
			{
				throw new RuntimeException(ex);
			}
		}

		/// <summary>Set owner of a path (i.e.</summary>
		/// <remarks>
		/// Set owner of a path (i.e. a file or a directory).
		/// The parameters username and groupname cannot both be null.
		/// </remarks>
		/// <param name="p">The path</param>
		/// <param name="username">If it is null, the original username remains unchanged.</param>
		/// <param name="groupname">If it is null, the original groupname remains unchanged.</param>
		/// <exception cref="System.IO.IOException"/>
		public override void SetOwner(Path p, string username, string groupname)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Setowner.ToString();
			@params[OwnerParam] = username;
			@params[GroupParam] = groupname;
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Setowner.GetMethod
				(), @params, p, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
		}

		/// <summary>Set permission of a path.</summary>
		/// <param name="p">path.</param>
		/// <param name="permission">permission.</param>
		/// <exception cref="System.IO.IOException"/>
		public override void SetPermission(Path p, FsPermission permission)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Setpermission.ToString();
			@params[PermissionParam] = PermissionToString(permission);
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Setpermission.GetMethod
				(), @params, p, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
		}

		/// <summary>Set access time of a file</summary>
		/// <param name="p">The path</param>
		/// <param name="mtime">
		/// Set the modification time of this file.
		/// The number of milliseconds since Jan 1, 1970.
		/// A value of -1 means that this call should not set modification time.
		/// </param>
		/// <param name="atime">
		/// Set the access time of this file.
		/// The number of milliseconds since Jan 1, 1970.
		/// A value of -1 means that this call should not set access time.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public override void SetTimes(Path p, long mtime, long atime)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Settimes.ToString();
			@params[ModificationTimeParam] = System.Convert.ToString(mtime);
			@params[AccessTimeParam] = System.Convert.ToString(atime);
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Settimes.GetMethod
				(), @params, p, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
		}

		/// <summary>Set replication for an existing file.</summary>
		/// <param name="src">file name</param>
		/// <param name="replication">new replication</param>
		/// <returns>
		/// true if successful;
		/// false if file does not exist or is a directory
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public override bool SetReplication(Path src, short replication)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Setreplication.ToString();
			@params[ReplicationParam] = short.ToString(replication);
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Setreplication.
				GetMethod(), @params, src, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
			return (bool)json[SetReplicationJson];
		}

		/// <summary>Modify the ACL entries for a file.</summary>
		/// <param name="path">Path to modify</param>
		/// <param name="aclSpec">describing modifications</param>
		/// <exception cref="System.IO.IOException"/>
		public override void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Modifyaclentries.ToString();
			@params[AclspecParam] = AclEntry.AclSpecToString(aclSpec);
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Modifyaclentries
				.GetMethod(), @params, path, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
		}

		/// <summary>Remove the specified ACL entries from a file</summary>
		/// <param name="path">Path to modify</param>
		/// <param name="aclSpec">describing entries to remove</param>
		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Removeaclentries.ToString();
			@params[AclspecParam] = AclEntry.AclSpecToString(aclSpec);
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Removeaclentries
				.GetMethod(), @params, path, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
		}

		/// <summary>Removes the default ACL for the given file</summary>
		/// <param name="path">Path from which to remove the default ACL.</param>
		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDefaultAcl(Path path)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Removedefaultacl.ToString();
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Removedefaultacl
				.GetMethod(), @params, path, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
		}

		/// <summary>Remove all ACLs from a file</summary>
		/// <param name="path">Path from which to remove all ACLs</param>
		/// <exception cref="System.IO.IOException"/>
		public override void RemoveAcl(Path path)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Removeacl.ToString();
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Removeacl.GetMethod
				(), @params, path, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
		}

		/// <summary>Set the ACLs for the given file</summary>
		/// <param name="path">Path to modify</param>
		/// <param name="aclSpec">
		/// describing modifications, must include
		/// entries for user, group, and others for compatibility
		/// with permission bits.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public override void SetAcl(Path path, IList<AclEntry> aclSpec)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Setacl.ToString();
			@params[AclspecParam] = AclEntry.AclSpecToString(aclSpec);
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Setacl.GetMethod
				(), @params, path, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
		}

		/// <summary>Get the ACL information for a given file</summary>
		/// <param name="path">Path to acquire ACL info for</param>
		/// <returns>the ACL information in JSON format</returns>
		/// <exception cref="System.IO.IOException"/>
		public override AclStatus GetAclStatus(Path path)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Getaclstatus.ToString();
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Getaclstatus.GetMethod
				(), @params, path, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
			json = (JSONObject)json[AclStatusJson];
			return CreateAclStatus(json);
		}

		private FileStatus CreateFileStatus(Path parent, JSONObject json)
		{
			string pathSuffix = (string)json[PathSuffixJson];
			Path path = (pathSuffix.Equals(string.Empty)) ? parent : new Path(parent, pathSuffix
				);
			HttpFSFileSystem.FILE_TYPE type = HttpFSFileSystem.FILE_TYPE.ValueOf((string)json
				[TypeJson]);
			long len = (long)json[LengthJson];
			string owner = (string)json[OwnerJson];
			string group = (string)json[GroupJson];
			FsPermission permission = new FsPermission(short.ParseShort((string)json[PermissionJson
				], 8));
			long aTime = (long)json[AccessTimeJson];
			long mTime = (long)json[ModificationTimeJson];
			long blockSize = (long)json[BlockSizeJson];
			short replication = ((long)json[ReplicationJson]);
			FileStatus fileStatus = null;
			switch (type)
			{
				case HttpFSFileSystem.FILE_TYPE.File:
				case HttpFSFileSystem.FILE_TYPE.Directory:
				{
					fileStatus = new FileStatus(len, (type == HttpFSFileSystem.FILE_TYPE.Directory), 
						replication, blockSize, mTime, aTime, permission, owner, group, path);
					break;
				}

				case HttpFSFileSystem.FILE_TYPE.Symlink:
				{
					Path symLink = null;
					fileStatus = new FileStatus(len, false, replication, blockSize, mTime, aTime, permission
						, owner, group, symLink, path);
					break;
				}
			}
			return fileStatus;
		}

		/// <summary>Convert the given JSON object into an AclStatus</summary>
		/// <param name="json">Input JSON representing the ACLs</param>
		/// <returns>Resulting AclStatus</returns>
		private AclStatus CreateAclStatus(JSONObject json)
		{
			AclStatus.Builder aclStatusBuilder = new AclStatus.Builder().Owner((string)json[OwnerJson
				]).Group((string)json[GroupJson]).StickyBit((bool)json[AclStickyBitJson]);
			JSONArray entries = (JSONArray)json[AclEntriesJson];
			foreach (object e in entries)
			{
				aclStatusBuilder.AddEntry(AclEntry.ParseAclEntry(e.ToString(), true));
			}
			return aclStatusBuilder.Build();
		}

		/// <exception cref="System.IO.IOException"/>
		public override ContentSummary GetContentSummary(Path f)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Getcontentsummary.ToString();
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Getcontentsummary
				.GetMethod(), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)((JSONObject)HttpFSUtils.JsonParse(conn))[ContentSummaryJson
				];
			return new ContentSummary.Builder().Length((long)json[ContentSummaryLengthJson]).
				FileCount((long)json[ContentSummaryFileCountJson]).DirectoryCount((long)json[ContentSummaryDirectoryCountJson
				]).Quota((long)json[ContentSummaryQuotaJson]).SpaceConsumed((long)json[ContentSummarySpaceConsumedJson
				]).SpaceQuota((long)json[ContentSummarySpaceQuotaJson]).Build();
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileChecksum GetFileChecksum(Path f)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Getfilechecksum.ToString();
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Getfilechecksum
				.GetMethod(), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)((JSONObject)HttpFSUtils.JsonParse(conn))[FileChecksumJson
				];
			return new _FileChecksum_1035(json);
		}

		private sealed class _FileChecksum_1035 : FileChecksum
		{
			public _FileChecksum_1035(JSONObject json)
			{
				this.json = json;
			}

			public override string GetAlgorithmName()
			{
				return (string)json[HttpFSFileSystem.ChecksumAlgorithmJson];
			}

			public override int GetLength()
			{
				return ((long)json[HttpFSFileSystem.ChecksumLengthJson]);
			}

			public override byte[] GetBytes()
			{
				return StringUtils.HexStringToByte((string)json[HttpFSFileSystem.ChecksumBytesJson
					]);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(DataOutput @out)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(DataInput @in)
			{
				throw new NotSupportedException();
			}

			private readonly JSONObject json;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationToken
			(string renewer)
		{
			try
			{
				return UserGroupInformation.GetCurrentUser().DoAs(new _PrivilegedExceptionAction_1069
					(this, renewer));
			}
			catch (Exception ex)
			{
				if (ex is IOException)
				{
					throw (IOException)ex;
				}
				else
				{
					throw new IOException(ex);
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_1069 : PrivilegedExceptionAction<
			Org.Apache.Hadoop.Security.Token.Token<object>>
		{
			public _PrivilegedExceptionAction_1069(HttpFSFileSystem _enclosing, string renewer
				)
			{
				this._enclosing = _enclosing;
				this.renewer = renewer;
			}

			/// <exception cref="System.Exception"/>
			public Org.Apache.Hadoop.Security.Token.Token<object> Run()
			{
				return this._enclosing.authURL.GetDelegationToken(this._enclosing.uri.ToURL(), this
					._enclosing.authToken, renewer);
			}

			private readonly HttpFSFileSystem _enclosing;

			private readonly string renewer;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long RenewDelegationToken<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			try
			{
				return UserGroupInformation.GetCurrentUser().DoAs(new _PrivilegedExceptionAction_1089
					(this));
			}
			catch (Exception ex)
			{
				if (ex is IOException)
				{
					throw (IOException)ex;
				}
				else
				{
					throw new IOException(ex);
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_1089 : PrivilegedExceptionAction<
			long>
		{
			public _PrivilegedExceptionAction_1089(HttpFSFileSystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public long Run()
			{
				return this._enclosing.authURL.RenewDelegationToken(this._enclosing.uri.ToURL(), 
					this._enclosing.authToken);
			}

			private readonly HttpFSFileSystem _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CancelDelegationToken<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			authURL.CancelDelegationToken(uri.ToURL(), authToken);
		}

		public virtual Org.Apache.Hadoop.Security.Token.Token<object> GetRenewToken()
		{
			return null;
		}

		//TODO : for renewer
		public virtual void SetDelegationToken<T>(Org.Apache.Hadoop.Security.Token.Token<
			T> token)
			where T : TokenIdentifier
		{
		}

		//TODO : for renewer
		/// <exception cref="System.IO.IOException"/>
		public override void SetXAttr(Path f, string name, byte[] value, EnumSet<XAttrSetFlag
			> flag)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Setxattr.ToString();
			@params[XattrNameParam] = name;
			if (value != null)
			{
				@params[XattrValueParam] = XAttrCodec.EncodeValue(value, XAttrCodec.Hex);
			}
			@params[XattrSetFlagParam] = EnumSetParam.ToString(flag);
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Setxattr.GetMethod
				(), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
		}

		/// <exception cref="System.IO.IOException"/>
		public override byte[] GetXAttr(Path f, string name)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Getxattrs.ToString();
			@params[XattrNameParam] = name;
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Getxattrs.GetMethod
				(), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
			IDictionary<string, byte[]> xAttrs = CreateXAttrMap((JSONArray)json[XattrsJson]);
			return xAttrs != null ? xAttrs[name] : null;
		}

		/// <summary>Convert xAttrs json to xAttrs map</summary>
		/// <exception cref="System.IO.IOException"/>
		private IDictionary<string, byte[]> CreateXAttrMap(JSONArray jsonArray)
		{
			IDictionary<string, byte[]> xAttrs = Maps.NewHashMap();
			foreach (object obj in jsonArray)
			{
				JSONObject jsonObj = (JSONObject)obj;
				string name = (string)jsonObj[XattrNameJson];
				byte[] value = XAttrCodec.DecodeValue((string)jsonObj[XattrValueJson]);
				xAttrs[name] = value;
			}
			return xAttrs;
		}

		/// <summary>Convert xAttr names json to names list</summary>
		/// <exception cref="System.IO.IOException"/>
		private IList<string> CreateXAttrNames(string xattrNamesStr)
		{
			JSONParser parser = new JSONParser();
			JSONArray jsonArray;
			try
			{
				jsonArray = (JSONArray)parser.Parse(xattrNamesStr);
				IList<string> names = Lists.NewArrayListWithCapacity(jsonArray.Count);
				foreach (object name in jsonArray)
				{
					names.AddItem((string)name);
				}
				return names;
			}
			catch (ParseException e)
			{
				throw new IOException("JSON parser error, " + e.Message, e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path f)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Getxattrs.ToString();
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Getxattrs.GetMethod
				(), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
			return CreateXAttrMap((JSONArray)json[XattrsJson]);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, byte[]> GetXAttrs(Path f, IList<string> names
			)
		{
			Preconditions.CheckArgument(names != null && !names.IsEmpty(), "XAttr names cannot be null or empty."
				);
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Getxattrs.ToString();
			IDictionary<string, IList<string>> multiValuedParams = Maps.NewHashMap();
			multiValuedParams[XattrNameParam] = names;
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Getxattrs.GetMethod
				(), @params, multiValuedParams, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
			return CreateXAttrMap((JSONArray)json[XattrsJson]);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> ListXAttrs(Path f)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Listxattrs.ToString();
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Listxattrs.GetMethod
				(), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
			JSONObject json = (JSONObject)HttpFSUtils.JsonParse(conn);
			return CreateXAttrNames((string)json[XattrnamesJson]);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveXAttr(Path f, string name)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[OpParam] = HttpFSFileSystem.Operation.Removexattr.ToString();
			@params[XattrNameParam] = name;
			HttpURLConnection conn = GetConnection(HttpFSFileSystem.Operation.Removexattr.GetMethod
				(), @params, f, true);
			HttpExceptionUtils.ValidateResponse(conn, HttpURLConnection.HttpOk);
		}
	}
}
