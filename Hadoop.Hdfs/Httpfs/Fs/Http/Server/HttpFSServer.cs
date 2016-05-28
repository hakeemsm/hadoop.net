using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet.Http;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Http.Client;
using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Lib.Servlet;
using Org.Apache.Hadoop.Lib.Wsrs;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Json.Simple;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Server
{
	/// <summary>Main class of HttpFSServer server.</summary>
	/// <remarks>
	/// Main class of HttpFSServer server.
	/// <p>
	/// The <code>HttpFSServer</code> class uses Jersey JAX-RS to binds HTTP requests to the
	/// different operations.
	/// </remarks>
	public class HttpFSServer
	{
		private static Logger AuditLog = LoggerFactory.GetLogger("httpfsaudit");

		/// <summary>
		/// Executes a
		/// <see cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccess.FileSystemExecutor{T}"/
		/// 	>
		/// using a filesystem for the effective
		/// user.
		/// </summary>
		/// <param name="ugi">user making the request.</param>
		/// <param name="executor">FileSystemExecutor to execute.</param>
		/// <returns>FileSystemExecutor response</returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurrs.</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException">
		/// thrown if a FileSystemAccess releated error occurred. Thrown
		/// exceptions are handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		private T FsExecute<T>(UserGroupInformation ugi, FileSystemAccess.FileSystemExecutor
			<T> executor)
		{
			FileSystemAccess fsAccess = HttpFSServerWebApp.Get().Get<FileSystemAccess>();
			Configuration conf = HttpFSServerWebApp.Get().Get<FileSystemAccess>().GetFileSystemConfiguration
				();
			return fsAccess.Execute(ugi.GetShortUserName(), conf, executor);
		}

		/// <summary>Returns a filesystem instance.</summary>
		/// <remarks>
		/// Returns a filesystem instance. The fileystem instance is wired for release at the completion of
		/// the current Servlet request via the
		/// <see cref="Org.Apache.Hadoop.Lib.Servlet.FileSystemReleaseFilter"/>
		/// .
		/// <p>
		/// If a do-as user is specified, the current user must be a valid proxyuser, otherwise an
		/// <code>AccessControlException</code> will be thrown.
		/// </remarks>
		/// <param name="ugi">principal for whom the filesystem instance is.</param>
		/// <returns>a filesystem for the specified user or do-as user.</returns>
		/// <exception cref="System.IO.IOException">
		/// thrown if an IO error occurred. Thrown exceptions are
		/// handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException">
		/// thrown if a FileSystemAccess releated error occurred. Thrown
		/// exceptions are handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		private FileSystem CreateFileSystem(UserGroupInformation ugi)
		{
			string hadoopUser = ugi.GetShortUserName();
			FileSystemAccess fsAccess = HttpFSServerWebApp.Get().Get<FileSystemAccess>();
			Configuration conf = HttpFSServerWebApp.Get().Get<FileSystemAccess>().GetFileSystemConfiguration
				();
			FileSystem fs = fsAccess.CreateFileSystem(hadoopUser, conf);
			FileSystemReleaseFilter.SetFileSystem(fs);
			return fs;
		}

		private void EnforceRootPath(HttpFSFileSystem.Operation op, string path)
		{
			if (!path.Equals("/"))
			{
				throw new NotSupportedException(MessageFormat.Format("Operation [{0}], invalid path [{1}], must be '/'"
					, op, path));
			}
		}

		/// <summary>Special binding for '/' as it is not handled by the wildcard binding.</summary>
		/// <param name="op">the HttpFS operation of the request.</param>
		/// <param name="params">the HttpFS parameters of the request.</param>
		/// <returns>the request response.</returns>
		/// <exception cref="System.IO.IOException">
		/// thrown if an IO error occurred. Thrown exceptions are
		/// handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException">
		/// thrown if a FileSystemAccess releated
		/// error occurred. Thrown exceptions are handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		[GET]
		public virtual Response GetRoot(HttpFSParametersProvider.OperationParam op, Parameters
			 @params, HttpServletRequest request)
		{
			return Get(string.Empty, op, @params, request);
		}

		private string MakeAbsolute(string path)
		{
			return "/" + ((path != null) ? path : string.Empty);
		}

		/// <summary>Binding to handle GET requests, supported operations are</summary>
		/// <param name="path">the path for operation.</param>
		/// <param name="op">the HttpFS operation of the request.</param>
		/// <param name="params">the HttpFS parameters of the request.</param>
		/// <returns>the request response.</returns>
		/// <exception cref="System.IO.IOException">
		/// thrown if an IO error occurred. Thrown exceptions are
		/// handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException">
		/// thrown if a FileSystemAccess releated
		/// error occurred. Thrown exceptions are handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		[GET]
		public virtual Response Get(string path, HttpFSParametersProvider.OperationParam 
			op, Parameters @params, HttpServletRequest request)
		{
			UserGroupInformation user = HttpUserGroupInformation.Get();
			Response response;
			path = MakeAbsolute(path);
			MDC.Put(HttpFSFileSystem.OpParam, op.Value().ToString());
			MDC.Put("hostname", request.GetRemoteAddr());
			switch (op.Value())
			{
				case HttpFSFileSystem.Operation.Open:
				{
					//Invoking the command directly using an unmanaged FileSystem that is
					// released by the FileSystemReleaseFilter
					FSOperations.FSOpen command = new FSOperations.FSOpen(path);
					FileSystem fs = CreateFileSystem(user);
					InputStream @is = command.Execute(fs);
					long offset = @params.Get<HttpFSParametersProvider.OffsetParam>(HttpFSParametersProvider.OffsetParam
						.Name);
					long len = @params.Get<HttpFSParametersProvider.LenParam>(HttpFSParametersProvider.LenParam
						.Name);
					AuditLog.Info("[{}] offset [{}] len [{}]", new object[] { path, offset, len });
					InputStreamEntity entity = new InputStreamEntity(@is, offset, len);
					response = Response.Ok(entity).Type(MediaType.ApplicationOctetStream).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Getfilestatus:
				{
					FSOperations.FSFileStatus command = new FSOperations.FSFileStatus(path);
					IDictionary json = FsExecute(user, command);
					AuditLog.Info("[{}]", path);
					response = Response.Ok(json).Type(MediaType.ApplicationJson).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Liststatus:
				{
					string filter = @params.Get<HttpFSParametersProvider.FilterParam>(HttpFSParametersProvider.FilterParam
						.Name);
					FSOperations.FSListStatus command = new FSOperations.FSListStatus(path, filter);
					IDictionary json = FsExecute(user, command);
					AuditLog.Info("[{}] filter [{}]", path, (filter != null) ? filter : "-");
					response = Response.Ok(json).Type(MediaType.ApplicationJson).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Gethomedirectory:
				{
					EnforceRootPath(op.Value(), path);
					FSOperations.FSHomeDir command = new FSOperations.FSHomeDir();
					JSONObject json = FsExecute(user, command);
					AuditLog.Info(string.Empty);
					response = Response.Ok(json).Type(MediaType.ApplicationJson).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Instrumentation:
				{
					EnforceRootPath(op.Value(), path);
					Groups groups = HttpFSServerWebApp.Get().Get<Groups>();
					IList<string> userGroups = groups.GetGroups(user.GetShortUserName());
					if (!userGroups.Contains(HttpFSServerWebApp.Get().GetAdminGroup()))
					{
						throw new AccessControlException("User not in HttpFSServer admin group");
					}
					Instrumentation instrumentation = HttpFSServerWebApp.Get().Get<Instrumentation>();
					IDictionary snapshot = instrumentation.GetSnapshot();
					response = Response.Ok(snapshot).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Getcontentsummary:
				{
					FSOperations.FSContentSummary command = new FSOperations.FSContentSummary(path);
					IDictionary json = FsExecute(user, command);
					AuditLog.Info("[{}]", path);
					response = Response.Ok(json).Type(MediaType.ApplicationJson).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Getfilechecksum:
				{
					FSOperations.FSFileChecksum command = new FSOperations.FSFileChecksum(path);
					IDictionary json = FsExecute(user, command);
					AuditLog.Info("[{}]", path);
					response = Response.Ok(json).Type(MediaType.ApplicationJson).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Getfileblocklocations:
				{
					response = Response.Status(Response.Status.BadRequest).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Getaclstatus:
				{
					FSOperations.FSAclStatus command = new FSOperations.FSAclStatus(path);
					IDictionary json = FsExecute(user, command);
					AuditLog.Info("ACL status for [{}]", path);
					response = Response.Ok(json).Type(MediaType.ApplicationJson).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Getxattrs:
				{
					IList<string> xattrNames = @params.GetValues<HttpFSParametersProvider.XAttrNameParam
						>(HttpFSParametersProvider.XAttrNameParam.Name);
					XAttrCodec encoding = @params.Get<HttpFSParametersProvider.XAttrEncodingParam>(HttpFSParametersProvider.XAttrEncodingParam
						.Name);
					FSOperations.FSGetXAttrs command = new FSOperations.FSGetXAttrs(path, xattrNames, 
						encoding);
					IDictionary json = FsExecute(user, command);
					AuditLog.Info("XAttrs for [{}]", path);
					response = Response.Ok(json).Type(MediaType.ApplicationJson).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Listxattrs:
				{
					FSOperations.FSListXAttrs command = new FSOperations.FSListXAttrs(path);
					IDictionary json = FsExecute(user, command);
					AuditLog.Info("XAttr names for [{}]", path);
					response = Response.Ok(json).Type(MediaType.ApplicationJson).Build();
					break;
				}

				default:
				{
					throw new IOException(MessageFormat.Format("Invalid HTTP GET operation [{0}]", op
						.Value()));
				}
			}
			return response;
		}

		/// <summary>Binding to handle DELETE requests.</summary>
		/// <param name="path">the path for operation.</param>
		/// <param name="op">the HttpFS operation of the request.</param>
		/// <param name="params">the HttpFS parameters of the request.</param>
		/// <returns>the request response.</returns>
		/// <exception cref="System.IO.IOException">
		/// thrown if an IO error occurred. Thrown exceptions are
		/// handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException">
		/// thrown if a FileSystemAccess releated
		/// error occurred. Thrown exceptions are handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		[DELETE]
		public virtual Response Delete(string path, HttpFSParametersProvider.OperationParam
			 op, Parameters @params, HttpServletRequest request)
		{
			UserGroupInformation user = HttpUserGroupInformation.Get();
			Response response;
			path = MakeAbsolute(path);
			MDC.Put(HttpFSFileSystem.OpParam, op.Value().ToString());
			MDC.Put("hostname", request.GetRemoteAddr());
			switch (op.Value())
			{
				case HttpFSFileSystem.Operation.Delete:
				{
					bool recursive = @params.Get<HttpFSParametersProvider.RecursiveParam>(HttpFSParametersProvider.RecursiveParam
						.Name);
					AuditLog.Info("[{}] recursive [{}]", path, recursive);
					FSOperations.FSDelete command = new FSOperations.FSDelete(path, recursive);
					JSONObject json = FsExecute(user, command);
					response = Response.Ok(json).Type(MediaType.ApplicationJson).Build();
					break;
				}

				default:
				{
					throw new IOException(MessageFormat.Format("Invalid HTTP DELETE operation [{0}]", 
						op.Value()));
				}
			}
			return response;
		}

		/// <summary>Binding to handle POST requests.</summary>
		/// <param name="is">the inputstream for the request payload.</param>
		/// <param name="uriInfo">the of the request.</param>
		/// <param name="path">the path for operation.</param>
		/// <param name="op">the HttpFS operation of the request.</param>
		/// <param name="params">the HttpFS parameters of the request.</param>
		/// <returns>the request response.</returns>
		/// <exception cref="System.IO.IOException">
		/// thrown if an IO error occurred. Thrown exceptions are
		/// handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException">
		/// thrown if a FileSystemAccess releated
		/// error occurred. Thrown exceptions are handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		[POST]
		public virtual Response Post(InputStream @is, UriInfo uriInfo, string path, HttpFSParametersProvider.OperationParam
			 op, Parameters @params, HttpServletRequest request)
		{
			UserGroupInformation user = HttpUserGroupInformation.Get();
			Response response;
			path = MakeAbsolute(path);
			MDC.Put(HttpFSFileSystem.OpParam, op.Value().ToString());
			MDC.Put("hostname", request.GetRemoteAddr());
			switch (op.Value())
			{
				case HttpFSFileSystem.Operation.Append:
				{
					bool hasData = @params.Get<HttpFSParametersProvider.DataParam>(HttpFSParametersProvider.DataParam
						.Name);
					if (!hasData)
					{
						response = Response.TemporaryRedirect(CreateUploadRedirectionURL(uriInfo, HttpFSFileSystem.Operation
							.Append)).Build();
					}
					else
					{
						FSOperations.FSAppend command = new FSOperations.FSAppend(@is, path);
						FsExecute(user, command);
						AuditLog.Info("[{}]", path);
						response = Response.Ok().Type(MediaType.ApplicationJson).Build();
					}
					break;
				}

				case HttpFSFileSystem.Operation.Concat:
				{
					System.Console.Out.WriteLine("HTTPFS SERVER CONCAT");
					string sources = @params.Get<HttpFSParametersProvider.SourcesParam>(HttpFSParametersProvider.SourcesParam
						.Name);
					FSOperations.FSConcat command = new FSOperations.FSConcat(path, sources.Split(","
						));
					FsExecute(user, command);
					AuditLog.Info("[{}]", path);
					System.Console.Out.WriteLine("SENT RESPONSE");
					response = Response.Ok().Build();
					break;
				}

				case HttpFSFileSystem.Operation.Truncate:
				{
					long newLength = @params.Get<HttpFSParametersProvider.NewLengthParam>(HttpFSParametersProvider.NewLengthParam
						.Name);
					FSOperations.FSTruncate command = new FSOperations.FSTruncate(path, newLength);
					JSONObject json = FsExecute(user, command);
					AuditLog.Info("Truncate [{}] to length [{}]", path, newLength);
					response = Response.Ok(json).Type(MediaType.ApplicationJson).Build();
					break;
				}

				default:
				{
					throw new IOException(MessageFormat.Format("Invalid HTTP POST operation [{0}]", op
						.Value()));
				}
			}
			return response;
		}

		/// <summary>Creates the URL for an upload operation (create or append).</summary>
		/// <param name="uriInfo">uri info of the request.</param>
		/// <param name="uploadOperation">operation for the upload URL.</param>
		/// <returns>the URI for uploading data.</returns>
		protected internal virtual URI CreateUploadRedirectionURL<_T0>(UriInfo uriInfo, Enum
			<_T0> uploadOperation)
			where _T0 : Enum<E>
		{
			UriBuilder uriBuilder = uriInfo.GetRequestUriBuilder();
			uriBuilder = uriBuilder.ReplaceQueryParam(HttpFSParametersProvider.OperationParam
				.Name, uploadOperation).QueryParam(HttpFSParametersProvider.DataParam.Name, true
				);
			return uriBuilder.Build(null);
		}

		/// <summary>Binding to handle PUT requests.</summary>
		/// <param name="is">the inputstream for the request payload.</param>
		/// <param name="uriInfo">the of the request.</param>
		/// <param name="path">the path for operation.</param>
		/// <param name="op">the HttpFS operation of the request.</param>
		/// <param name="params">the HttpFS parameters of the request.</param>
		/// <returns>the request response.</returns>
		/// <exception cref="System.IO.IOException">
		/// thrown if an IO error occurred. Thrown exceptions are
		/// handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Service.FileSystemAccessException">
		/// thrown if a FileSystemAccess releated
		/// error occurred. Thrown exceptions are handled by
		/// <see cref="HttpFSExceptionProvider"/>
		/// .
		/// </exception>
		[PUT]
		public virtual Response Put(InputStream @is, UriInfo uriInfo, string path, HttpFSParametersProvider.OperationParam
			 op, Parameters @params, HttpServletRequest request)
		{
			UserGroupInformation user = HttpUserGroupInformation.Get();
			Response response;
			path = MakeAbsolute(path);
			MDC.Put(HttpFSFileSystem.OpParam, op.Value().ToString());
			MDC.Put("hostname", request.GetRemoteAddr());
			switch (op.Value())
			{
				case HttpFSFileSystem.Operation.Create:
				{
					bool hasData = @params.Get<HttpFSParametersProvider.DataParam>(HttpFSParametersProvider.DataParam
						.Name);
					if (!hasData)
					{
						response = Response.TemporaryRedirect(CreateUploadRedirectionURL(uriInfo, HttpFSFileSystem.Operation
							.Create)).Build();
					}
					else
					{
						short permission = @params.Get<HttpFSParametersProvider.PermissionParam>(HttpFSParametersProvider.PermissionParam
							.Name);
						bool @override = @params.Get<HttpFSParametersProvider.OverwriteParam>(HttpFSParametersProvider.OverwriteParam
							.Name);
						short replication = @params.Get<HttpFSParametersProvider.ReplicationParam>(HttpFSParametersProvider.ReplicationParam
							.Name);
						long blockSize = @params.Get<HttpFSParametersProvider.BlockSizeParam>(HttpFSParametersProvider.BlockSizeParam
							.Name);
						FSOperations.FSCreate command = new FSOperations.FSCreate(@is, path, permission, 
							@override, replication, blockSize);
						FsExecute(user, command);
						AuditLog.Info("[{}] permission [{}] override [{}] replication [{}] blockSize [{}]"
							, new object[] { path, permission, @override, replication, blockSize });
						response = Response.Status(Response.Status.Created).Build();
					}
					break;
				}

				case HttpFSFileSystem.Operation.Setxattr:
				{
					string xattrName = @params.Get<HttpFSParametersProvider.XAttrNameParam>(HttpFSParametersProvider.XAttrNameParam
						.Name);
					string xattrValue = @params.Get<HttpFSParametersProvider.XAttrValueParam>(HttpFSParametersProvider.XAttrValueParam
						.Name);
					EnumSet<XAttrSetFlag> flag = @params.Get<HttpFSParametersProvider.XAttrSetFlagParam
						>(HttpFSParametersProvider.XAttrSetFlagParam.Name);
					FSOperations.FSSetXAttr command = new FSOperations.FSSetXAttr(path, xattrName, xattrValue
						, flag);
					FsExecute(user, command);
					AuditLog.Info("[{}] to xAttr [{}]", path, xattrName);
					response = Response.Ok().Build();
					break;
				}

				case HttpFSFileSystem.Operation.Removexattr:
				{
					string xattrName = @params.Get<HttpFSParametersProvider.XAttrNameParam>(HttpFSParametersProvider.XAttrNameParam
						.Name);
					FSOperations.FSRemoveXAttr command = new FSOperations.FSRemoveXAttr(path, xattrName
						);
					FsExecute(user, command);
					AuditLog.Info("[{}] removed xAttr [{}]", path, xattrName);
					response = Response.Ok().Build();
					break;
				}

				case HttpFSFileSystem.Operation.Mkdirs:
				{
					short permission = @params.Get<HttpFSParametersProvider.PermissionParam>(HttpFSParametersProvider.PermissionParam
						.Name);
					FSOperations.FSMkdirs command = new FSOperations.FSMkdirs(path, permission);
					JSONObject json = FsExecute(user, command);
					AuditLog.Info("[{}] permission [{}]", path, permission);
					response = Response.Ok(json).Type(MediaType.ApplicationJson).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Rename:
				{
					string toPath = @params.Get<HttpFSParametersProvider.DestinationParam>(HttpFSParametersProvider.DestinationParam
						.Name);
					FSOperations.FSRename command = new FSOperations.FSRename(path, toPath);
					JSONObject json = FsExecute(user, command);
					AuditLog.Info("[{}] to [{}]", path, toPath);
					response = Response.Ok(json).Type(MediaType.ApplicationJson).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Setowner:
				{
					string owner = @params.Get<HttpFSParametersProvider.OwnerParam>(HttpFSParametersProvider.OwnerParam
						.Name);
					string group = @params.Get<HttpFSParametersProvider.GroupParam>(HttpFSParametersProvider.GroupParam
						.Name);
					FSOperations.FSSetOwner command = new FSOperations.FSSetOwner(path, owner, group);
					FsExecute(user, command);
					AuditLog.Info("[{}] to (O/G)[{}]", path, owner + ":" + group);
					response = Response.Ok().Build();
					break;
				}

				case HttpFSFileSystem.Operation.Setpermission:
				{
					short permission = @params.Get<HttpFSParametersProvider.PermissionParam>(HttpFSParametersProvider.PermissionParam
						.Name);
					FSOperations.FSSetPermission command = new FSOperations.FSSetPermission(path, permission
						);
					FsExecute(user, command);
					AuditLog.Info("[{}] to [{}]", path, permission);
					response = Response.Ok().Build();
					break;
				}

				case HttpFSFileSystem.Operation.Setreplication:
				{
					short replication = @params.Get<HttpFSParametersProvider.ReplicationParam>(HttpFSParametersProvider.ReplicationParam
						.Name);
					FSOperations.FSSetReplication command = new FSOperations.FSSetReplication(path, replication
						);
					JSONObject json = FsExecute(user, command);
					AuditLog.Info("[{}] to [{}]", path, replication);
					response = Response.Ok(json).Build();
					break;
				}

				case HttpFSFileSystem.Operation.Settimes:
				{
					long modifiedTime = @params.Get<HttpFSParametersProvider.ModifiedTimeParam>(HttpFSParametersProvider.ModifiedTimeParam
						.Name);
					long accessTime = @params.Get<HttpFSParametersProvider.AccessTimeParam>(HttpFSParametersProvider.AccessTimeParam
						.Name);
					FSOperations.FSSetTimes command = new FSOperations.FSSetTimes(path, modifiedTime, 
						accessTime);
					FsExecute(user, command);
					AuditLog.Info("[{}] to (M/A)[{}]", path, modifiedTime + ":" + accessTime);
					response = Response.Ok().Build();
					break;
				}

				case HttpFSFileSystem.Operation.Setacl:
				{
					string aclSpec = @params.Get<HttpFSParametersProvider.AclPermissionParam>(HttpFSParametersProvider.AclPermissionParam
						.Name);
					FSOperations.FSSetAcl command = new FSOperations.FSSetAcl(path, aclSpec);
					FsExecute(user, command);
					AuditLog.Info("[{}] to acl [{}]", path, aclSpec);
					response = Response.Ok().Build();
					break;
				}

				case HttpFSFileSystem.Operation.Removeacl:
				{
					FSOperations.FSRemoveAcl command = new FSOperations.FSRemoveAcl(path);
					FsExecute(user, command);
					AuditLog.Info("[{}] removed acl", path);
					response = Response.Ok().Build();
					break;
				}

				case HttpFSFileSystem.Operation.Modifyaclentries:
				{
					string aclSpec = @params.Get<HttpFSParametersProvider.AclPermissionParam>(HttpFSParametersProvider.AclPermissionParam
						.Name);
					FSOperations.FSModifyAclEntries command = new FSOperations.FSModifyAclEntries(path
						, aclSpec);
					FsExecute(user, command);
					AuditLog.Info("[{}] modify acl entry with [{}]", path, aclSpec);
					response = Response.Ok().Build();
					break;
				}

				case HttpFSFileSystem.Operation.Removeaclentries:
				{
					string aclSpec = @params.Get<HttpFSParametersProvider.AclPermissionParam>(HttpFSParametersProvider.AclPermissionParam
						.Name);
					FSOperations.FSRemoveAclEntries command = new FSOperations.FSRemoveAclEntries(path
						, aclSpec);
					FsExecute(user, command);
					AuditLog.Info("[{}] remove acl entry [{}]", path, aclSpec);
					response = Response.Ok().Build();
					break;
				}

				case HttpFSFileSystem.Operation.Removedefaultacl:
				{
					FSOperations.FSRemoveDefaultAcl command = new FSOperations.FSRemoveDefaultAcl(path
						);
					FsExecute(user, command);
					AuditLog.Info("[{}] remove default acl", path);
					response = Response.Ok().Build();
					break;
				}

				default:
				{
					throw new IOException(MessageFormat.Format("Invalid HTTP PUT operation [{0}]", op
						.Value()));
				}
			}
			return response;
		}
	}
}
