using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Javax.Servlet;
using Javax.Servlet.Http;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Web.Resources
{
	/// <summary>Web-hdfs NameNode implementation.</summary>
	public class NamenodeWebHdfsMethods
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(NamenodeWebHdfsMethods)
			);

		private static readonly UriFsPathParam Root = new UriFsPathParam(string.Empty);

		private static readonly ThreadLocal<string> RemoteAddress = new ThreadLocal<string
			>();

		/// <returns>the remote client address.</returns>
		public static string GetRemoteAddress()
		{
			return RemoteAddress.Get();
		}

		public static IPAddress GetRemoteIp()
		{
			try
			{
				return Sharpen.Extensions.GetAddressByName(GetRemoteAddress());
			}
			catch (Exception)
			{
				return null;
			}
		}

		/// <summary>Returns true if a WebHdfs request is in progress.</summary>
		/// <remarks>
		/// Returns true if a WebHdfs request is in progress.  Akin to
		/// <see cref="Org.Apache.Hadoop.Ipc.Server.IsRpcInvocation()"/>
		/// .
		/// </remarks>
		public static bool IsWebHdfsInvocation()
		{
			return GetRemoteAddress() != null;
		}

		[Context]
		private ServletContext context;

		[Context]
		private HttpServletRequest request;

		[Context]
		private HttpServletResponse response;

		private void Init<_T0>(UserGroupInformation ugi, DelegationParam delegation, UserParam
			 username, DoAsParam doAsUser, UriFsPathParam path, HttpOpParam<_T0> op, params 
			Param<object, object>[] parameters)
			where _T0 : Enum<E>
		{
			if (Log.IsTraceEnabled())
			{
				Log.Trace("HTTP " + op.GetValue().GetType() + ": " + op + ", " + path + ", ugi=" 
					+ ugi + ", " + username + ", " + doAsUser + Param.ToSortedString(", ", parameters
					));
			}
			//clear content type
			response.SetContentType(null);
			// set the remote address, if coming in via a trust proxy server then
			// the address with be that of the proxied client
			RemoteAddress.Set(JspHelper.GetRemoteAddr(request));
		}

		private void Reset()
		{
			RemoteAddress.Set(null);
		}

		/// <exception cref="System.IO.IOException"/>
		private static NamenodeProtocols GetRPCServer(NameNode namenode)
		{
			NamenodeProtocols np = namenode.GetRpcServer();
			if (np == null)
			{
				throw new RetriableException("Namenode is in startup mode");
			}
			return np;
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal static DatanodeInfo ChooseDatanode(NameNode namenode, string path, HttpOpParam.OP
			 op, long openOffset, long blocksize, string excludeDatanodes)
		{
			BlockManager bm = namenode.GetNamesystem().GetBlockManager();
			HashSet<Node> excludes = new HashSet<Node>();
			if (excludeDatanodes != null)
			{
				foreach (string host in StringUtils.GetTrimmedStringCollection(excludeDatanodes))
				{
					int idx = host.IndexOf(":");
					if (idx != -1)
					{
						excludes.AddItem(bm.GetDatanodeManager().GetDatanodeByXferAddr(Sharpen.Runtime.Substring
							(host, 0, idx), System.Convert.ToInt32(Sharpen.Runtime.Substring(host, idx + 1))
							));
					}
					else
					{
						excludes.AddItem(bm.GetDatanodeManager().GetDatanodeByHost(host));
					}
				}
			}
			if (op == PutOpParam.OP.Create)
			{
				//choose a datanode near to client 
				DatanodeDescriptor clientNode = bm.GetDatanodeManager().GetDatanodeByHost(GetRemoteAddress
					());
				if (clientNode != null)
				{
					DatanodeStorageInfo[] storages = bm.ChooseTarget4WebHDFS(path, clientNode, excludes
						, blocksize);
					if (storages.Length > 0)
					{
						return storages[0].GetDatanodeDescriptor();
					}
				}
			}
			else
			{
				if (op == GetOpParam.OP.Open || op == GetOpParam.OP.Getfilechecksum || op == PostOpParam.OP
					.Append)
				{
					//choose a datanode containing a replica 
					NamenodeProtocols np = GetRPCServer(namenode);
					HdfsFileStatus status = np.GetFileInfo(path);
					if (status == null)
					{
						throw new FileNotFoundException("File " + path + " not found.");
					}
					long len = status.GetLen();
					if (op == GetOpParam.OP.Open)
					{
						if (openOffset < 0L || (openOffset >= len && len > 0))
						{
							throw new IOException("Offset=" + openOffset + " out of the range [0, " + len + "); "
								 + op + ", path=" + path);
						}
					}
					if (len > 0)
					{
						long offset = op == GetOpParam.OP.Open ? openOffset : len - 1;
						LocatedBlocks locations = np.GetBlockLocations(path, offset, 1);
						int count = locations.LocatedBlockCount();
						if (count > 0)
						{
							return BestNode(locations.Get(0).GetLocations(), excludes);
						}
					}
				}
			}
			return (DatanodeDescriptor)bm.GetDatanodeManager().GetNetworkTopology().ChooseRandom
				(NodeBase.Root);
		}

		/// <summary>Choose the datanode to redirect the request.</summary>
		/// <remarks>
		/// Choose the datanode to redirect the request. Note that the nodes have been
		/// sorted based on availability and network distances, thus it is sufficient
		/// to return the first element of the node here.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static DatanodeInfo BestNode(DatanodeInfo[] nodes, HashSet<Node> excludes
			)
		{
			foreach (DatanodeInfo dn in nodes)
			{
				if (false == dn.IsDecommissioned() && false == excludes.Contains(dn))
				{
					return dn;
				}
			}
			throw new IOException("No active nodes contain this block");
		}

		/// <exception cref="System.IO.IOException"/>
		private Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> GenerateDelegationToken
			(NameNode namenode, UserGroupInformation ugi, string renewer)
		{
			Credentials c = DelegationTokenSecretManager.CreateCredentials(namenode, ugi, renewer
				 != null ? renewer : ugi.GetShortUserName());
			if (c == null)
			{
				return null;
			}
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> t = c.GetAllTokens().GetEnumerator
				().Next();
			Text kind = request.GetScheme().Equals("http") ? WebHdfsFileSystem.TokenKind : SWebHdfsFileSystem
				.TokenKind;
			t.SetKind(kind);
			return t;
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.IO.IOException"/>
		private URI RedirectURI(NameNode namenode, UserGroupInformation ugi, DelegationParam
			 delegation, UserParam username, DoAsParam doAsUser, string path, HttpOpParam.OP
			 op, long openOffset, long blocksize, string excludeDatanodes, params Param<object
			, object>[] parameters)
		{
			DatanodeInfo dn;
			try
			{
				dn = ChooseDatanode(namenode, path, op, openOffset, blocksize, excludeDatanodes);
			}
			catch (NetworkTopology.InvalidTopologyException ite)
			{
				throw new IOException("Failed to find datanode, suggest to check cluster health."
					, ite);
			}
			string delegationQuery;
			if (!UserGroupInformation.IsSecurityEnabled())
			{
				//security disabled
				delegationQuery = Param.ToSortedString("&", doAsUser, username);
			}
			else
			{
				if (delegation.GetValue() != null)
				{
					//client has provided a token
					delegationQuery = "&" + delegation;
				}
				else
				{
					//generate a token
					Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> t = GenerateDelegationToken
						(namenode, ugi, request.GetUserPrincipal().GetName());
					delegationQuery = "&" + new DelegationParam(t.EncodeToUrlString());
				}
			}
			string query = op.ToQueryString() + delegationQuery + "&" + new NamenodeAddressParam
				(namenode) + Param.ToSortedString("&", parameters);
			string uripath = WebHdfsFileSystem.PathPrefix + path;
			string scheme = request.GetScheme();
			int port = "http".Equals(scheme) ? dn.GetInfoPort() : dn.GetInfoSecurePort();
			URI uri = new URI(scheme, null, dn.GetHostName(), port, uripath, query, null);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("redirectURI=" + uri);
			}
			return uri;
		}

		/// <summary>Handle HTTP PUT request for the root.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[PUT]
		public virtual Response PutRoot(UserGroupInformation ugi, DelegationParam delegation
			, UserParam username, DoAsParam doAsUser, PutOpParam op, DestinationParam destination
			, OwnerParam owner, GroupParam group, PermissionParam permission, OverwriteParam
			 overwrite, BufferSizeParam bufferSize, ReplicationParam replication, BlockSizeParam
			 blockSize, ModificationTimeParam modificationTime, AccessTimeParam accessTime, 
			RenameOptionSetParam renameOptions, CreateParentParam createParent, TokenArgumentParam
			 delegationTokenArgument, AclPermissionParam aclPermission, XAttrNameParam xattrName
			, XAttrValueParam xattrValue, XAttrSetFlagParam xattrSetFlag, SnapshotNameParam 
			snapshotName, OldSnapshotNameParam oldSnapshotName, ExcludeDatanodesParam excludeDatanodes
			)
		{
			return Put(ugi, delegation, username, doAsUser, Root, op, destination, owner, group
				, permission, overwrite, bufferSize, replication, blockSize, modificationTime, accessTime
				, renameOptions, createParent, delegationTokenArgument, aclPermission, xattrName
				, xattrValue, xattrSetFlag, snapshotName, oldSnapshotName, excludeDatanodes);
		}

		/// <summary>Handle HTTP PUT request.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[PUT]
		public virtual Response Put(UserGroupInformation ugi, DelegationParam delegation, 
			UserParam username, DoAsParam doAsUser, UriFsPathParam path, PutOpParam op, DestinationParam
			 destination, OwnerParam owner, GroupParam group, PermissionParam permission, OverwriteParam
			 overwrite, BufferSizeParam bufferSize, ReplicationParam replication, BlockSizeParam
			 blockSize, ModificationTimeParam modificationTime, AccessTimeParam accessTime, 
			RenameOptionSetParam renameOptions, CreateParentParam createParent, TokenArgumentParam
			 delegationTokenArgument, AclPermissionParam aclPermission, XAttrNameParam xattrName
			, XAttrValueParam xattrValue, XAttrSetFlagParam xattrSetFlag, SnapshotNameParam 
			snapshotName, OldSnapshotNameParam oldSnapshotName, ExcludeDatanodesParam excludeDatanodes
			)
		{
			Init(ugi, delegation, username, doAsUser, path, op, destination, owner, group, permission
				, overwrite, bufferSize, replication, blockSize, modificationTime, accessTime, renameOptions
				, delegationTokenArgument, aclPermission, xattrName, xattrValue, xattrSetFlag, snapshotName
				, oldSnapshotName, excludeDatanodes);
			return ugi.DoAs(new _PrivilegedExceptionAction_426(this, ugi, delegation, username
				, doAsUser, path, op, destination, owner, group, permission, overwrite, bufferSize
				, replication, blockSize, modificationTime, accessTime, renameOptions, createParent
				, delegationTokenArgument, aclPermission, xattrName, xattrValue, xattrSetFlag, snapshotName
				, oldSnapshotName, excludeDatanodes));
		}

		private sealed class _PrivilegedExceptionAction_426 : PrivilegedExceptionAction<Response
			>
		{
			public _PrivilegedExceptionAction_426(NamenodeWebHdfsMethods _enclosing, UserGroupInformation
				 ugi, DelegationParam delegation, UserParam username, DoAsParam doAsUser, UriFsPathParam
				 path, PutOpParam op, DestinationParam destination, OwnerParam owner, GroupParam
				 group, PermissionParam permission, OverwriteParam overwrite, BufferSizeParam bufferSize
				, ReplicationParam replication, BlockSizeParam blockSize, ModificationTimeParam 
				modificationTime, AccessTimeParam accessTime, RenameOptionSetParam renameOptions
				, CreateParentParam createParent, TokenArgumentParam delegationTokenArgument, AclPermissionParam
				 aclPermission, XAttrNameParam xattrName, XAttrValueParam xattrValue, XAttrSetFlagParam
				 xattrSetFlag, SnapshotNameParam snapshotName, OldSnapshotNameParam oldSnapshotName
				, ExcludeDatanodesParam excludeDatanodes)
			{
				this._enclosing = _enclosing;
				this.ugi = ugi;
				this.delegation = delegation;
				this.username = username;
				this.doAsUser = doAsUser;
				this.path = path;
				this.op = op;
				this.destination = destination;
				this.owner = owner;
				this.group = group;
				this.permission = permission;
				this.overwrite = overwrite;
				this.bufferSize = bufferSize;
				this.replication = replication;
				this.blockSize = blockSize;
				this.modificationTime = modificationTime;
				this.accessTime = accessTime;
				this.renameOptions = renameOptions;
				this.createParent = createParent;
				this.delegationTokenArgument = delegationTokenArgument;
				this.aclPermission = aclPermission;
				this.xattrName = xattrName;
				this.xattrValue = xattrValue;
				this.xattrSetFlag = xattrSetFlag;
				this.snapshotName = snapshotName;
				this.oldSnapshotName = oldSnapshotName;
				this.excludeDatanodes = excludeDatanodes;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.URISyntaxException"/>
			public Response Run()
			{
				try
				{
					return this._enclosing.Put(ugi, delegation, username, doAsUser, path.GetAbsolutePath
						(), op, destination, owner, group, permission, overwrite, bufferSize, replication
						, blockSize, modificationTime, accessTime, renameOptions, createParent, delegationTokenArgument
						, aclPermission, xattrName, xattrValue, xattrSetFlag, snapshotName, oldSnapshotName
						, excludeDatanodes);
				}
				finally
				{
					this._enclosing.Reset();
				}
			}

			private readonly NamenodeWebHdfsMethods _enclosing;

			private readonly UserGroupInformation ugi;

			private readonly DelegationParam delegation;

			private readonly UserParam username;

			private readonly DoAsParam doAsUser;

			private readonly UriFsPathParam path;

			private readonly PutOpParam op;

			private readonly DestinationParam destination;

			private readonly OwnerParam owner;

			private readonly GroupParam group;

			private readonly PermissionParam permission;

			private readonly OverwriteParam overwrite;

			private readonly BufferSizeParam bufferSize;

			private readonly ReplicationParam replication;

			private readonly BlockSizeParam blockSize;

			private readonly ModificationTimeParam modificationTime;

			private readonly AccessTimeParam accessTime;

			private readonly RenameOptionSetParam renameOptions;

			private readonly CreateParentParam createParent;

			private readonly TokenArgumentParam delegationTokenArgument;

			private readonly AclPermissionParam aclPermission;

			private readonly XAttrNameParam xattrName;

			private readonly XAttrValueParam xattrValue;

			private readonly XAttrSetFlagParam xattrSetFlag;

			private readonly SnapshotNameParam snapshotName;

			private readonly OldSnapshotNameParam oldSnapshotName;

			private readonly ExcludeDatanodesParam excludeDatanodes;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		private Response Put(UserGroupInformation ugi, DelegationParam delegation, UserParam
			 username, DoAsParam doAsUser, string fullpath, PutOpParam op, DestinationParam 
			destination, OwnerParam owner, GroupParam group, PermissionParam permission, OverwriteParam
			 overwrite, BufferSizeParam bufferSize, ReplicationParam replication, BlockSizeParam
			 blockSize, ModificationTimeParam modificationTime, AccessTimeParam accessTime, 
			RenameOptionSetParam renameOptions, CreateParentParam createParent, TokenArgumentParam
			 delegationTokenArgument, AclPermissionParam aclPermission, XAttrNameParam xattrName
			, XAttrValueParam xattrValue, XAttrSetFlagParam xattrSetFlag, SnapshotNameParam 
			snapshotName, OldSnapshotNameParam oldSnapshotName, ExcludeDatanodesParam exclDatanodes
			)
		{
			Configuration conf = (Configuration)context.GetAttribute(JspHelper.CurrentConf);
			NameNode namenode = (NameNode)context.GetAttribute("name.node");
			NamenodeProtocols np = GetRPCServer(namenode);
			switch (op.GetValue())
			{
				case PutOpParam.OP.Create:
				{
					URI uri = RedirectURI(namenode, ugi, delegation, username, doAsUser, fullpath, op
						.GetValue(), -1L, blockSize.GetValue(conf), exclDatanodes.GetValue(), permission
						, overwrite, bufferSize, replication, blockSize);
					return Response.TemporaryRedirect(uri).Type(MediaType.ApplicationOctetStream).Build
						();
				}

				case PutOpParam.OP.Mkdirs:
				{
					bool b = np.Mkdirs(fullpath, permission.GetFsPermission(), true);
					string js = JsonUtil.ToJsonString("boolean", b);
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case PutOpParam.OP.Createsymlink:
				{
					np.CreateSymlink(destination.GetValue(), fullpath, PermissionParam.GetDefaultFsPermission
						(), createParent.GetValue());
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				case PutOpParam.OP.Rename:
				{
					EnumSet<Options.Rename> s = renameOptions.GetValue();
					if (s.IsEmpty())
					{
						bool b = np.Rename(fullpath, destination.GetValue());
						string js = JsonUtil.ToJsonString("boolean", b);
						return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
					}
					else
					{
						np.Rename2(fullpath, destination.GetValue(), Sharpen.Collections.ToArray(s, new Options.Rename
							[s.Count]));
						return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
					}
					goto case PutOpParam.OP.Setreplication;
				}

				case PutOpParam.OP.Setreplication:
				{
					bool b = np.SetReplication(fullpath, replication.GetValue(conf));
					string js = JsonUtil.ToJsonString("boolean", b);
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case PutOpParam.OP.Setowner:
				{
					if (owner.GetValue() == null && group.GetValue() == null)
					{
						throw new ArgumentException("Both owner and group are empty.");
					}
					np.SetOwner(fullpath, owner.GetValue(), group.GetValue());
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				case PutOpParam.OP.Setpermission:
				{
					np.SetPermission(fullpath, permission.GetFsPermission());
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				case PutOpParam.OP.Settimes:
				{
					np.SetTimes(fullpath, modificationTime.GetValue(), accessTime.GetValue());
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				case PutOpParam.OP.Renewdelegationtoken:
				{
					Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
						<DelegationTokenIdentifier>();
					token.DecodeFromUrlString(delegationTokenArgument.GetValue());
					long expiryTime = np.RenewDelegationToken(token);
					string js = JsonUtil.ToJsonString("long", expiryTime);
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case PutOpParam.OP.Canceldelegationtoken:
				{
					Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
						<DelegationTokenIdentifier>();
					token.DecodeFromUrlString(delegationTokenArgument.GetValue());
					np.CancelDelegationToken(token);
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				case PutOpParam.OP.Modifyaclentries:
				{
					np.ModifyAclEntries(fullpath, aclPermission.GetAclPermission(true));
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				case PutOpParam.OP.Removeaclentries:
				{
					np.RemoveAclEntries(fullpath, aclPermission.GetAclPermission(false));
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				case PutOpParam.OP.Removedefaultacl:
				{
					np.RemoveDefaultAcl(fullpath);
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				case PutOpParam.OP.Removeacl:
				{
					np.RemoveAcl(fullpath);
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				case PutOpParam.OP.Setacl:
				{
					np.SetAcl(fullpath, aclPermission.GetAclPermission(true));
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				case PutOpParam.OP.Setxattr:
				{
					np.SetXAttr(fullpath, XAttrHelper.BuildXAttr(xattrName.GetXAttrName(), xattrValue
						.GetXAttrValue()), xattrSetFlag.GetFlag());
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				case PutOpParam.OP.Removexattr:
				{
					np.RemoveXAttr(fullpath, XAttrHelper.BuildXAttr(xattrName.GetXAttrName()));
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				case PutOpParam.OP.Createsnapshot:
				{
					string snapshotPath = np.CreateSnapshot(fullpath, snapshotName.GetValue());
					string js = JsonUtil.ToJsonString(typeof(Path).Name, snapshotPath);
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case PutOpParam.OP.Renamesnapshot:
				{
					np.RenameSnapshot(fullpath, oldSnapshotName.GetValue(), snapshotName.GetValue());
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				default:
				{
					throw new NotSupportedException(op + " is not supported");
				}
			}
		}

		/// <summary>Handle HTTP POST request for the root.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[POST]
		public virtual Response PostRoot(UserGroupInformation ugi, DelegationParam delegation
			, UserParam username, DoAsParam doAsUser, PostOpParam op, ConcatSourcesParam concatSrcs
			, BufferSizeParam bufferSize, ExcludeDatanodesParam excludeDatanodes, NewLengthParam
			 newLength)
		{
			return Post(ugi, delegation, username, doAsUser, Root, op, concatSrcs, bufferSize
				, excludeDatanodes, newLength);
		}

		/// <summary>Handle HTTP POST request.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[POST]
		public virtual Response Post(UserGroupInformation ugi, DelegationParam delegation
			, UserParam username, DoAsParam doAsUser, UriFsPathParam path, PostOpParam op, ConcatSourcesParam
			 concatSrcs, BufferSizeParam bufferSize, ExcludeDatanodesParam excludeDatanodes, 
			NewLengthParam newLength)
		{
			Init(ugi, delegation, username, doAsUser, path, op, concatSrcs, bufferSize, excludeDatanodes
				, newLength);
			return ugi.DoAs(new _PrivilegedExceptionAction_654(this, ugi, delegation, username
				, doAsUser, path, op, concatSrcs, bufferSize, excludeDatanodes, newLength));
		}

		private sealed class _PrivilegedExceptionAction_654 : PrivilegedExceptionAction<Response
			>
		{
			public _PrivilegedExceptionAction_654(NamenodeWebHdfsMethods _enclosing, UserGroupInformation
				 ugi, DelegationParam delegation, UserParam username, DoAsParam doAsUser, UriFsPathParam
				 path, PostOpParam op, ConcatSourcesParam concatSrcs, BufferSizeParam bufferSize
				, ExcludeDatanodesParam excludeDatanodes, NewLengthParam newLength)
			{
				this._enclosing = _enclosing;
				this.ugi = ugi;
				this.delegation = delegation;
				this.username = username;
				this.doAsUser = doAsUser;
				this.path = path;
				this.op = op;
				this.concatSrcs = concatSrcs;
				this.bufferSize = bufferSize;
				this.excludeDatanodes = excludeDatanodes;
				this.newLength = newLength;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.URISyntaxException"/>
			public Response Run()
			{
				try
				{
					return this._enclosing.Post(ugi, delegation, username, doAsUser, path.GetAbsolutePath
						(), op, concatSrcs, bufferSize, excludeDatanodes, newLength);
				}
				finally
				{
					this._enclosing.Reset();
				}
			}

			private readonly NamenodeWebHdfsMethods _enclosing;

			private readonly UserGroupInformation ugi;

			private readonly DelegationParam delegation;

			private readonly UserParam username;

			private readonly DoAsParam doAsUser;

			private readonly UriFsPathParam path;

			private readonly PostOpParam op;

			private readonly ConcatSourcesParam concatSrcs;

			private readonly BufferSizeParam bufferSize;

			private readonly ExcludeDatanodesParam excludeDatanodes;

			private readonly NewLengthParam newLength;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		private Response Post(UserGroupInformation ugi, DelegationParam delegation, UserParam
			 username, DoAsParam doAsUser, string fullpath, PostOpParam op, ConcatSourcesParam
			 concatSrcs, BufferSizeParam bufferSize, ExcludeDatanodesParam excludeDatanodes, 
			NewLengthParam newLength)
		{
			NameNode namenode = (NameNode)context.GetAttribute("name.node");
			NamenodeProtocols np = GetRPCServer(namenode);
			switch (op.GetValue())
			{
				case PostOpParam.OP.Append:
				{
					URI uri = RedirectURI(namenode, ugi, delegation, username, doAsUser, fullpath, op
						.GetValue(), -1L, -1L, excludeDatanodes.GetValue(), bufferSize);
					return Response.TemporaryRedirect(uri).Type(MediaType.ApplicationOctetStream).Build
						();
				}

				case PostOpParam.OP.Concat:
				{
					np.Concat(fullpath, concatSrcs.GetAbsolutePaths());
					return Response.Ok().Build();
				}

				case PostOpParam.OP.Truncate:
				{
					// We treat each rest request as a separate client.
					bool b = np.Truncate(fullpath, newLength.GetValue(), "DFSClient_" + DFSUtil.GetSecureRandom
						().NextLong());
					string js = JsonUtil.ToJsonString("boolean", b);
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				default:
				{
					throw new NotSupportedException(op + " is not supported");
				}
			}
		}

		/// <summary>Handle HTTP GET request for the root.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[GET]
		public virtual Response GetRoot(UserGroupInformation ugi, DelegationParam delegation
			, UserParam username, DoAsParam doAsUser, GetOpParam op, OffsetParam offset, LengthParam
			 length, RenewerParam renewer, BufferSizeParam bufferSize, IList<XAttrNameParam>
			 xattrNames, XAttrEncodingParam xattrEncoding, ExcludeDatanodesParam excludeDatanodes
			, FsActionParam fsAction, TokenKindParam tokenKind, TokenServiceParam tokenService
			)
		{
			return Get(ugi, delegation, username, doAsUser, Root, op, offset, length, renewer
				, bufferSize, xattrNames, xattrEncoding, excludeDatanodes, fsAction, tokenKind, 
				tokenService);
		}

		/// <summary>Handle HTTP GET request.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[GET]
		public virtual Response Get(UserGroupInformation ugi, DelegationParam delegation, 
			UserParam username, DoAsParam doAsUser, UriFsPathParam path, GetOpParam op, OffsetParam
			 offset, LengthParam length, RenewerParam renewer, BufferSizeParam bufferSize, IList
			<XAttrNameParam> xattrNames, XAttrEncodingParam xattrEncoding, ExcludeDatanodesParam
			 excludeDatanodes, FsActionParam fsAction, TokenKindParam tokenKind, TokenServiceParam
			 tokenService)
		{
			Init(ugi, delegation, username, doAsUser, path, op, offset, length, renewer, bufferSize
				, xattrEncoding, excludeDatanodes, fsAction, tokenKind, tokenService);
			return ugi.DoAs(new _PrivilegedExceptionAction_790(this, ugi, delegation, username
				, doAsUser, path, op, offset, length, renewer, bufferSize, xattrNames, xattrEncoding
				, excludeDatanodes, fsAction, tokenKind, tokenService));
		}

		private sealed class _PrivilegedExceptionAction_790 : PrivilegedExceptionAction<Response
			>
		{
			public _PrivilegedExceptionAction_790(NamenodeWebHdfsMethods _enclosing, UserGroupInformation
				 ugi, DelegationParam delegation, UserParam username, DoAsParam doAsUser, UriFsPathParam
				 path, GetOpParam op, OffsetParam offset, LengthParam length, RenewerParam renewer
				, BufferSizeParam bufferSize, IList<XAttrNameParam> xattrNames, XAttrEncodingParam
				 xattrEncoding, ExcludeDatanodesParam excludeDatanodes, FsActionParam fsAction, 
				TokenKindParam tokenKind, TokenServiceParam tokenService)
			{
				this._enclosing = _enclosing;
				this.ugi = ugi;
				this.delegation = delegation;
				this.username = username;
				this.doAsUser = doAsUser;
				this.path = path;
				this.op = op;
				this.offset = offset;
				this.length = length;
				this.renewer = renewer;
				this.bufferSize = bufferSize;
				this.xattrNames = xattrNames;
				this.xattrEncoding = xattrEncoding;
				this.excludeDatanodes = excludeDatanodes;
				this.fsAction = fsAction;
				this.tokenKind = tokenKind;
				this.tokenService = tokenService;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.URISyntaxException"/>
			public Response Run()
			{
				try
				{
					return this._enclosing.Get(ugi, delegation, username, doAsUser, path.GetAbsolutePath
						(), op, offset, length, renewer, bufferSize, xattrNames, xattrEncoding, excludeDatanodes
						, fsAction, tokenKind, tokenService);
				}
				finally
				{
					this._enclosing.Reset();
				}
			}

			private readonly NamenodeWebHdfsMethods _enclosing;

			private readonly UserGroupInformation ugi;

			private readonly DelegationParam delegation;

			private readonly UserParam username;

			private readonly DoAsParam doAsUser;

			private readonly UriFsPathParam path;

			private readonly GetOpParam op;

			private readonly OffsetParam offset;

			private readonly LengthParam length;

			private readonly RenewerParam renewer;

			private readonly BufferSizeParam bufferSize;

			private readonly IList<XAttrNameParam> xattrNames;

			private readonly XAttrEncodingParam xattrEncoding;

			private readonly ExcludeDatanodesParam excludeDatanodes;

			private readonly FsActionParam fsAction;

			private readonly TokenKindParam tokenKind;

			private readonly TokenServiceParam tokenService;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		private Response Get(UserGroupInformation ugi, DelegationParam delegation, UserParam
			 username, DoAsParam doAsUser, string fullpath, GetOpParam op, OffsetParam offset
			, LengthParam length, RenewerParam renewer, BufferSizeParam bufferSize, IList<XAttrNameParam
			> xattrNames, XAttrEncodingParam xattrEncoding, ExcludeDatanodesParam excludeDatanodes
			, FsActionParam fsAction, TokenKindParam tokenKind, TokenServiceParam tokenService
			)
		{
			NameNode namenode = (NameNode)context.GetAttribute("name.node");
			NamenodeProtocols np = GetRPCServer(namenode);
			switch (op.GetValue())
			{
				case GetOpParam.OP.Open:
				{
					URI uri = RedirectURI(namenode, ugi, delegation, username, doAsUser, fullpath, op
						.GetValue(), offset.GetValue(), -1L, excludeDatanodes.GetValue(), offset, length
						, bufferSize);
					return Response.TemporaryRedirect(uri).Type(MediaType.ApplicationOctetStream).Build
						();
				}

				case GetOpParam.OP.GetBlockLocations:
				{
					long offsetValue = offset.GetValue();
					long lengthValue = length.GetValue();
					LocatedBlocks locatedblocks = np.GetBlockLocations(fullpath, offsetValue, lengthValue
						 != null ? lengthValue : long.MaxValue);
					string js = JsonUtil.ToJsonString(locatedblocks);
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case GetOpParam.OP.Getfilestatus:
				{
					HdfsFileStatus status = np.GetFileInfo(fullpath);
					if (status == null)
					{
						throw new FileNotFoundException("File does not exist: " + fullpath);
					}
					string js = JsonUtil.ToJsonString(status, true);
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case GetOpParam.OP.Liststatus:
				{
					StreamingOutput streaming = GetListingStream(np, fullpath);
					return Response.Ok(streaming).Type(MediaType.ApplicationJson).Build();
				}

				case GetOpParam.OP.Getcontentsummary:
				{
					ContentSummary contentsummary = np.GetContentSummary(fullpath);
					string js = JsonUtil.ToJsonString(contentsummary);
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case GetOpParam.OP.Getfilechecksum:
				{
					URI uri = RedirectURI(namenode, ugi, delegation, username, doAsUser, fullpath, op
						.GetValue(), -1L, -1L, null);
					return Response.TemporaryRedirect(uri).Type(MediaType.ApplicationOctetStream).Build
						();
				}

				case GetOpParam.OP.Getdelegationtoken:
				{
					if (delegation.GetValue() != null)
					{
						throw new ArgumentException(delegation.GetName() + " parameter is not null.");
					}
					Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token = GenerateDelegationToken
						(namenode, ugi, renewer.GetValue());
					string setServiceName = tokenService.GetValue();
					string setKind = tokenKind.GetValue();
					if (setServiceName != null)
					{
						token.SetService(new Text(setServiceName));
					}
					if (setKind != null)
					{
						token.SetKind(new Text(setKind));
					}
					string js = JsonUtil.ToJsonString(token);
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case GetOpParam.OP.Gethomedirectory:
				{
					string js = JsonUtil.ToJsonString(typeof(Path).Name, WebHdfsFileSystem.GetHomeDirectoryString
						(ugi));
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case GetOpParam.OP.Getaclstatus:
				{
					AclStatus status = np.GetAclStatus(fullpath);
					if (status == null)
					{
						throw new FileNotFoundException("File does not exist: " + fullpath);
					}
					string js = JsonUtil.ToJsonString(status);
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case GetOpParam.OP.Getxattrs:
				{
					IList<string> names = null;
					if (xattrNames != null)
					{
						names = Lists.NewArrayListWithCapacity(xattrNames.Count);
						foreach (XAttrNameParam xattrName in xattrNames)
						{
							if (xattrName.GetXAttrName() != null)
							{
								names.AddItem(xattrName.GetXAttrName());
							}
						}
					}
					IList<XAttr> xAttrs = np.GetXAttrs(fullpath, (names != null && !names.IsEmpty()) ? 
						XAttrHelper.BuildXAttrs(names) : null);
					string js = JsonUtil.ToJsonString(xAttrs, xattrEncoding.GetEncoding());
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case GetOpParam.OP.Listxattrs:
				{
					IList<XAttr> xAttrs = np.ListXAttrs(fullpath);
					string js = JsonUtil.ToJsonString(xAttrs);
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case GetOpParam.OP.Checkaccess:
				{
					np.CheckAccess(fullpath, FsAction.GetFsAction(fsAction.GetValue()));
					return Response.Ok().Build();
				}

				default:
				{
					throw new NotSupportedException(op + " is not supported");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static DirectoryListing GetDirectoryListing(NamenodeProtocols np, string 
			p, byte[] startAfter)
		{
			DirectoryListing listing = np.GetListing(p, startAfter, false);
			if (listing == null)
			{
				// the directory does not exist
				throw new FileNotFoundException("File " + p + " does not exist.");
			}
			return listing;
		}

		/// <exception cref="System.IO.IOException"/>
		private static StreamingOutput GetListingStream(NamenodeProtocols np, string p)
		{
			// allows exceptions like FNF or ACE to prevent http response of 200 for
			// a failure since we can't (currently) return error responses in the
			// middle of a streaming operation
			DirectoryListing firstDirList = GetDirectoryListing(np, p, HdfsFileStatus.EmptyName
				);
			// must save ugi because the streaming object will be executed outside
			// the remote user's ugi
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			return new _StreamingOutput_956(ugi, firstDirList, np, p);
		}

		private sealed class _StreamingOutput_956 : StreamingOutput
		{
			public _StreamingOutput_956(UserGroupInformation ugi, DirectoryListing firstDirList
				, NamenodeProtocols np, string p)
			{
				this.ugi = ugi;
				this.firstDirList = firstDirList;
				this.np = np;
				this.p = p;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Write(OutputStream outstream)
			{
				PrintWriter @out = new PrintWriter(new OutputStreamWriter(outstream, Charsets.Utf8
					));
				@out.WriteLine("{\"" + typeof(FileStatus).Name + "es\":{\"" + typeof(FileStatus).
					Name + "\":[");
				try
				{
					// restore remote user's ugi
					ugi.DoAs(new _PrivilegedExceptionAction_966(firstDirList, np, p, @out));
				}
				catch (Exception e)
				{
					// send each segment of the directory listing
					// stop if last segment
					throw new IOException(e);
				}
				@out.WriteLine();
				@out.WriteLine("]}}");
				@out.Flush();
			}

			private sealed class _PrivilegedExceptionAction_966 : PrivilegedExceptionAction<Void
				>
			{
				public _PrivilegedExceptionAction_966(DirectoryListing firstDirList, NamenodeProtocols
					 np, string p, PrintWriter @out)
				{
					this.firstDirList = firstDirList;
					this.np = np;
					this.p = p;
					this.@out = @out;
				}

				/// <exception cref="System.IO.IOException"/>
				public Void Run()
				{
					long n = 0;
					for (DirectoryListing dirList = firstDirList; ; dirList = NamenodeWebHdfsMethods.
						GetDirectoryListing(np, p, dirList.GetLastName()))
					{
						foreach (HdfsFileStatus s in dirList.GetPartialListing())
						{
							if (n++ > 0)
							{
								@out.WriteLine(',');
							}
							@out.Write(JsonUtil.ToJsonString(s, false));
						}
						if (!dirList.HasMore())
						{
							break;
						}
					}
					return null;
				}

				private readonly DirectoryListing firstDirList;

				private readonly NamenodeProtocols np;

				private readonly string p;

				private readonly PrintWriter @out;
			}

			private readonly UserGroupInformation ugi;

			private readonly DirectoryListing firstDirList;

			private readonly NamenodeProtocols np;

			private readonly string p;
		}

		/// <summary>Handle HTTP DELETE request for the root.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[DELETE]
		public virtual Response DeleteRoot(UserGroupInformation ugi, DelegationParam delegation
			, UserParam username, DoAsParam doAsUser, DeleteOpParam op, RecursiveParam recursive
			, SnapshotNameParam snapshotName)
		{
			return Delete(ugi, delegation, username, doAsUser, Root, op, recursive, snapshotName
				);
		}

		/// <summary>Handle HTTP DELETE request.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[DELETE]
		public virtual Response Delete(UserGroupInformation ugi, DelegationParam delegation
			, UserParam username, DoAsParam doAsUser, UriFsPathParam path, DeleteOpParam op, 
			RecursiveParam recursive, SnapshotNameParam snapshotName)
		{
			Init(ugi, delegation, username, doAsUser, path, op, recursive, snapshotName);
			return ugi.DoAs(new _PrivilegedExceptionAction_1045(this, ugi, delegation, username
				, doAsUser, path, op, recursive, snapshotName));
		}

		private sealed class _PrivilegedExceptionAction_1045 : PrivilegedExceptionAction<
			Response>
		{
			public _PrivilegedExceptionAction_1045(NamenodeWebHdfsMethods _enclosing, UserGroupInformation
				 ugi, DelegationParam delegation, UserParam username, DoAsParam doAsUser, UriFsPathParam
				 path, DeleteOpParam op, RecursiveParam recursive, SnapshotNameParam snapshotName
				)
			{
				this._enclosing = _enclosing;
				this.ugi = ugi;
				this.delegation = delegation;
				this.username = username;
				this.doAsUser = doAsUser;
				this.path = path;
				this.op = op;
				this.recursive = recursive;
				this.snapshotName = snapshotName;
			}

			/// <exception cref="System.IO.IOException"/>
			public Response Run()
			{
				try
				{
					return this._enclosing.Delete(ugi, delegation, username, doAsUser, path.GetAbsolutePath
						(), op, recursive, snapshotName);
				}
				finally
				{
					this._enclosing.Reset();
				}
			}

			private readonly NamenodeWebHdfsMethods _enclosing;

			private readonly UserGroupInformation ugi;

			private readonly DelegationParam delegation;

			private readonly UserParam username;

			private readonly DoAsParam doAsUser;

			private readonly UriFsPathParam path;

			private readonly DeleteOpParam op;

			private readonly RecursiveParam recursive;

			private readonly SnapshotNameParam snapshotName;
		}

		/// <exception cref="System.IO.IOException"/>
		private Response Delete(UserGroupInformation ugi, DelegationParam delegation, UserParam
			 username, DoAsParam doAsUser, string fullpath, DeleteOpParam op, RecursiveParam
			 recursive, SnapshotNameParam snapshotName)
		{
			NameNode namenode = (NameNode)context.GetAttribute("name.node");
			NamenodeProtocols np = GetRPCServer(namenode);
			switch (op.GetValue())
			{
				case DeleteOpParam.OP.Delete:
				{
					bool b = np.Delete(fullpath, recursive.GetValue());
					string js = JsonUtil.ToJsonString("boolean", b);
					return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
				}

				case DeleteOpParam.OP.Deletesnapshot:
				{
					np.DeleteSnapshot(fullpath, snapshotName.GetValue());
					return Response.Ok().Type(MediaType.ApplicationOctetStream).Build();
				}

				default:
				{
					throw new NotSupportedException(op + " is not supported");
				}
			}
		}
	}
}
