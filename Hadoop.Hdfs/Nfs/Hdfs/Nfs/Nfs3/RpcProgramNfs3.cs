using System;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Nfs;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Nfs.Nfs3.Request;
using Org.Apache.Hadoop.Nfs.Nfs3.Response;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>RPC program corresponding to nfs daemon.</summary>
	/// <remarks>
	/// RPC program corresponding to nfs daemon. See
	/// <see cref="Nfs3"/>
	/// .
	/// </remarks>
	public class RpcProgramNfs3 : RpcProgram, Nfs3Interface
	{
		public const int DefaultUmask = 0x12;

		public static readonly FsPermission umask = new FsPermission((short)DefaultUmask);

		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.RpcProgramNfs3
			));

		private readonly NfsConfiguration config;

		private readonly WriteManager writeManager;

		private readonly IdMappingServiceProvider iug;

		private readonly DFSClientCache clientCache;

		private readonly NfsExports exports;

		private readonly short replication;

		private readonly long blockSize;

		private readonly int bufferSize;

		private readonly bool aixCompatMode;

		private string writeDumpDir;

		private readonly RpcCallCache rpcCallCache;

		private JvmPauseMonitor pauseMonitor;

		private Nfs3HttpServer infoServer = null;

		internal static Nfs3Metrics metrics;

		private string superuser;

		/// <exception cref="System.IO.IOException"/>
		public RpcProgramNfs3(NfsConfiguration config, DatagramSocket registrationSocket, 
			bool allowInsecurePorts)
			: base("NFS3", "localhost", config.GetInt(NfsConfigKeys.DfsNfsServerPortKey, NfsConfigKeys
				.DfsNfsServerPortDefault), Nfs3Constant.Program, Nfs3Constant.Version, Nfs3Constant
				.Version, registrationSocket, allowInsecurePorts)
		{
			// The dir save dump files
			this.config = config;
			config.Set(FsPermission.UmaskLabel, "000");
			iug = new ShellBasedIdMapping(config);
			aixCompatMode = config.GetBoolean(NfsConfigKeys.AixCompatModeKey, NfsConfigKeys.AixCompatModeDefault
				);
			exports = NfsExports.GetInstance(config);
			writeManager = new WriteManager(iug, config, aixCompatMode);
			clientCache = new DFSClientCache(config);
			replication = (short)config.GetInt(DFSConfigKeys.DfsReplicationKey, DFSConfigKeys
				.DfsReplicationDefault);
			blockSize = config.GetLongBytes(DFSConfigKeys.DfsBlockSizeKey, DFSConfigKeys.DfsBlockSizeDefault
				);
			bufferSize = config.GetInt(CommonConfigurationKeysPublic.IoFileBufferSizeKey, CommonConfigurationKeysPublic
				.IoFileBufferSizeDefault);
			writeDumpDir = config.Get(NfsConfigKeys.DfsNfsFileDumpDirKey, NfsConfigKeys.DfsNfsFileDumpDirDefault
				);
			bool enableDump = config.GetBoolean(NfsConfigKeys.DfsNfsFileDumpKey, NfsConfigKeys
				.DfsNfsFileDumpDefault);
			UserGroupInformation.SetConfiguration(config);
			SecurityUtil.Login(config, NfsConfigKeys.DfsNfsKeytabFileKey, NfsConfigKeys.DfsNfsKerberosPrincipalKey
				);
			superuser = config.Get(NfsConfigKeys.NfsSuperuserKey, NfsConfigKeys.NfsSuperuserDefault
				);
			Log.Info("Configured HDFS superuser is " + superuser);
			if (!enableDump)
			{
				writeDumpDir = null;
			}
			else
			{
				ClearDirectory(writeDumpDir);
			}
			rpcCallCache = new RpcCallCache("NFS3", 256);
			infoServer = new Nfs3HttpServer(config);
		}

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.RpcProgramNfs3 CreateRpcProgramNfs3
			(NfsConfiguration config, DatagramSocket registrationSocket, bool allowInsecurePorts
			)
		{
			DefaultMetricsSystem.Initialize("Nfs3");
			string displayName = DNS.GetDefaultHost("default", "default") + config.GetInt(NfsConfigKeys
				.DfsNfsServerPortKey, NfsConfigKeys.DfsNfsServerPortDefault);
			metrics = Nfs3Metrics.Create(config, displayName);
			return new Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.RpcProgramNfs3(config, registrationSocket
				, allowInsecurePorts);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ClearDirectory(string writeDumpDir)
		{
			FilePath dumpDir = new FilePath(writeDumpDir);
			if (dumpDir.Exists())
			{
				Log.Info("Delete current dump directory " + writeDumpDir);
				if (!(FileUtil.FullyDelete(dumpDir)))
				{
					throw new IOException("Cannot remove current dump directory: " + dumpDir);
				}
			}
			Log.Info("Create new dump directory " + writeDumpDir);
			if (!dumpDir.Mkdirs())
			{
				throw new IOException("Cannot create dump directory " + dumpDir);
			}
		}

		public override void StartDaemons()
		{
			if (pauseMonitor == null)
			{
				pauseMonitor = new JvmPauseMonitor(config);
				pauseMonitor.Start();
				metrics.GetJvmMetrics().SetPauseMonitor(pauseMonitor);
			}
			writeManager.StartAsyncDataService();
			try
			{
				infoServer.Start();
			}
			catch (IOException e)
			{
				Log.Error("failed to start web server", e);
			}
		}

		public override void StopDaemons()
		{
			if (writeManager != null)
			{
				writeManager.ShutdownAsyncDataService();
			}
			if (pauseMonitor != null)
			{
				pauseMonitor.Stop();
			}
			// Stop the web server
			if (infoServer != null)
			{
				try
				{
					infoServer.Stop();
				}
				catch (Exception e)
				{
					Log.Warn("Exception shutting down web server", e);
				}
			}
		}

		[VisibleForTesting]
		internal virtual Nfs3HttpServer GetInfoServer()
		{
			return this.infoServer;
		}

		// Checks the type of IOException and maps it to appropriate Nfs3Status code.
		private int MapErrorStatus(IOException e)
		{
			if (e is FileNotFoundException)
			{
				return Nfs3Status.Nfs3errStale;
			}
			else
			{
				if (e is AccessControlException)
				{
					return Nfs3Status.Nfs3errAcces;
				}
				else
				{
					return Nfs3Status.Nfs3errIo;
				}
			}
		}

		/// <summary>RPC call handlers</summary>
		public virtual NFS3Response NullProcedure()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS NULL");
			}
			return new NFS3Response(Nfs3Status.Nfs3Ok);
		}

		public virtual GETATTR3Response Getattr(XDR xdr, RpcInfo info)
		{
			return Getattr(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual GETATTR3Response Getattr(XDR xdr, SecurityHandler securityHandler
			, EndPoint remoteAddress)
		{
			GETATTR3Response response = new GETATTR3Response(Nfs3Status.Nfs3Ok);
			if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadOnly))
			{
				response.SetStatus(Nfs3Status.Nfs3errAcces);
				return response;
			}
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			GETATTR3Request request;
			try
			{
				request = GETATTR3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid GETATTR request");
				response.SetStatus(Nfs3Status.Nfs3errInval);
				return response;
			}
			FileHandle handle = request.GetHandle();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("GETATTR for fileId: " + handle.GetFileId() + " client: " + remoteAddress
					);
			}
			Nfs3FileAttributes attrs = null;
			try
			{
				attrs = writeManager.GetFileAttr(dfsClient, handle, iug);
			}
			catch (RemoteException r)
			{
				Log.Warn("Exception ", r);
				IOException io = r.UnwrapRemoteException();
				if (io is AuthorizationException)
				{
					return new GETATTR3Response(Nfs3Status.Nfs3errAcces);
				}
				else
				{
					return new GETATTR3Response(Nfs3Status.Nfs3errIo);
				}
			}
			catch (IOException e)
			{
				Log.Info("Can't get file attribute, fileId=" + handle.GetFileId(), e);
				int status = MapErrorStatus(e);
				response.SetStatus(status);
				return response;
			}
			if (attrs == null)
			{
				Log.Error("Can't get path for fileId: " + handle.GetFileId());
				response.SetStatus(Nfs3Status.Nfs3errStale);
				return response;
			}
			response.SetPostOpAttr(attrs);
			return response;
		}

		// Set attribute, don't support setting "size". For file/dir creation, mode is
		// set during creation and setMode should be false here.
		/// <exception cref="System.IO.IOException"/>
		private void SetattrInternal(DFSClient dfsClient, string fileIdPath, SetAttr3 newAttr
			, bool setMode)
		{
			EnumSet<SetAttr3.SetAttrField> updateFields = newAttr.GetUpdateFields();
			if (setMode && updateFields.Contains(SetAttr3.SetAttrField.Mode))
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("set new mode: " + newAttr.GetMode());
				}
				dfsClient.SetPermission(fileIdPath, new FsPermission((short)(newAttr.GetMode())));
			}
			if (updateFields.Contains(SetAttr3.SetAttrField.Uid) || updateFields.Contains(SetAttr3.SetAttrField
				.Gid))
			{
				string uname = updateFields.Contains(SetAttr3.SetAttrField.Uid) ? iug.GetUserName
					(newAttr.GetUid(), IdMappingConstant.UnknownUser) : null;
				string gname = updateFields.Contains(SetAttr3.SetAttrField.Gid) ? iug.GetGroupName
					(newAttr.GetGid(), IdMappingConstant.UnknownGroup) : null;
				dfsClient.SetOwner(fileIdPath, uname, gname);
			}
			long atime = updateFields.Contains(SetAttr3.SetAttrField.Atime) ? newAttr.GetAtime
				().GetMilliSeconds() : -1;
			long mtime = updateFields.Contains(SetAttr3.SetAttrField.Mtime) ? newAttr.GetMtime
				().GetMilliSeconds() : -1;
			if (atime != -1 || mtime != -1)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("set atime: " + +atime + " mtime: " + mtime);
				}
				dfsClient.SetTimes(fileIdPath, mtime, atime);
			}
		}

		public virtual SETATTR3Response Setattr(XDR xdr, RpcInfo info)
		{
			return Setattr(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual SETATTR3Response Setattr(XDR xdr, SecurityHandler securityHandler
			, EndPoint remoteAddress)
		{
			SETATTR3Response response = new SETATTR3Response(Nfs3Status.Nfs3Ok);
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			SETATTR3Request request;
			try
			{
				request = SETATTR3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid SETATTR request");
				response.SetStatus(Nfs3Status.Nfs3errInval);
				return response;
			}
			FileHandle handle = request.GetHandle();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS SETATTR fileId: " + handle.GetFileId() + " client: " + remoteAddress
					);
			}
			if (request.GetAttr().GetUpdateFields().Contains(SetAttr3.SetAttrField.Size))
			{
				Log.Error("Setting file size is not supported when setattr, fileId: " + handle.GetFileId
					());
				response.SetStatus(Nfs3Status.Nfs3errInval);
				return response;
			}
			string fileIdPath = Nfs3Utils.GetFileIdPath(handle);
			Nfs3FileAttributes preOpAttr = null;
			try
			{
				preOpAttr = Nfs3Utils.GetFileAttr(dfsClient, fileIdPath, iug);
				if (preOpAttr == null)
				{
					Log.Info("Can't get path for fileId: " + handle.GetFileId());
					response.SetStatus(Nfs3Status.Nfs3errStale);
					return response;
				}
				WccAttr preOpWcc = Nfs3Utils.GetWccAttr(preOpAttr);
				if (request.IsCheck())
				{
					if (!preOpAttr.GetCtime().Equals(request.GetCtime()))
					{
						WccData wccData = new WccData(preOpWcc, preOpAttr);
						return new SETATTR3Response(Nfs3Status.Nfs3errNotSync, wccData);
					}
				}
				// check the write access privilege
				if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadWrite))
				{
					return new SETATTR3Response(Nfs3Status.Nfs3errAcces, new WccData(preOpWcc, preOpAttr
						));
				}
				SetattrInternal(dfsClient, fileIdPath, request.GetAttr(), true);
				Nfs3FileAttributes postOpAttr = Nfs3Utils.GetFileAttr(dfsClient, fileIdPath, iug);
				WccData wccData_1 = new WccData(preOpWcc, postOpAttr);
				return new SETATTR3Response(Nfs3Status.Nfs3Ok, wccData_1);
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				WccData wccData = null;
				try
				{
					wccData = Nfs3Utils.CreateWccData(Nfs3Utils.GetWccAttr(preOpAttr), dfsClient, fileIdPath
						, iug);
				}
				catch (IOException e1)
				{
					Log.Info("Can't get postOpAttr for fileIdPath: " + fileIdPath, e1);
				}
				int status = MapErrorStatus(e);
				return new SETATTR3Response(status, wccData);
			}
		}

		public virtual LOOKUP3Response Lookup(XDR xdr, RpcInfo info)
		{
			return Lookup(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual LOOKUP3Response Lookup(XDR xdr, SecurityHandler securityHandler, 
			EndPoint remoteAddress)
		{
			LOOKUP3Response response = new LOOKUP3Response(Nfs3Status.Nfs3Ok);
			if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadOnly))
			{
				response.SetStatus(Nfs3Status.Nfs3errAcces);
				return response;
			}
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			LOOKUP3Request request;
			try
			{
				request = LOOKUP3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid LOOKUP request");
				return new LOOKUP3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle dirHandle = request.GetHandle();
			string fileName = request.GetName();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS LOOKUP dir fileId: " + dirHandle.GetFileId() + " name: " + fileName
					 + " client: " + remoteAddress);
			}
			try
			{
				string dirFileIdPath = Nfs3Utils.GetFileIdPath(dirHandle);
				Nfs3FileAttributes postOpObjAttr = writeManager.GetFileAttr(dfsClient, dirHandle, 
					fileName);
				if (postOpObjAttr == null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("NFS LOOKUP fileId: " + dirHandle.GetFileId() + " name: " + fileName + 
							" does not exist");
					}
					Nfs3FileAttributes postOpDirAttr = Nfs3Utils.GetFileAttr(dfsClient, dirFileIdPath
						, iug);
					return new LOOKUP3Response(Nfs3Status.Nfs3errNoent, null, null, postOpDirAttr);
				}
				Nfs3FileAttributes postOpDirAttr_1 = Nfs3Utils.GetFileAttr(dfsClient, dirFileIdPath
					, iug);
				if (postOpDirAttr_1 == null)
				{
					Log.Info("Can't get path for dir fileId: " + dirHandle.GetFileId());
					return new LOOKUP3Response(Nfs3Status.Nfs3errStale);
				}
				FileHandle fileHandle = new FileHandle(postOpObjAttr.GetFileId());
				return new LOOKUP3Response(Nfs3Status.Nfs3Ok, fileHandle, postOpObjAttr, postOpDirAttr_1
					);
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				int status = MapErrorStatus(e);
				return new LOOKUP3Response(status);
			}
		}

		public virtual ACCESS3Response Access(XDR xdr, RpcInfo info)
		{
			return Access(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual ACCESS3Response Access(XDR xdr, SecurityHandler securityHandler, 
			EndPoint remoteAddress)
		{
			ACCESS3Response response = new ACCESS3Response(Nfs3Status.Nfs3Ok);
			if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadOnly))
			{
				response.SetStatus(Nfs3Status.Nfs3errAcces);
				return response;
			}
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			ACCESS3Request request;
			try
			{
				request = ACCESS3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid ACCESS request");
				return new ACCESS3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle handle = request.GetHandle();
			Nfs3FileAttributes attrs;
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS ACCESS fileId: " + handle.GetFileId() + " client: " + remoteAddress
					);
			}
			try
			{
				attrs = writeManager.GetFileAttr(dfsClient, handle, iug);
				if (attrs == null)
				{
					Log.Error("Can't get path for fileId: " + handle.GetFileId());
					return new ACCESS3Response(Nfs3Status.Nfs3errStale);
				}
				if (iug.GetUserName(securityHandler.GetUid(), "unknown").Equals(superuser))
				{
					int access = Nfs3Constant.Access3Lookup | Nfs3Constant.Access3Delete | Nfs3Constant
						.Access3Execute | Nfs3Constant.Access3Extend | Nfs3Constant.Access3Modify | Nfs3Constant
						.Access3Read;
					return new ACCESS3Response(Nfs3Status.Nfs3Ok, attrs, access);
				}
				int access_1 = Nfs3Utils.GetAccessRightsForUserGroup(securityHandler.GetUid(), securityHandler
					.GetGid(), securityHandler.GetAuxGids(), attrs);
				return new ACCESS3Response(Nfs3Status.Nfs3Ok, attrs, access_1);
			}
			catch (RemoteException r)
			{
				Log.Warn("Exception ", r);
				IOException io = r.UnwrapRemoteException();
				if (io is AuthorizationException)
				{
					return new ACCESS3Response(Nfs3Status.Nfs3errAcces);
				}
				else
				{
					return new ACCESS3Response(Nfs3Status.Nfs3errIo);
				}
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				int status = MapErrorStatus(e);
				return new ACCESS3Response(status);
			}
		}

		public virtual READLINK3Response Readlink(XDR xdr, RpcInfo info)
		{
			return Readlink(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual READLINK3Response Readlink(XDR xdr, SecurityHandler securityHandler
			, EndPoint remoteAddress)
		{
			READLINK3Response response = new READLINK3Response(Nfs3Status.Nfs3Ok);
			if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadOnly))
			{
				response.SetStatus(Nfs3Status.Nfs3errAcces);
				return response;
			}
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			READLINK3Request request;
			try
			{
				request = READLINK3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid READLINK request");
				return new READLINK3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle handle = request.GetHandle();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS READLINK fileId: " + handle.GetFileId() + " client: " + remoteAddress
					);
			}
			string fileIdPath = Nfs3Utils.GetFileIdPath(handle);
			try
			{
				string target = dfsClient.GetLinkTarget(fileIdPath);
				Nfs3FileAttributes postOpAttr = Nfs3Utils.GetFileAttr(dfsClient, fileIdPath, iug);
				if (postOpAttr == null)
				{
					Log.Info("Can't get path for fileId: " + handle.GetFileId());
					return new READLINK3Response(Nfs3Status.Nfs3errStale);
				}
				if (postOpAttr.GetType() != NfsFileType.Nfslnk.ToValue())
				{
					Log.Error("Not a symlink, fileId: " + handle.GetFileId());
					return new READLINK3Response(Nfs3Status.Nfs3errInval);
				}
				if (target == null)
				{
					Log.Error("Symlink target should not be null, fileId: " + handle.GetFileId());
					return new READLINK3Response(Nfs3Status.Nfs3errServerfault);
				}
				int rtmax = config.GetInt(NfsConfigKeys.DfsNfsMaxReadTransferSizeKey, NfsConfigKeys
					.DfsNfsMaxReadTransferSizeDefault);
				if (rtmax < Sharpen.Runtime.GetBytesForString(target, Sharpen.Extensions.GetEncoding
					("UTF-8")).Length)
				{
					Log.Error("Link size: " + Sharpen.Runtime.GetBytesForString(target, Sharpen.Extensions.GetEncoding
						("UTF-8")).Length + " is larger than max transfer size: " + rtmax);
					return new READLINK3Response(Nfs3Status.Nfs3errIo, postOpAttr, new byte[0]);
				}
				return new READLINK3Response(Nfs3Status.Nfs3Ok, postOpAttr, Sharpen.Runtime.GetBytesForString
					(target, Sharpen.Extensions.GetEncoding("UTF-8")));
			}
			catch (IOException e)
			{
				Log.Warn("Readlink error: " + e.GetType(), e);
				int status = MapErrorStatus(e);
				return new READLINK3Response(status);
			}
		}

		public virtual READ3Response Read(XDR xdr, RpcInfo info)
		{
			return Read(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual READ3Response Read(XDR xdr, SecurityHandler securityHandler, EndPoint
			 remoteAddress)
		{
			READ3Response response = new READ3Response(Nfs3Status.Nfs3Ok);
			string userName = securityHandler.GetUser();
			if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadOnly))
			{
				response.SetStatus(Nfs3Status.Nfs3errAcces);
				return response;
			}
			DFSClient dfsClient = clientCache.GetDfsClient(userName);
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			READ3Request request;
			try
			{
				request = READ3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid READ request");
				return new READ3Response(Nfs3Status.Nfs3errInval);
			}
			long offset = request.GetOffset();
			int count = request.GetCount();
			FileHandle handle = request.GetHandle();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS READ fileId: " + handle.GetFileId() + " offset: " + offset + " count: "
					 + count + " client: " + remoteAddress);
			}
			Nfs3FileAttributes attrs;
			bool eof;
			if (count == 0)
			{
				// Only do access check.
				try
				{
					// Don't read from cache. Client may not have read permission.
					attrs = Nfs3Utils.GetFileAttr(dfsClient, Nfs3Utils.GetFileIdPath(handle), iug);
				}
				catch (IOException e)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Get error accessing file, fileId: " + handle.GetFileId(), e);
					}
					return new READ3Response(Nfs3Status.Nfs3errIo);
				}
				if (attrs == null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Can't get path for fileId: " + handle.GetFileId());
					}
					return new READ3Response(Nfs3Status.Nfs3errNoent);
				}
				int access = Nfs3Utils.GetAccessRightsForUserGroup(securityHandler.GetUid(), securityHandler
					.GetGid(), securityHandler.GetAuxGids(), attrs);
				if ((access & Nfs3Constant.Access3Read) != 0)
				{
					eof = offset >= attrs.GetSize();
					return new READ3Response(Nfs3Status.Nfs3Ok, attrs, 0, eof, ByteBuffer.Wrap(new byte
						[0]));
				}
				else
				{
					return new READ3Response(Nfs3Status.Nfs3errAcces);
				}
			}
			// In case there is buffered data for the same file, flush it. This can be
			// optimized later by reading from the cache.
			int ret = writeManager.CommitBeforeRead(dfsClient, handle, offset + count);
			if (ret != Nfs3Status.Nfs3Ok)
			{
				Log.Warn("commitBeforeRead didn't succeed with ret=" + ret + ". Read may not get most recent data."
					);
			}
			try
			{
				int rtmax = config.GetInt(NfsConfigKeys.DfsNfsMaxReadTransferSizeKey, NfsConfigKeys
					.DfsNfsMaxReadTransferSizeDefault);
				int buffSize = Math.Min(rtmax, count);
				byte[] readbuffer = new byte[buffSize];
				int readCount = 0;
				for (int i = 0; i < 1; ++i)
				{
					FSDataInputStream fis = clientCache.GetDfsInputStream(userName, Nfs3Utils.GetFileIdPath
						(handle));
					if (fis == null)
					{
						return new READ3Response(Nfs3Status.Nfs3errAcces);
					}
					try
					{
						readCount = fis.Read(offset, readbuffer, 0, count);
						metrics.IncrBytesRead(readCount);
					}
					catch (IOException e)
					{
						// TODO: A cleaner way is to throw a new type of exception
						// which requires incompatible changes.
						if (e.Message.Equals("Stream closed"))
						{
							clientCache.InvalidateDfsInputStream(userName, Nfs3Utils.GetFileIdPath(handle));
							continue;
						}
						else
						{
							throw;
						}
					}
				}
				attrs = Nfs3Utils.GetFileAttr(dfsClient, Nfs3Utils.GetFileIdPath(handle), iug);
				if (readCount < count)
				{
					Log.Info("Partical read. Asked offset: " + offset + " count: " + count + " and read back: "
						 + readCount + " file size: " + attrs.GetSize());
				}
				// HDFS returns -1 for read beyond file size.
				if (readCount < 0)
				{
					readCount = 0;
				}
				eof = (offset + readCount) >= attrs.GetSize();
				return new READ3Response(Nfs3Status.Nfs3Ok, attrs, readCount, eof, ByteBuffer.Wrap
					(readbuffer));
			}
			catch (IOException e)
			{
				Log.Warn("Read error: " + e.GetType() + " offset: " + offset + " count: " + count
					, e);
				int status = MapErrorStatus(e);
				return new READ3Response(status);
			}
		}

		public virtual WRITE3Response Write(XDR xdr, RpcInfo info)
		{
			SecurityHandler securityHandler = GetSecurityHandler(info);
			RpcCall rpcCall = (RpcCall)info.Header();
			int xid = rpcCall.GetXid();
			EndPoint remoteAddress = info.RemoteAddress();
			return Write(xdr, info.Channel(), xid, securityHandler, remoteAddress);
		}

		[VisibleForTesting]
		internal virtual WRITE3Response Write(XDR xdr, Org.Jboss.Netty.Channel.Channel channel
			, int xid, SecurityHandler securityHandler, EndPoint remoteAddress)
		{
			WRITE3Response response = new WRITE3Response(Nfs3Status.Nfs3Ok);
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			WRITE3Request request;
			try
			{
				request = WRITE3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid WRITE request");
				return new WRITE3Response(Nfs3Status.Nfs3errInval);
			}
			long offset = request.GetOffset();
			int count = request.GetCount();
			Nfs3Constant.WriteStableHow stableHow = request.GetStableHow();
			byte[] data = ((byte[])request.GetData().Array());
			if (data.Length < count)
			{
				Log.Error("Invalid argument, data size is less than count in request");
				return new WRITE3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle handle = request.GetHandle();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS WRITE fileId: " + handle.GetFileId() + " offset: " + offset + " length: "
					 + count + " stableHow: " + stableHow.GetValue() + " xid: " + xid + " client: " 
					+ remoteAddress);
			}
			Nfs3FileAttributes preOpAttr = null;
			try
			{
				preOpAttr = writeManager.GetFileAttr(dfsClient, handle, iug);
				if (preOpAttr == null)
				{
					Log.Error("Can't get path for fileId: " + handle.GetFileId());
					return new WRITE3Response(Nfs3Status.Nfs3errStale);
				}
				if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadWrite))
				{
					return new WRITE3Response(Nfs3Status.Nfs3errAcces, new WccData(Nfs3Utils.GetWccAttr
						(preOpAttr), preOpAttr), 0, stableHow, Nfs3Constant.WriteCommitVerf);
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("requested offset=" + offset + " and current filesize=" + preOpAttr.GetSize
						());
				}
				writeManager.HandleWrite(dfsClient, request, channel, xid, preOpAttr);
			}
			catch (IOException e)
			{
				Log.Info("Error writing to fileId " + handle.GetFileId() + " at offset " + offset
					 + " and length " + data.Length, e);
				// Try to return WccData
				Nfs3FileAttributes postOpAttr = null;
				try
				{
					postOpAttr = writeManager.GetFileAttr(dfsClient, handle, iug);
				}
				catch (IOException e1)
				{
					Log.Info("Can't get postOpAttr for fileId: " + handle.GetFileId(), e1);
				}
				WccAttr attr = preOpAttr == null ? null : Nfs3Utils.GetWccAttr(preOpAttr);
				WccData fileWcc = new WccData(attr, postOpAttr);
				int status = MapErrorStatus(e);
				return new WRITE3Response(status, fileWcc, 0, request.GetStableHow(), Nfs3Constant
					.WriteCommitVerf);
			}
			return null;
		}

		public virtual CREATE3Response Create(XDR xdr, RpcInfo info)
		{
			return Create(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual CREATE3Response Create(XDR xdr, SecurityHandler securityHandler, 
			EndPoint remoteAddress)
		{
			CREATE3Response response = new CREATE3Response(Nfs3Status.Nfs3Ok);
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			CREATE3Request request;
			try
			{
				request = CREATE3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid CREATE request");
				return new CREATE3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle dirHandle = request.GetHandle();
			string fileName = request.GetName();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS CREATE dir fileId: " + dirHandle.GetFileId() + " filename: " + fileName
					 + " client: " + remoteAddress);
			}
			int createMode = request.GetMode();
			if ((createMode != Nfs3Constant.CreateExclusive) && request.GetObjAttr().GetUpdateFields
				().Contains(SetAttr3.SetAttrField.Size) && request.GetObjAttr().GetSize() != 0)
			{
				Log.Error("Setting file size is not supported when creating file: " + fileName + 
					" dir fileId: " + dirHandle.GetFileId());
				return new CREATE3Response(Nfs3Status.Nfs3errInval);
			}
			HdfsDataOutputStream fos = null;
			string dirFileIdPath = Nfs3Utils.GetFileIdPath(dirHandle);
			Nfs3FileAttributes preOpDirAttr = null;
			Nfs3FileAttributes postOpObjAttr = null;
			FileHandle fileHandle = null;
			WccData dirWcc = null;
			try
			{
				preOpDirAttr = Nfs3Utils.GetFileAttr(dfsClient, dirFileIdPath, iug);
				if (preOpDirAttr == null)
				{
					Log.Error("Can't get path for dirHandle: " + dirHandle);
					return new CREATE3Response(Nfs3Status.Nfs3errStale);
				}
				if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadWrite))
				{
					return new CREATE3Response(Nfs3Status.Nfs3errAcces, null, preOpDirAttr, new WccData
						(Nfs3Utils.GetWccAttr(preOpDirAttr), preOpDirAttr));
				}
				string fileIdPath = Nfs3Utils.GetFileIdPath(dirHandle) + "/" + fileName;
				SetAttr3 setAttr3 = request.GetObjAttr();
				System.Diagnostics.Debug.Assert((setAttr3 != null));
				FsPermission permission = setAttr3.GetUpdateFields().Contains(SetAttr3.SetAttrField
					.Mode) ? new FsPermission((short)setAttr3.GetMode()) : FsPermission.GetDefault()
					.ApplyUMask(umask);
				EnumSet<CreateFlag> flag = (createMode != Nfs3Constant.CreateExclusive) ? EnumSet
					.Of(CreateFlag.Create, CreateFlag.Overwrite) : EnumSet.Of(CreateFlag.Create);
				fos = dfsClient.CreateWrappedOutputStream(dfsClient.Create(fileIdPath, permission
					, flag, false, replication, blockSize, null, bufferSize, null), null);
				if ((createMode == Nfs3Constant.CreateUnchecked) || (createMode == Nfs3Constant.CreateGuarded
					))
				{
					// Set group if it's not specified in the request.
					if (!setAttr3.GetUpdateFields().Contains(SetAttr3.SetAttrField.Gid))
					{
						setAttr3.GetUpdateFields().AddItem(SetAttr3.SetAttrField.Gid);
						setAttr3.SetGid(securityHandler.GetGid());
					}
					SetattrInternal(dfsClient, fileIdPath, setAttr3, false);
				}
				postOpObjAttr = Nfs3Utils.GetFileAttr(dfsClient, fileIdPath, iug);
				dirWcc = Nfs3Utils.CreateWccData(Nfs3Utils.GetWccAttr(preOpDirAttr), dfsClient, dirFileIdPath
					, iug);
				// Add open stream
				OpenFileCtx openFileCtx = new OpenFileCtx(fos, postOpObjAttr, writeDumpDir + "/" 
					+ postOpObjAttr.GetFileId(), dfsClient, iug, aixCompatMode, config);
				fileHandle = new FileHandle(postOpObjAttr.GetFileId());
				if (!writeManager.AddOpenFileStream(fileHandle, openFileCtx))
				{
					Log.Warn("Can't add more stream, close it." + " Future write will become append");
					fos.Close();
					fos = null;
				}
				else
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Opened stream for file: " + fileName + ", fileId: " + fileHandle.GetFileId
							());
					}
				}
			}
			catch (IOException e)
			{
				Log.Error("Exception", e);
				if (fos != null)
				{
					try
					{
						fos.Close();
					}
					catch (IOException e1)
					{
						Log.Error("Can't close stream for dirFileId: " + dirHandle.GetFileId() + " filename: "
							 + fileName, e1);
					}
				}
				if (dirWcc == null)
				{
					try
					{
						dirWcc = Nfs3Utils.CreateWccData(Nfs3Utils.GetWccAttr(preOpDirAttr), dfsClient, dirFileIdPath
							, iug);
					}
					catch (IOException e1)
					{
						Log.Error("Can't get postOpDirAttr for dirFileId: " + dirHandle.GetFileId(), e1);
					}
				}
				int status = MapErrorStatus(e);
				return new CREATE3Response(status, fileHandle, postOpObjAttr, dirWcc);
			}
			return new CREATE3Response(Nfs3Status.Nfs3Ok, fileHandle, postOpObjAttr, dirWcc);
		}

		public virtual MKDIR3Response Mkdir(XDR xdr, RpcInfo info)
		{
			return Mkdir(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual MKDIR3Response Mkdir(XDR xdr, SecurityHandler securityHandler, EndPoint
			 remoteAddress)
		{
			MKDIR3Response response = new MKDIR3Response(Nfs3Status.Nfs3Ok);
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			MKDIR3Request request;
			try
			{
				request = MKDIR3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid MKDIR request");
				return new MKDIR3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle dirHandle = request.GetHandle();
			string fileName = request.GetName();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS MKDIR dirId: " + dirHandle.GetFileId() + " filename: " + fileName 
					+ " client: " + remoteAddress);
			}
			if (request.GetObjAttr().GetUpdateFields().Contains(SetAttr3.SetAttrField.Size))
			{
				Log.Error("Setting file size is not supported when mkdir: " + fileName + " in dirHandle"
					 + dirHandle);
				return new MKDIR3Response(Nfs3Status.Nfs3errInval);
			}
			string dirFileIdPath = Nfs3Utils.GetFileIdPath(dirHandle);
			Nfs3FileAttributes preOpDirAttr = null;
			Nfs3FileAttributes postOpDirAttr = null;
			Nfs3FileAttributes postOpObjAttr = null;
			FileHandle objFileHandle = null;
			try
			{
				preOpDirAttr = Nfs3Utils.GetFileAttr(dfsClient, dirFileIdPath, iug);
				if (preOpDirAttr == null)
				{
					Log.Info("Can't get path for dir fileId: " + dirHandle.GetFileId());
					return new MKDIR3Response(Nfs3Status.Nfs3errStale);
				}
				if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadWrite))
				{
					return new MKDIR3Response(Nfs3Status.Nfs3errAcces, null, preOpDirAttr, new WccData
						(Nfs3Utils.GetWccAttr(preOpDirAttr), preOpDirAttr));
				}
				string fileIdPath = dirFileIdPath + "/" + fileName;
				SetAttr3 setAttr3 = request.GetObjAttr();
				FsPermission permission = setAttr3.GetUpdateFields().Contains(SetAttr3.SetAttrField
					.Mode) ? new FsPermission((short)setAttr3.GetMode()) : FsPermission.GetDefault()
					.ApplyUMask(umask);
				if (!dfsClient.Mkdirs(fileIdPath, permission, false))
				{
					WccData dirWcc = Nfs3Utils.CreateWccData(Nfs3Utils.GetWccAttr(preOpDirAttr), dfsClient
						, dirFileIdPath, iug);
					return new MKDIR3Response(Nfs3Status.Nfs3errIo, null, null, dirWcc);
				}
				// Set group if it's not specified in the request.
				if (!setAttr3.GetUpdateFields().Contains(SetAttr3.SetAttrField.Gid))
				{
					setAttr3.GetUpdateFields().AddItem(SetAttr3.SetAttrField.Gid);
					setAttr3.SetGid(securityHandler.GetGid());
				}
				SetattrInternal(dfsClient, fileIdPath, setAttr3, false);
				postOpObjAttr = Nfs3Utils.GetFileAttr(dfsClient, fileIdPath, iug);
				objFileHandle = new FileHandle(postOpObjAttr.GetFileId());
				WccData dirWcc_1 = Nfs3Utils.CreateWccData(Nfs3Utils.GetWccAttr(preOpDirAttr), dfsClient
					, dirFileIdPath, iug);
				return new MKDIR3Response(Nfs3Status.Nfs3Ok, new FileHandle(postOpObjAttr.GetFileId
					()), postOpObjAttr, dirWcc_1);
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				// Try to return correct WccData
				if (postOpDirAttr == null)
				{
					try
					{
						postOpDirAttr = Nfs3Utils.GetFileAttr(dfsClient, dirFileIdPath, iug);
					}
					catch (IOException)
					{
						Log.Info("Can't get postOpDirAttr for " + dirFileIdPath, e);
					}
				}
				WccData dirWcc = new WccData(Nfs3Utils.GetWccAttr(preOpDirAttr), postOpDirAttr);
				int status = MapErrorStatus(e);
				return new MKDIR3Response(status, objFileHandle, postOpObjAttr, dirWcc);
			}
		}

		public virtual READDIR3Response Mknod(XDR xdr, RpcInfo info)
		{
			return new READDIR3Response(Nfs3Status.Nfs3errNotsupp);
		}

		public virtual REMOVE3Response Remove(XDR xdr, RpcInfo info)
		{
			return Remove(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual REMOVE3Response Remove(XDR xdr, SecurityHandler securityHandler, 
			EndPoint remoteAddress)
		{
			REMOVE3Response response = new REMOVE3Response(Nfs3Status.Nfs3Ok);
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			REMOVE3Request request;
			try
			{
				request = REMOVE3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid REMOVE request");
				return new REMOVE3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle dirHandle = request.GetHandle();
			string fileName = request.GetName();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS REMOVE dir fileId: " + dirHandle.GetFileId() + " fileName: " + fileName
					 + " client: " + remoteAddress);
			}
			string dirFileIdPath = Nfs3Utils.GetFileIdPath(dirHandle);
			Nfs3FileAttributes preOpDirAttr = null;
			Nfs3FileAttributes postOpDirAttr = null;
			try
			{
				preOpDirAttr = Nfs3Utils.GetFileAttr(dfsClient, dirFileIdPath, iug);
				if (preOpDirAttr == null)
				{
					Log.Info("Can't get path for dir fileId: " + dirHandle.GetFileId());
					return new REMOVE3Response(Nfs3Status.Nfs3errStale);
				}
				WccData errWcc = new WccData(Nfs3Utils.GetWccAttr(preOpDirAttr), preOpDirAttr);
				if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadWrite))
				{
					return new REMOVE3Response(Nfs3Status.Nfs3errAcces, errWcc);
				}
				string fileIdPath = dirFileIdPath + "/" + fileName;
				HdfsFileStatus fstat = Nfs3Utils.GetFileStatus(dfsClient, fileIdPath);
				if (fstat == null)
				{
					return new REMOVE3Response(Nfs3Status.Nfs3errNoent, errWcc);
				}
				if (fstat.IsDir())
				{
					return new REMOVE3Response(Nfs3Status.Nfs3errIsdir, errWcc);
				}
				bool result = dfsClient.Delete(fileIdPath, false);
				WccData dirWcc = Nfs3Utils.CreateWccData(Nfs3Utils.GetWccAttr(preOpDirAttr), dfsClient
					, dirFileIdPath, iug);
				if (!result)
				{
					return new REMOVE3Response(Nfs3Status.Nfs3errAcces, dirWcc);
				}
				return new REMOVE3Response(Nfs3Status.Nfs3Ok, dirWcc);
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				// Try to return correct WccData
				if (postOpDirAttr == null)
				{
					try
					{
						postOpDirAttr = Nfs3Utils.GetFileAttr(dfsClient, dirFileIdPath, iug);
					}
					catch (IOException e1)
					{
						Log.Info("Can't get postOpDirAttr for " + dirFileIdPath, e1);
					}
				}
				WccData dirWcc = new WccData(Nfs3Utils.GetWccAttr(preOpDirAttr), postOpDirAttr);
				int status = MapErrorStatus(e);
				return new REMOVE3Response(status, dirWcc);
			}
		}

		public virtual RMDIR3Response Rmdir(XDR xdr, RpcInfo info)
		{
			return Rmdir(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual RMDIR3Response Rmdir(XDR xdr, SecurityHandler securityHandler, EndPoint
			 remoteAddress)
		{
			RMDIR3Response response = new RMDIR3Response(Nfs3Status.Nfs3Ok);
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			RMDIR3Request request;
			try
			{
				request = RMDIR3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid RMDIR request");
				return new RMDIR3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle dirHandle = request.GetHandle();
			string fileName = request.GetName();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS RMDIR dir fileId: " + dirHandle.GetFileId() + " fileName: " + fileName
					 + " client: " + remoteAddress);
			}
			string dirFileIdPath = Nfs3Utils.GetFileIdPath(dirHandle);
			Nfs3FileAttributes preOpDirAttr = null;
			Nfs3FileAttributes postOpDirAttr = null;
			try
			{
				preOpDirAttr = Nfs3Utils.GetFileAttr(dfsClient, dirFileIdPath, iug);
				if (preOpDirAttr == null)
				{
					Log.Info("Can't get path for dir fileId: " + dirHandle.GetFileId());
					return new RMDIR3Response(Nfs3Status.Nfs3errStale);
				}
				WccData errWcc = new WccData(Nfs3Utils.GetWccAttr(preOpDirAttr), preOpDirAttr);
				if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadWrite))
				{
					return new RMDIR3Response(Nfs3Status.Nfs3errAcces, errWcc);
				}
				string fileIdPath = dirFileIdPath + "/" + fileName;
				HdfsFileStatus fstat = Nfs3Utils.GetFileStatus(dfsClient, fileIdPath);
				if (fstat == null)
				{
					return new RMDIR3Response(Nfs3Status.Nfs3errNoent, errWcc);
				}
				if (!fstat.IsDir())
				{
					return new RMDIR3Response(Nfs3Status.Nfs3errNotdir, errWcc);
				}
				if (fstat.GetChildrenNum() > 0)
				{
					return new RMDIR3Response(Nfs3Status.Nfs3errNotempty, errWcc);
				}
				bool result = dfsClient.Delete(fileIdPath, false);
				WccData dirWcc = Nfs3Utils.CreateWccData(Nfs3Utils.GetWccAttr(preOpDirAttr), dfsClient
					, dirFileIdPath, iug);
				if (!result)
				{
					return new RMDIR3Response(Nfs3Status.Nfs3errAcces, dirWcc);
				}
				return new RMDIR3Response(Nfs3Status.Nfs3Ok, dirWcc);
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				// Try to return correct WccData
				if (postOpDirAttr == null)
				{
					try
					{
						postOpDirAttr = Nfs3Utils.GetFileAttr(dfsClient, dirFileIdPath, iug);
					}
					catch (IOException e1)
					{
						Log.Info("Can't get postOpDirAttr for " + dirFileIdPath, e1);
					}
				}
				WccData dirWcc = new WccData(Nfs3Utils.GetWccAttr(preOpDirAttr), postOpDirAttr);
				int status = MapErrorStatus(e);
				return new RMDIR3Response(status, dirWcc);
			}
		}

		public virtual RENAME3Response Rename(XDR xdr, RpcInfo info)
		{
			return Rename(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual RENAME3Response Rename(XDR xdr, SecurityHandler securityHandler, 
			EndPoint remoteAddress)
		{
			RENAME3Response response = new RENAME3Response(Nfs3Status.Nfs3Ok);
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			RENAME3Request request = null;
			try
			{
				request = RENAME3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid RENAME request");
				return new RENAME3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle fromHandle = request.GetFromDirHandle();
			string fromName = request.GetFromName();
			FileHandle toHandle = request.GetToDirHandle();
			string toName = request.GetToName();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS RENAME from: " + fromHandle.GetFileId() + "/" + fromName + " to: "
					 + toHandle.GetFileId() + "/" + toName + " client: " + remoteAddress);
			}
			string fromDirFileIdPath = Nfs3Utils.GetFileIdPath(fromHandle);
			string toDirFileIdPath = Nfs3Utils.GetFileIdPath(toHandle);
			Nfs3FileAttributes fromPreOpAttr = null;
			Nfs3FileAttributes toPreOpAttr = null;
			WccData fromDirWcc = null;
			WccData toDirWcc = null;
			try
			{
				fromPreOpAttr = Nfs3Utils.GetFileAttr(dfsClient, fromDirFileIdPath, iug);
				if (fromPreOpAttr == null)
				{
					Log.Info("Can't get path for fromHandle fileId: " + fromHandle.GetFileId());
					return new RENAME3Response(Nfs3Status.Nfs3errStale);
				}
				toPreOpAttr = Nfs3Utils.GetFileAttr(dfsClient, toDirFileIdPath, iug);
				if (toPreOpAttr == null)
				{
					Log.Info("Can't get path for toHandle fileId: " + toHandle.GetFileId());
					return new RENAME3Response(Nfs3Status.Nfs3errStale);
				}
				if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadWrite))
				{
					WccData fromWcc = new WccData(Nfs3Utils.GetWccAttr(fromPreOpAttr), fromPreOpAttr);
					WccData toWcc = new WccData(Nfs3Utils.GetWccAttr(toPreOpAttr), toPreOpAttr);
					return new RENAME3Response(Nfs3Status.Nfs3errAcces, fromWcc, toWcc);
				}
				string src = fromDirFileIdPath + "/" + fromName;
				string dst = toDirFileIdPath + "/" + toName;
				dfsClient.Rename(src, dst, Options.Rename.None);
				// Assemble the reply
				fromDirWcc = Nfs3Utils.CreateWccData(Nfs3Utils.GetWccAttr(fromPreOpAttr), dfsClient
					, fromDirFileIdPath, iug);
				toDirWcc = Nfs3Utils.CreateWccData(Nfs3Utils.GetWccAttr(toPreOpAttr), dfsClient, 
					toDirFileIdPath, iug);
				return new RENAME3Response(Nfs3Status.Nfs3Ok, fromDirWcc, toDirWcc);
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				// Try to return correct WccData
				try
				{
					fromDirWcc = Nfs3Utils.CreateWccData(Nfs3Utils.GetWccAttr(fromPreOpAttr), dfsClient
						, fromDirFileIdPath, iug);
					toDirWcc = Nfs3Utils.CreateWccData(Nfs3Utils.GetWccAttr(toPreOpAttr), dfsClient, 
						toDirFileIdPath, iug);
				}
				catch (IOException e1)
				{
					Log.Info("Can't get postOpDirAttr for " + fromDirFileIdPath + " or" + toDirFileIdPath
						, e1);
				}
				int status = MapErrorStatus(e);
				return new RENAME3Response(status, fromDirWcc, toDirWcc);
			}
		}

		public virtual SYMLINK3Response Symlink(XDR xdr, RpcInfo info)
		{
			return Symlink(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual SYMLINK3Response Symlink(XDR xdr, SecurityHandler securityHandler
			, EndPoint remoteAddress)
		{
			SYMLINK3Response response = new SYMLINK3Response(Nfs3Status.Nfs3Ok);
			if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadWrite))
			{
				response.SetStatus(Nfs3Status.Nfs3errAcces);
				return response;
			}
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			SYMLINK3Request request;
			try
			{
				request = SYMLINK3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid SYMLINK request");
				response.SetStatus(Nfs3Status.Nfs3errInval);
				return response;
			}
			FileHandle dirHandle = request.GetHandle();
			string name = request.GetName();
			string symData = request.GetSymData();
			string linkDirIdPath = Nfs3Utils.GetFileIdPath(dirHandle);
			// Don't do any name check to source path, just leave it to HDFS
			string linkIdPath = linkDirIdPath + "/" + name;
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS SYMLINK, target: " + symData + " link: " + linkIdPath + " client: "
					 + remoteAddress);
			}
			try
			{
				WccData dirWcc = response.GetDirWcc();
				WccAttr preOpAttr = Nfs3Utils.GetWccAttr(dfsClient, linkDirIdPath);
				dirWcc.SetPreOpAttr(preOpAttr);
				dfsClient.CreateSymlink(symData, linkIdPath, false);
				// Set symlink attr is considered as to change the attr of the target
				// file. So no need to set symlink attr here after it's created.
				HdfsFileStatus linkstat = dfsClient.GetFileLinkInfo(linkIdPath);
				Nfs3FileAttributes objAttr = Nfs3Utils.GetNfs3FileAttrFromFileStatus(linkstat, iug
					);
				dirWcc.SetPostOpAttr(Nfs3Utils.GetFileAttr(dfsClient, linkDirIdPath, iug));
				return new SYMLINK3Response(Nfs3Status.Nfs3Ok, new FileHandle(objAttr.GetFileId()
					), objAttr, dirWcc);
			}
			catch (IOException e)
			{
				Log.Warn("Exception: " + e);
				int status = MapErrorStatus(e);
				response.SetStatus(status);
				return response;
			}
		}

		public virtual READDIR3Response Link(XDR xdr, RpcInfo info)
		{
			return new READDIR3Response(Nfs3Status.Nfs3errNotsupp);
		}

		/// <summary>Used by readdir and readdirplus to get dirents.</summary>
		/// <remarks>
		/// Used by readdir and readdirplus to get dirents. It retries the listing if
		/// the startAfter can't be found anymore.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private DirectoryListing ListPaths(DFSClient dfsClient, string dirFileIdPath, byte
			[] startAfter)
		{
			DirectoryListing dlisting;
			try
			{
				dlisting = dfsClient.ListPaths(dirFileIdPath, startAfter);
			}
			catch (RemoteException e)
			{
				IOException io = e.UnwrapRemoteException();
				if (!(io is DirectoryListingStartAfterNotFoundException))
				{
					throw io;
				}
				// This happens when startAfter was just deleted
				Log.Info("Cookie couldn't be found: " + new string(startAfter, Sharpen.Extensions.GetEncoding
					("UTF-8")) + ", do listing from beginning");
				dlisting = dfsClient.ListPaths(dirFileIdPath, HdfsFileStatus.EmptyName);
			}
			return dlisting;
		}

		public virtual READDIR3Response Readdir(XDR xdr, RpcInfo info)
		{
			return Readdir(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		public virtual READDIR3Response Readdir(XDR xdr, SecurityHandler securityHandler, 
			EndPoint remoteAddress)
		{
			READDIR3Response response = new READDIR3Response(Nfs3Status.Nfs3Ok);
			if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadOnly))
			{
				response.SetStatus(Nfs3Status.Nfs3errAcces);
				return response;
			}
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			READDIR3Request request;
			try
			{
				request = READDIR3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid READDIR request");
				return new READDIR3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle handle = request.GetHandle();
			long cookie = request.GetCookie();
			if (cookie < 0)
			{
				Log.Error("Invalid READDIR request, with negative cookie: " + cookie);
				return new READDIR3Response(Nfs3Status.Nfs3errInval);
			}
			long count = request.GetCount();
			if (count <= 0)
			{
				Log.Info("Nonpositive count in invalid READDIR request: " + count);
				return new READDIR3Response(Nfs3Status.Nfs3Ok);
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS READDIR fileId: " + handle.GetFileId() + " cookie: " + cookie + " count: "
					 + count + " client: " + remoteAddress);
			}
			HdfsFileStatus dirStatus;
			DirectoryListing dlisting;
			Nfs3FileAttributes postOpAttr;
			long dotdotFileId = 0;
			try
			{
				string dirFileIdPath = Nfs3Utils.GetFileIdPath(handle);
				dirStatus = dfsClient.GetFileInfo(dirFileIdPath);
				if (dirStatus == null)
				{
					Log.Info("Can't get path for fileId: " + handle.GetFileId());
					return new READDIR3Response(Nfs3Status.Nfs3errStale);
				}
				if (!dirStatus.IsDir())
				{
					Log.Error("Can't readdir for regular file, fileId: " + handle.GetFileId());
					return new READDIR3Response(Nfs3Status.Nfs3errNotdir);
				}
				long cookieVerf = request.GetCookieVerf();
				if ((cookieVerf != 0) && (cookieVerf != dirStatus.GetModificationTime()))
				{
					if (aixCompatMode)
					{
						// The AIX NFS client misinterprets RFC-1813 and will repeatedly send
						// the same cookieverf value even across VFS-level readdir calls,
						// instead of getting a new cookieverf for every VFS-level readdir
						// call, and reusing the cookieverf only in the event that multiple
						// incremental NFS-level readdir calls must be made to fetch all of
						// the directory entries. This means that whenever a readdir call is
						// made by an AIX NFS client for a given directory, and that directory
						// is subsequently modified, thus changing its mtime, no later readdir
						// calls will succeed from AIX for that directory until the FS is
						// unmounted/remounted. See HDFS-6549 for more info.
						Log.Warn("AIX compatibility mode enabled, ignoring cookieverf " + "mismatches.");
					}
					else
					{
						Log.Error("CookieVerf mismatch. request cookieVerf: " + cookieVerf + " dir cookieVerf: "
							 + dirStatus.GetModificationTime());
						return new READDIR3Response(Nfs3Status.Nfs3errBadCookie, Nfs3Utils.GetFileAttr(dfsClient
							, dirFileIdPath, iug));
					}
				}
				if (cookie == 0)
				{
					// Get dotdot fileId
					string dotdotFileIdPath = dirFileIdPath + "/..";
					HdfsFileStatus dotdotStatus = dfsClient.GetFileInfo(dotdotFileIdPath);
					if (dotdotStatus == null)
					{
						// This should not happen
						throw new IOException("Can't get path for handle path: " + dotdotFileIdPath);
					}
					dotdotFileId = dotdotStatus.GetFileId();
				}
				// Get the list from the resume point
				byte[] startAfter;
				if (cookie == 0)
				{
					startAfter = HdfsFileStatus.EmptyName;
				}
				else
				{
					string inodeIdPath = Nfs3Utils.GetFileIdPath(cookie);
					startAfter = Sharpen.Runtime.GetBytesForString(inodeIdPath, Sharpen.Extensions.GetEncoding
						("UTF-8"));
				}
				dlisting = ListPaths(dfsClient, dirFileIdPath, startAfter);
				postOpAttr = Nfs3Utils.GetFileAttr(dfsClient, dirFileIdPath, iug);
				if (postOpAttr == null)
				{
					Log.Error("Can't get path for fileId: " + handle.GetFileId());
					return new READDIR3Response(Nfs3Status.Nfs3errStale);
				}
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				int status = MapErrorStatus(e);
				return new READDIR3Response(status);
			}
			HdfsFileStatus[] fstatus = dlisting.GetPartialListing();
			int n = (int)Math.Min(fstatus.Length, count - 2);
			bool eof = (n >= fstatus.Length) && !dlisting.HasMore();
			READDIR3Response.Entry3[] entries;
			if (cookie == 0)
			{
				entries = new READDIR3Response.Entry3[n + 2];
				entries[0] = new READDIR3Response.Entry3(postOpAttr.GetFileId(), ".", 0);
				entries[1] = new READDIR3Response.Entry3(dotdotFileId, "..", dotdotFileId);
				for (int i = 2; i < n + 2; i++)
				{
					entries[i] = new READDIR3Response.Entry3(fstatus[i - 2].GetFileId(), fstatus[i - 
						2].GetLocalName(), fstatus[i - 2].GetFileId());
				}
			}
			else
			{
				// Resume from last readdirplus. If the cookie is "..", the result
				// list is up the directory content since HDFS uses name as resume point.    
				entries = new READDIR3Response.Entry3[n];
				for (int i = 0; i < n; i++)
				{
					entries[i] = new READDIR3Response.Entry3(fstatus[i].GetFileId(), fstatus[i].GetLocalName
						(), fstatus[i].GetFileId());
				}
			}
			READDIR3Response.DirList3 dirList = new READDIR3Response.DirList3(entries, eof);
			return new READDIR3Response(Nfs3Status.Nfs3Ok, postOpAttr, dirStatus.GetModificationTime
				(), dirList);
		}

		public virtual READDIRPLUS3Response Readdirplus(XDR xdr, RpcInfo info)
		{
			return Readdirplus(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual READDIRPLUS3Response Readdirplus(XDR xdr, SecurityHandler securityHandler
			, EndPoint remoteAddress)
		{
			if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadOnly))
			{
				return new READDIRPLUS3Response(Nfs3Status.Nfs3errAcces);
			}
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				return new READDIRPLUS3Response(Nfs3Status.Nfs3errServerfault);
			}
			READDIRPLUS3Request request = null;
			try
			{
				request = READDIRPLUS3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid READDIRPLUS request");
				return new READDIRPLUS3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle handle = request.GetHandle();
			long cookie = request.GetCookie();
			if (cookie < 0)
			{
				Log.Error("Invalid READDIRPLUS request, with negative cookie: " + cookie);
				return new READDIRPLUS3Response(Nfs3Status.Nfs3errInval);
			}
			long dirCount = request.GetDirCount();
			if (dirCount <= 0)
			{
				Log.Info("Nonpositive dircount in invalid READDIRPLUS request: " + dirCount);
				return new READDIRPLUS3Response(Nfs3Status.Nfs3errInval);
			}
			int maxCount = request.GetMaxCount();
			if (maxCount <= 0)
			{
				Log.Info("Nonpositive maxcount in invalid READDIRPLUS request: " + maxCount);
				return new READDIRPLUS3Response(Nfs3Status.Nfs3errInval);
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS READDIRPLUS fileId: " + handle.GetFileId() + " cookie: " + cookie 
					+ " dirCount: " + dirCount + " maxCount: " + maxCount + " client: " + remoteAddress
					);
			}
			HdfsFileStatus dirStatus;
			DirectoryListing dlisting;
			Nfs3FileAttributes postOpDirAttr;
			long dotdotFileId = 0;
			HdfsFileStatus dotdotStatus = null;
			try
			{
				string dirFileIdPath = Nfs3Utils.GetFileIdPath(handle);
				dirStatus = dfsClient.GetFileInfo(dirFileIdPath);
				if (dirStatus == null)
				{
					Log.Info("Can't get path for fileId: " + handle.GetFileId());
					return new READDIRPLUS3Response(Nfs3Status.Nfs3errStale);
				}
				if (!dirStatus.IsDir())
				{
					Log.Error("Can't readdirplus for regular file, fileId: " + handle.GetFileId());
					return new READDIRPLUS3Response(Nfs3Status.Nfs3errNotdir);
				}
				long cookieVerf = request.GetCookieVerf();
				if ((cookieVerf != 0) && (cookieVerf != dirStatus.GetModificationTime()))
				{
					if (aixCompatMode)
					{
						// The AIX NFS client misinterprets RFC-1813 and will repeatedly send
						// the same cookieverf value even across VFS-level readdir calls,
						// instead of getting a new cookieverf for every VFS-level readdir
						// call. This means that whenever a readdir call is made by an AIX NFS
						// client for a given directory, and that directory is subsequently
						// modified, thus changing its mtime, no later readdir calls will
						// succeed for that directory from AIX until the FS is
						// unmounted/remounted. See HDFS-6549 for more info.
						Log.Warn("AIX compatibility mode enabled, ignoring cookieverf " + "mismatches.");
					}
					else
					{
						Log.Error("cookieverf mismatch. request cookieverf: " + cookieVerf + " dir cookieverf: "
							 + dirStatus.GetModificationTime());
						return new READDIRPLUS3Response(Nfs3Status.Nfs3errBadCookie, Nfs3Utils.GetFileAttr
							(dfsClient, dirFileIdPath, iug), 0, null);
					}
				}
				if (cookie == 0)
				{
					// Get dotdot fileId
					string dotdotFileIdPath = dirFileIdPath + "/..";
					dotdotStatus = dfsClient.GetFileInfo(dotdotFileIdPath);
					if (dotdotStatus == null)
					{
						// This should not happen
						throw new IOException("Can't get path for handle path: " + dotdotFileIdPath);
					}
					dotdotFileId = dotdotStatus.GetFileId();
				}
				// Get the list from the resume point
				byte[] startAfter;
				if (cookie == 0)
				{
					startAfter = HdfsFileStatus.EmptyName;
				}
				else
				{
					string inodeIdPath = Nfs3Utils.GetFileIdPath(cookie);
					startAfter = Sharpen.Runtime.GetBytesForString(inodeIdPath, Sharpen.Extensions.GetEncoding
						("UTF-8"));
				}
				dlisting = ListPaths(dfsClient, dirFileIdPath, startAfter);
				postOpDirAttr = Nfs3Utils.GetFileAttr(dfsClient, dirFileIdPath, iug);
				if (postOpDirAttr == null)
				{
					Log.Info("Can't get path for fileId: " + handle.GetFileId());
					return new READDIRPLUS3Response(Nfs3Status.Nfs3errStale);
				}
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				int status = MapErrorStatus(e);
				return new READDIRPLUS3Response(status);
			}
			// Set up the dirents in the response
			HdfsFileStatus[] fstatus = dlisting.GetPartialListing();
			int n = (int)Math.Min(fstatus.Length, dirCount - 2);
			bool eof = (n >= fstatus.Length) && !dlisting.HasMore();
			READDIRPLUS3Response.EntryPlus3[] entries;
			if (cookie == 0)
			{
				entries = new READDIRPLUS3Response.EntryPlus3[n + 2];
				entries[0] = new READDIRPLUS3Response.EntryPlus3(postOpDirAttr.GetFileId(), ".", 
					0, postOpDirAttr, new FileHandle(postOpDirAttr.GetFileId()));
				entries[1] = new READDIRPLUS3Response.EntryPlus3(dotdotFileId, "..", dotdotFileId
					, Nfs3Utils.GetNfs3FileAttrFromFileStatus(dotdotStatus, iug), new FileHandle(dotdotFileId
					));
				for (int i = 2; i < n + 2; i++)
				{
					long fileId = fstatus[i - 2].GetFileId();
					FileHandle childHandle = new FileHandle(fileId);
					Nfs3FileAttributes attr;
					try
					{
						attr = writeManager.GetFileAttr(dfsClient, childHandle, iug);
					}
					catch (IOException e)
					{
						Log.Error("Can't get file attributes for fileId: " + fileId, e);
						continue;
					}
					entries[i] = new READDIRPLUS3Response.EntryPlus3(fileId, fstatus[i - 2].GetLocalName
						(), fileId, attr, childHandle);
				}
			}
			else
			{
				// Resume from last readdirplus. If the cookie is "..", the result
				// list is up the directory content since HDFS uses name as resume point.
				entries = new READDIRPLUS3Response.EntryPlus3[n];
				for (int i = 0; i < n; i++)
				{
					long fileId = fstatus[i].GetFileId();
					FileHandle childHandle = new FileHandle(fileId);
					Nfs3FileAttributes attr;
					try
					{
						attr = writeManager.GetFileAttr(dfsClient, childHandle, iug);
					}
					catch (IOException e)
					{
						Log.Error("Can't get file attributes for fileId: " + fileId, e);
						continue;
					}
					entries[i] = new READDIRPLUS3Response.EntryPlus3(fileId, fstatus[i].GetLocalName(
						), fileId, attr, childHandle);
				}
			}
			READDIRPLUS3Response.DirListPlus3 dirListPlus = new READDIRPLUS3Response.DirListPlus3
				(entries, eof);
			return new READDIRPLUS3Response(Nfs3Status.Nfs3Ok, postOpDirAttr, dirStatus.GetModificationTime
				(), dirListPlus);
		}

		public virtual FSSTAT3Response Fsstat(XDR xdr, RpcInfo info)
		{
			return Fsstat(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual FSSTAT3Response Fsstat(XDR xdr, SecurityHandler securityHandler, 
			EndPoint remoteAddress)
		{
			FSSTAT3Response response = new FSSTAT3Response(Nfs3Status.Nfs3Ok);
			if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadOnly))
			{
				response.SetStatus(Nfs3Status.Nfs3errAcces);
				return response;
			}
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			FSSTAT3Request request;
			try
			{
				request = FSSTAT3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid FSSTAT request");
				return new FSSTAT3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle handle = request.GetHandle();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS FSSTAT fileId: " + handle.GetFileId() + " client: " + remoteAddress
					);
			}
			try
			{
				FsStatus fsStatus = dfsClient.GetDiskStatus();
				long totalBytes = fsStatus.GetCapacity();
				long freeBytes = fsStatus.GetRemaining();
				Nfs3FileAttributes attrs = writeManager.GetFileAttr(dfsClient, handle, iug);
				if (attrs == null)
				{
					Log.Info("Can't get path for fileId: " + handle.GetFileId());
					return new FSSTAT3Response(Nfs3Status.Nfs3errStale);
				}
				long maxFsObjects = config.GetLong("dfs.max.objects", 0);
				if (maxFsObjects == 0)
				{
					// A value of zero in HDFS indicates no limit to the number
					// of objects that dfs supports. Using Integer.MAX_VALUE instead of
					// Long.MAX_VALUE so 32bit client won't complain.
					maxFsObjects = int.MaxValue;
				}
				return new FSSTAT3Response(Nfs3Status.Nfs3Ok, attrs, totalBytes, freeBytes, freeBytes
					, maxFsObjects, maxFsObjects, maxFsObjects, 0);
			}
			catch (RemoteException r)
			{
				Log.Warn("Exception ", r);
				IOException io = r.UnwrapRemoteException();
				if (io is AuthorizationException)
				{
					return new FSSTAT3Response(Nfs3Status.Nfs3errAcces);
				}
				else
				{
					return new FSSTAT3Response(Nfs3Status.Nfs3errIo);
				}
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				int status = MapErrorStatus(e);
				return new FSSTAT3Response(status);
			}
		}

		public virtual FSINFO3Response Fsinfo(XDR xdr, RpcInfo info)
		{
			return Fsinfo(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual FSINFO3Response Fsinfo(XDR xdr, SecurityHandler securityHandler, 
			EndPoint remoteAddress)
		{
			FSINFO3Response response = new FSINFO3Response(Nfs3Status.Nfs3Ok);
			if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadOnly))
			{
				response.SetStatus(Nfs3Status.Nfs3errAcces);
				return response;
			}
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			FSINFO3Request request;
			try
			{
				request = FSINFO3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid FSINFO request");
				return new FSINFO3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle handle = request.GetHandle();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS FSINFO fileId: " + handle.GetFileId() + " client: " + remoteAddress
					);
			}
			try
			{
				int rtmax = config.GetInt(NfsConfigKeys.DfsNfsMaxReadTransferSizeKey, NfsConfigKeys
					.DfsNfsMaxReadTransferSizeDefault);
				int wtmax = config.GetInt(NfsConfigKeys.DfsNfsMaxWriteTransferSizeKey, NfsConfigKeys
					.DfsNfsMaxWriteTransferSizeDefault);
				int dtperf = config.GetInt(NfsConfigKeys.DfsNfsMaxReaddirTransferSizeKey, NfsConfigKeys
					.DfsNfsMaxReaddirTransferSizeDefault);
				Nfs3FileAttributes attrs = Nfs3Utils.GetFileAttr(dfsClient, Nfs3Utils.GetFileIdPath
					(handle), iug);
				if (attrs == null)
				{
					Log.Info("Can't get path for fileId: " + handle.GetFileId());
					return new FSINFO3Response(Nfs3Status.Nfs3errStale);
				}
				int fsProperty = Nfs3Constant.Fsf3Cansettime | Nfs3Constant.Fsf3Homogeneous;
				return new FSINFO3Response(Nfs3Status.Nfs3Ok, attrs, rtmax, rtmax, 1, wtmax, wtmax
					, 1, dtperf, long.MaxValue, new NfsTime(1), fsProperty);
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				int status = MapErrorStatus(e);
				return new FSINFO3Response(status);
			}
		}

		public virtual PATHCONF3Response Pathconf(XDR xdr, RpcInfo info)
		{
			return Pathconf(xdr, GetSecurityHandler(info), info.RemoteAddress());
		}

		[VisibleForTesting]
		internal virtual PATHCONF3Response Pathconf(XDR xdr, SecurityHandler securityHandler
			, EndPoint remoteAddress)
		{
			PATHCONF3Response response = new PATHCONF3Response(Nfs3Status.Nfs3Ok);
			if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadOnly))
			{
				response.SetStatus(Nfs3Status.Nfs3errAcces);
				return response;
			}
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			PATHCONF3Request request;
			try
			{
				request = PATHCONF3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid PATHCONF request");
				return new PATHCONF3Response(Nfs3Status.Nfs3errInval);
			}
			FileHandle handle = request.GetHandle();
			Nfs3FileAttributes attrs;
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS PATHCONF fileId: " + handle.GetFileId() + " client: " + remoteAddress
					);
			}
			try
			{
				attrs = Nfs3Utils.GetFileAttr(dfsClient, Nfs3Utils.GetFileIdPath(handle), iug);
				if (attrs == null)
				{
					Log.Info("Can't get path for fileId: " + handle.GetFileId());
					return new PATHCONF3Response(Nfs3Status.Nfs3errStale);
				}
				return new PATHCONF3Response(Nfs3Status.Nfs3Ok, attrs, 0, HdfsConstants.MaxPathLength
					, true, false, false, true);
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				int status = MapErrorStatus(e);
				return new PATHCONF3Response(status);
			}
		}

		public virtual COMMIT3Response Commit(XDR xdr, RpcInfo info)
		{
			SecurityHandler securityHandler = GetSecurityHandler(info);
			RpcCall rpcCall = (RpcCall)info.Header();
			int xid = rpcCall.GetXid();
			EndPoint remoteAddress = info.RemoteAddress();
			return Commit(xdr, info.Channel(), xid, securityHandler, remoteAddress);
		}

		[VisibleForTesting]
		internal virtual COMMIT3Response Commit(XDR xdr, Org.Jboss.Netty.Channel.Channel 
			channel, int xid, SecurityHandler securityHandler, EndPoint remoteAddress)
		{
			COMMIT3Response response = new COMMIT3Response(Nfs3Status.Nfs3Ok);
			DFSClient dfsClient = clientCache.GetDfsClient(securityHandler.GetUser());
			if (dfsClient == null)
			{
				response.SetStatus(Nfs3Status.Nfs3errServerfault);
				return response;
			}
			COMMIT3Request request;
			try
			{
				request = COMMIT3Request.Deserialize(xdr);
			}
			catch (IOException)
			{
				Log.Error("Invalid COMMIT request");
				response.SetStatus(Nfs3Status.Nfs3errInval);
				return response;
			}
			FileHandle handle = request.GetHandle();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("NFS COMMIT fileId: " + handle.GetFileId() + " offset=" + request.GetOffset
					() + " count=" + request.GetCount() + " client: " + remoteAddress);
			}
			string fileIdPath = Nfs3Utils.GetFileIdPath(handle);
			Nfs3FileAttributes preOpAttr = null;
			try
			{
				preOpAttr = Nfs3Utils.GetFileAttr(dfsClient, fileIdPath, iug);
				if (preOpAttr == null)
				{
					Log.Info("Can't get path for fileId: " + handle.GetFileId());
					return new COMMIT3Response(Nfs3Status.Nfs3errStale);
				}
				if (!CheckAccessPrivilege(remoteAddress, AccessPrivilege.ReadWrite))
				{
					return new COMMIT3Response(Nfs3Status.Nfs3errAcces, new WccData(Nfs3Utils.GetWccAttr
						(preOpAttr), preOpAttr), Nfs3Constant.WriteCommitVerf);
				}
				long commitOffset = (request.GetCount() == 0) ? 0 : (request.GetOffset() + request
					.GetCount());
				// Insert commit as an async request
				writeManager.HandleCommit(dfsClient, handle, commitOffset, channel, xid, preOpAttr
					);
				return null;
			}
			catch (IOException e)
			{
				Log.Warn("Exception ", e);
				Nfs3FileAttributes postOpAttr = null;
				try
				{
					postOpAttr = writeManager.GetFileAttr(dfsClient, handle, iug);
				}
				catch (IOException e1)
				{
					Log.Info("Can't get postOpAttr for fileId: " + handle.GetFileId(), e1);
				}
				WccData fileWcc = new WccData(Nfs3Utils.GetWccAttr(preOpAttr), postOpAttr);
				int status = MapErrorStatus(e);
				return new COMMIT3Response(status, fileWcc, Nfs3Constant.WriteCommitVerf);
			}
		}

		private SecurityHandler GetSecurityHandler(Credentials credentials, Verifier verifier
			)
		{
			if (credentials is CredentialsSys)
			{
				return new SysSecurityHandler((CredentialsSys)credentials, iug);
			}
			else
			{
				// TODO: support GSS and handle other cases
				return null;
			}
		}

		private SecurityHandler GetSecurityHandler(RpcInfo info)
		{
			RpcCall rpcCall = (RpcCall)info.Header();
			return GetSecurityHandler(rpcCall.GetCredential(), rpcCall.GetVerifier());
		}

		protected override void HandleInternal(ChannelHandlerContext ctx, RpcInfo info)
		{
			RpcCall rpcCall = (RpcCall)info.Header();
			Nfs3Constant.NFSPROC3 nfsproc3 = Nfs3Constant.NFSPROC3.FromValue(rpcCall.GetProcedure
				());
			int xid = rpcCall.GetXid();
			byte[] data = new byte[info.Data().ReadableBytes()];
			info.Data().ReadBytes(data);
			XDR xdr = new XDR(data);
			XDR @out = new XDR();
			IPAddress client = ((IPEndPoint)info.RemoteAddress()).Address;
			Credentials credentials = rpcCall.GetCredential();
			// Ignore auth only for NFSPROC3_NULL, especially for Linux clients.
			if (nfsproc3 != Nfs3Constant.NFSPROC3.Null)
			{
				if (credentials.GetFlavor() != RpcAuthInfo.AuthFlavor.AuthSys && credentials.GetFlavor
					() != RpcAuthInfo.AuthFlavor.RpcsecGss)
				{
					Log.Info("Wrong RPC AUTH flavor, " + credentials.GetFlavor() + " is not AUTH_SYS or RPCSEC_GSS."
						);
					XDR reply = new XDR();
					RpcDeniedReply rdr = new RpcDeniedReply(xid, RpcReply.ReplyState.MsgAccepted, RpcDeniedReply.RejectState
						.AuthError, new VerifierNone());
					rdr.Write(reply);
					ChannelBuffer buf = ChannelBuffers.WrappedBuffer(reply.AsReadOnlyWrap().Buffer());
					RpcResponse rsp = new RpcResponse(buf, info.RemoteAddress());
					RpcUtil.SendRpcResponse(ctx, rsp);
					return;
				}
			}
			if (!IsIdempotent(rpcCall))
			{
				RpcCallCache.CacheEntry entry = rpcCallCache.CheckOrAddToCache(client, xid);
				if (entry != null)
				{
					// in cache
					if (entry.IsCompleted())
					{
						Log.Info("Sending the cached reply to retransmitted request " + xid);
						RpcUtil.SendRpcResponse(ctx, entry.GetResponse());
						return;
					}
					else
					{
						// else request is in progress
						Log.Info("Retransmitted request, transaction still in progress " + xid);
						// Ignore the request and do nothing
						return;
					}
				}
			}
			// Since write and commit could be async, they use their own startTime and
			// only record success requests.
			long startTime = Runtime.NanoTime();
			NFS3Response response = null;
			if (nfsproc3 == Nfs3Constant.NFSPROC3.Null)
			{
				response = NullProcedure();
			}
			else
			{
				if (nfsproc3 == Nfs3Constant.NFSPROC3.Getattr)
				{
					response = Getattr(xdr, info);
					metrics.AddGetattr(Nfs3Utils.GetElapsedTime(startTime));
				}
				else
				{
					if (nfsproc3 == Nfs3Constant.NFSPROC3.Setattr)
					{
						response = Setattr(xdr, info);
						metrics.AddSetattr(Nfs3Utils.GetElapsedTime(startTime));
					}
					else
					{
						if (nfsproc3 == Nfs3Constant.NFSPROC3.Lookup)
						{
							response = Lookup(xdr, info);
							metrics.AddLookup(Nfs3Utils.GetElapsedTime(startTime));
						}
						else
						{
							if (nfsproc3 == Nfs3Constant.NFSPROC3.Access)
							{
								response = Access(xdr, info);
								metrics.AddAccess(Nfs3Utils.GetElapsedTime(startTime));
							}
							else
							{
								if (nfsproc3 == Nfs3Constant.NFSPROC3.Readlink)
								{
									response = Readlink(xdr, info);
									metrics.AddReadlink(Nfs3Utils.GetElapsedTime(startTime));
								}
								else
								{
									if (nfsproc3 == Nfs3Constant.NFSPROC3.Read)
									{
										if (Log.IsDebugEnabled())
										{
											Log.Debug(Nfs3Utils.ReadRpcStart + xid);
										}
										response = Read(xdr, info);
										if (Log.IsDebugEnabled() && (nfsproc3 == Nfs3Constant.NFSPROC3.Read))
										{
											Log.Debug(Nfs3Utils.ReadRpcEnd + xid);
										}
										metrics.AddRead(Nfs3Utils.GetElapsedTime(startTime));
									}
									else
									{
										if (nfsproc3 == Nfs3Constant.NFSPROC3.Write)
										{
											if (Log.IsDebugEnabled())
											{
												Log.Debug(Nfs3Utils.WriteRpcStart + xid);
											}
											response = Write(xdr, info);
										}
										else
										{
											// Write end debug trace is in Nfs3Utils.writeChannel
											if (nfsproc3 == Nfs3Constant.NFSPROC3.Create)
											{
												response = Create(xdr, info);
												metrics.AddCreate(Nfs3Utils.GetElapsedTime(startTime));
											}
											else
											{
												if (nfsproc3 == Nfs3Constant.NFSPROC3.Mkdir)
												{
													response = Mkdir(xdr, info);
													metrics.AddMkdir(Nfs3Utils.GetElapsedTime(startTime));
												}
												else
												{
													if (nfsproc3 == Nfs3Constant.NFSPROC3.Symlink)
													{
														response = Symlink(xdr, info);
														metrics.AddSymlink(Nfs3Utils.GetElapsedTime(startTime));
													}
													else
													{
														if (nfsproc3 == Nfs3Constant.NFSPROC3.Mknod)
														{
															response = Mknod(xdr, info);
															metrics.AddMknod(Nfs3Utils.GetElapsedTime(startTime));
														}
														else
														{
															if (nfsproc3 == Nfs3Constant.NFSPROC3.Remove)
															{
																response = Remove(xdr, info);
																metrics.AddRemove(Nfs3Utils.GetElapsedTime(startTime));
															}
															else
															{
																if (nfsproc3 == Nfs3Constant.NFSPROC3.Rmdir)
																{
																	response = Rmdir(xdr, info);
																	metrics.AddRmdir(Nfs3Utils.GetElapsedTime(startTime));
																}
																else
																{
																	if (nfsproc3 == Nfs3Constant.NFSPROC3.Rename)
																	{
																		response = Rename(xdr, info);
																		metrics.AddRename(Nfs3Utils.GetElapsedTime(startTime));
																	}
																	else
																	{
																		if (nfsproc3 == Nfs3Constant.NFSPROC3.Link)
																		{
																			response = Link(xdr, info);
																			metrics.AddLink(Nfs3Utils.GetElapsedTime(startTime));
																		}
																		else
																		{
																			if (nfsproc3 == Nfs3Constant.NFSPROC3.Readdir)
																			{
																				response = Readdir(xdr, info);
																				metrics.AddReaddir(Nfs3Utils.GetElapsedTime(startTime));
																			}
																			else
																			{
																				if (nfsproc3 == Nfs3Constant.NFSPROC3.Readdirplus)
																				{
																					response = Readdirplus(xdr, info);
																					metrics.AddReaddirplus(Nfs3Utils.GetElapsedTime(startTime));
																				}
																				else
																				{
																					if (nfsproc3 == Nfs3Constant.NFSPROC3.Fsstat)
																					{
																						response = Fsstat(xdr, info);
																						metrics.AddFsstat(Nfs3Utils.GetElapsedTime(startTime));
																					}
																					else
																					{
																						if (nfsproc3 == Nfs3Constant.NFSPROC3.Fsinfo)
																						{
																							response = Fsinfo(xdr, info);
																							metrics.AddFsinfo(Nfs3Utils.GetElapsedTime(startTime));
																						}
																						else
																						{
																							if (nfsproc3 == Nfs3Constant.NFSPROC3.Pathconf)
																							{
																								response = Pathconf(xdr, info);
																								metrics.AddPathconf(Nfs3Utils.GetElapsedTime(startTime));
																							}
																							else
																							{
																								if (nfsproc3 == Nfs3Constant.NFSPROC3.Commit)
																								{
																									response = Commit(xdr, info);
																								}
																								else
																								{
																									// Invalid procedure
																									RpcAcceptedReply.GetInstance(xid, RpcAcceptedReply.AcceptState.ProcUnavail, new VerifierNone
																										()).Write(@out);
																								}
																							}
																						}
																					}
																				}
																			}
																		}
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			if (response == null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("No sync response, expect an async response for request XID=" + rpcCall
						.GetXid());
				}
				return;
			}
			// TODO: currently we just return VerifierNone
			@out = response.Serialize(@out, xid, new VerifierNone());
			ChannelBuffer buf_1 = ChannelBuffers.WrappedBuffer(@out.AsReadOnlyWrap().Buffer()
				);
			RpcResponse rsp_1 = new RpcResponse(buf_1, info.RemoteAddress());
			if (!IsIdempotent(rpcCall))
			{
				rpcCallCache.CallCompleted(client, xid, rsp_1);
			}
			RpcUtil.SendRpcResponse(ctx, rsp_1);
		}

		protected override bool IsIdempotent(RpcCall call)
		{
			Nfs3Constant.NFSPROC3 nfsproc3 = Nfs3Constant.NFSPROC3.FromValue(call.GetProcedure
				());
			return nfsproc3 == null || nfsproc3.IsIdempotent();
		}

		private bool CheckAccessPrivilege(EndPoint remoteAddress, AccessPrivilege expected
			)
		{
			// Port monitoring
			if (!DoPortMonitoring(remoteAddress))
			{
				return false;
			}
			// Check export table
			if (exports == null)
			{
				return false;
			}
			IPAddress client = ((IPEndPoint)remoteAddress).Address;
			AccessPrivilege access = exports.GetAccessPrivilege(client);
			if (access == AccessPrivilege.None)
			{
				return false;
			}
			if (access == AccessPrivilege.ReadOnly && expected == AccessPrivilege.ReadWrite)
			{
				return false;
			}
			return true;
		}

		[VisibleForTesting]
		internal virtual WriteManager GetWriteManager()
		{
			return this.writeManager;
		}
	}
}
