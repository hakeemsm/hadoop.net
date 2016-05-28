using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Mount;
using Org.Apache.Hadoop.Nfs;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Org.Apache.Hadoop.Security;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Mount
{
	/// <summary>RPC program corresponding to mountd daemon.</summary>
	/// <remarks>
	/// RPC program corresponding to mountd daemon. See
	/// <see cref="Mountd"/>
	/// .
	/// </remarks>
	public class RpcProgramMountd : RpcProgram, MountInterface
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Nfs.Mount.RpcProgramMountd
			));

		public const int Program = 100005;

		public const int Version1 = 1;

		public const int Version2 = 2;

		public const int Version3 = 3;

		private readonly DFSClient dfsClient;

		/// <summary>Synchronized list</summary>
		private readonly IList<MountEntry> mounts;

		/// <summary>List that is unmodifiable</summary>
		private readonly IList<string> exports;

		private readonly NfsExports hostsMatcher;

		/// <exception cref="System.IO.IOException"/>
		public RpcProgramMountd(NfsConfiguration config, DatagramSocket registrationSocket
			, bool allowInsecurePorts)
			: base("mountd", "localhost", config.GetInt(NfsConfigKeys.DfsNfsMountdPortKey, NfsConfigKeys
				.DfsNfsMountdPortDefault), Program, Version1, Version3, registrationSocket, allowInsecurePorts
				)
		{
			// Note that RPC cache is not enabled
			exports = new AList<string>();
			exports.AddItem(config.Get(NfsConfigKeys.DfsNfsExportPointKey, NfsConfigKeys.DfsNfsExportPointDefault
				));
			this.hostsMatcher = NfsExports.GetInstance(config);
			this.mounts = Sharpen.Collections.SynchronizedList(new AList<MountEntry>());
			UserGroupInformation.SetConfiguration(config);
			SecurityUtil.Login(config, NfsConfigKeys.DfsNfsKeytabFileKey, NfsConfigKeys.DfsNfsKerberosPrincipalKey
				);
			this.dfsClient = new DFSClient(NameNode.GetAddress(config), config);
		}

		public override XDR NullOp(XDR @out, int xid, IPAddress client)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("MOUNT NULLOP : " + " client: " + client);
			}
			return RpcAcceptedReply.GetAcceptInstance(xid, new VerifierNone()).Write(@out);
		}

		public override XDR Mnt(XDR xdr, XDR @out, int xid, IPAddress client)
		{
			if (hostsMatcher == null)
			{
				return MountResponse.WriteMNTResponse(Nfs3Status.Nfs3errAcces, @out, xid, null);
			}
			AccessPrivilege accessPrivilege = hostsMatcher.GetAccessPrivilege(client);
			if (accessPrivilege == AccessPrivilege.None)
			{
				return MountResponse.WriteMNTResponse(Nfs3Status.Nfs3errAcces, @out, xid, null);
			}
			string path = xdr.ReadString();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("MOUNT MNT path: " + path + " client: " + client);
			}
			string host = client.GetHostName();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Got host: " + host + " path: " + path);
			}
			if (!exports.Contains(path))
			{
				Log.Info("Path " + path + " is not shared.");
				MountResponse.WriteMNTResponse(Nfs3Status.Nfs3errNoent, @out, xid, null);
				return @out;
			}
			FileHandle handle = null;
			try
			{
				HdfsFileStatus exFileStatus = dfsClient.GetFileInfo(path);
				handle = new FileHandle(exFileStatus.GetFileId());
			}
			catch (IOException e)
			{
				Log.Error("Can't get handle for export:" + path, e);
				MountResponse.WriteMNTResponse(Nfs3Status.Nfs3errNoent, @out, xid, null);
				return @out;
			}
			System.Diagnostics.Debug.Assert((handle != null));
			Log.Info("Giving handle (fileId:" + handle.GetFileId() + ") to client for export "
				 + path);
			mounts.AddItem(new MountEntry(host, path));
			MountResponse.WriteMNTResponse(Nfs3Status.Nfs3Ok, @out, xid, handle.GetContent());
			return @out;
		}

		public override XDR Dump(XDR @out, int xid, IPAddress client)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("MOUNT NULLOP : " + " client: " + client);
			}
			IList<MountEntry> copy = new AList<MountEntry>(mounts);
			MountResponse.WriteMountList(@out, xid, copy);
			return @out;
		}

		public override XDR Umnt(XDR xdr, XDR @out, int xid, IPAddress client)
		{
			string path = xdr.ReadString();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("MOUNT UMNT path: " + path + " client: " + client);
			}
			string host = client.GetHostName();
			mounts.Remove(new MountEntry(host, path));
			RpcAcceptedReply.GetAcceptInstance(xid, new VerifierNone()).Write(@out);
			return @out;
		}

		public override XDR Umntall(XDR @out, int xid, IPAddress client)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("MOUNT UMNTALL : " + " client: " + client);
			}
			mounts.Clear();
			return RpcAcceptedReply.GetAcceptInstance(xid, new VerifierNone()).Write(@out);
		}

		protected override void HandleInternal(ChannelHandlerContext ctx, RpcInfo info)
		{
			RpcCall rpcCall = (RpcCall)info.Header();
			MountInterface.MNTPROC mntproc = MountInterface.MNTPROC.FromValue(rpcCall.GetProcedure
				());
			int xid = rpcCall.GetXid();
			byte[] data = new byte[info.Data().ReadableBytes()];
			info.Data().ReadBytes(data);
			XDR xdr = new XDR(data);
			XDR @out = new XDR();
			IPAddress client = ((IPEndPoint)info.RemoteAddress()).Address;
			if (mntproc == MountInterface.MNTPROC.Null)
			{
				@out = NullOp(@out, xid, client);
			}
			else
			{
				if (mntproc == MountInterface.MNTPROC.Mnt)
				{
					// Only do port monitoring for MNT
					if (!DoPortMonitoring(info.RemoteAddress()))
					{
						@out = MountResponse.WriteMNTResponse(Nfs3Status.Nfs3errAcces, @out, xid, null);
					}
					else
					{
						@out = Mnt(xdr, @out, xid, client);
					}
				}
				else
				{
					if (mntproc == MountInterface.MNTPROC.Dump)
					{
						@out = Dump(@out, xid, client);
					}
					else
					{
						if (mntproc == MountInterface.MNTPROC.Umnt)
						{
							@out = Umnt(xdr, @out, xid, client);
						}
						else
						{
							if (mntproc == MountInterface.MNTPROC.Umntall)
							{
								Umntall(@out, xid, client);
							}
							else
							{
								if (mntproc == MountInterface.MNTPROC.Export)
								{
									// Currently only support one NFS export
									IList<NfsExports> hostsMatchers = new AList<NfsExports>();
									if (hostsMatcher != null)
									{
										hostsMatchers.AddItem(hostsMatcher);
										@out = MountResponse.WriteExportList(@out, xid, exports, hostsMatchers);
									}
									else
									{
										// This means there are no valid exports provided.
										RpcAcceptedReply.GetInstance(xid, RpcAcceptedReply.AcceptState.ProcUnavail, new VerifierNone
											()).Write(@out);
									}
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
			ChannelBuffer buf = ChannelBuffers.WrappedBuffer(@out.AsReadOnlyWrap().Buffer());
			RpcResponse rsp = new RpcResponse(buf, info.RemoteAddress());
			RpcUtil.SendRpcResponse(ctx, rsp);
		}

		protected override bool IsIdempotent(RpcCall call)
		{
			// Not required, because cache is turned off
			return false;
		}

		[VisibleForTesting]
		public virtual IList<string> GetExports()
		{
			return this.exports;
		}
	}
}
