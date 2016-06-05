using System.Collections.Generic;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Nfs;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Mount
{
	/// <summary>Helper class for sending MountResponse</summary>
	public class MountResponse
	{
		public const int MntOk = 0;

		/// <summary>Hidden constructor</summary>
		private MountResponse()
		{
		}

		/// <summary>
		/// Response for RPC call
		/// <see cref="MNTPROC.Mnt"/>
		/// 
		/// </summary>
		public static XDR WriteMNTResponse(int status, XDR xdr, int xid, byte[] handle)
		{
			RpcAcceptedReply.GetAcceptInstance(xid, new VerifierNone()).Write(xdr);
			xdr.WriteInt(status);
			if (status == MntOk)
			{
				xdr.WriteVariableOpaque(handle);
				// Only MountV3 returns a list of supported authFlavors
				xdr.WriteInt(1);
				xdr.WriteInt(RpcAuthInfo.AuthFlavor.AuthSys.GetValue());
			}
			return xdr;
		}

		/// <summary>
		/// Response for RPC call
		/// <see cref="MNTPROC.Dump"/>
		/// 
		/// </summary>
		public static XDR WriteMountList(XDR xdr, int xid, IList<MountEntry> mounts)
		{
			RpcAcceptedReply.GetAcceptInstance(xid, new VerifierNone()).Write(xdr);
			foreach (MountEntry mountEntry in mounts)
			{
				xdr.WriteBoolean(true);
				// Value follows yes
				xdr.WriteString(mountEntry.GetHost());
				xdr.WriteString(mountEntry.GetPath());
			}
			xdr.WriteBoolean(false);
			// Value follows no
			return xdr;
		}

		/// <summary>
		/// Response for RPC call
		/// <see cref="MNTPROC.Export"/>
		/// 
		/// </summary>
		public static XDR WriteExportList(XDR xdr, int xid, IList<string> exports, IList<
			NfsExports> hostMatcher)
		{
			System.Diagnostics.Debug.Assert((exports.Count == hostMatcher.Count));
			RpcAcceptedReply.GetAcceptInstance(xid, new VerifierNone()).Write(xdr);
			for (int i = 0; i < exports.Count; i++)
			{
				xdr.WriteBoolean(true);
				// Value follows - yes
				xdr.WriteString(exports[i]);
				// List host groups
				string[] hostGroups = hostMatcher[i].GetHostGroupList();
				if (hostGroups.Length > 0)
				{
					for (int j = 0; j < hostGroups.Length; j++)
					{
						xdr.WriteBoolean(true);
						// Value follows - yes
						xdr.WriteVariableOpaque(Runtime.GetBytesForString(hostGroups[j], Charsets
							.Utf8));
					}
				}
				xdr.WriteBoolean(false);
			}
			// Value follows - no more group
			xdr.WriteBoolean(false);
			// Value follows - no
			return xdr;
		}
	}
}
