using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Nfs;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Nfs.Nfs3.Response;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Security;
using Org.Jboss.Netty.Buffer;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>Utility/helper methods related to NFS</summary>
	public class Nfs3Utils
	{
		public const string InodeidPathPrefix = "/.reserved/.inodes/";

		public const string ReadRpcStart = "READ_RPC_CALL_START____";

		public const string ReadRpcEnd = "READ_RPC_CALL_END______";

		public const string WriteRpcStart = "WRITE_RPC_CALL_START____";

		public const string WriteRpcEnd = "WRITE_RPC_CALL_END______";

		public static string GetFileIdPath(FileHandle handle)
		{
			return GetFileIdPath(handle.GetFileId());
		}

		public static string GetFileIdPath(long fileId)
		{
			return InodeidPathPrefix + fileId;
		}

		/// <exception cref="System.IO.IOException"/>
		public static HdfsFileStatus GetFileStatus(DFSClient client, string fileIdPath)
		{
			return client.GetFileLinkInfo(fileIdPath);
		}

		public static Nfs3FileAttributes GetNfs3FileAttrFromFileStatus(HdfsFileStatus fs, 
			IdMappingServiceProvider iug)
		{
			NfsFileType fileType = fs.IsDir() ? NfsFileType.Nfsdir : NfsFileType.Nfsreg;
			fileType = fs.IsSymlink() ? NfsFileType.Nfslnk : fileType;
			int nlink = (fileType == NfsFileType.Nfsdir) ? fs.GetChildrenNum() + 2 : 1;
			long size = (fileType == NfsFileType.Nfsdir) ? GetDirSize(fs.GetChildrenNum()) : 
				fs.GetLen();
			return new Nfs3FileAttributes(fileType, nlink, fs.GetPermission().ToShort(), iug.
				GetUidAllowingUnknown(fs.GetOwner()), iug.GetGidAllowingUnknown(fs.GetGroup()), 
				size, 0, fs.GetFileId(), fs.GetModificationTime(), fs.GetAccessTime(), new Nfs3FileAttributes.Specdata3
				());
		}

		/* fsid */
		/// <exception cref="System.IO.IOException"/>
		public static Nfs3FileAttributes GetFileAttr(DFSClient client, string fileIdPath, 
			IdMappingServiceProvider iug)
		{
			HdfsFileStatus fs = GetFileStatus(client, fileIdPath);
			return fs == null ? null : GetNfs3FileAttrFromFileStatus(fs, iug);
		}

		/// <summary>HDFS directory size is always zero.</summary>
		/// <remarks>
		/// HDFS directory size is always zero. Try to return something meaningful
		/// here. Assume each child take 32bytes.
		/// </remarks>
		public static long GetDirSize(int childNum)
		{
			return (childNum + 2) * 32;
		}

		/// <exception cref="System.IO.IOException"/>
		public static WccAttr GetWccAttr(DFSClient client, string fileIdPath)
		{
			HdfsFileStatus fstat = GetFileStatus(client, fileIdPath);
			if (fstat == null)
			{
				return null;
			}
			long size = fstat.IsDir() ? GetDirSize(fstat.GetChildrenNum()) : fstat.GetLen();
			return new WccAttr(size, new NfsTime(fstat.GetModificationTime()), new NfsTime(fstat
				.GetModificationTime()));
		}

		public static WccAttr GetWccAttr(Nfs3FileAttributes attr)
		{
			return attr == null ? new WccAttr() : new WccAttr(attr.GetSize(), attr.GetMtime()
				, attr.GetCtime());
		}

		// TODO: maybe not efficient
		/// <exception cref="System.IO.IOException"/>
		public static WccData CreateWccData(WccAttr preOpAttr, DFSClient dfsClient, string
			 fileIdPath, IdMappingServiceProvider iug)
		{
			Nfs3FileAttributes postOpDirAttr = GetFileAttr(dfsClient, fileIdPath, iug);
			return new WccData(preOpAttr, postOpDirAttr);
		}

		/// <summary>Send a write response to the netty network socket channel</summary>
		public static void WriteChannel(Org.Jboss.Netty.Channel.Channel channel, XDR @out
			, int xid)
		{
			if (channel == null)
			{
				RpcProgramNfs3.Log.Info("Null channel should only happen in tests. Do nothing.");
				return;
			}
			if (RpcProgramNfs3.Log.IsDebugEnabled())
			{
				RpcProgramNfs3.Log.Debug(WriteRpcEnd + xid);
			}
			ChannelBuffer outBuf = XDR.WriteMessageTcp(@out, true);
			channel.Write(outBuf);
		}

		public static void WriteChannelCommit(Org.Jboss.Netty.Channel.Channel channel, XDR
			 @out, int xid)
		{
			if (RpcProgramNfs3.Log.IsDebugEnabled())
			{
				RpcProgramNfs3.Log.Debug("Commit done:" + xid);
			}
			ChannelBuffer outBuf = XDR.WriteMessageTcp(@out, true);
			channel.Write(outBuf);
		}

		private static bool IsSet(int access, int bits)
		{
			return (access & bits) == bits;
		}

		public static int GetAccessRights(int mode, int type)
		{
			int rtn = 0;
			if (IsSet(mode, Nfs3Constant.AccessModeRead))
			{
				rtn |= Nfs3Constant.Access3Read;
				// LOOKUP is only meaningful for dir
				if (type == NfsFileType.Nfsdir.ToValue())
				{
					rtn |= Nfs3Constant.Access3Lookup;
				}
			}
			if (IsSet(mode, Nfs3Constant.AccessModeWrite))
			{
				rtn |= Nfs3Constant.Access3Modify;
				rtn |= Nfs3Constant.Access3Extend;
				// Set delete bit, UNIX may ignore it for regular file since it's up to
				// parent dir op permission
				rtn |= Nfs3Constant.Access3Delete;
			}
			if (IsSet(mode, Nfs3Constant.AccessModeExecute))
			{
				if (type == NfsFileType.Nfsreg.ToValue())
				{
					rtn |= Nfs3Constant.Access3Execute;
				}
				else
				{
					rtn |= Nfs3Constant.Access3Lookup;
				}
			}
			return rtn;
		}

		public static int GetAccessRightsForUserGroup(int uid, int gid, int[] auxGids, Nfs3FileAttributes
			 attr)
		{
			int mode = attr.GetMode();
			if (uid == attr.GetUid())
			{
				return GetAccessRights(mode >> 6, attr.GetType());
			}
			if (gid == attr.GetGid())
			{
				return GetAccessRights(mode >> 3, attr.GetType());
			}
			// Check for membership in auxiliary groups
			if (auxGids != null)
			{
				foreach (int auxGid in auxGids)
				{
					if (attr.GetGid() == auxGid)
					{
						return GetAccessRights(mode >> 3, attr.GetType());
					}
				}
			}
			return GetAccessRights(mode, attr.GetType());
		}

		public static long BytesToLong(byte[] data)
		{
			long n = unchecked((long)(0xffL)) & data[0];
			for (int i = 1; i < 8; i++)
			{
				n = (n << 8) | (unchecked((long)(0xffL)) & data[i]);
			}
			return n;
		}

		public static byte[] LongToByte(long v)
		{
			byte[] data = new byte[8];
			data[0] = unchecked((byte)((long)(((ulong)v) >> 56)));
			data[1] = unchecked((byte)((long)(((ulong)v) >> 48)));
			data[2] = unchecked((byte)((long)(((ulong)v) >> 40)));
			data[3] = unchecked((byte)((long)(((ulong)v) >> 32)));
			data[4] = unchecked((byte)((long)(((ulong)v) >> 24)));
			data[5] = unchecked((byte)((long)(((ulong)v) >> 16)));
			data[6] = unchecked((byte)((long)(((ulong)v) >> 8)));
			data[7] = unchecked((byte)((long)(((ulong)v) >> 0)));
			return data;
		}

		public static long GetElapsedTime(long startTimeNano)
		{
			return Runtime.NanoTime() - startTimeNano;
		}
	}
}
