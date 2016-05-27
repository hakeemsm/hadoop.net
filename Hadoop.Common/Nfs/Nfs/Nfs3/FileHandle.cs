using System.Text;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3
{
	/// <summary>This is a file handle use by the NFS clients.</summary>
	/// <remarks>
	/// This is a file handle use by the NFS clients.
	/// Server returns this handle to the client, which is used by the client
	/// on subsequent operations to reference the file.
	/// </remarks>
	public class FileHandle
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Nfs.Nfs3.FileHandle
			));

		private const string Hexes = "0123456789abcdef";

		private const int HandleLen = 32;

		private byte[] handle;

		private long fileId = -1;

		public FileHandle()
		{
			// Opaque handle
			handle = null;
		}

		/// <summary>Handle is a 32 bytes number.</summary>
		/// <remarks>Handle is a 32 bytes number. For HDFS, the last 8 bytes is fileId.</remarks>
		public FileHandle(long v)
		{
			fileId = v;
			handle = new byte[HandleLen];
			handle[0] = unchecked((byte)((long)(((ulong)v) >> 56)));
			handle[1] = unchecked((byte)((long)(((ulong)v) >> 48)));
			handle[2] = unchecked((byte)((long)(((ulong)v) >> 40)));
			handle[3] = unchecked((byte)((long)(((ulong)v) >> 32)));
			handle[4] = unchecked((byte)((long)(((ulong)v) >> 24)));
			handle[5] = unchecked((byte)((long)(((ulong)v) >> 16)));
			handle[6] = unchecked((byte)((long)(((ulong)v) >> 8)));
			handle[7] = unchecked((byte)((long)(((ulong)v) >> 0)));
			for (int i = 8; i < HandleLen; i++)
			{
				handle[i] = unchecked((byte)0);
			}
		}

		public FileHandle(string s)
		{
			MessageDigest digest;
			try
			{
				digest = MessageDigest.GetInstance("MD5");
				handle = new byte[HandleLen];
			}
			catch (NoSuchAlgorithmException)
			{
				Log.Warn("MD5 MessageDigest unavailable.");
				handle = null;
				return;
			}
			byte[] @in = Sharpen.Runtime.GetBytesForString(s, Charsets.Utf8);
			digest.Update(@in);
			byte[] digestbytes = digest.Digest();
			for (int i = 0; i < 16; i++)
			{
				handle[i] = unchecked((byte)0);
			}
			for (int i_1 = 16; i_1 < 32; i_1++)
			{
				handle[i_1] = digestbytes[i_1 - 16];
			}
		}

		public virtual bool Serialize(XDR @out)
		{
			@out.WriteInt(handle.Length);
			@out.WriteFixedOpaque(handle);
			return true;
		}

		private long BytesToLong(byte[] data)
		{
			ByteBuffer buffer = ByteBuffer.Allocate(8);
			for (int i = 0; i < 8; i++)
			{
				buffer.Put(data[i]);
			}
			buffer.Flip();
			// need flip
			return buffer.GetLong();
		}

		public virtual bool Deserialize(XDR xdr)
		{
			if (!XDR.VerifyLength(xdr, 32))
			{
				return false;
			}
			int size = xdr.ReadInt();
			handle = xdr.ReadFixedOpaque(size);
			fileId = BytesToLong(handle);
			return true;
		}

		private static string Hex(byte b)
		{
			StringBuilder strBuilder = new StringBuilder();
			strBuilder.Append(Hexes[(b & unchecked((int)(0xF0))) >> 4]).Append(Hexes[(b & unchecked(
				(int)(0x0F)))]);
			return strBuilder.ToString();
		}

		public virtual long GetFileId()
		{
			return fileId;
		}

		public virtual byte[] GetContent()
		{
			return handle.MemberwiseClone();
		}

		public override string ToString()
		{
			StringBuilder s = new StringBuilder();
			for (int i = 0; i < handle.Length; i++)
			{
				s.Append(Hex(handle[i]));
			}
			return s.ToString();
		}

		public override bool Equals(object o)
		{
			if (this == o)
			{
				return true;
			}
			if (!(o is Org.Apache.Hadoop.Nfs.Nfs3.FileHandle))
			{
				return false;
			}
			Org.Apache.Hadoop.Nfs.Nfs3.FileHandle h = (Org.Apache.Hadoop.Nfs.Nfs3.FileHandle)
				o;
			return Arrays.Equals(handle, h.handle);
		}

		public override int GetHashCode()
		{
			return Arrays.HashCode(handle);
		}
	}
}
