using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// An implementation of the abstract class
	/// <see cref="EditLogInputStream"/>
	/// ,
	/// which is used to updates HDFS meta-data state on a backup node.
	/// </summary>
	/// <seealso cref="org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol#journal(org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration,int,int,byte[])
	/// 	"/>
	internal class EditLogBackupInputStream : EditLogInputStream
	{
		internal readonly string address;

		private readonly EditLogBackupInputStream.ByteBufferInputStream inner;

		private DataInputStream @in;

		private FSEditLogOp.Reader reader = null;

		private FSEditLogLoader.PositionTrackingInputStream tracker = null;

		private int version = 0;

		/// <summary>A ByteArrayInputStream, which lets modify the underlying byte array.</summary>
		private class ByteBufferInputStream : ByteArrayInputStream
		{
			internal ByteBufferInputStream()
				: base(new byte[0])
			{
			}

			// sender address
			internal virtual void SetData(byte[] newBytes)
			{
				base.buf = newBytes;
				base.count = newBytes == null ? 0 : newBytes.Length;
				base.mark = 0;
				Reset();
			}

			/// <summary>Number of bytes read from the stream so far.</summary>
			internal virtual int Length()
			{
				return count;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal EditLogBackupInputStream(string name)
		{
			address = name;
			inner = new EditLogBackupInputStream.ByteBufferInputStream();
			@in = null;
			reader = null;
		}

		public override string GetName()
		{
			return address;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override FSEditLogOp NextOp()
		{
			Preconditions.CheckState(reader != null, "Must call setBytes() before readOp()");
			return reader.ReadOp(false);
		}

		protected internal override FSEditLogOp NextValidOp()
		{
			try
			{
				return reader.ReadOp(true);
			}
			catch (IOException e)
			{
				throw new RuntimeException("got unexpected IOException " + e, e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int GetVersion(bool verifyVersion)
		{
			return this.version;
		}

		public override long GetPosition()
		{
			return tracker.GetPos();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			@in.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override long Length()
		{
			// file size + size of both buffers
			return inner.Length();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetBytes(byte[] newBytes, int version)
		{
			inner.SetData(newBytes);
			tracker = new FSEditLogLoader.PositionTrackingInputStream(inner);
			@in = new DataInputStream(tracker);
			this.version = version;
			reader = new FSEditLogOp.Reader(@in, tracker, version);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Clear()
		{
			SetBytes(null, 0);
			reader = null;
			this.version = 0;
		}

		public override long GetFirstTxId()
		{
			return HdfsConstants.InvalidTxid;
		}

		public override long GetLastTxId()
		{
			return HdfsConstants.InvalidTxid;
		}

		public override bool IsInProgress()
		{
			return true;
		}

		public override void SetMaxOpSize(int maxOpSize)
		{
			reader.SetMaxOpSize(maxOpSize);
		}

		public override bool IsLocalLog()
		{
			return true;
		}
	}
}
