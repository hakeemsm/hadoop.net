using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// An implementation of the abstract class
	/// <see cref="EditLogOutputStream"/>
	/// ,
	/// which streams edits to a backup node.
	/// </summary>
	/// <seealso cref="org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol#journal(org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration,int,int,byte[])
	/// 	"/>
	internal class EditLogBackupOutputStream : EditLogOutputStream
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(EditLogFileOutputStream
			));

		internal const int DefaultBufferSize = 256;

		private readonly JournalProtocol backupNode;

		private readonly NamenodeRegistration bnRegistration;

		private readonly JournalInfo journalInfo;

		private readonly DataOutputBuffer @out;

		private EditsDoubleBuffer doubleBuf;

		/// <exception cref="System.IO.IOException"/>
		internal EditLogBackupOutputStream(NamenodeRegistration bnReg, JournalInfo journalInfo
			)
			: base()
		{
			// RPC proxy to backup node
			// backup node registration
			// active node registration
			// serialized output sent to backup node
			// backup node
			// active name-node
			this.bnRegistration = bnReg;
			this.journalInfo = journalInfo;
			IPEndPoint bnAddress = NetUtils.CreateSocketAddr(bnRegistration.GetAddress());
			try
			{
				this.backupNode = NameNodeProxies.CreateNonHAProxy<JournalProtocol>(new HdfsConfiguration
					(), bnAddress, UserGroupInformation.GetCurrentUser(), true).GetProxy();
			}
			catch (IOException e)
			{
				Storage.Log.Error("Error connecting to: " + bnAddress, e);
				throw;
			}
			this.doubleBuf = new EditsDoubleBuffer(DefaultBufferSize);
			this.@out = new DataOutputBuffer(DefaultBufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(FSEditLogOp op)
		{
			// EditLogOutputStream
			doubleBuf.WriteOp(op);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void WriteRaw(byte[] bytes, int offset, int length)
		{
			throw new IOException("Not supported");
		}

		/// <summary>There is no persistent storage.</summary>
		/// <remarks>There is no persistent storage. Just clear the buffers.</remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void Create(int layoutVersion)
		{
			// EditLogOutputStream
			System.Diagnostics.Debug.Assert(doubleBuf.IsFlushed(), "previous data is not flushed yet"
				);
			this.doubleBuf = new EditsDoubleBuffer(DefaultBufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			// EditLogOutputStream
			// close should have been called after all pending transactions 
			// have been flushed & synced.
			int size = doubleBuf.CountBufferedBytes();
			if (size != 0)
			{
				throw new IOException("BackupEditStream has " + size + " records still to be flushed and cannot be closed."
					);
			}
			RPC.StopProxy(backupNode);
			// stop the RPC threads
			doubleBuf.Close();
			doubleBuf = null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Abort()
		{
			RPC.StopProxy(backupNode);
			doubleBuf = null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetReadyToFlush()
		{
			// EditLogOutputStream
			doubleBuf.SetReadyToFlush();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void FlushAndSync(bool durable)
		{
			// EditLogOutputStream
			System.Diagnostics.Debug.Assert(@out.GetLength() == 0, "Output buffer is not empty"
				);
			if (doubleBuf.IsFlushed())
			{
				Log.Info("Nothing to flush");
				return;
			}
			int numReadyTxns = doubleBuf.CountReadyTxns();
			long firstTxToFlush = doubleBuf.GetFirstReadyTxId();
			doubleBuf.FlushTo(@out);
			if (@out.GetLength() > 0)
			{
				System.Diagnostics.Debug.Assert(numReadyTxns > 0);
				byte[] data = Arrays.CopyOf(@out.GetData(), @out.GetLength());
				@out.Reset();
				System.Diagnostics.Debug.Assert(@out.GetLength() == 0, "Output buffer is not empty"
					);
				backupNode.Journal(journalInfo, 0, firstTxToFlush, numReadyTxns, data);
			}
		}

		/// <summary>Get backup node registration.</summary>
		internal virtual NamenodeRegistration GetRegistration()
		{
			return bnRegistration;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void StartLogSegment(long txId)
		{
			backupNode.StartLogSegment(journalInfo, 0, txId);
		}
	}
}
