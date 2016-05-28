using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Protocol
{
	public class RequestInfo
	{
		private readonly string jid;

		private long epoch;

		private long ipcSerialNumber;

		private readonly long committedTxId;

		public RequestInfo(string jid, long epoch, long ipcSerialNumber, long committedTxId
			)
		{
			this.jid = jid;
			this.epoch = epoch;
			this.ipcSerialNumber = ipcSerialNumber;
			this.committedTxId = committedTxId;
		}

		public virtual long GetEpoch()
		{
			return epoch;
		}

		public virtual void SetEpoch(long epoch)
		{
			this.epoch = epoch;
		}

		public virtual string GetJournalId()
		{
			return jid;
		}

		public virtual long GetIpcSerialNumber()
		{
			return ipcSerialNumber;
		}

		public virtual void SetIpcSerialNumber(long ipcSerialNumber)
		{
			this.ipcSerialNumber = ipcSerialNumber;
		}

		public virtual long GetCommittedTxId()
		{
			return committedTxId;
		}

		public virtual bool HasCommittedTxId()
		{
			return (committedTxId != HdfsConstants.InvalidTxid);
		}
	}
}
