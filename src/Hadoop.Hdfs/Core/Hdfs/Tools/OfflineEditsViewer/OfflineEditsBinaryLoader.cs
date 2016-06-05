using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineEditsViewer
{
	/// <summary>OfflineEditsBinaryLoader loads edits from a binary edits file</summary>
	internal class OfflineEditsBinaryLoader : OfflineEditsLoader
	{
		private readonly OfflineEditsVisitor visitor;

		private readonly EditLogInputStream inputStream;

		private readonly bool fixTxIds;

		private readonly bool recoveryMode;

		private long nextTxId;

		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Tools.OfflineEditsViewer.OfflineEditsBinaryLoader
			).FullName);

		/// <summary>Constructor</summary>
		public OfflineEditsBinaryLoader(OfflineEditsVisitor visitor, EditLogInputStream inputStream
			, OfflineEditsViewer.Flags flags)
		{
			this.visitor = visitor;
			this.inputStream = inputStream;
			this.fixTxIds = flags.GetFixTxIds();
			this.recoveryMode = flags.GetRecoveryMode();
			this.nextTxId = -1;
		}

		/// <summary>Loads edits file, uses visitor to process all elements</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void LoadEdits()
		{
			try
			{
				visitor.Start(inputStream.GetVersion(true));
				while (true)
				{
					try
					{
						FSEditLogOp op = inputStream.ReadOp();
						if (op == null)
						{
							break;
						}
						if (fixTxIds)
						{
							if (nextTxId <= 0)
							{
								nextTxId = op.GetTransactionId();
								if (nextTxId <= 0)
								{
									nextTxId = 1;
								}
							}
							op.SetTransactionId(nextTxId);
							nextTxId++;
						}
						visitor.VisitOp(op);
					}
					catch (IOException e)
					{
						if (!recoveryMode)
						{
							// Tell the visitor to clean up, then re-throw the exception
							Log.Error("Got IOException at position " + inputStream.GetPosition());
							visitor.Close(e);
							throw;
						}
						Log.Error("Got IOException while reading stream!  Resyncing.", e);
						inputStream.Resync();
					}
					catch (RuntimeException e)
					{
						if (!recoveryMode)
						{
							// Tell the visitor to clean up, then re-throw the exception
							Log.Error("Got RuntimeException at position " + inputStream.GetPosition());
							visitor.Close(e);
							throw;
						}
						Log.Error("Got RuntimeException while reading stream!  Resyncing.", e);
						inputStream.Resync();
					}
				}
				visitor.Close(null);
			}
			finally
			{
				IOUtils.Cleanup(Log, inputStream);
			}
		}
	}
}
