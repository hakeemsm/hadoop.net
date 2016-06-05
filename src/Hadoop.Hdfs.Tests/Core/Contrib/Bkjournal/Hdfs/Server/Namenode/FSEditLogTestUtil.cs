using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Utilities for testing edit logs</summary>
	public class FSEditLogTestUtil
	{
		private static FSEditLogOp.OpInstanceCache cache = new FSEditLogOp.OpInstanceCache
			();

		public static FSEditLogOp GetNoOpInstance()
		{
			return FSEditLogOp.LogSegmentOp.GetInstance(cache, FSEditLogOpCodes.OpEndLogSegment
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public static long CountTransactionsInStream(EditLogInputStream @in)
		{
			FSEditLogLoader.EditLogValidation validation = FSEditLogLoader.ValidateEditLog(@in
				);
			return (validation.GetEndTxId() - @in.GetFirstTxId()) + 1;
		}
	}
}
