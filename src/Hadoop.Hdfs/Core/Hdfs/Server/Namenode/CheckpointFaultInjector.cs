using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Utility class to faciliate some fault injection tests for the checkpointing
	/// process.
	/// </summary>
	internal class CheckpointFaultInjector
	{
		internal static CheckpointFaultInjector instance = new CheckpointFaultInjector();

		internal static CheckpointFaultInjector GetInstance()
		{
			return instance;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void BeforeGetImageSetsHeaders()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AfterSecondaryCallsRollEditLog()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DuringMerge()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AfterSecondaryUploadsNewImage()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AboutToSendFile(FilePath localfile)
		{
		}

		public virtual bool ShouldSendShortFile(FilePath localfile)
		{
			return false;
		}

		public virtual bool ShouldCorruptAByte(FilePath localfile)
		{
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AfterMD5Rename()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void BeforeEditsRename()
		{
		}
	}
}
