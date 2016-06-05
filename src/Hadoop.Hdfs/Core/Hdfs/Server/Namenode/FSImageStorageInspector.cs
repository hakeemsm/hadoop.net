using System.Collections.Generic;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Interface responsible for inspecting a set of storage directories and devising
	/// a plan to load the namespace from them.
	/// </summary>
	internal abstract class FSImageStorageInspector
	{
		/// <summary>Inspect the contents of the given storage directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal abstract void InspectDirectory(Storage.StorageDirectory sd);

		/// <returns>false if any of the storage directories have an unfinalized upgrade</returns>
		internal abstract bool IsUpgradeFinalized();

		/// <summary>Get the image files which should be loaded into the filesystem.</summary>
		/// <exception cref="System.IO.IOException">if not enough files are available (eg no image found in any directory)
		/// 	</exception>
		internal abstract IList<FSImageStorageInspector.FSImageFile> GetLatestImages();

		/// <summary>Get the minimum tx id which should be loaded with this set of images.</summary>
		internal abstract long GetMaxSeenTxId();

		/// <returns>
		/// true if the directories are in such a state that the image should be re-saved
		/// following the load
		/// </returns>
		internal abstract bool NeedToSave();

		/// <summary>Record of an image that has been located and had its filename parsed.</summary>
		internal class FSImageFile
		{
			internal readonly Storage.StorageDirectory sd;

			internal readonly long txId;

			private readonly FilePath file;

			internal FSImageFile(Storage.StorageDirectory sd, FilePath file, long txId)
			{
				System.Diagnostics.Debug.Assert(txId >= 0 || txId == HdfsConstants.InvalidTxid, "Invalid txid on "
					 + file + ": " + txId);
				this.sd = sd;
				this.txId = txId;
				this.file = file;
			}

			internal virtual FilePath GetFile()
			{
				return file;
			}

			public virtual long GetCheckpointTxId()
			{
				return txId;
			}

			public override string ToString()
			{
				return string.Format("FSImageFile(file=%s, cpktTxId=%019d)", file.ToString(), txId
					);
			}
		}
	}
}
