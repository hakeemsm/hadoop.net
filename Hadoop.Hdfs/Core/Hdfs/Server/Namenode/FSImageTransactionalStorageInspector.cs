using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	internal class FSImageTransactionalStorageInspector : FSImageStorageInspector
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImageTransactionalStorageInspector
			));

		private bool needToSave = false;

		private bool isUpgradeFinalized = true;

		internal readonly IList<FSImageStorageInspector.FSImageFile> foundImages = new AList
			<FSImageStorageInspector.FSImageFile>();

		private long maxSeenTxId = 0;

		private readonly IList<Sharpen.Pattern> namePatterns = Lists.NewArrayList();

		internal FSImageTransactionalStorageInspector()
			: this(EnumSet.Of(NNStorage.NameNodeFile.Image))
		{
		}

		internal FSImageTransactionalStorageInspector(EnumSet<NNStorage.NameNodeFile> nnfs
			)
		{
			foreach (NNStorage.NameNodeFile nnf in nnfs)
			{
				Sharpen.Pattern pattern = Sharpen.Pattern.Compile(nnf.GetName() + "_(\\d+)");
				namePatterns.AddItem(pattern);
			}
		}

		private Matcher MatchPattern(string name)
		{
			foreach (Sharpen.Pattern p in namePatterns)
			{
				Matcher m = p.Matcher(name);
				if (m.Matches())
				{
					return m;
				}
			}
			return null;
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void InspectDirectory(Storage.StorageDirectory sd)
		{
			// Was the directory just formatted?
			if (!sd.GetVersionFile().Exists())
			{
				Log.Info("No version file in " + sd.GetRoot());
				needToSave |= true;
				return;
			}
			// Check for a seen_txid file, which marks a minimum transaction ID that
			// must be included in our load plan.
			try
			{
				maxSeenTxId = Math.Max(maxSeenTxId, NNStorage.ReadTransactionIdFile(sd));
			}
			catch (IOException ioe)
			{
				Log.Warn("Unable to determine the max transaction ID seen by " + sd, ioe);
				return;
			}
			FilePath currentDir = sd.GetCurrentDir();
			FilePath[] filesInStorage;
			try
			{
				filesInStorage = FileUtil.ListFiles(currentDir);
			}
			catch (IOException ioe)
			{
				Log.Warn("Unable to inspect storage directory " + currentDir, ioe);
				return;
			}
			foreach (FilePath f in filesInStorage)
			{
				Log.Debug("Checking file " + f);
				string name = f.GetName();
				// Check for fsimage_*
				Matcher imageMatch = this.MatchPattern(name);
				if (imageMatch != null)
				{
					if (sd.GetStorageDirType().IsOfType(NNStorage.NameNodeDirType.Image))
					{
						try
						{
							long txid = long.Parse(imageMatch.Group(1));
							foundImages.AddItem(new FSImageStorageInspector.FSImageFile(sd, f, txid));
						}
						catch (FormatException)
						{
							Log.Error("Image file " + f + " has improperly formatted " + "transaction ID");
						}
					}
					else
					{
						// skip
						Log.Warn("Found image file at " + f + " but storage directory is " + "not configured to contain images."
							);
					}
				}
			}
			// set finalized flag
			isUpgradeFinalized = isUpgradeFinalized && !sd.GetPreviousDir().Exists();
		}

		internal override bool IsUpgradeFinalized()
		{
			return isUpgradeFinalized;
		}

		/// <returns>
		/// the image files that have the most recent associated
		/// transaction IDs.  If there are multiple storage directories which
		/// contain equal images, we'll return them all.
		/// </returns>
		/// <exception cref="System.IO.FileNotFoundException">if not images are found.</exception>
		/// <exception cref="System.IO.IOException"/>
		internal override IList<FSImageStorageInspector.FSImageFile> GetLatestImages()
		{
			List<FSImageStorageInspector.FSImageFile> ret = new List<FSImageStorageInspector.FSImageFile
				>();
			foreach (FSImageStorageInspector.FSImageFile img in foundImages)
			{
				if (ret.IsEmpty())
				{
					ret.AddItem(img);
				}
				else
				{
					FSImageStorageInspector.FSImageFile cur = ret.GetFirst();
					if (cur.txId == img.txId)
					{
						ret.AddItem(img);
					}
					else
					{
						if (cur.txId < img.txId)
						{
							ret.Clear();
							ret.AddItem(img);
						}
					}
				}
			}
			if (ret.IsEmpty())
			{
				throw new FileNotFoundException("No valid image files found");
			}
			return ret;
		}

		public virtual IList<FSImageStorageInspector.FSImageFile> GetFoundImages()
		{
			return ImmutableList.CopyOf(foundImages);
		}

		internal override bool NeedToSave()
		{
			return needToSave;
		}

		internal override long GetMaxSeenTxId()
		{
			return maxSeenTxId;
		}
	}
}
