using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestFSImageStorageInspector
	{
		/// <summary>Simple test with image, edits, and inprogress edits</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCurrentStorageInspector()
		{
			FSImageTransactionalStorageInspector inspector = new FSImageTransactionalStorageInspector
				();
			Storage.StorageDirectory mockDir = FSImageTestUtil.MockStorageDirectory(NNStorage.NameNodeDirType
				.ImageAndEdits, false, "/foo/current/" + NNStorage.GetImageFileName(123), "/foo/current/"
				 + NNStorage.GetFinalizedEditsFileName(123, 456), "/foo/current/" + NNStorage.GetImageFileName
				(456), "/foo/current/" + NNStorage.GetInProgressEditsFileName(457));
			inspector.InspectDirectory(mockDir);
			NUnit.Framework.Assert.AreEqual(2, inspector.foundImages.Count);
			FSImageStorageInspector.FSImageFile latestImage = inspector.GetLatestImages()[0];
			NUnit.Framework.Assert.AreEqual(456, latestImage.txId);
			NUnit.Framework.Assert.AreSame(mockDir, latestImage.sd);
			NUnit.Framework.Assert.IsTrue(inspector.IsUpgradeFinalized());
			NUnit.Framework.Assert.AreEqual(new FilePath("/foo/current/" + NNStorage.GetImageFileName
				(456)), latestImage.GetFile());
		}
	}
}
