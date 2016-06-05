using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestFetchImage
	{
		private static readonly FilePath FetchedImageFile = new FilePath(Runtime.GetProperty
			("test.build.dir"), "target/fetched-image-dir");

		private static readonly Sharpen.Pattern ImageRegex = Sharpen.Pattern.Compile("fsimage_(\\d+)"
			);

		// Shamelessly stolen from NNStorage.
		[AfterClass]
		public static void Cleanup()
		{
			FileUtil.FullyDelete(FetchedImageFile);
		}

		/// <summary>
		/// Download a few fsimages using `hdfs dfsadmin -fetchImage ...' and verify
		/// the results.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFetchImage()
		{
			FetchedImageFile.Mkdirs();
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = null;
			try
			{
				DFSAdmin dfsAdmin = new DFSAdmin();
				dfsAdmin.SetConf(conf);
				RunFetchImage(dfsAdmin, cluster);
				fs = cluster.GetFileSystem();
				fs.Mkdirs(new Path("/foo"));
				fs.Mkdirs(new Path("/foo2"));
				fs.Mkdirs(new Path("/foo3"));
				cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, 
					false);
				cluster.GetNameNodeRpc().SaveNamespace();
				cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, 
					false);
				RunFetchImage(dfsAdmin, cluster);
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Run `hdfs dfsadmin -fetchImage ...' and verify that the downloaded image is
		/// correct.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private static void RunFetchImage(DFSAdmin dfsAdmin, MiniDFSCluster cluster)
		{
			int retVal = dfsAdmin.Run(new string[] { "-fetchImage", FetchedImageFile.GetPath(
				) });
			NUnit.Framework.Assert.AreEqual(0, retVal);
			FilePath highestImageOnNn = GetHighestFsImageOnCluster(cluster);
			MD5Hash expected = MD5FileUtils.ComputeMd5ForFile(highestImageOnNn);
			MD5Hash actual = MD5FileUtils.ComputeMd5ForFile(new FilePath(FetchedImageFile, highestImageOnNn
				.GetName()));
			NUnit.Framework.Assert.AreEqual(expected, actual);
		}

		/// <returns>the fsimage with highest transaction ID in the cluster.</returns>
		private static FilePath GetHighestFsImageOnCluster(MiniDFSCluster cluster)
		{
			long highestImageTxId = -1;
			FilePath highestImageOnNn = null;
			foreach (URI nameDir in cluster.GetNameDirs(0))
			{
				foreach (FilePath imageFile in new FilePath(new FilePath(nameDir), "current").ListFiles
					())
				{
					Matcher imageMatch = ImageRegex.Matcher(imageFile.GetName());
					if (imageMatch.Matches())
					{
						long imageTxId = long.Parse(imageMatch.Group(1));
						if (imageTxId > highestImageTxId)
						{
							highestImageTxId = imageTxId;
							highestImageOnNn = imageFile;
						}
					}
				}
			}
			return highestImageOnNn;
		}
	}
}
