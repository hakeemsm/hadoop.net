using System;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class TestLocalCacheDirectoryManager
	{
		public virtual void TestHierarchicalSubDirectoryCreation()
		{
			// setting per directory file limit to 1.
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.NmLocalCacheMaxFilesPerDirectory, "37");
			LocalCacheDirectoryManager hDir = new LocalCacheDirectoryManager(conf);
			// Test root directory path = ""
			NUnit.Framework.Assert.IsTrue(hDir.GetRelativePathForLocalization().IsEmpty());
			// Testing path generation from "0" to "0/0/z/z"
			for (int i = 1; i <= 37 * 36 * 36; i++)
			{
				StringBuilder sb = new StringBuilder();
				string num = Sharpen.Extensions.ToString(i - 1, 36);
				if (num.Length == 1)
				{
					sb.Append(num[0]);
				}
				else
				{
					sb.Append(Sharpen.Extensions.ToString(System.Convert.ToInt32(Sharpen.Runtime.Substring
						(num, 0, 1), 36) - 1, 36));
				}
				for (int j = 1; j < num.Length; j++)
				{
					sb.Append(Path.Separator).Append(num[j]);
				}
				NUnit.Framework.Assert.AreEqual(sb.ToString(), hDir.GetRelativePathForLocalization
					());
			}
			string testPath1 = "4";
			string testPath2 = "2";
			/*
			* Making sure directory "4" and "2" becomes non-full so that they are
			* reused for future getRelativePathForLocalization() calls in the order
			* they are freed.
			*/
			hDir.DecrementFileCountForPath(testPath1);
			hDir.DecrementFileCountForPath(testPath2);
			// After below call directory "4" should become full.
			NUnit.Framework.Assert.AreEqual(testPath1, hDir.GetRelativePathForLocalization());
			NUnit.Framework.Assert.AreEqual(testPath2, hDir.GetRelativePathForLocalization());
		}

		public virtual void TestMinimumPerDirectoryFileLimit()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.NmLocalCacheMaxFilesPerDirectory, "1");
			Exception e = null;
			NodeManager.NMContext nmContext = new NodeManager.NMContext(new NMContainerTokenSecretManager
				(conf), new NMTokenSecretManagerInNM(), null, new ApplicationACLsManager(conf), 
				new NMNullStateStoreService());
			ResourceLocalizationService service = new ResourceLocalizationService(null, null, 
				null, null, nmContext);
			try
			{
				service.Init(conf);
			}
			catch (Exception e1)
			{
				e = e1;
			}
			NUnit.Framework.Assert.IsNotNull(e);
			NUnit.Framework.Assert.AreEqual(typeof(YarnRuntimeException), e.GetType());
			NUnit.Framework.Assert.AreEqual(e.Message, YarnConfiguration.NmLocalCacheMaxFilesPerDirectory
				 + " parameter is configured with a value less than 37.");
		}

		public virtual void TestDirectoryStateChangeFromFullToNonFull()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.NmLocalCacheMaxFilesPerDirectory, "40");
			LocalCacheDirectoryManager dir = new LocalCacheDirectoryManager(conf);
			// checking for first four paths
			string rootPath = string.Empty;
			string firstSubDir = "0";
			for (int i = 0; i < 4; i++)
			{
				NUnit.Framework.Assert.AreEqual(rootPath, dir.GetRelativePathForLocalization());
			}
			// Releasing two files from the root directory.
			dir.DecrementFileCountForPath(rootPath);
			dir.DecrementFileCountForPath(rootPath);
			// Space for two files should be available in root directory.
			NUnit.Framework.Assert.AreEqual(rootPath, dir.GetRelativePathForLocalization());
			NUnit.Framework.Assert.AreEqual(rootPath, dir.GetRelativePathForLocalization());
			// As no space is now available in root directory so it should be from
			// first sub directory
			NUnit.Framework.Assert.AreEqual(firstSubDir, dir.GetRelativePathForLocalization()
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestDirectoryConversion()
		{
			for (int i = 0; i < 10000; ++i)
			{
				string path = LocalCacheDirectoryManager.Directory.GetRelativePath(i);
				NUnit.Framework.Assert.AreEqual("Incorrect conversion for " + i, i, LocalCacheDirectoryManager.Directory
					.GetDirectoryNumber(path));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestIncrementFileCountForPath()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.NmLocalCacheMaxFilesPerDirectory, LocalCacheDirectoryManager
				.DirectoriesPerLevel + 2);
			LocalCacheDirectoryManager mgr = new LocalCacheDirectoryManager(conf);
			string rootPath = string.Empty;
			mgr.IncrementFileCountForPath(rootPath);
			NUnit.Framework.Assert.AreEqual(rootPath, mgr.GetRelativePathForLocalization());
			NUnit.Framework.Assert.IsFalse("root dir should be full", rootPath.Equals(mgr.GetRelativePathForLocalization
				()));
			// finish filling the other directory
			mgr.GetRelativePathForLocalization();
			// free up space in the root dir
			mgr.DecrementFileCountForPath(rootPath);
			mgr.DecrementFileCountForPath(rootPath);
			NUnit.Framework.Assert.AreEqual(rootPath, mgr.GetRelativePathForLocalization());
			NUnit.Framework.Assert.AreEqual(rootPath, mgr.GetRelativePathForLocalization());
			string otherDir = mgr.GetRelativePathForLocalization();
			NUnit.Framework.Assert.IsFalse("root dir should be full", otherDir.Equals(rootPath
				));
			string deepDir0 = "d/e/e/p/0";
			string deepDir1 = "d/e/e/p/1";
			string deepDir2 = "d/e/e/p/2";
			string deepDir3 = "d/e/e/p/3";
			mgr.IncrementFileCountForPath(deepDir0);
			NUnit.Framework.Assert.AreEqual(otherDir, mgr.GetRelativePathForLocalization());
			NUnit.Framework.Assert.AreEqual(deepDir0, mgr.GetRelativePathForLocalization());
			NUnit.Framework.Assert.AreEqual("total dir count incorrect after increment", deepDir1
				, mgr.GetRelativePathForLocalization());
			mgr.IncrementFileCountForPath(deepDir2);
			mgr.IncrementFileCountForPath(deepDir1);
			mgr.IncrementFileCountForPath(deepDir2);
			NUnit.Framework.Assert.AreEqual(deepDir3, mgr.GetRelativePathForLocalization());
		}
	}
}
