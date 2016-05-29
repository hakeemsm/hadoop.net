using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Sharedcache
{
	public class TestSharedCacheUploader
	{
		/// <summary>If verifyAccess fails, the upload should fail</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailVerifyAccess()
		{
			SharedCacheUploader spied = CreateSpiedUploader();
			Org.Mockito.Mockito.DoReturn(false).When(spied).VerifyAccess();
			NUnit.Framework.Assert.IsFalse(spied.Call());
		}

		/// <summary>If rename fails, the upload should fail</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameFail()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.SharedCacheEnabled, true);
			LocalResource resource = Org.Mockito.Mockito.Mock<LocalResource>();
			Path localPath = Org.Mockito.Mockito.Mock<Path>();
			Org.Mockito.Mockito.When(localPath.GetName()).ThenReturn("foo.jar");
			string user = "joe";
			SCMUploaderProtocol scmClient = Org.Mockito.Mockito.Mock<SCMUploaderProtocol>();
			SCMUploaderNotifyResponse response = Org.Mockito.Mockito.Mock<SCMUploaderNotifyResponse
				>();
			Org.Mockito.Mockito.When(response.GetAccepted()).ThenReturn(true);
			Org.Mockito.Mockito.When(scmClient.Notify(Matchers.IsA<SCMUploaderNotifyRequest>(
				))).ThenReturn(response);
			FileSystem fs = Org.Mockito.Mockito.Mock<FileSystem>();
			// return false when rename is called
			Org.Mockito.Mockito.When(fs.Rename(Matchers.IsA<Path>(), Matchers.IsA<Path>())).ThenReturn
				(false);
			FileSystem localFs = FileSystem.GetLocal(conf);
			SharedCacheUploader spied = CreateSpiedUploader(resource, localPath, user, conf, 
				scmClient, fs, localFs);
			// stub verifyAccess() to return true
			Org.Mockito.Mockito.DoReturn(true).When(spied).VerifyAccess();
			// stub getActualPath()
			Org.Mockito.Mockito.DoReturn(localPath).When(spied).GetActualPath();
			// stub computeChecksum()
			Org.Mockito.Mockito.DoReturn("abcdef0123456789").When(spied).ComputeChecksum(Matchers.IsA
				<Path>());
			// stub uploadFile() to return true
			Org.Mockito.Mockito.DoReturn(true).When(spied).UploadFile(Matchers.IsA<Path>(), Matchers.IsA
				<Path>());
			NUnit.Framework.Assert.IsFalse(spied.Call());
		}

		/// <summary>
		/// If verifyAccess, uploadFile, rename, and notification succeed, the upload
		/// should succeed
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSuccess()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.SharedCacheEnabled, true);
			LocalResource resource = Org.Mockito.Mockito.Mock<LocalResource>();
			Path localPath = Org.Mockito.Mockito.Mock<Path>();
			Org.Mockito.Mockito.When(localPath.GetName()).ThenReturn("foo.jar");
			string user = "joe";
			SCMUploaderProtocol scmClient = Org.Mockito.Mockito.Mock<SCMUploaderProtocol>();
			SCMUploaderNotifyResponse response = Org.Mockito.Mockito.Mock<SCMUploaderNotifyResponse
				>();
			Org.Mockito.Mockito.When(response.GetAccepted()).ThenReturn(true);
			Org.Mockito.Mockito.When(scmClient.Notify(Matchers.IsA<SCMUploaderNotifyRequest>(
				))).ThenReturn(response);
			FileSystem fs = Org.Mockito.Mockito.Mock<FileSystem>();
			// return false when rename is called
			Org.Mockito.Mockito.When(fs.Rename(Matchers.IsA<Path>(), Matchers.IsA<Path>())).ThenReturn
				(true);
			FileSystem localFs = FileSystem.GetLocal(conf);
			SharedCacheUploader spied = CreateSpiedUploader(resource, localPath, user, conf, 
				scmClient, fs, localFs);
			// stub verifyAccess() to return true
			Org.Mockito.Mockito.DoReturn(true).When(spied).VerifyAccess();
			// stub getActualPath()
			Org.Mockito.Mockito.DoReturn(localPath).When(spied).GetActualPath();
			// stub computeChecksum()
			Org.Mockito.Mockito.DoReturn("abcdef0123456789").When(spied).ComputeChecksum(Matchers.IsA
				<Path>());
			// stub uploadFile() to return true
			Org.Mockito.Mockito.DoReturn(true).When(spied).UploadFile(Matchers.IsA<Path>(), Matchers.IsA
				<Path>());
			// stub notifySharedCacheManager to return true
			Org.Mockito.Mockito.DoReturn(true).When(spied).NotifySharedCacheManager(Matchers.IsA
				<string>(), Matchers.IsA<string>());
			NUnit.Framework.Assert.IsTrue(spied.Call());
		}

		/// <summary>
		/// If verifyAccess, uploadFile, and rename succed, but it receives a nay from
		/// SCM, the file should be deleted
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNotifySCMFail()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.SharedCacheEnabled, true);
			LocalResource resource = Org.Mockito.Mockito.Mock<LocalResource>();
			Path localPath = Org.Mockito.Mockito.Mock<Path>();
			Org.Mockito.Mockito.When(localPath.GetName()).ThenReturn("foo.jar");
			string user = "joe";
			FileSystem fs = Org.Mockito.Mockito.Mock<FileSystem>();
			// return false when rename is called
			Org.Mockito.Mockito.When(fs.Rename(Matchers.IsA<Path>(), Matchers.IsA<Path>())).ThenReturn
				(true);
			FileSystem localFs = FileSystem.GetLocal(conf);
			SharedCacheUploader spied = CreateSpiedUploader(resource, localPath, user, conf, 
				null, fs, localFs);
			// stub verifyAccess() to return true
			Org.Mockito.Mockito.DoReturn(true).When(spied).VerifyAccess();
			// stub getActualPath()
			Org.Mockito.Mockito.DoReturn(localPath).When(spied).GetActualPath();
			// stub computeChecksum()
			Org.Mockito.Mockito.DoReturn("abcdef0123456789").When(spied).ComputeChecksum(Matchers.IsA
				<Path>());
			// stub uploadFile() to return true
			Org.Mockito.Mockito.DoReturn(true).When(spied).UploadFile(Matchers.IsA<Path>(), Matchers.IsA
				<Path>());
			// stub notifySharedCacheManager to return true
			Org.Mockito.Mockito.DoReturn(false).When(spied).NotifySharedCacheManager(Matchers.IsA
				<string>(), Matchers.IsA<string>());
			NUnit.Framework.Assert.IsFalse(spied.Call());
			Org.Mockito.Mockito.Verify(fs).Delete(Matchers.IsA<Path>(), Matchers.AnyBoolean()
				);
		}

		/// <summary>If resource is public, verifyAccess should succeed</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVerifyAccessPublicResource()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.SharedCacheEnabled, true);
			LocalResource resource = Org.Mockito.Mockito.Mock<LocalResource>();
			// give public visibility
			Org.Mockito.Mockito.When(resource.GetVisibility()).ThenReturn(LocalResourceVisibility
				.Public);
			Path localPath = Org.Mockito.Mockito.Mock<Path>();
			Org.Mockito.Mockito.When(localPath.GetName()).ThenReturn("foo.jar");
			string user = "joe";
			SCMUploaderProtocol scmClient = Org.Mockito.Mockito.Mock<SCMUploaderProtocol>();
			FileSystem fs = Org.Mockito.Mockito.Mock<FileSystem>();
			FileSystem localFs = FileSystem.GetLocal(conf);
			SharedCacheUploader spied = CreateSpiedUploader(resource, localPath, user, conf, 
				scmClient, fs, localFs);
			NUnit.Framework.Assert.IsTrue(spied.VerifyAccess());
		}

		/// <summary>
		/// If the localPath does not exists, getActualPath should get to one level
		/// down
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetActualPath()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.SharedCacheEnabled, true);
			LocalResource resource = Org.Mockito.Mockito.Mock<LocalResource>();
			// give public visibility
			Org.Mockito.Mockito.When(resource.GetVisibility()).ThenReturn(LocalResourceVisibility
				.Public);
			Path localPath = new Path("foo.jar");
			string user = "joe";
			SCMUploaderProtocol scmClient = Org.Mockito.Mockito.Mock<SCMUploaderProtocol>();
			FileSystem fs = Org.Mockito.Mockito.Mock<FileSystem>();
			FileSystem localFs = Org.Mockito.Mockito.Mock<FileSystem>();
			// stub it to return a status that indicates a directory
			FileStatus status = Org.Mockito.Mockito.Mock<FileStatus>();
			Org.Mockito.Mockito.When(status.IsDirectory()).ThenReturn(true);
			Org.Mockito.Mockito.When(localFs.GetFileStatus(localPath)).ThenReturn(status);
			SharedCacheUploader spied = CreateSpiedUploader(resource, localPath, user, conf, 
				scmClient, fs, localFs);
			Path actualPath = spied.GetActualPath();
			NUnit.Framework.Assert.AreEqual(actualPath.GetName(), localPath.GetName());
			NUnit.Framework.Assert.AreEqual(actualPath.GetParent().GetName(), localPath.GetName
				());
		}

		/// <exception cref="System.IO.IOException"/>
		private SharedCacheUploader CreateSpiedUploader()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.SharedCacheEnabled, true);
			LocalResource resource = Org.Mockito.Mockito.Mock<LocalResource>();
			Path localPath = Org.Mockito.Mockito.Mock<Path>();
			string user = "foo";
			SCMUploaderProtocol scmClient = Org.Mockito.Mockito.Mock<SCMUploaderProtocol>();
			FileSystem fs = FileSystem.Get(conf);
			FileSystem localFs = FileSystem.GetLocal(conf);
			return CreateSpiedUploader(resource, localPath, user, conf, scmClient, fs, localFs
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private SharedCacheUploader CreateSpiedUploader(LocalResource resource, Path localPath
			, string user, Configuration conf, SCMUploaderProtocol scmClient, FileSystem fs, 
			FileSystem localFs)
		{
			SharedCacheUploader uploader = new SharedCacheUploader(resource, localPath, user, 
				conf, scmClient, fs, localFs);
			return Org.Mockito.Mockito.Spy(uploader);
		}
	}
}
