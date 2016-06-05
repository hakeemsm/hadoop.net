using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestNodeManagerReboot
	{
		internal static readonly FilePath basedir = new FilePath("target", typeof(TestNodeManagerReboot
			).FullName);

		internal static readonly FilePath logsDir = new FilePath(basedir, "logs");

		internal static readonly FilePath nmLocalDir = new FilePath(basedir, "nm0");

		internal static readonly FilePath localResourceDir = new FilePath(basedir, "resource"
			);

		internal static readonly string user = Runtime.GetProperty("user.name");

		private FileContext localFS;

		private TestNodeManagerReboot.MyNodeManager nm;

		private DeletionService delService;

		internal static readonly Log Log = LogFactory.GetLog(typeof(TestNodeManagerReboot
			));

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		[SetUp]
		public virtual void Setup()
		{
			localFS = FileContext.GetLocalFSFileContext();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			localFS.Delete(new Path(basedir.GetPath()), true);
			if (nm != null)
			{
				nm.Stop();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestClearLocalDirWhenNodeReboot()
		{
			nm = new TestNodeManagerReboot.MyNodeManager(this);
			nm.Start();
			ContainerManagementProtocol containerManager = nm.GetContainerManager();
			// create files under fileCache
			CreateFiles(nmLocalDir.GetAbsolutePath(), ContainerLocalizer.Filecache, 100);
			localResourceDir.Mkdirs();
			ContainerLaunchContext containerLaunchContext = Records.NewRecord<ContainerLaunchContext
				>();
			// Construct the Container-id
			ContainerId cId = CreateContainerId();
			URL localResourceUri = ConverterUtils.GetYarnUrlFromPath(localFS.MakeQualified(new 
				Path(localResourceDir.GetAbsolutePath())));
			LocalResource localResource = LocalResource.NewInstance(localResourceUri, LocalResourceType
				.File, LocalResourceVisibility.Application, -1, localResourceDir.LastModified());
			string destinationFile = "dest_file";
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			localResources[destinationFile] = localResource;
			containerLaunchContext.SetLocalResources(localResources);
			IList<string> commands = new AList<string>();
			containerLaunchContext.SetCommands(commands);
			NodeId nodeId = nm.GetNMContext().GetNodeId();
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
				, TestContainerManager.CreateContainerToken(cId, 0, nodeId, destinationFile, nm.
				GetNMContext().GetContainerTokenSecretManager()));
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			UserGroupInformation currentUser = UserGroupInformation.CreateRemoteUser(cId.GetApplicationAttemptId
				().ToString());
			NMTokenIdentifier nmIdentifier = new NMTokenIdentifier(cId.GetApplicationAttemptId
				(), nodeId, user, 123);
			currentUser.AddTokenIdentifier(nmIdentifier);
			currentUser.DoAs(new _PrivilegedExceptionAction_152(this, allRequests));
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(cId);
			GetContainerStatusesRequest request = GetContainerStatusesRequest.NewInstance(containerIds
				);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = nm.GetNMContext().GetContainers()[request.GetContainerIds()[0]];
			int MaxTries = 20;
			int numTries = 0;
			while (!container.GetContainerState().Equals(ContainerState.Done) && numTries <= 
				MaxTries)
			{
				try
				{
					Sharpen.Thread.Sleep(500);
				}
				catch (Exception)
				{
				}
				// Do nothing
				numTries++;
			}
			NUnit.Framework.Assert.AreEqual(ContainerState.Done, container.GetContainerState(
				));
			NUnit.Framework.Assert.IsTrue("The container should create a subDir named currentUser: "
				 + user + "under localDir/usercache", NumOfLocalDirs(nmLocalDir.GetAbsolutePath(
				), ContainerLocalizer.Usercache) > 0);
			NUnit.Framework.Assert.IsTrue("There should be files or Dirs under nm_private when "
				 + "container is launched", NumOfLocalDirs(nmLocalDir.GetAbsolutePath(), ResourceLocalizationService
				.NmPrivateDir) > 0);
			// restart the NodeManager
			RestartNM(MaxTries);
			CheckNumOfLocalDirs();
			Org.Mockito.Mockito.Verify(delService, Org.Mockito.Mockito.Times(1)).Delete((string
				)Matchers.IsNull(), Matchers.ArgThat(new TestNodeManagerReboot.PathInclude(this, 
				ResourceLocalizationService.NmPrivateDir + "_DEL_")));
			Org.Mockito.Mockito.Verify(delService, Org.Mockito.Mockito.Times(1)).Delete((string
				)Matchers.IsNull(), Matchers.ArgThat(new TestNodeManagerReboot.PathInclude(this, 
				ContainerLocalizer.Filecache + "_DEL_")));
			Org.Mockito.Mockito.Verify(delService, Org.Mockito.Mockito.Times(1)).ScheduleFileDeletionTask
				(Matchers.ArgThat(new TestNodeManagerReboot.FileDeletionInclude(this, user, null
				, new string[] { destinationFile })));
			Org.Mockito.Mockito.Verify(delService, Org.Mockito.Mockito.Times(1)).ScheduleFileDeletionTask
				(Matchers.ArgThat(new TestNodeManagerReboot.FileDeletionInclude(this, null, ContainerLocalizer
				.Usercache + "_DEL_", new string[] {  })));
			// restart the NodeManager again
			// this time usercache directory should be empty
			RestartNM(MaxTries);
			CheckNumOfLocalDirs();
		}

		private sealed class _PrivilegedExceptionAction_152 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_152(TestNodeManagerReboot _enclosing, StartContainersRequest
				 allRequests)
			{
				this._enclosing = _enclosing;
				this.allRequests = allRequests;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public Void Run()
			{
				this._enclosing.nm.GetContainerManager().StartContainers(allRequests);
				return null;
			}

			private readonly TestNodeManagerReboot _enclosing;

			private readonly StartContainersRequest allRequests;
		}

		private void RestartNM(int maxTries)
		{
			nm.Stop();
			nm = new TestNodeManagerReboot.MyNodeManager(this);
			nm.Start();
			int numTries = 0;
			while ((NumOfLocalDirs(nmLocalDir.GetAbsolutePath(), ContainerLocalizer.Usercache
				) > 0 || NumOfLocalDirs(nmLocalDir.GetAbsolutePath(), ContainerLocalizer.Filecache
				) > 0 || NumOfLocalDirs(nmLocalDir.GetAbsolutePath(), ResourceLocalizationService
				.NmPrivateDir) > 0) && numTries < maxTries)
			{
				try
				{
					Sharpen.Thread.Sleep(500);
				}
				catch (Exception)
				{
				}
				// Do nothing
				numTries++;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckNumOfLocalDirs()
		{
			NUnit.Framework.Assert.IsTrue("After NM reboots, all local files should be deleted"
				, NumOfLocalDirs(nmLocalDir.GetAbsolutePath(), ContainerLocalizer.Usercache) == 
				0 && NumOfLocalDirs(nmLocalDir.GetAbsolutePath(), ContainerLocalizer.Filecache) 
				== 0 && NumOfLocalDirs(nmLocalDir.GetAbsolutePath(), ResourceLocalizationService
				.NmPrivateDir) == 0);
			NUnit.Framework.Assert.IsTrue("After NM reboots, usercache_DEL_* directory should be deleted"
				, NumOfUsercacheDELDirs(nmLocalDir.GetAbsolutePath()) == 0);
		}

		private int NumOfLocalDirs(string localDir, string localSubDir)
		{
			FilePath[] listOfFiles = new FilePath(localDir, localSubDir).ListFiles();
			if (listOfFiles == null)
			{
				return 0;
			}
			else
			{
				return listOfFiles.Length;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int NumOfUsercacheDELDirs(string localDir)
		{
			int count = 0;
			RemoteIterator<FileStatus> fileStatus = localFS.ListStatus(new Path(localDir));
			while (fileStatus.HasNext())
			{
				FileStatus status = fileStatus.Next();
				if (status.GetPath().GetName().Matches(".*" + ContainerLocalizer.Usercache + "_DEL_.*"
					))
				{
					count++;
				}
			}
			return count;
		}

		private void CreateFiles(string dir, string subDir, int numOfFiles)
		{
			for (int i = 0; i < numOfFiles; i++)
			{
				FilePath newFile = new FilePath(dir + "/" + subDir, "file_" + (i + 1));
				try
				{
					newFile.CreateNewFile();
				}
				catch (IOException)
				{
				}
			}
		}

		// Do nothing
		private ContainerId CreateContainerId()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 0);
			return containerId;
		}

		private class MyNodeManager : NodeManager
		{
			public MyNodeManager(TestNodeManagerReboot _enclosing)
				: base()
			{
				this._enclosing = _enclosing;
				this.Init(this.CreateNMConfig());
			}

			protected internal override NodeStatusUpdater CreateNodeStatusUpdater(Context context
				, Dispatcher dispatcher, NodeHealthCheckerService healthChecker)
			{
				MockNodeStatusUpdater myNodeStatusUpdater = new MockNodeStatusUpdater(context, dispatcher
					, healthChecker, this.metrics);
				return myNodeStatusUpdater;
			}

			protected internal override DeletionService CreateDeletionService(ContainerExecutor
				 exec)
			{
				this._enclosing.delService = Org.Mockito.Mockito.Spy(new DeletionService(exec));
				return this._enclosing.delService;
			}

			private YarnConfiguration CreateNMConfig()
			{
				YarnConfiguration conf = new YarnConfiguration();
				conf.SetInt(YarnConfiguration.NmPmemMb, 5 * 1024);
				// 5GB
				conf.Set(YarnConfiguration.NmAddress, "127.0.0.1:12345");
				conf.Set(YarnConfiguration.NmLocalizerAddress, "127.0.0.1:12346");
				conf.Set(YarnConfiguration.NmLogDirs, TestNodeManagerReboot.logsDir.GetAbsolutePath
					());
				conf.Set(YarnConfiguration.NmLocalDirs, TestNodeManagerReboot.nmLocalDir.GetAbsolutePath
					());
				conf.SetLong(YarnConfiguration.NmLogRetainSeconds, 1);
				return conf;
			}

			private readonly TestNodeManagerReboot _enclosing;
		}

		internal class PathInclude : ArgumentMatcher<Path>
		{
			internal readonly string part;

			internal PathInclude(TestNodeManagerReboot _enclosing, string part)
			{
				this._enclosing = _enclosing;
				this.part = part;
			}

			public override bool Matches(object o)
			{
				return ((Path)o).GetName().IndexOf(this.part) != -1;
			}

			private readonly TestNodeManagerReboot _enclosing;
		}

		internal class FileDeletionInclude : ArgumentMatcher<DeletionService.FileDeletionTask
			>
		{
			internal readonly string user;

			internal readonly string subDirIncludes;

			internal readonly string[] baseDirIncludes;

			public FileDeletionInclude(TestNodeManagerReboot _enclosing, string user, string 
				subDirIncludes, string[] baseDirIncludes)
			{
				this._enclosing = _enclosing;
				this.user = user;
				this.subDirIncludes = subDirIncludes;
				this.baseDirIncludes = baseDirIncludes;
			}

			public override bool Matches(object o)
			{
				DeletionService.FileDeletionTask fd = (DeletionService.FileDeletionTask)o;
				if (fd.GetUser() == null && this.user != null)
				{
					return false;
				}
				else
				{
					if (fd.GetUser() != null && this.user == null)
					{
						return false;
					}
					else
					{
						if (fd.GetUser() != null && this.user != null)
						{
							return fd.GetUser().Equals(this.user);
						}
					}
				}
				if (!this.ComparePaths(fd.GetSubDir(), this.subDirIncludes))
				{
					return false;
				}
				if (this.baseDirIncludes == null && fd.GetBaseDirs() != null)
				{
					return false;
				}
				else
				{
					if (this.baseDirIncludes != null && fd.GetBaseDirs() == null)
					{
						return false;
					}
					else
					{
						if (this.baseDirIncludes != null && fd.GetBaseDirs() != null)
						{
							if (this.baseDirIncludes.Length != fd.GetBaseDirs().Count)
							{
								return false;
							}
							for (int i = 0; i < this.baseDirIncludes.Length; i++)
							{
								if (!this.ComparePaths(fd.GetBaseDirs()[i], this.baseDirIncludes[i]))
								{
									return false;
								}
							}
						}
					}
				}
				return true;
			}

			public virtual bool ComparePaths(Path p1, string p2)
			{
				if (p1 == null && p2 != null)
				{
					return false;
				}
				else
				{
					if (p1 != null && p2 == null)
					{
						return false;
					}
					else
					{
						if (p1 != null && p2 != null)
						{
							return p1.ToUri().GetPath().Contains(p2.ToString());
						}
					}
				}
				return true;
			}

			private readonly TestNodeManagerReboot _enclosing;
		}
	}
}
