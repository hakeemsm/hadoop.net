using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class TestContainerLocalizer
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestContainerLocalizer
			));

		internal static readonly Path basedir = new Path("target", typeof(TestContainerLocalizer
			).FullName);

		internal static readonly FsPermission CacheDirPerm = new FsPermission((short)0x1c8
			);

		internal const string appUser = "yak";

		internal const string appId = "app_RM_0";

		internal const string containerId = "container_0";

		internal static readonly IPEndPoint nmAddr = new IPEndPoint("foobar", 8040);

		private AbstractFileSystem spylfs;

		private Random random;

		private IList<Path> localDirs;

		private Path tokenPath;

		private LocalizationProtocol nmProxy;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLocalizerMain()
		{
			FileContext fs = FileContext.GetLocalFSFileContext();
			spylfs = Org.Mockito.Mockito.Spy(fs.GetDefaultFileSystem());
			ContainerLocalizer localizer = SetupContainerLocalizerForTest();
			// verify created cache
			IList<Path> privCacheList = new AList<Path>();
			IList<Path> appCacheList = new AList<Path>();
			foreach (Path p in localDirs)
			{
				Path @base = new Path(new Path(p, ContainerLocalizer.Usercache), appUser);
				Path privcache = new Path(@base, ContainerLocalizer.Filecache);
				privCacheList.AddItem(privcache);
				Path appDir = new Path(@base, new Path(ContainerLocalizer.Appcache, appId));
				Path appcache = new Path(appDir, ContainerLocalizer.Filecache);
				appCacheList.AddItem(appcache);
			}
			// mock heartbeat responses from NM
			ResourceLocalizationSpec rsrcA = GetMockRsrc(random, LocalResourceVisibility.Private
				, privCacheList[0]);
			ResourceLocalizationSpec rsrcB = GetMockRsrc(random, LocalResourceVisibility.Private
				, privCacheList[0]);
			ResourceLocalizationSpec rsrcC = GetMockRsrc(random, LocalResourceVisibility.Application
				, appCacheList[0]);
			ResourceLocalizationSpec rsrcD = GetMockRsrc(random, LocalResourceVisibility.Private
				, privCacheList[0]);
			Org.Mockito.Mockito.When(nmProxy.Heartbeat(Matchers.IsA<LocalizerStatus>())).ThenReturn
				(new MockLocalizerHeartbeatResponse(LocalizerAction.Live, Sharpen.Collections.SingletonList
				(rsrcA))).ThenReturn(new MockLocalizerHeartbeatResponse(LocalizerAction.Live, Sharpen.Collections
				.SingletonList(rsrcB))).ThenReturn(new MockLocalizerHeartbeatResponse(LocalizerAction
				.Live, Sharpen.Collections.SingletonList(rsrcC))).ThenReturn(new MockLocalizerHeartbeatResponse
				(LocalizerAction.Live, Sharpen.Collections.SingletonList(rsrcD))).ThenReturn(new 
				MockLocalizerHeartbeatResponse(LocalizerAction.Live, Sharpen.Collections.EmptyList
				<ResourceLocalizationSpec>())).ThenReturn(new MockLocalizerHeartbeatResponse(LocalizerAction
				.Die, null));
			LocalResource tRsrcA = rsrcA.GetResource();
			LocalResource tRsrcB = rsrcB.GetResource();
			LocalResource tRsrcC = rsrcC.GetResource();
			LocalResource tRsrcD = rsrcD.GetResource();
			Org.Mockito.Mockito.DoReturn(new TestContainerLocalizer.FakeDownload(rsrcA.GetResource
				().GetResource().GetFile(), true)).When(localizer).Download(Matchers.IsA<Path>()
				, Matchers.Eq(tRsrcA), Matchers.IsA<UserGroupInformation>());
			Org.Mockito.Mockito.DoReturn(new TestContainerLocalizer.FakeDownload(rsrcB.GetResource
				().GetResource().GetFile(), true)).When(localizer).Download(Matchers.IsA<Path>()
				, Matchers.Eq(tRsrcB), Matchers.IsA<UserGroupInformation>());
			Org.Mockito.Mockito.DoReturn(new TestContainerLocalizer.FakeDownload(rsrcC.GetResource
				().GetResource().GetFile(), true)).When(localizer).Download(Matchers.IsA<Path>()
				, Matchers.Eq(tRsrcC), Matchers.IsA<UserGroupInformation>());
			Org.Mockito.Mockito.DoReturn(new TestContainerLocalizer.FakeDownload(rsrcD.GetResource
				().GetResource().GetFile(), true)).When(localizer).Download(Matchers.IsA<Path>()
				, Matchers.Eq(tRsrcD), Matchers.IsA<UserGroupInformation>());
			// run localization
			NUnit.Framework.Assert.AreEqual(0, localizer.RunLocalization(nmAddr));
			foreach (Path p_1 in localDirs)
			{
				Path @base = new Path(new Path(p_1, ContainerLocalizer.Usercache), appUser);
				Path privcache = new Path(@base, ContainerLocalizer.Filecache);
				// $x/usercache/$user/filecache
				Org.Mockito.Mockito.Verify(spylfs).Mkdir(Matchers.Eq(privcache), Matchers.Eq(CacheDirPerm
					), Matchers.Eq(false));
				Path appDir = new Path(@base, new Path(ContainerLocalizer.Appcache, appId));
				// $x/usercache/$user/appcache/$appId/filecache
				Path appcache = new Path(appDir, ContainerLocalizer.Filecache);
				Org.Mockito.Mockito.Verify(spylfs).Mkdir(Matchers.Eq(appcache), Matchers.Eq(CacheDirPerm
					), Matchers.Eq(false));
			}
			// verify tokens read at expected location
			Org.Mockito.Mockito.Verify(spylfs).Open(tokenPath);
			// verify downloaded resources reported to NM
			Org.Mockito.Mockito.Verify(nmProxy).Heartbeat(Matchers.ArgThat(new TestContainerLocalizer.HBMatches
				(rsrcA.GetResource())));
			Org.Mockito.Mockito.Verify(nmProxy).Heartbeat(Matchers.ArgThat(new TestContainerLocalizer.HBMatches
				(rsrcB.GetResource())));
			Org.Mockito.Mockito.Verify(nmProxy).Heartbeat(Matchers.ArgThat(new TestContainerLocalizer.HBMatches
				(rsrcC.GetResource())));
			Org.Mockito.Mockito.Verify(nmProxy).Heartbeat(Matchers.ArgThat(new TestContainerLocalizer.HBMatches
				(rsrcD.GetResource())));
			// verify all HB use localizerID provided
			Org.Mockito.Mockito.Verify(nmProxy, Org.Mockito.Mockito.Never()).Heartbeat(Matchers.ArgThat
				(new _ArgumentMatcher_193()));
		}

		private sealed class _ArgumentMatcher_193 : ArgumentMatcher<LocalizerStatus>
		{
			public _ArgumentMatcher_193()
			{
			}

			public override bool Matches(object o)
			{
				LocalizerStatus status = (LocalizerStatus)o;
				return !TestContainerLocalizer.containerId.Equals(status.GetLocalizerId());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalizerTokenIsGettingRemoved()
		{
			FileContext fs = FileContext.GetLocalFSFileContext();
			spylfs = Org.Mockito.Mockito.Spy(fs.GetDefaultFileSystem());
			ContainerLocalizer localizer = SetupContainerLocalizerForTest();
			Org.Mockito.Mockito.DoNothing().When(localizer).LocalizeFiles(Matchers.Any<LocalizationProtocol
				>(), Matchers.Any<CompletionService>(), Matchers.Any<UserGroupInformation>());
			localizer.RunLocalization(nmAddr);
			Org.Mockito.Mockito.Verify(spylfs, Org.Mockito.Mockito.Times(1)).Delete(tokenPath
				, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLocalizerClosesFilesystems()
		{
			// mocked generics
			// verify filesystems are closed when localizer doesn't fail
			FileContext fs = FileContext.GetLocalFSFileContext();
			spylfs = Org.Mockito.Mockito.Spy(fs.GetDefaultFileSystem());
			ContainerLocalizer localizer = SetupContainerLocalizerForTest();
			Org.Mockito.Mockito.DoNothing().When(localizer).LocalizeFiles(Matchers.Any<LocalizationProtocol
				>(), Matchers.Any<CompletionService>(), Matchers.Any<UserGroupInformation>());
			Org.Mockito.Mockito.Verify(localizer, Org.Mockito.Mockito.Never()).CloseFileSystems
				(Matchers.Any<UserGroupInformation>());
			localizer.RunLocalization(nmAddr);
			Org.Mockito.Mockito.Verify(localizer).CloseFileSystems(Matchers.Any<UserGroupInformation
				>());
			spylfs = Org.Mockito.Mockito.Spy(fs.GetDefaultFileSystem());
			// verify filesystems are closed when localizer fails
			localizer = SetupContainerLocalizerForTest();
			Org.Mockito.Mockito.DoThrow(new YarnRuntimeException("Forced Failure")).When(localizer
				).LocalizeFiles(Matchers.Any<LocalizationProtocol>(), Matchers.Any<CompletionService
				>(), Matchers.Any<UserGroupInformation>());
			Org.Mockito.Mockito.Verify(localizer, Org.Mockito.Mockito.Never()).CloseFileSystems
				(Matchers.Any<UserGroupInformation>());
			localizer.RunLocalization(nmAddr);
			Org.Mockito.Mockito.Verify(localizer).CloseFileSystems(Matchers.Any<UserGroupInformation
				>());
		}

		/// <exception cref="System.Exception"/>
		private ContainerLocalizer SetupContainerLocalizerForTest()
		{
			// mocked generics
			// don't actually create dirs
			Org.Mockito.Mockito.DoNothing().When(spylfs).Mkdir(Matchers.IsA<Path>(), Matchers.IsA
				<FsPermission>(), Matchers.AnyBoolean());
			Configuration conf = new Configuration();
			FileContext lfs = FileContext.GetFileContext(spylfs, conf);
			localDirs = new AList<Path>();
			for (int i = 0; i < 4; ++i)
			{
				localDirs.AddItem(lfs.MakeQualified(new Path(basedir, i + string.Empty)));
			}
			RecordFactory mockRF = GetMockLocalizerRecordFactory();
			ContainerLocalizer concreteLoc = new ContainerLocalizer(lfs, appUser, appId, containerId
				, localDirs, mockRF);
			ContainerLocalizer localizer = Org.Mockito.Mockito.Spy(concreteLoc);
			// return credential stream instead of opening local file
			random = new Random();
			long seed = random.NextLong();
			System.Console.Out.WriteLine("SEED: " + seed);
			random.SetSeed(seed);
			DataInputBuffer appTokens = CreateFakeCredentials(random, 10);
			tokenPath = lfs.MakeQualified(new Path(string.Format(ContainerLocalizer.TokenFileNameFmt
				, containerId)));
			Org.Mockito.Mockito.DoReturn(new FSDataInputStream(new FakeFSDataInputStream(appTokens
				))).When(spylfs).Open(tokenPath);
			nmProxy = Org.Mockito.Mockito.Mock<LocalizationProtocol>();
			Org.Mockito.Mockito.DoReturn(nmProxy).When(localizer).GetProxy(nmAddr);
			Org.Mockito.Mockito.DoNothing().When(localizer).Sleep(Matchers.AnyInt());
			// return result instantly for deterministic test
			ExecutorService syncExec = Org.Mockito.Mockito.Mock<ExecutorService>();
			CompletionService<Path> cs = Org.Mockito.Mockito.Mock<CompletionService>();
			Org.Mockito.Mockito.When(cs.Submit(Matchers.IsA<Callable>())).ThenAnswer(new _Answer_279
				());
			Org.Mockito.Mockito.DoReturn(syncExec).When(localizer).CreateDownloadThreadPool();
			Org.Mockito.Mockito.DoReturn(cs).When(localizer).CreateCompletionService(syncExec
				);
			return localizer;
		}

		private sealed class _Answer_279 : Answer<Future<Path>>
		{
			public _Answer_279()
			{
			}

			/// <exception cref="System.Exception"/>
			public Future<Path> Answer(InvocationOnMock invoc)
			{
				Future<Path> done = Org.Mockito.Mockito.Mock<Future>();
				Org.Mockito.Mockito.When(done.IsDone()).ThenReturn(true);
				TestContainerLocalizer.FakeDownload d = (TestContainerLocalizer.FakeDownload)invoc
					.GetArguments()[0];
				Org.Mockito.Mockito.When(done.Get()).ThenReturn(d.Call());
				return done;
			}
		}

		internal class HBMatches : ArgumentMatcher<LocalizerStatus>
		{
			internal readonly LocalResource rsrc;

			internal HBMatches(LocalResource rsrc)
			{
				this.rsrc = rsrc;
			}

			public override bool Matches(object o)
			{
				LocalizerStatus status = (LocalizerStatus)o;
				foreach (LocalResourceStatus localized in status.GetResources())
				{
					switch (localized.GetStatus())
					{
						case ResourceStatusType.FetchSuccess:
						{
							if (localized.GetLocalPath().GetFile().Contains(rsrc.GetResource().GetFile()))
							{
								return true;
							}
							break;
						}

						default:
						{
							NUnit.Framework.Assert.Fail("Unexpected: " + localized.GetStatus());
							break;
						}
					}
				}
				return false;
			}
		}

		internal class FakeDownload : Callable<Path>
		{
			private readonly Path localPath;

			private readonly bool succeed;

			internal FakeDownload(string absPath, bool succeed)
			{
				this.localPath = new Path("file:///localcache" + absPath);
				this.succeed = succeed;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual Path Call()
			{
				if (!succeed)
				{
					throw new IOException("FAIL " + localPath);
				}
				return localPath;
			}
		}

		internal static RecordFactory GetMockLocalizerRecordFactory()
		{
			RecordFactory mockRF = Org.Mockito.Mockito.Mock<RecordFactory>();
			Org.Mockito.Mockito.When(mockRF.NewRecordInstance(Matchers.Same(typeof(LocalResourceStatus
				)))).ThenAnswer(new _Answer_340());
			Org.Mockito.Mockito.When(mockRF.NewRecordInstance(Matchers.Same(typeof(LocalizerStatus
				)))).ThenAnswer(new _Answer_348());
			return mockRF;
		}

		private sealed class _Answer_340 : Answer<LocalResourceStatus>
		{
			public _Answer_340()
			{
			}

			/// <exception cref="System.Exception"/>
			public LocalResourceStatus Answer(InvocationOnMock invoc)
			{
				return new MockLocalResourceStatus();
			}
		}

		private sealed class _Answer_348 : Answer<LocalizerStatus>
		{
			public _Answer_348()
			{
			}

			/// <exception cref="System.Exception"/>
			public LocalizerStatus Answer(InvocationOnMock invoc)
			{
				return new MockLocalizerStatus();
			}
		}

		internal static ResourceLocalizationSpec GetMockRsrc(Random r, LocalResourceVisibility
			 vis, Path p)
		{
			ResourceLocalizationSpec resourceLocalizationSpec = Org.Mockito.Mockito.Mock<ResourceLocalizationSpec
				>();
			LocalResource rsrc = Org.Mockito.Mockito.Mock<LocalResource>();
			string name = long.ToHexString(r.NextLong());
			URL uri = Org.Mockito.Mockito.Mock<URL>();
			Org.Mockito.Mockito.When(uri.GetScheme()).ThenReturn("file");
			Org.Mockito.Mockito.When(uri.GetHost()).ThenReturn(null);
			Org.Mockito.Mockito.When(uri.GetFile()).ThenReturn("/local/" + vis + "/" + name);
			Org.Mockito.Mockito.When(rsrc.GetResource()).ThenReturn(uri);
			Org.Mockito.Mockito.When(rsrc.GetSize()).ThenReturn(r.Next(1024) + 1024L);
			Org.Mockito.Mockito.When(rsrc.GetTimestamp()).ThenReturn(r.Next(1024) + 2048L);
			Org.Mockito.Mockito.When(rsrc.GetType()).ThenReturn(LocalResourceType.File);
			Org.Mockito.Mockito.When(rsrc.GetVisibility()).ThenReturn(vis);
			Org.Mockito.Mockito.When(resourceLocalizationSpec.GetResource()).ThenReturn(rsrc);
			Org.Mockito.Mockito.When(resourceLocalizationSpec.GetDestinationDirectory()).ThenReturn
				(ConverterUtils.GetYarnUrlFromPath(p));
			return resourceLocalizationSpec;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static DataInputBuffer CreateFakeCredentials(Random r, int nTok)
		{
			Credentials creds = new Credentials();
			byte[] password = new byte[20];
			Text kind = new Text();
			Text service = new Text();
			Text alias = new Text();
			for (int i = 0; i < nTok; ++i)
			{
				byte[] identifier = Sharpen.Runtime.GetBytesForString(("idef" + i));
				r.NextBytes(password);
				kind.Set("kind" + i);
				service.Set("service" + i);
				alias.Set("token" + i);
				Org.Apache.Hadoop.Security.Token.Token token = new Org.Apache.Hadoop.Security.Token.Token
					(identifier, password, kind, service);
				creds.AddToken(alias, token);
			}
			DataOutputBuffer buf = new DataOutputBuffer();
			creds.WriteTokenStorageToStream(buf);
			DataInputBuffer ret = new DataInputBuffer();
			ret.Reset(buf.GetData(), 0, buf.GetLength());
			return ret;
		}
	}
}
