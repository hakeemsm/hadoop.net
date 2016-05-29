using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store
{
	public class TestInMemorySCMStore : SCMStoreBaseTest
	{
		private InMemorySCMStore store;

		private AppChecker checker;

		internal override Type GetStoreClass()
		{
			return typeof(InMemorySCMStore);
		}

		[SetUp]
		public virtual void Setup()
		{
			this.checker = Org.Mockito.Mockito.Spy(new DummyAppChecker());
			this.store = Org.Mockito.Mockito.Spy(new InMemorySCMStore(checker));
		}

		[TearDown]
		public virtual void Cleanup()
		{
			if (this.store != null)
			{
				this.store.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		private void StartEmptyStore()
		{
			Org.Mockito.Mockito.DoReturn(new AList<ApplicationId>()).When(checker).GetActiveApplications
				();
			Org.Mockito.Mockito.DoReturn(new Dictionary<string, string>()).When(store).GetInitialCachedResources
				(Matchers.IsA<FileSystem>(), Matchers.IsA<Configuration>());
			this.store.Init(new Configuration());
			this.store.Start();
		}

		/// <exception cref="System.Exception"/>
		private IDictionary<string, string> StartStoreWithResources()
		{
			IDictionary<string, string> initialCachedResources = new Dictionary<string, string
				>();
			int count = 10;
			for (int i = 0; i < count; i++)
			{
				string key = i.ToString();
				string fileName = key + ".jar";
				initialCachedResources[key] = fileName;
			}
			Org.Mockito.Mockito.DoReturn(new AList<ApplicationId>()).When(checker).GetActiveApplications
				();
			Org.Mockito.Mockito.DoReturn(initialCachedResources).When(store).GetInitialCachedResources
				(Matchers.IsA<FileSystem>(), Matchers.IsA<Configuration>());
			this.store.Init(new Configuration());
			this.store.Start();
			return initialCachedResources;
		}

		/// <exception cref="System.Exception"/>
		private void StartStoreWithApps()
		{
			AList<ApplicationId> list = new AList<ApplicationId>();
			int count = 5;
			for (int i = 0; i < count; i++)
			{
				list.AddItem(CreateAppId(i, i));
			}
			Org.Mockito.Mockito.DoReturn(list).When(checker).GetActiveApplications();
			Org.Mockito.Mockito.DoReturn(new Dictionary<string, string>()).When(store).GetInitialCachedResources
				(Matchers.IsA<FileSystem>(), Matchers.IsA<Configuration>());
			this.store.Init(new Configuration());
			this.store.Start();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddResourceConcurrency()
		{
			StartEmptyStore();
			string key = "key1";
			int count = 5;
			ExecutorService exec = Executors.NewFixedThreadPool(count);
			IList<Future<string>> futures = new AList<Future<string>>(count);
			CountDownLatch start = new CountDownLatch(1);
			for (int i = 0; i < count; i++)
			{
				string fileName = "foo-" + i + ".jar";
				Callable<string> task = new _Callable_129(this, start, key, fileName);
				futures.AddItem(exec.Submit(task));
			}
			// start them all at the same time
			start.CountDown();
			// check the result; they should all agree with the value
			ICollection<string> results = new HashSet<string>();
			foreach (Future<string> future in futures)
			{
				results.AddItem(future.Get());
			}
			NUnit.Framework.Assert.AreSame(1, results.Count);
			exec.Shutdown();
		}

		private sealed class _Callable_129 : Callable<string>
		{
			public _Callable_129(TestInMemorySCMStore _enclosing, CountDownLatch start, string
				 key, string fileName)
			{
				this._enclosing = _enclosing;
				this.start = start;
				this.key = key;
				this.fileName = fileName;
			}

			/// <exception cref="System.Exception"/>
			public string Call()
			{
				start.Await();
				string result = this._enclosing.store.AddResource(key, fileName);
				System.Console.Out.WriteLine("fileName: " + fileName + ", result: " + result);
				return result;
			}

			private readonly TestInMemorySCMStore _enclosing;

			private readonly CountDownLatch start;

			private readonly string key;

			private readonly string fileName;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddResourceRefNonExistentResource()
		{
			StartEmptyStore();
			string key = "key1";
			ApplicationId id = CreateAppId(1, 1L);
			// try adding an app id without adding the key first
			NUnit.Framework.Assert.IsNull(store.AddResourceReference(key, new SharedCacheResourceReference
				(id, "user")));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveResourceEmptyRefs()
		{
			StartEmptyStore();
			string key = "key1";
			string fileName = "foo.jar";
			// first add resource
			store.AddResource(key, fileName);
			// try removing the resource; it should return true
			NUnit.Framework.Assert.IsTrue(store.RemoveResource(key));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddResourceRefRemoveResource()
		{
			StartEmptyStore();
			string key = "key1";
			ApplicationId id = CreateAppId(1, 1L);
			string user = "user";
			// add the resource, and then add a resource ref
			store.AddResource(key, "foo.jar");
			store.AddResourceReference(key, new SharedCacheResourceReference(id, user));
			// removeResource should return false
			NUnit.Framework.Assert.IsTrue(!store.RemoveResource(key));
			// the resource and the ref should be intact
			ICollection<SharedCacheResourceReference> refs = store.GetResourceReferences(key);
			NUnit.Framework.Assert.IsTrue(refs != null);
			NUnit.Framework.Assert.AreEqual(Sharpen.Collections.Singleton(new SharedCacheResourceReference
				(id, user)), refs);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddResourceRefConcurrency()
		{
			StartEmptyStore();
			string key = "key1";
			string user = "user";
			string fileName = "foo.jar";
			// first add the resource
			store.AddResource(key, fileName);
			// make concurrent addResourceRef calls (clients)
			int count = 5;
			ExecutorService exec = Executors.NewFixedThreadPool(count);
			IList<Future<string>> futures = new AList<Future<string>>(count);
			CountDownLatch start = new CountDownLatch(1);
			for (int i = 0; i < count; i++)
			{
				ApplicationId id = CreateAppId(i, i);
				Callable<string> task = new _Callable_205(this, start, key, id, user);
				futures.AddItem(exec.Submit(task));
			}
			// start them all at the same time
			start.CountDown();
			// check the result
			ICollection<string> results = new HashSet<string>();
			foreach (Future<string> future in futures)
			{
				results.AddItem(future.Get());
			}
			// they should all have the same file name
			NUnit.Framework.Assert.AreSame(1, results.Count);
			NUnit.Framework.Assert.AreEqual(Sharpen.Collections.Singleton(fileName), results);
			// there should be 5 refs as a result
			ICollection<SharedCacheResourceReference> refs = store.GetResourceReferences(key);
			NUnit.Framework.Assert.AreSame(count, refs.Count);
			exec.Shutdown();
		}

		private sealed class _Callable_205 : Callable<string>
		{
			public _Callable_205(TestInMemorySCMStore _enclosing, CountDownLatch start, string
				 key, ApplicationId id, string user)
			{
				this._enclosing = _enclosing;
				this.start = start;
				this.key = key;
				this.id = id;
				this.user = user;
			}

			/// <exception cref="System.Exception"/>
			public string Call()
			{
				start.Await();
				return this._enclosing.store.AddResourceReference(key, new SharedCacheResourceReference
					(id, user));
			}

			private readonly TestInMemorySCMStore _enclosing;

			private readonly CountDownLatch start;

			private readonly string key;

			private readonly ApplicationId id;

			private readonly string user;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddResourceRefAddResourceConcurrency()
		{
			StartEmptyStore();
			string key = "key1";
			string fileName = "foo.jar";
			string user = "user";
			ApplicationId id = CreateAppId(1, 1L);
			// add the resource and add the resource ref at the same time
			ExecutorService exec = Executors.NewFixedThreadPool(2);
			CountDownLatch start = new CountDownLatch(1);
			Callable<string> addKeyTask = new _Callable_240(this, start, key, fileName);
			Callable<string> addAppIdTask = new _Callable_246(this, start, key, id, user);
			Future<string> addAppIdFuture = exec.Submit(addAppIdTask);
			Future<string> addKeyFuture = exec.Submit(addKeyTask);
			// start them at the same time
			start.CountDown();
			// get the results
			string addKeyResult = addKeyFuture.Get();
			string addAppIdResult = addAppIdFuture.Get();
			NUnit.Framework.Assert.AreEqual(fileName, addKeyResult);
			System.Console.Out.WriteLine("addAppId() result: " + addAppIdResult);
			// it may be null or the fileName depending on the timing
			NUnit.Framework.Assert.IsTrue(addAppIdResult == null || addAppIdResult.Equals(fileName
				));
			exec.Shutdown();
		}

		private sealed class _Callable_240 : Callable<string>
		{
			public _Callable_240(TestInMemorySCMStore _enclosing, CountDownLatch start, string
				 key, string fileName)
			{
				this._enclosing = _enclosing;
				this.start = start;
				this.key = key;
				this.fileName = fileName;
			}

			/// <exception cref="System.Exception"/>
			public string Call()
			{
				start.Await();
				return this._enclosing.store.AddResource(key, fileName);
			}

			private readonly TestInMemorySCMStore _enclosing;

			private readonly CountDownLatch start;

			private readonly string key;

			private readonly string fileName;
		}

		private sealed class _Callable_246 : Callable<string>
		{
			public _Callable_246(TestInMemorySCMStore _enclosing, CountDownLatch start, string
				 key, ApplicationId id, string user)
			{
				this._enclosing = _enclosing;
				this.start = start;
				this.key = key;
				this.id = id;
				this.user = user;
			}

			/// <exception cref="System.Exception"/>
			public string Call()
			{
				start.Await();
				return this._enclosing.store.AddResourceReference(key, new SharedCacheResourceReference
					(id, user));
			}

			private readonly TestInMemorySCMStore _enclosing;

			private readonly CountDownLatch start;

			private readonly string key;

			private readonly ApplicationId id;

			private readonly string user;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveRef()
		{
			StartEmptyStore();
			string key = "key1";
			string fileName = "foo.jar";
			string user = "user";
			// first add the resource
			store.AddResource(key, fileName);
			// add a ref
			ApplicationId id = CreateAppId(1, 1L);
			SharedCacheResourceReference myRef = new SharedCacheResourceReference(id, user);
			string result = store.AddResourceReference(key, myRef);
			NUnit.Framework.Assert.AreEqual(fileName, result);
			ICollection<SharedCacheResourceReference> refs = store.GetResourceReferences(key);
			NUnit.Framework.Assert.AreSame(1, refs.Count);
			NUnit.Framework.Assert.AreEqual(Sharpen.Collections.Singleton(myRef), refs);
			// remove the same ref
			store.RemoveResourceReferences(key, Sharpen.Collections.Singleton(myRef), true);
			ICollection<SharedCacheResourceReference> newRefs = store.GetResourceReferences(key
				);
			NUnit.Framework.Assert.IsTrue(newRefs == null || newRefs.IsEmpty());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBootstrapping()
		{
			IDictionary<string, string> initialCachedResources = StartStoreWithResources();
			int count = initialCachedResources.Count;
			ApplicationId id = CreateAppId(1, 1L);
			// the entries from the cached entries should now exist
			for (int i = 0; i < count; i++)
			{
				string key = i.ToString();
				string fileName = key + ".jar";
				string result = store.AddResourceReference(key, new SharedCacheResourceReference(
					id, "user"));
				// the value should not be null (i.e. it has the key) and the filename should match
				NUnit.Framework.Assert.AreEqual(fileName, result);
				// the initial input should be emptied
				NUnit.Framework.Assert.IsTrue(initialCachedResources.IsEmpty());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEvictableWithInitialApps()
		{
			StartStoreWithApps();
			NUnit.Framework.Assert.IsFalse(store.IsResourceEvictable("key", Org.Mockito.Mockito.Mock
				<FileStatus>()));
		}

		private ApplicationId CreateAppId(int id, long timestamp)
		{
			return ApplicationId.NewInstance(timestamp, id);
		}
	}
}
