using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestIdentityHashStore
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestIdentityHashStore)
			.FullName);

		private class Key
		{
			private readonly string name;

			internal Key(string name)
			{
				this.name = name;
			}

			public override int GetHashCode()
			{
				throw new RuntimeException("should not be used!");
			}

			public override bool Equals(object o)
			{
				if (!(o is TestIdentityHashStore.Key))
				{
					return false;
				}
				TestIdentityHashStore.Key other = (TestIdentityHashStore.Key)o;
				return name.Equals(other.name);
			}
		}

		public virtual void TestStartingWithZeroCapacity()
		{
			IdentityHashStore<TestIdentityHashStore.Key, int> store = new IdentityHashStore<TestIdentityHashStore.Key
				, int>(0);
			store.VisitAll(new _Visitor_61());
			NUnit.Framework.Assert.IsTrue(store.IsEmpty());
			TestIdentityHashStore.Key key1 = new TestIdentityHashStore.Key("key1");
			int value1 = 100;
			store.Put(key1, value1);
			NUnit.Framework.Assert.IsTrue(!store.IsEmpty());
			NUnit.Framework.Assert.AreEqual(value1, store.Get(key1));
			store.VisitAll(new _Visitor_73(key1));
			NUnit.Framework.Assert.AreEqual(value1, store.Remove(key1));
			NUnit.Framework.Assert.IsTrue(store.IsEmpty());
		}

		private sealed class _Visitor_61 : IdentityHashStore.Visitor<TestIdentityHashStore.Key
			, int>
		{
			public _Visitor_61()
			{
			}

			public void Accept(TestIdentityHashStore.Key k, int v)
			{
				NUnit.Framework.Assert.Fail("found key " + k + " in empty IdentityHashStore.");
			}
		}

		private sealed class _Visitor_73 : IdentityHashStore.Visitor<TestIdentityHashStore.Key
			, int>
		{
			public _Visitor_73(TestIdentityHashStore.Key key1)
			{
				this.key1 = key1;
			}

			public void Accept(TestIdentityHashStore.Key k, int v)
			{
				NUnit.Framework.Assert.AreEqual(key1, k);
			}

			private readonly TestIdentityHashStore.Key key1;
		}

		public virtual void TestDuplicateInserts()
		{
			IdentityHashStore<TestIdentityHashStore.Key, int> store = new IdentityHashStore<TestIdentityHashStore.Key
				, int>(4);
			store.VisitAll(new _Visitor_87());
			NUnit.Framework.Assert.IsTrue(store.IsEmpty());
			TestIdentityHashStore.Key key1 = new TestIdentityHashStore.Key("key1");
			int value1 = 100;
			int value2 = 200;
			int value3 = 300;
			store.Put(key1, value1);
			TestIdentityHashStore.Key equalToKey1 = new TestIdentityHashStore.Key("key1");
			// IdentityHashStore compares by object equality, not equals()
			NUnit.Framework.Assert.IsNull(store.Get(equalToKey1));
			NUnit.Framework.Assert.IsTrue(!store.IsEmpty());
			NUnit.Framework.Assert.AreEqual(value1, store.Get(key1));
			store.Put(key1, value2);
			store.Put(key1, value3);
			IList<int> allValues = new List<int>();
			store.VisitAll(new _Visitor_109(allValues));
			NUnit.Framework.Assert.AreEqual(3, allValues.Count);
			for (int i = 0; i < 3; i++)
			{
				int value = store.Remove(key1);
				NUnit.Framework.Assert.IsTrue(allValues.Remove(value));
			}
			NUnit.Framework.Assert.IsNull(store.Remove(key1));
			NUnit.Framework.Assert.IsTrue(store.IsEmpty());
		}

		private sealed class _Visitor_87 : IdentityHashStore.Visitor<TestIdentityHashStore.Key
			, int>
		{
			public _Visitor_87()
			{
			}

			public void Accept(TestIdentityHashStore.Key k, int v)
			{
				NUnit.Framework.Assert.Fail("found key " + k + " in empty IdentityHashStore.");
			}
		}

		private sealed class _Visitor_109 : IdentityHashStore.Visitor<TestIdentityHashStore.Key
			, int>
		{
			public _Visitor_109(IList<int> allValues)
			{
				this.allValues = allValues;
			}

			public void Accept(TestIdentityHashStore.Key k, int v)
			{
				allValues.AddItem(v);
			}

			private readonly IList<int> allValues;
		}

		public virtual void TestAdditionsAndRemovals()
		{
			IdentityHashStore<TestIdentityHashStore.Key, int> store = new IdentityHashStore<TestIdentityHashStore.Key
				, int>(0);
			int NumKeys = 1000;
			Log.Debug("generating " + NumKeys + " keys");
			IList<TestIdentityHashStore.Key> keys = new AList<TestIdentityHashStore.Key>(NumKeys
				);
			for (int i = 0; i < NumKeys; i++)
			{
				keys.AddItem(new TestIdentityHashStore.Key("key " + i));
			}
			for (int i_1 = 0; i_1 < NumKeys; i_1++)
			{
				store.Put(keys[i_1], i_1);
			}
			store.VisitAll(new _Visitor_137(keys));
			for (int i_2 = 0; i_2 < NumKeys; i_2++)
			{
				NUnit.Framework.Assert.AreEqual(Sharpen.Extensions.ValueOf(i_2), store.Remove(keys
					[i_2]));
			}
			store.VisitAll(new _Visitor_147());
			NUnit.Framework.Assert.IsTrue("expected the store to be " + "empty, but found " +
				 store.NumElements() + " elements.", store.IsEmpty());
			NUnit.Framework.Assert.AreEqual(1024, store.Capacity());
		}

		private sealed class _Visitor_137 : IdentityHashStore.Visitor<TestIdentityHashStore.Key
			, int>
		{
			public _Visitor_137(IList<TestIdentityHashStore.Key> keys)
			{
				this.keys = keys;
			}

			public void Accept(TestIdentityHashStore.Key k, int v)
			{
				NUnit.Framework.Assert.IsTrue(keys.Contains(k));
			}

			private readonly IList<TestIdentityHashStore.Key> keys;
		}

		private sealed class _Visitor_147 : IdentityHashStore.Visitor<TestIdentityHashStore.Key
			, int>
		{
			public _Visitor_147()
			{
			}

			public void Accept(TestIdentityHashStore.Key k, int v)
			{
				NUnit.Framework.Assert.Fail("expected all entries to be removed");
			}
		}
	}
}
