using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestReflectionUtils
	{
		private static Type[] toConstruct = new Type[] { typeof(string), typeof(TestReflectionUtils
			), typeof(Hashtable) };

		private Exception failure = null;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			ReflectionUtils.ClearCache();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCache()
		{
			Assert.Equal(0, CacheSize());
			DoTestCache();
			Assert.Equal(toConstruct.Length, CacheSize());
			ReflectionUtils.ClearCache();
			Assert.Equal(0, CacheSize());
		}

		private void DoTestCache()
		{
			for (int i = 0; i < toConstruct.Length; i++)
			{
				Type cl = toConstruct[i];
				object x = ReflectionUtils.NewInstance(cl, null);
				object y = ReflectionUtils.NewInstance(cl, null);
				Assert.Equal(cl, x.GetType());
				Assert.Equal(cl, y.GetType());
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestThreadSafe()
		{
			Sharpen.Thread[] th = new Sharpen.Thread[32];
			for (int i = 0; i < th.Length; i++)
			{
				th[i] = new _Thread_67(this);
				th[i].Start();
			}
			for (int i_1 = 0; i_1 < th.Length; i_1++)
			{
				th[i_1].Join();
			}
			if (failure != null)
			{
				Sharpen.Runtime.PrintStackTrace(failure);
				NUnit.Framework.Assert.Fail(failure.Message);
			}
		}

		private sealed class _Thread_67 : Sharpen.Thread
		{
			public _Thread_67(TestReflectionUtils _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				try
				{
					this._enclosing.DoTestCache();
				}
				catch (Exception t)
				{
					this._enclosing.failure = t;
				}
			}

			private readonly TestReflectionUtils _enclosing;
		}

		/// <exception cref="System.Exception"/>
		private int CacheSize()
		{
			return ReflectionUtils.GetCacheSize();
		}

		[Fact]
		public virtual void TestCantCreate()
		{
			try
			{
				ReflectionUtils.NewInstance<TestReflectionUtils.NoDefaultCtor>(null);
				NUnit.Framework.Assert.Fail("invalid call should fail");
			}
			catch (RuntimeException rte)
			{
				Assert.Equal(typeof(MissingMethodException), rte.InnerException
					.GetType());
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCacheDoesntLeak()
		{
			int iterations = 9999;
			// very fast, but a bit less reliable - bigger numbers force GC
			for (int i = 0; i < iterations; i++)
			{
				URLClassLoader loader = new URLClassLoader(new Uri[0], GetType().GetClassLoader()
					);
				Type cl = Sharpen.Runtime.GetType("org.apache.hadoop.util.TestReflectionUtils$LoadedInChild"
					, false, loader);
				object o = ReflectionUtils.NewInstance(cl, null);
				Assert.Equal(cl, o.GetType());
			}
			System.GC.Collect();
			Assert.True(CacheSize() + " too big", CacheSize() < iterations);
		}

		[Fact]
		public virtual void TestGetDeclaredFieldsIncludingInherited()
		{
			TestReflectionUtils.Parent child = new _Parent_118();
			IList<FieldInfo> fields = ReflectionUtils.GetDeclaredFieldsIncludingInherited(child
				.GetType());
			bool containsParentField = false;
			bool containsChildField = false;
			foreach (FieldInfo field in fields)
			{
				if (field.Name.Equals("parentField"))
				{
					containsParentField = true;
				}
				else
				{
					if (field.Name.Equals("childField"))
					{
						containsChildField = true;
					}
				}
			}
			IList<MethodInfo> methods = ReflectionUtils.GetDeclaredMethodsIncludingInherited(
				child.GetType());
			bool containsParentMethod = false;
			bool containsChildMethod = false;
			foreach (MethodInfo method in methods)
			{
				if (method.Name.Equals("getParentField"))
				{
					containsParentMethod = true;
				}
				else
				{
					if (method.Name.Equals("getChildField"))
					{
						containsChildMethod = true;
					}
				}
			}
			Assert.True("Missing parent field", containsParentField);
			Assert.True("Missing child field", containsChildField);
			Assert.True("Missing parent method", containsParentMethod);
			Assert.True("Missing child method", containsChildMethod);
		}

		private sealed class _Parent_118 : TestReflectionUtils.Parent
		{
			public _Parent_118()
			{
			}

			private int childField;

			public int GetChildField()
			{
				return this.childField;
			}
		}

		private class Parent
		{
			private int parentField;

			// Used for testGetDeclaredFieldsIncludingInherited
			public virtual int GetParentField()
			{
				return this.parentField;
			}

			internal Parent(TestReflectionUtils _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestReflectionUtils _enclosing;
		}

		private class LoadedInChild
		{
		}

		public class NoDefaultCtor
		{
			public NoDefaultCtor(int x)
			{
			}
		}
	}
}
