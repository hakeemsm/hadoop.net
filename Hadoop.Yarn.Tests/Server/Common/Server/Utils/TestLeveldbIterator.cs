using System;
using System.IO;
using System.Reflection;
using Org.Iq80.Leveldb;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Server.Utils
{
	public class TestLeveldbIterator
	{
		private class CallInfo
		{
			internal string methodName;

			internal object[] args;

			internal Type[] argTypes;

			public CallInfo(string methodName, params object[] args)
			{
				this.methodName = methodName;
				this.args = args;
				argTypes = new Type[args.Length];
				for (int i = 0; i < args.Length; ++i)
				{
					argTypes[i] = args[i].GetType();
				}
			}
		}

		private static TestLeveldbIterator.CallInfo[] RtexcMethods = new TestLeveldbIterator.CallInfo
			[] { new TestLeveldbIterator.CallInfo("seek", new byte[0]), new TestLeveldbIterator.CallInfo
			("seekToFirst"), new TestLeveldbIterator.CallInfo("seekToLast"), new TestLeveldbIterator.CallInfo
			("hasNext"), new TestLeveldbIterator.CallInfo("next"), new TestLeveldbIterator.CallInfo
			("peekNext"), new TestLeveldbIterator.CallInfo("hasPrev"), new TestLeveldbIterator.CallInfo
			("prev"), new TestLeveldbIterator.CallInfo("peekPrev"), new TestLeveldbIterator.CallInfo
			("remove") };

		// array of methods that should throw DBException instead of raw
		// runtime exceptions
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExceptionHandling()
		{
			InvocationHandler rtExcHandler = new _InvocationHandler_69();
			DBIterator dbiter = (DBIterator)Proxy.NewProxyInstance(typeof(DBIterator).GetClassLoader
				(), new Type[] { typeof(DBIterator) }, rtExcHandler);
			LeveldbIterator iter = new LeveldbIterator(dbiter);
			foreach (TestLeveldbIterator.CallInfo ci in RtexcMethods)
			{
				MethodInfo method = iter.GetType().GetMethod(ci.methodName, ci.argTypes);
				NUnit.Framework.Assert.IsNotNull("unable to locate method " + ci.methodName, method
					);
				try
				{
					method.Invoke(iter, ci.args);
					NUnit.Framework.Assert.Fail("operation should have thrown");
				}
				catch (TargetInvocationException ite)
				{
					Exception exc = ite.InnerException;
					NUnit.Framework.Assert.IsTrue("Method " + ci.methodName + " threw non-DBException: "
						 + exc, exc is DBException);
					NUnit.Framework.Assert.IsFalse("Method " + ci.methodName + " double-wrapped DBException"
						, exc.InnerException is DBException);
				}
			}
			// check close() throws IOException
			try
			{
				iter.Close();
				NUnit.Framework.Assert.Fail("operation shoul have thrown");
			}
			catch (IOException)
			{
			}
		}

		private sealed class _InvocationHandler_69 : InvocationHandler
		{
			public _InvocationHandler_69()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Invoke(object proxy, MethodInfo method, object[] args)
			{
				throw new RuntimeException("forced runtime error");
			}
		}
		// expected
	}
}
