using System;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;


namespace Org.Apache.Hadoop.Test
{
	public abstract class MockitoUtil
	{
		/// <summary>Return a mock object for an IPC protocol.</summary>
		/// <remarks>
		/// Return a mock object for an IPC protocol. This special
		/// method is necessary, since the IPC proxies have to implement
		/// Closeable in addition to their protocol interface.
		/// </remarks>
		/// <param name="clazz">the protocol class</param>
		public static T MockProtocol<T>()
		{
			System.Type clazz = typeof(T);
			return Org.Mockito.Mockito.Mock(clazz, Org.Mockito.Mockito.WithSettings().ExtraInterfaces
				(typeof(IDisposable)));
		}

		/// <summary>
		/// Throw an exception from the mock/spy only in the case that the
		/// call stack at the time the method has a line which matches the given
		/// pattern.
		/// </summary>
		/// <param name="t">the Throwable to throw</param>
		/// <param name="pattern">the pattern against which to match the call stack trace</param>
		/// <returns>the stub in progress</returns>
		public static Stubber DoThrowWhenCallStackMatches(Exception t, string pattern)
		{
			return Org.Mockito.Mockito.DoAnswer(new _Answer_51(t, pattern));
		}

		private sealed class _Answer_51 : Answer<object>
		{
			public _Answer_51(Exception t, string pattern)
			{
				this.t = t;
				this.pattern = pattern;
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocation)
			{
				t.SetStackTrace(Thread.CurrentThread().GetStackTrace());
				foreach (StackTraceElement elem in t.GetStackTrace())
				{
					if (elem.ToString().Matches(pattern))
					{
						throw t;
					}
				}
				return invocation.CallRealMethod();
			}

			private readonly Exception t;

			private readonly string pattern;
		}
	}
}
