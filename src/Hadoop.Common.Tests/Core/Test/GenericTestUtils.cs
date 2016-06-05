using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Mockito.Invocation;
using Org.Slf4j;

using Management;

namespace Org.Apache.Hadoop.Test
{
	/// <summary>Test provides some very generic helpers which might be used across the tests
	/// 	</summary>
	public abstract class GenericTestUtils
	{
		private static readonly AtomicInteger sequence = new AtomicInteger();

		public static void DisableLog(Log log)
		{
			// We expect that commons-logging is a wrapper around Log4j.
			DisableLog((Log4JLogger)log);
		}

		public static Logger ToLog4j(Logger logger)
		{
			return LogManager.GetLogger(logger.GetName());
		}

		public static void DisableLog(Log4JLogger log)
		{
			log.GetLogger().SetLevel(Level.Off);
		}

		public static void DisableLog(Logger logger)
		{
			logger.SetLevel(Level.Off);
		}

		public static void DisableLog(Logger logger)
		{
			DisableLog(ToLog4j(logger));
		}

		public static void SetLogLevel(Log log, Level level)
		{
			// We expect that commons-logging is a wrapper around Log4j.
			SetLogLevel((Log4JLogger)log, level);
		}

		public static void SetLogLevel(Log4JLogger log, Level level)
		{
			log.GetLogger().SetLevel(level);
		}

		public static void SetLogLevel(Logger logger, Level level)
		{
			logger.SetLevel(level);
		}

		public static void SetLogLevel(Logger logger, Level level)
		{
			SetLogLevel(ToLog4j(logger), level);
		}

		/// <summary>Extracts the name of the method where the invocation has happened</summary>
		/// <returns>String name of the invoking method</returns>
		public static string GetMethodName()
		{
			return Thread.CurrentThread().GetStackTrace()[2].GetMethodName();
		}

		/// <summary>Generates a process-wide unique sequence number.</summary>
		/// <returns>an unique sequence number</returns>
		public static int UniqueSequenceId()
		{
			return sequence.IncrementAndGet();
		}

		/// <summary>Assert that a given file exists.</summary>
		public static void AssertExists(FilePath f)
		{
			Assert.True("File " + f + " should exist", f.Exists());
		}

		/// <summary>List all of the files in 'dir' that match the regex 'pattern'.</summary>
		/// <remarks>
		/// List all of the files in 'dir' that match the regex 'pattern'.
		/// Then check that this list is identical to 'expectedMatches'.
		/// </remarks>
		/// <exception cref="System.IO.IOException">if the dir is inaccessible</exception>
		public static void AssertGlobEquals(FilePath dir, string pattern, params string[]
			 expectedMatches)
		{
			ICollection<string> found = Sets.NewTreeSet();
			foreach (FilePath f in FileUtil.ListFiles(dir))
			{
				if (f.GetName().Matches(pattern))
				{
					found.AddItem(f.GetName());
				}
			}
			ICollection<string> expectedSet = Sets.NewTreeSet(Arrays.AsList(expectedMatches));
			Assert.Equal("Bad files matching " + pattern + " in " + dir, Joiner
				.On(",").Join(expectedSet), Joiner.On(",").Join(found));
		}

		public static void AssertExceptionContains(string @string, Exception t)
		{
			string msg = t.Message;
			Assert.True("Expected to find '" + @string + "' but got unexpected exception:"
				 + StringUtils.StringifyException(t), msg.Contains(@string));
		}

		/// <exception cref="TimeoutException"/>
		/// <exception cref="System.Exception"/>
		public static void WaitFor(Supplier<bool> check, int checkEveryMillis, int waitForMillis
			)
		{
			long st = Time.Now();
			do
			{
				bool result = check.Get();
				if (result)
				{
					return;
				}
				Thread.Sleep(checkEveryMillis);
			}
			while (Time.Now() - st < waitForMillis);
			throw new TimeoutException("Timed out waiting for condition. " + "Thread diagnostics:\n"
				 + TimedOutTestsListener.BuildThreadDiagnosticString());
		}

		public class LogCapturer
		{
			private StringWriter sw = new StringWriter();

			private WriterAppender appender;

			private Logger logger;

			public static GenericTestUtils.LogCapturer CaptureLogs(Log l)
			{
				Logger logger = ((Log4JLogger)l).GetLogger();
				GenericTestUtils.LogCapturer c = new GenericTestUtils.LogCapturer(logger);
				return c;
			}

			private LogCapturer(Logger logger)
			{
				this.logger = logger;
				Layout layout = Logger.GetRootLogger().GetAppender("stdout").GetLayout();
				WriterAppender wa = new WriterAppender(layout, sw);
				logger.AddAppender(wa);
			}

			public virtual string GetOutput()
			{
				return sw.ToString();
			}

			public virtual void StopCapturing()
			{
				logger.RemoveAppender(appender);
			}
		}

		/// <summary>
		/// Mockito answer helper that triggers one latch as soon as the
		/// method is called, then waits on another before continuing.
		/// </summary>
		public class DelayAnswer : Org.Mockito.Stubbing.Answer<object>
		{
			private readonly Log Log;

			private readonly CountDownLatch fireLatch = new CountDownLatch(1);

			private readonly CountDownLatch waitLatch = new CountDownLatch(1);

			private readonly CountDownLatch resultLatch = new CountDownLatch(1);

			private readonly AtomicInteger fireCounter = new AtomicInteger(0);

			private readonly AtomicInteger resultCounter = new AtomicInteger(0);

			private volatile Exception thrown;

			private volatile object returnValue;

			public DelayAnswer(Log log)
			{
				// Result fields set after proceed() is called.
				this.Log = log;
			}

			/// <summary>Wait until the method is called.</summary>
			/// <exception cref="System.Exception"/>
			public virtual void WaitForCall()
			{
				fireLatch.Await();
			}

			/// <summary>Tell the method to proceed.</summary>
			/// <remarks>
			/// Tell the method to proceed.
			/// This should only be called after waitForCall()
			/// </remarks>
			public virtual void Proceed()
			{
				waitLatch.CountDown();
			}

			/// <exception cref="System.Exception"/>
			public virtual object Answer(InvocationOnMock invocation)
			{
				Log.Info("DelayAnswer firing fireLatch");
				fireCounter.GetAndIncrement();
				fireLatch.CountDown();
				try
				{
					Log.Info("DelayAnswer waiting on waitLatch");
					waitLatch.Await();
					Log.Info("DelayAnswer delay complete");
				}
				catch (Exception ie)
				{
					throw new IOException("Interrupted waiting on latch", ie);
				}
				return PassThrough(invocation);
			}

			/// <exception cref="System.Exception"/>
			protected internal virtual object PassThrough(InvocationOnMock invocation)
			{
				try
				{
					object ret = invocation.CallRealMethod();
					returnValue = ret;
					return ret;
				}
				catch (Exception t)
				{
					thrown = t;
					throw;
				}
				finally
				{
					resultCounter.IncrementAndGet();
					resultLatch.CountDown();
				}
			}

			/// <summary>
			/// After calling proceed(), this will wait until the call has
			/// completed and a result has been returned to the caller.
			/// </summary>
			/// <exception cref="System.Exception"/>
			public virtual void WaitForResult()
			{
				resultLatch.Await();
			}

			/// <summary>
			/// After the call has gone through, return any exception that
			/// was thrown, or null if no exception was thrown.
			/// </summary>
			public virtual Exception GetThrown()
			{
				return thrown;
			}

			/// <summary>
			/// After the call has gone through, return the call's return value,
			/// or null in case it was void or an exception was thrown.
			/// </summary>
			public virtual object GetReturnValue()
			{
				return returnValue;
			}

			public virtual int GetFireCount()
			{
				return fireCounter.Get();
			}

			public virtual int GetResultCount()
			{
				return resultCounter.Get();
			}
		}

		/// <summary>
		/// An Answer implementation that simply forwards all calls through
		/// to a delegate.
		/// </summary>
		/// <remarks>
		/// An Answer implementation that simply forwards all calls through
		/// to a delegate.
		/// This is useful as the default Answer for a mock object, to create
		/// something like a spy on an RPC proxy. For example:
		/// <code>
		/// NamenodeProtocol origNNProxy = secondary.getNameNode();
		/// NamenodeProtocol spyNNProxy = Mockito.mock(NameNodeProtocol.class,
		/// new DelegateAnswer(origNNProxy);
		/// doThrow(...).when(spyNNProxy).getBlockLocations(...);
		/// ...
		/// </code>
		/// </remarks>
		public class DelegateAnswer : Org.Mockito.Stubbing.Answer<object>
		{
			private readonly object delegate_;

			private readonly Log log;

			public DelegateAnswer(object delegate_)
				: this(null, delegate_)
			{
			}

			public DelegateAnswer(Log log, object delegate_)
			{
				this.log = log;
				this.delegate_ = delegate_;
			}

			/// <exception cref="System.Exception"/>
			public virtual object Answer(InvocationOnMock invocation)
			{
				try
				{
					if (log != null)
					{
						log.Info("Call to " + invocation + " on " + delegate_, new Exception("TRACE"));
					}
					return invocation.GetMethod().Invoke(delegate_, invocation.GetArguments());
				}
				catch (TargetInvocationException ite)
				{
					throw ite.InnerException;
				}
			}
		}

		/// <summary>
		/// An Answer implementation which sleeps for a random number of milliseconds
		/// between 0 and a configurable value before delegating to the real
		/// implementation of the method.
		/// </summary>
		/// <remarks>
		/// An Answer implementation which sleeps for a random number of milliseconds
		/// between 0 and a configurable value before delegating to the real
		/// implementation of the method. This can be useful for drawing out race
		/// conditions.
		/// </remarks>
		public class SleepAnswer : Org.Mockito.Stubbing.Answer<object>
		{
			private readonly int maxSleepTime;

			private static Random r = new Random();

			public SleepAnswer(int maxSleepTime)
			{
				this.maxSleepTime = maxSleepTime;
			}

			/// <exception cref="System.Exception"/>
			public virtual object Answer(InvocationOnMock invocation)
			{
				bool interrupted = false;
				try
				{
					Thread.Sleep(r.Next(maxSleepTime));
				}
				catch (Exception)
				{
					interrupted = true;
				}
				try
				{
					return invocation.CallRealMethod();
				}
				finally
				{
					if (interrupted)
					{
						Thread.CurrentThread().Interrupt();
					}
				}
			}
		}

		public static void AssertDoesNotMatch(string output, string pattern)
		{
			NUnit.Framework.Assert.IsFalse("Expected output to match /" + pattern + "/" + " but got:\n"
				 + output, Pattern.Compile(pattern).Matcher(output).Find());
		}

		public static void AssertMatches(string output, string pattern)
		{
			Assert.True("Expected output to match /" + pattern + "/" + " but got:\n"
				 + output, Pattern.Compile(pattern).Matcher(output).Find());
		}

		public static void AssertValueNear(long expected, long actual, long allowedError)
		{
			AssertValueWithinRange(expected - allowedError, expected + allowedError, actual);
		}

		public static void AssertValueWithinRange(long expectedMin, long expectedMax, long
			 actual)
		{
			Assert.True("Expected " + actual + " to be in range (" + expectedMin
				 + "," + expectedMax + ")", expectedMin <= actual && actual <= expectedMax);
		}

		/// <summary>
		/// Assert that there are no threads running whose name matches the
		/// given regular expression.
		/// </summary>
		/// <param name="regex">the regex to match against</param>
		public static void AssertNoThreadsMatching(string regex)
		{
			Pattern pattern = Pattern.Compile(regex);
			ThreadMXBean threadBean = ManagementFactory.GetThreadMXBean();
			ThreadInfo[] infos = threadBean.GetThreadInfo(threadBean.GetAllThreadIds(), 20);
			foreach (ThreadInfo info in infos)
			{
				if (info == null)
				{
					continue;
				}
				if (pattern.Matcher(info.GetThreadName()).Matches())
				{
					NUnit.Framework.Assert.Fail("Leaked thread: " + info + "\n" + Joiner.On("\n").Join
						(info.GetStackTrace()));
				}
			}
		}

		/// <summary>Skip test if native build profile of Maven is not activated.</summary>
		/// <remarks>
		/// Skip test if native build profile of Maven is not activated.
		/// Sub-project using this must set 'runningWithNative' property to true
		/// in the definition of native profile in pom.xml.
		/// </remarks>
		public static void AssumeInNativeProfile()
		{
			Assume.AssumeTrue(Extensions.ValueOf(Runtime.GetProperty("runningWithNative"
				, "false")));
		}
	}
}
