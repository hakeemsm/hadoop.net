using System;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	public class TestYarnUncaughtExceptionHandler
	{
		private static readonly YarnUncaughtExceptionHandler exHandler = new YarnUncaughtExceptionHandler
			();

		/// <summary>
		/// Throw
		/// <c>YarnRuntimeException</c>
		/// inside thread and
		/// check
		/// <c>YarnUncaughtExceptionHandler</c>
		/// instance
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUncaughtExceptionHandlerWithRuntimeException()
		{
			YarnUncaughtExceptionHandler spyYarnHandler = Org.Mockito.Mockito.Spy(exHandler);
			YarnRuntimeException yarnException = new YarnRuntimeException("test-yarn-runtime-exception"
				);
			Sharpen.Thread yarnThread = new Sharpen.Thread(new _Runnable_45(yarnException));
			yarnThread.SetUncaughtExceptionHandler(spyYarnHandler);
			NUnit.Framework.Assert.AreSame(spyYarnHandler, yarnThread.GetUncaughtExceptionHandler
				());
			yarnThread.Start();
			yarnThread.Join();
			Org.Mockito.Mockito.Verify(spyYarnHandler).UncaughtException(yarnThread, yarnException
				);
		}

		private sealed class _Runnable_45 : Runnable
		{
			public _Runnable_45(YarnRuntimeException yarnException)
			{
				this.yarnException = yarnException;
			}

			public void Run()
			{
				throw yarnException;
			}

			private readonly YarnRuntimeException yarnException;
		}

		/// <summary>
		/// <p>
		/// Throw
		/// <c>Error</c>
		/// inside thread and
		/// check
		/// <c>YarnUncaughtExceptionHandler</c>
		/// instance
		/// <p>
		/// Used
		/// <c>ExitUtil</c>
		/// class to avoid jvm exit through
		/// <c>System.exit(-1)</c>
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUncaughtExceptionHandlerWithError()
		{
			ExitUtil.DisableSystemExit();
			YarnUncaughtExceptionHandler spyErrorHandler = Org.Mockito.Mockito.Spy(exHandler);
			Error error = new Error("test-error");
			Sharpen.Thread errorThread = new Sharpen.Thread(new _Runnable_75(error));
			errorThread.SetUncaughtExceptionHandler(spyErrorHandler);
			NUnit.Framework.Assert.AreSame(spyErrorHandler, errorThread.GetUncaughtExceptionHandler
				());
			errorThread.Start();
			errorThread.Join();
			Org.Mockito.Mockito.Verify(spyErrorHandler).UncaughtException(errorThread, error);
		}

		private sealed class _Runnable_75 : Runnable
		{
			public _Runnable_75(Error error)
			{
				this.error = error;
			}

			public void Run()
			{
				throw error;
			}

			private readonly Error error;
		}

		/// <summary>
		/// <p>
		/// Throw
		/// <c>OutOfMemoryError</c>
		/// inside thread and
		/// check
		/// <c>YarnUncaughtExceptionHandler</c>
		/// instance
		/// <p>
		/// Used
		/// <c>ExitUtil</c>
		/// class to avoid jvm exit through
		/// <c>Runtime.getRuntime().halt(-1)</c>
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUncaughtExceptionHandlerWithOutOfMemoryError()
		{
			ExitUtil.DisableSystemHalt();
			YarnUncaughtExceptionHandler spyOomHandler = Org.Mockito.Mockito.Spy(exHandler);
			OutOfMemoryException oomError = new OutOfMemoryException("out-of-memory-error");
			Sharpen.Thread oomThread = new Sharpen.Thread(new _Runnable_104(oomError));
			oomThread.SetUncaughtExceptionHandler(spyOomHandler);
			NUnit.Framework.Assert.AreSame(spyOomHandler, oomThread.GetUncaughtExceptionHandler
				());
			oomThread.Start();
			oomThread.Join();
			Org.Mockito.Mockito.Verify(spyOomHandler).UncaughtException(oomThread, oomError);
		}

		private sealed class _Runnable_104 : Runnable
		{
			public _Runnable_104(OutOfMemoryException oomError)
			{
				this.oomError = oomError;
			}

			public void Run()
			{
				throw oomError;
			}

			private readonly OutOfMemoryException oomError;
		}
	}
}
