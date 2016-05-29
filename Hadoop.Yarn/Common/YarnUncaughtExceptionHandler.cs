using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	/// <summary>
	/// This class is intended to be installed by calling
	/// <see cref="Sharpen.Thread.SetDefaultUncaughtExceptionHandler(Sharpen.Thread.UncaughtExceptionHandler)
	/// 	"/>
	/// In the main entry point.  It is intended to try and cleanly shut down
	/// programs using the Yarn Event framework.
	/// Note: Right now it only will shut down the program if a Error is caught, but
	/// not any other exception.  Anything else is just logged.
	/// </summary>
	public class YarnUncaughtExceptionHandler : Sharpen.Thread.UncaughtExceptionHandler
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(YarnUncaughtExceptionHandler
			));

		public virtual void UncaughtException(Sharpen.Thread t, Exception e)
		{
			if (ShutdownHookManager.Get().IsShutdownInProgress())
			{
				Log.Error("Thread " + t + " threw an Throwable, but we are shutting " + "down, so ignoring this"
					, e);
			}
			else
			{
				if (e is Error)
				{
					try
					{
						Log.Fatal("Thread " + t + " threw an Error.  Shutting down now...", e);
					}
					catch
					{
					}
					//We don't want to not exit because of an issue with logging
					if (e is OutOfMemoryException)
					{
						//After catching an OOM java says it is undefined behavior, so don't
						//even try to clean up or we can get stuck on shutdown.
						try
						{
							System.Console.Error.WriteLine("Halting due to Out Of Memory Error...");
						}
						catch
						{
						}
						//Again we done want to exit because of logging issues.
						ExitUtil.Halt(-1);
					}
					else
					{
						ExitUtil.Terminate(-1);
					}
				}
				else
				{
					Log.Error("Thread " + t + " threw an Exception.", e);
				}
			}
		}
	}
}
