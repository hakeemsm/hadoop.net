using System;
using Org.Apache.Hadoop.Lib.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Lang
{
	/// <summary>
	/// Adapter class that allows <code>Runnable</code>s and <code>Callable</code>s to
	/// be treated as the other.
	/// </summary>
	public class RunnableCallable : Callable<Void>, Runnable
	{
		private Runnable runnable;

		private Callable<object> callable;

		/// <summary>Constructor that takes a runnable.</summary>
		/// <param name="runnable">runnable.</param>
		public RunnableCallable(Runnable runnable)
		{
			this.runnable = Check.NotNull(runnable, "runnable");
		}

		/// <summary>Constructor that takes a callable.</summary>
		/// <param name="callable">callable.</param>
		public RunnableCallable(Callable<object> callable)
		{
			this.callable = Check.NotNull(callable, "callable");
		}

		/// <summary>Invokes the wrapped callable/runnable as a callable.</summary>
		/// <returns>void</returns>
		/// <exception cref="System.Exception">thrown by the wrapped callable/runnable invocation.
		/// 	</exception>
		public virtual Void Call()
		{
			if (runnable != null)
			{
				runnable.Run();
			}
			else
			{
				callable.Call();
			}
			return null;
		}

		/// <summary>Invokes the wrapped callable/runnable as a runnable.</summary>
		/// <exception cref="Sharpen.RuntimeException">thrown by the wrapped callable/runnable invocation.
		/// 	</exception>
		public virtual void Run()
		{
			if (runnable != null)
			{
				runnable.Run();
			}
			else
			{
				try
				{
					callable.Call();
				}
				catch (Exception ex)
				{
					throw new RuntimeException(ex);
				}
			}
		}

		/// <summary>Returns the class name of the wrapper callable/runnable.</summary>
		/// <returns>the class name of the wrapper callable/runnable.</returns>
		public override string ToString()
		{
			return (runnable != null) ? runnable.GetType().Name : callable.GetType().Name;
		}
	}
}
