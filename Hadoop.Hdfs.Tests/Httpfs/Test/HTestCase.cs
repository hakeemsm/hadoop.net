using System;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	public abstract class HTestCase
	{
		public const string TestWaitforRatioProp = "test.waitfor.ratio";

		static HTestCase()
		{
			SysPropsForTestsLoader.Init();
		}

		private static float WaitforRatioDefault = float.ParseFloat(Runtime.GetProperty(TestWaitforRatioProp
			, "1"));

		private float waitForRatio = WaitforRatioDefault;

		[Rule]
		public MethodRule testDir = new TestDirHelper();

		[Rule]
		public MethodRule jettyTestHelper = new TestJettyHelper();

		[Rule]
		public MethodRule exceptionHelper = new TestExceptionHelper();

		/// <summary>
		/// Sets the 'wait for ratio' used in the
		/// <see cref="Sleep(long)"/>
		/// ,
		/// <see cref="WaitFor(int, Predicate)"/>
		/// and
		/// <see cref="WaitFor(int, bool, Predicate)"/>
		/// method for the current
		/// test class.
		/// <p/>
		/// This is useful when running tests in slow machine for tests
		/// that are time sensitive.
		/// </summary>
		/// <param name="ratio">the 'wait for ratio' to set.</param>
		protected internal virtual void SetWaitForRatio(float ratio)
		{
			waitForRatio = ratio;
		}

		/*
		* Returns the 'wait for ratio' used in the {@link #sleep(long)},
		* {@link #waitFor(int, Predicate)} and
		* {@link #waitFor(int, boolean, Predicate)} methods for the current
		* test class.
		* <p/>
		* This is useful when running tests in slow machine for tests
		* that are time sensitive.
		* <p/>
		* The default value is obtained from the Java System property
		* <code>test.wait.for.ratio</code> which defaults to <code>1</code>.
		*
		* @return the 'wait for ratio' for the current test class.
		*/
		protected internal virtual float GetWaitForRatio()
		{
			return waitForRatio;
		}

		/// <summary>
		/// A predicate 'closure' used by the
		/// <see cref="HTestCase.WaitFor(int, Predicate)"/>
		/// and
		/// <see cref="HTestCase.WaitFor(int, bool, Predicate)"/>
		/// methods.
		/// </summary>
		public interface Predicate
		{
			/// <summary>Perform a predicate evaluation.</summary>
			/// <returns>the boolean result of the evaluation.</returns>
			/// <exception cref="System.Exception">thrown if the predicate evaluation could not evaluate.
			/// 	</exception>
			bool Evaluate();
		}

		/// <summary>Makes the current thread sleep for the specified number of milliseconds.
		/// 	</summary>
		/// <remarks>
		/// Makes the current thread sleep for the specified number of milliseconds.
		/// <p/>
		/// The sleep time is multiplied by the
		/// <see cref="GetWaitForRatio()"/>
		/// .
		/// </remarks>
		/// <param name="time">the number of milliseconds to sleep.</param>
		protected internal virtual void Sleep(long time)
		{
			try
			{
				Sharpen.Thread.Sleep((long)(GetWaitForRatio() * time));
			}
			catch (Exception ex)
			{
				System.Console.Error.WriteLine(MessageFormat.Format("Sleep interrupted, {0}", ex.
					ToString()));
			}
		}

		/// <summary>
		/// Waits up to the specified timeout for the given
		/// <see cref="Predicate"/>
		/// to
		/// become <code>true</code>, failing the test if the timeout is reached
		/// and the Predicate is still <code>false</code>.
		/// <p/>
		/// The timeout time is multiplied by the
		/// <see cref="GetWaitForRatio()"/>
		/// .
		/// </summary>
		/// <param name="timeout">the timeout in milliseconds to wait for the predicate.</param>
		/// <param name="predicate">the predicate ot evaluate.</param>
		/// <returns>
		/// the effective wait, in milli-seconds until the predicate become
		/// <code>true</code>.
		/// </returns>
		protected internal virtual long WaitFor(int timeout, HTestCase.Predicate predicate
			)
		{
			return WaitFor(timeout, false, predicate);
		}

		/// <summary>
		/// Waits up to the specified timeout for the given
		/// <see cref="Predicate"/>
		/// to
		/// become <code>true</code>.
		/// <p/>
		/// The timeout time is multiplied by the
		/// <see cref="GetWaitForRatio()"/>
		/// .
		/// </summary>
		/// <param name="timeout">the timeout in milliseconds to wait for the predicate.</param>
		/// <param name="failIfTimeout">
		/// indicates if the test should be failed if the
		/// predicate times out.
		/// </param>
		/// <param name="predicate">the predicate ot evaluate.</param>
		/// <returns>
		/// the effective wait, in milli-seconds until the predicate become
		/// <code>true</code> or <code>-1</code> if the predicate did not evaluate
		/// to <code>true</code>.
		/// </returns>
		protected internal virtual long WaitFor(int timeout, bool failIfTimeout, HTestCase.Predicate
			 predicate)
		{
			long started = Time.Now();
			long mustEnd = Time.Now() + (long)(GetWaitForRatio() * timeout);
			long lastEcho = 0;
			try
			{
				long waiting = mustEnd - Time.Now();
				System.Console.Out.WriteLine(MessageFormat.Format("Waiting up to [{0}] msec", waiting
					));
				bool eval;
				while (!(eval = predicate.Evaluate()) && Time.Now() < mustEnd)
				{
					if ((Time.Now() - lastEcho) > 5000)
					{
						waiting = mustEnd - Time.Now();
						System.Console.Out.WriteLine(MessageFormat.Format("Waiting up to [{0}] msec", waiting
							));
						lastEcho = Time.Now();
					}
					Sharpen.Thread.Sleep(100);
				}
				if (!eval)
				{
					if (failIfTimeout)
					{
						NUnit.Framework.Assert.Fail(MessageFormat.Format("Waiting timed out after [{0}] msec"
							, timeout));
					}
					else
					{
						System.Console.Out.WriteLine(MessageFormat.Format("Waiting timed out after [{0}] msec"
							, timeout));
					}
				}
				return (eval) ? Time.Now() - started : -1;
			}
			catch (Exception ex)
			{
				throw new RuntimeException(ex);
			}
		}
	}
}
