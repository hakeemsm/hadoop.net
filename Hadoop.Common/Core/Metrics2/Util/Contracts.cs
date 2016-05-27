using System;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Util
{
	/// <summary>Additional helpers (besides guava Preconditions) for programming by contract
	/// 	</summary>
	public class Contracts
	{
		private Contracts()
		{
		}

		/// <summary>Check an argument for false conditions</summary>
		/// <?/>
		/// <param name="arg">the argument to check</param>
		/// <param name="expression">the boolean expression for the condition</param>
		/// <param name="msg">
		/// the error message if
		/// <paramref name="expression"/>
		/// is false
		/// </param>
		/// <returns>the argument for convenience</returns>
		public static T CheckArg<T>(T arg, bool expression, object msg)
		{
			if (!expression)
			{
				throw new ArgumentException(msg.ToString() + ": " + arg);
			}
			return arg;
		}

		/// <summary>Check an argument for false conditions</summary>
		/// <param name="arg">the argument to check</param>
		/// <param name="expression">the boolean expression for the condition</param>
		/// <param name="msg">
		/// the error message if
		/// <paramref name="expression"/>
		/// is false
		/// </param>
		/// <returns>the argument for convenience</returns>
		public static int CheckArg(int arg, bool expression, object msg)
		{
			if (!expression)
			{
				throw new ArgumentException(msg.ToString() + ": " + arg);
			}
			return arg;
		}

		/// <summary>Check an argument for false conditions</summary>
		/// <param name="arg">the argument to check</param>
		/// <param name="expression">the boolean expression for the condition</param>
		/// <param name="msg">
		/// the error message if
		/// <paramref name="expression"/>
		/// is false
		/// </param>
		/// <returns>the argument for convenience</returns>
		public static long CheckArg(long arg, bool expression, object msg)
		{
			if (!expression)
			{
				throw new ArgumentException(msg.ToString() + ": " + arg);
			}
			return arg;
		}

		/// <summary>Check an argument for false conditions</summary>
		/// <param name="arg">the argument to check</param>
		/// <param name="expression">the boolean expression for the condition</param>
		/// <param name="msg">
		/// the error message if
		/// <paramref name="expression"/>
		/// is false
		/// </param>
		/// <returns>the argument for convenience</returns>
		public static float CheckArg(float arg, bool expression, object msg)
		{
			if (!expression)
			{
				throw new ArgumentException(msg.ToString() + ": " + arg);
			}
			return arg;
		}

		/// <summary>Check an argument for false conditions</summary>
		/// <param name="arg">the argument to check</param>
		/// <param name="expression">the boolean expression for the condition</param>
		/// <param name="msg">
		/// the error message if
		/// <paramref name="expression"/>
		/// is false
		/// </param>
		/// <returns>the argument for convenience</returns>
		public static double CheckArg(double arg, bool expression, object msg)
		{
			if (!expression)
			{
				throw new ArgumentException(msg.ToString() + ": " + arg);
			}
			return arg;
		}
	}
}
