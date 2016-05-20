using Sharpen;

namespace org.apache.hadoop.metrics2.util
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
		public static T checkArg<T>(T arg, bool expression, object msg)
		{
			if (!expression)
			{
				throw new System.ArgumentException(Sharpen.Runtime.getStringValueOf(msg) + ": " +
					 arg);
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
		public static int checkArg(int arg, bool expression, object msg)
		{
			if (!expression)
			{
				throw new System.ArgumentException(Sharpen.Runtime.getStringValueOf(msg) + ": " +
					 arg);
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
		public static long checkArg(long arg, bool expression, object msg)
		{
			if (!expression)
			{
				throw new System.ArgumentException(Sharpen.Runtime.getStringValueOf(msg) + ": " +
					 arg);
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
		public static float checkArg(float arg, bool expression, object msg)
		{
			if (!expression)
			{
				throw new System.ArgumentException(Sharpen.Runtime.getStringValueOf(msg) + ": " +
					 arg);
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
		public static double checkArg(double arg, bool expression, object msg)
		{
			if (!expression)
			{
				throw new System.ArgumentException(Sharpen.Runtime.getStringValueOf(msg) + ": " +
					 arg);
			}
			return arg;
		}
	}
}
