using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS.Shell;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>This class tests the command line parsing</summary>
	public class TestCommandFormat
	{
		private static IList<string> args;

		private static IList<string> expectedArgs;

		private static ICollection<string> expectedOpts;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			args = new AList<string>();
			expectedOpts = new HashSet<string>();
			expectedArgs = new AList<string>();
		}

		[NUnit.Framework.Test]
		public virtual void TestNoArgs()
		{
			CheckArgLimits(null, 0, 0);
			CheckArgLimits(null, 0, 1);
			CheckArgLimits(typeof(CommandFormat.NotEnoughArgumentsException), 1, 1);
			CheckArgLimits(typeof(CommandFormat.NotEnoughArgumentsException), 1, 2);
		}

		[NUnit.Framework.Test]
		public virtual void TestOneArg()
		{
			args = ListOf("a");
			expectedArgs = ListOf("a");
			CheckArgLimits(typeof(CommandFormat.TooManyArgumentsException), 0, 0);
			CheckArgLimits(null, 0, 1);
			CheckArgLimits(null, 1, 1);
			CheckArgLimits(null, 1, 2);
			CheckArgLimits(typeof(CommandFormat.NotEnoughArgumentsException), 2, 3);
		}

		[NUnit.Framework.Test]
		public virtual void TestTwoArgs()
		{
			args = ListOf("a", "b");
			expectedArgs = ListOf("a", "b");
			CheckArgLimits(typeof(CommandFormat.TooManyArgumentsException), 0, 0);
			CheckArgLimits(typeof(CommandFormat.TooManyArgumentsException), 1, 1);
			CheckArgLimits(null, 1, 2);
			CheckArgLimits(null, 2, 2);
			CheckArgLimits(null, 2, 3);
			CheckArgLimits(typeof(CommandFormat.NotEnoughArgumentsException), 3, 3);
		}

		[NUnit.Framework.Test]
		public virtual void TestOneOpt()
		{
			args = ListOf("-a");
			expectedOpts = SetOf("a");
			CheckArgLimits(typeof(CommandFormat.UnknownOptionException), 0, 0);
			CheckArgLimits(null, 0, 0, "a", "b");
			CheckArgLimits(typeof(CommandFormat.NotEnoughArgumentsException), 1, 1, "a", "b");
		}

		[NUnit.Framework.Test]
		public virtual void TestTwoOpts()
		{
			args = ListOf("-a", "-b");
			expectedOpts = SetOf("a", "b");
			CheckArgLimits(typeof(CommandFormat.UnknownOptionException), 0, 0);
			CheckArgLimits(null, 0, 0, "a", "b");
			CheckArgLimits(null, 0, 1, "a", "b");
			CheckArgLimits(typeof(CommandFormat.NotEnoughArgumentsException), 1, 1, "a", "b");
		}

		[NUnit.Framework.Test]
		public virtual void TestOptArg()
		{
			args = ListOf("-a", "b");
			expectedOpts = SetOf("a");
			expectedArgs = ListOf("b");
			CheckArgLimits(typeof(CommandFormat.UnknownOptionException), 0, 0);
			CheckArgLimits(typeof(CommandFormat.TooManyArgumentsException), 0, 0, "a", "b");
			CheckArgLimits(null, 0, 1, "a", "b");
			CheckArgLimits(null, 1, 1, "a", "b");
			CheckArgLimits(null, 1, 2, "a", "b");
			CheckArgLimits(typeof(CommandFormat.NotEnoughArgumentsException), 2, 2, "a", "b");
		}

		[NUnit.Framework.Test]
		public virtual void TestArgOpt()
		{
			args = ListOf("b", "-a");
			expectedArgs = ListOf("b", "-a");
			CheckArgLimits(typeof(CommandFormat.TooManyArgumentsException), 0, 0, "a", "b");
			CheckArgLimits(null, 1, 2, "a", "b");
			CheckArgLimits(null, 2, 2, "a", "b");
			CheckArgLimits(typeof(CommandFormat.NotEnoughArgumentsException), 3, 4, "a", "b");
		}

		[NUnit.Framework.Test]
		public virtual void TestOptStopOptArg()
		{
			args = ListOf("-a", "--", "-b", "c");
			expectedOpts = SetOf("a");
			expectedArgs = ListOf("-b", "c");
			CheckArgLimits(typeof(CommandFormat.UnknownOptionException), 0, 0);
			CheckArgLimits(typeof(CommandFormat.TooManyArgumentsException), 0, 1, "a", "b");
			CheckArgLimits(null, 2, 2, "a", "b");
			CheckArgLimits(typeof(CommandFormat.NotEnoughArgumentsException), 3, 4, "a", "b");
		}

		[NUnit.Framework.Test]
		public virtual void TestOptDashArg()
		{
			args = ListOf("-b", "-", "-c");
			expectedOpts = SetOf("b");
			expectedArgs = ListOf("-", "-c");
			CheckArgLimits(typeof(CommandFormat.UnknownOptionException), 0, 0);
			CheckArgLimits(typeof(CommandFormat.TooManyArgumentsException), 0, 0, "b", "c");
			CheckArgLimits(typeof(CommandFormat.TooManyArgumentsException), 1, 1, "b", "c");
			CheckArgLimits(null, 2, 2, "b", "c");
			CheckArgLimits(typeof(CommandFormat.NotEnoughArgumentsException), 3, 4, "b", "c");
		}

		[NUnit.Framework.Test]
		public virtual void TestOldArgsWithIndex()
		{
			string[] arrayArgs = new string[] { "ignore", "-a", "b", "-c" };
			{
				CommandFormat cf = new CommandFormat(0, 9, "a", "c");
				IList<string> parsedArgs = cf.Parse(arrayArgs, 0);
				NUnit.Framework.Assert.AreEqual(SetOf(), cf.GetOpts());
				NUnit.Framework.Assert.AreEqual(ListOf("ignore", "-a", "b", "-c"), parsedArgs);
			}
			{
				CommandFormat cf = new CommandFormat(0, 9, "a", "c");
				IList<string> parsedArgs = cf.Parse(arrayArgs, 1);
				NUnit.Framework.Assert.AreEqual(SetOf("a"), cf.GetOpts());
				NUnit.Framework.Assert.AreEqual(ListOf("b", "-c"), parsedArgs);
			}
			{
				CommandFormat cf = new CommandFormat(0, 9, "a", "c");
				IList<string> parsedArgs = cf.Parse(arrayArgs, 2);
				NUnit.Framework.Assert.AreEqual(SetOf(), cf.GetOpts());
				NUnit.Framework.Assert.AreEqual(ListOf("b", "-c"), parsedArgs);
			}
		}

		private static CommandFormat CheckArgLimits<T>(Type expectedErr, int min, int max
			, params string[] opts)
		{
			CommandFormat cf = new CommandFormat(min, max, opts);
			IList<string> parsedArgs = new AList<string>(args);
			Type cfError = null;
			try
			{
				cf.Parse(parsedArgs);
			}
			catch (ArgumentException e)
			{
				System.Console.Out.WriteLine(e.Message);
				cfError = e.GetType();
			}
			NUnit.Framework.Assert.AreEqual(expectedErr, cfError);
			if (expectedErr == null)
			{
				NUnit.Framework.Assert.AreEqual(expectedArgs, parsedArgs);
				NUnit.Framework.Assert.AreEqual(expectedOpts, cf.GetOpts());
			}
			return cf;
		}

		// Don't use generics to avoid warning:
		// unchecked generic array creation of type T[] for varargs parameter
		private static IList<string> ListOf(params string[] objects)
		{
			return Arrays.AsList(objects);
		}

		private static ICollection<string> SetOf(params string[] objects)
		{
			return new HashSet<string>(ListOf(objects));
		}
	}
}
