using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>This class tests the command line parsing</summary>
	public class TestCommandFormat
	{
		private static System.Collections.Generic.IList<string> args;

		private static System.Collections.Generic.IList<string> expectedArgs;

		private static System.Collections.Generic.ICollection<string> expectedOpts;

		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			args = new System.Collections.Generic.List<string>();
			expectedOpts = new java.util.HashSet<string>();
			expectedArgs = new System.Collections.Generic.List<string>();
		}

		[NUnit.Framework.Test]
		public virtual void testNoArgs()
		{
			checkArgLimits(null, 0, 0);
			checkArgLimits(null, 0, 1);
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.NotEnoughArgumentsException
				)), 1, 1);
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.NotEnoughArgumentsException
				)), 1, 2);
		}

		[NUnit.Framework.Test]
		public virtual void testOneArg()
		{
			args = listOf("a");
			expectedArgs = listOf("a");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.TooManyArgumentsException
				)), 0, 0);
			checkArgLimits(null, 0, 1);
			checkArgLimits(null, 1, 1);
			checkArgLimits(null, 1, 2);
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.NotEnoughArgumentsException
				)), 2, 3);
		}

		[NUnit.Framework.Test]
		public virtual void testTwoArgs()
		{
			args = listOf("a", "b");
			expectedArgs = listOf("a", "b");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.TooManyArgumentsException
				)), 0, 0);
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.TooManyArgumentsException
				)), 1, 1);
			checkArgLimits(null, 1, 2);
			checkArgLimits(null, 2, 2);
			checkArgLimits(null, 2, 3);
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.NotEnoughArgumentsException
				)), 3, 3);
		}

		[NUnit.Framework.Test]
		public virtual void testOneOpt()
		{
			args = listOf("-a");
			expectedOpts = setOf("a");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.UnknownOptionException
				)), 0, 0);
			checkArgLimits(null, 0, 0, "a", "b");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.NotEnoughArgumentsException
				)), 1, 1, "a", "b");
		}

		[NUnit.Framework.Test]
		public virtual void testTwoOpts()
		{
			args = listOf("-a", "-b");
			expectedOpts = setOf("a", "b");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.UnknownOptionException
				)), 0, 0);
			checkArgLimits(null, 0, 0, "a", "b");
			checkArgLimits(null, 0, 1, "a", "b");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.NotEnoughArgumentsException
				)), 1, 1, "a", "b");
		}

		[NUnit.Framework.Test]
		public virtual void testOptArg()
		{
			args = listOf("-a", "b");
			expectedOpts = setOf("a");
			expectedArgs = listOf("b");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.UnknownOptionException
				)), 0, 0);
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.TooManyArgumentsException
				)), 0, 0, "a", "b");
			checkArgLimits(null, 0, 1, "a", "b");
			checkArgLimits(null, 1, 1, "a", "b");
			checkArgLimits(null, 1, 2, "a", "b");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.NotEnoughArgumentsException
				)), 2, 2, "a", "b");
		}

		[NUnit.Framework.Test]
		public virtual void testArgOpt()
		{
			args = listOf("b", "-a");
			expectedArgs = listOf("b", "-a");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.TooManyArgumentsException
				)), 0, 0, "a", "b");
			checkArgLimits(null, 1, 2, "a", "b");
			checkArgLimits(null, 2, 2, "a", "b");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.NotEnoughArgumentsException
				)), 3, 4, "a", "b");
		}

		[NUnit.Framework.Test]
		public virtual void testOptStopOptArg()
		{
			args = listOf("-a", "--", "-b", "c");
			expectedOpts = setOf("a");
			expectedArgs = listOf("-b", "c");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.UnknownOptionException
				)), 0, 0);
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.TooManyArgumentsException
				)), 0, 1, "a", "b");
			checkArgLimits(null, 2, 2, "a", "b");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.NotEnoughArgumentsException
				)), 3, 4, "a", "b");
		}

		[NUnit.Framework.Test]
		public virtual void testOptDashArg()
		{
			args = listOf("-b", "-", "-c");
			expectedOpts = setOf("b");
			expectedArgs = listOf("-", "-c");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.UnknownOptionException
				)), 0, 0);
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.TooManyArgumentsException
				)), 0, 0, "b", "c");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.TooManyArgumentsException
				)), 1, 1, "b", "c");
			checkArgLimits(null, 2, 2, "b", "c");
			checkArgLimits(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.CommandFormat.NotEnoughArgumentsException
				)), 3, 4, "b", "c");
		}

		[NUnit.Framework.Test]
		public virtual void testOldArgsWithIndex()
		{
			string[] arrayArgs = new string[] { "ignore", "-a", "b", "-c" };
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(0, 9, "a", "c");
				System.Collections.Generic.IList<string> parsedArgs = cf.parse(arrayArgs, 0);
				NUnit.Framework.Assert.AreEqual(setOf(), cf.getOpts());
				NUnit.Framework.Assert.AreEqual(listOf("ignore", "-a", "b", "-c"), parsedArgs);
			}
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(0, 9, "a", "c");
				System.Collections.Generic.IList<string> parsedArgs = cf.parse(arrayArgs, 1);
				NUnit.Framework.Assert.AreEqual(setOf("a"), cf.getOpts());
				NUnit.Framework.Assert.AreEqual(listOf("b", "-c"), parsedArgs);
			}
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(0, 9, "a", "c");
				System.Collections.Generic.IList<string> parsedArgs = cf.parse(arrayArgs, 2);
				NUnit.Framework.Assert.AreEqual(setOf(), cf.getOpts());
				NUnit.Framework.Assert.AreEqual(listOf("b", "-c"), parsedArgs);
			}
		}

		private static org.apache.hadoop.fs.shell.CommandFormat checkArgLimits<T>(java.lang.Class
			 expectedErr, int min, int max, params string[] opts)
		{
			org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
				(min, max, opts);
			System.Collections.Generic.IList<string> parsedArgs = new System.Collections.Generic.List
				<string>(args);
			java.lang.Class cfError = null;
			try
			{
				cf.parse(parsedArgs);
			}
			catch (System.ArgumentException e)
			{
				System.Console.Out.WriteLine(e.Message);
				cfError = Sharpen.Runtime.getClassForObject(e);
			}
			NUnit.Framework.Assert.AreEqual(expectedErr, cfError);
			if (expectedErr == null)
			{
				NUnit.Framework.Assert.AreEqual(expectedArgs, parsedArgs);
				NUnit.Framework.Assert.AreEqual(expectedOpts, cf.getOpts());
			}
			return cf;
		}

		// Don't use generics to avoid warning:
		// unchecked generic array creation of type T[] for varargs parameter
		private static System.Collections.Generic.IList<string> listOf(params string[] objects
			)
		{
			return java.util.Arrays.asList(objects);
		}

		private static System.Collections.Generic.ICollection<string> setOf(params string
			[] objects)
		{
			return new java.util.HashSet<string>(listOf(objects));
		}
	}
}
