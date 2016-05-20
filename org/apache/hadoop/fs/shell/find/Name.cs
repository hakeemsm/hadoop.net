using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	/// <summary>
	/// Implements the -name expression for the
	/// <see cref="Find"/>
	/// command.
	/// </summary>
	internal sealed class Name : org.apache.hadoop.fs.shell.find.BaseExpression
	{
		/// <summary>Registers this expression with the specified factory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void registerExpression(org.apache.hadoop.fs.shell.find.ExpressionFactory
			 factory)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.Name
				)), "-name");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.Name.Iname
				)), "-iname");
		}

		private static readonly string[] USAGE = new string[] { "-name pattern", "-iname pattern"
			 };

		private static readonly string[] HELP = new string[] { "Evaluates as true if the basename of the file matches the"
			, "pattern using standard file system globbing.", "If -iname is used then the match is case insensitive."
			 };

		private org.apache.hadoop.fs.GlobPattern globPattern;

		private bool caseSensitive = true;

		/// <summary>Creates a case sensitive name expression.</summary>
		public Name()
			: this(true)
		{
		}

		/// <summary>
		/// Construct a Name
		/// <see cref="Expression"/>
		/// with a specified case sensitivity.
		/// </summary>
		/// <param name="caseSensitive">if true the comparisons are case sensitive.</param>
		private Name(bool caseSensitive)
			: base()
		{
			setUsage(USAGE);
			setHelp(HELP);
			setCaseSensitive(caseSensitive);
		}

		private void setCaseSensitive(bool caseSensitive)
		{
			this.caseSensitive = caseSensitive;
		}

		public override void addArguments(java.util.Deque<string> args)
		{
			addArguments(args, 1);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void prepare()
		{
			string argPattern = getArgument(1);
			if (!caseSensitive)
			{
				argPattern = org.apache.hadoop.util.StringUtils.toLowerCase(argPattern);
			}
			globPattern = new org.apache.hadoop.fs.GlobPattern(argPattern);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.shell.find.Result apply(org.apache.hadoop.fs.shell.PathData
			 item, int depth)
		{
			string name = getPath(item).getName();
			if (!caseSensitive)
			{
				name = org.apache.hadoop.util.StringUtils.toLowerCase(name);
			}
			if (globPattern.matches(name))
			{
				return org.apache.hadoop.fs.shell.find.Result.PASS;
			}
			else
			{
				return org.apache.hadoop.fs.shell.find.Result.FAIL;
			}
		}

		/// <summary>Case insensitive version of the -name expression.</summary>
		internal class Iname : org.apache.hadoop.fs.shell.find.FilterExpression
		{
			public Iname()
				: base(new org.apache.hadoop.fs.shell.find.Name(false))
			{
			}
		}
	}
}
