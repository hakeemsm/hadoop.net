using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	/// <summary>
	/// Implements the -print expression for the
	/// <see cref="Find"/>
	/// command.
	/// </summary>
	internal sealed class Print : org.apache.hadoop.fs.shell.find.BaseExpression
	{
		/// <summary>Registers this expression with the specified factory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void registerExpression(org.apache.hadoop.fs.shell.find.ExpressionFactory
			 factory)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.Print
				)), "-print");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.Print.Print0
				)), "-print0");
		}

		private static readonly string[] USAGE = new string[] { "-print", "-print0" };

		private static readonly string[] HELP = new string[] { "Always evaluates to true. Causes the current pathname to be"
			, "written to standard output followed by a newline. If the -print0", "expression is used then an ASCII NULL character is appended rather"
			, "than a newline." };

		private readonly string suffix;

		public Print()
			: this("\n")
		{
		}

		/// <summary>
		/// Construct a Print
		/// <see cref="Expression"/>
		/// with the specified suffix.
		/// </summary>
		private Print(string suffix)
			: base()
		{
			setUsage(USAGE);
			setHelp(HELP);
			this.suffix = suffix;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.shell.find.Result apply(org.apache.hadoop.fs.shell.PathData
			 item, int depth)
		{
			getOptions().getOut().Write(item.ToString() + suffix);
			return org.apache.hadoop.fs.shell.find.Result.PASS;
		}

		public override bool isAction()
		{
			return true;
		}

		/// <summary>Implements the -print0 expression.</summary>
		internal sealed class Print0 : org.apache.hadoop.fs.shell.find.FilterExpression
		{
			public Print0()
				: base(new org.apache.hadoop.fs.shell.find.Print("\x0"))
			{
			}
		}
	}
}
