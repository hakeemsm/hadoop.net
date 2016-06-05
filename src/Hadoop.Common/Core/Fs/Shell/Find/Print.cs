using Org.Apache.Hadoop.FS.Shell;


namespace Org.Apache.Hadoop.FS.Shell.Find
{
	/// <summary>
	/// Implements the -print expression for the
	/// <see cref="Find"/>
	/// command.
	/// </summary>
	internal sealed class Print : BaseExpression
	{
		/// <summary>Registers this expression with the specified factory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void RegisterExpression(ExpressionFactory factory)
		{
			factory.AddClass(typeof(Org.Apache.Hadoop.FS.Shell.Find.Print), "-print");
			factory.AddClass(typeof(Print.Print0), "-print0");
		}

		private static readonly string[] Usage = new string[] { "-print", "-print0" };

		private static readonly string[] Help = new string[] { "Always evaluates to true. Causes the current pathname to be"
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
			SetUsage(Usage);
			SetHelp(Help);
			this.suffix = suffix;
		}

		/// <exception cref="System.IO.IOException"/>
		public override Result Apply(PathData item, int depth)
		{
			GetOptions().GetOut().Write(item.ToString() + suffix);
			return Result.Pass;
		}

		public override bool IsAction()
		{
			return true;
		}

		/// <summary>Implements the -print0 expression.</summary>
		internal sealed class Print0 : FilterExpression
		{
			public Print0()
				: base(new Print("\x0"))
			{
			}
		}
	}
}
