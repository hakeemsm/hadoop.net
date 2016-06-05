using Org.Apache.Hadoop.FS.Shell;


namespace Org.Apache.Hadoop.FS.Shell.Find
{
	/// <summary>
	/// Implements the -a (and) operator for the
	/// <see cref="Find"/>
	/// command.
	/// </summary>
	internal sealed class And : BaseExpression
	{
		/// <summary>Registers this expression with the specified factory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void RegisterExpression(ExpressionFactory factory)
		{
			factory.AddClass(typeof(Org.Apache.Hadoop.FS.Shell.Find.And), "-a");
			factory.AddClass(typeof(Org.Apache.Hadoop.FS.Shell.Find.And), "-and");
		}

		private static readonly string[] Usage = new string[] { "expression -a expression"
			, "expression -and expression", "expression expression" };

		private static readonly string[] Help = new string[] { "Logical AND operator for joining two expressions. Returns"
			, "true if both child expressions return true. Implied by the", "juxtaposition of two expressions and so does not need to be"
			, "explicitly specified. The second expression will not be", "applied if the first fails."
			 };

		public And()
			: base()
		{
			SetUsage(Usage);
			SetHelp(Help);
		}

		/// <summary>
		/// Applies child expressions to the
		/// <see cref="Org.Apache.Hadoop.FS.Shell.PathData"/>
		/// item. If all pass then
		/// returns
		/// <see cref="Result.Pass"/>
		/// else returns the result of the first
		/// non-passing expression.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override Result Apply(PathData item, int depth)
		{
			Result result = Result.Pass;
			foreach (Expression child in GetChildren())
			{
				Result childResult = child.Apply(item, -1);
				result = result.Combine(childResult);
				if (!result.IsPass())
				{
					return result;
				}
			}
			return result;
		}

		public override bool IsOperator()
		{
			return true;
		}

		public override int GetPrecedence()
		{
			return 200;
		}

		public override void AddChildren(Deque<Expression> expressions)
		{
			AddChildren(expressions, 2);
		}
	}
}
