using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	/// <summary>
	/// Implements the -a (and) operator for the
	/// <see cref="Find"/>
	/// command.
	/// </summary>
	internal sealed class And : org.apache.hadoop.fs.shell.find.BaseExpression
	{
		/// <summary>Registers this expression with the specified factory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void registerExpression(org.apache.hadoop.fs.shell.find.ExpressionFactory
			 factory)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.And
				)), "-a");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.find.And
				)), "-and");
		}

		private static readonly string[] USAGE = new string[] { "expression -a expression"
			, "expression -and expression", "expression expression" };

		private static readonly string[] HELP = new string[] { "Logical AND operator for joining two expressions. Returns"
			, "true if both child expressions return true. Implied by the", "juxtaposition of two expressions and so does not need to be"
			, "explicitly specified. The second expression will not be", "applied if the first fails."
			 };

		public And()
			: base()
		{
			setUsage(USAGE);
			setHelp(HELP);
		}

		/// <summary>
		/// Applies child expressions to the
		/// <see cref="org.apache.hadoop.fs.shell.PathData"/>
		/// item. If all pass then
		/// returns
		/// <see cref="Result.PASS"/>
		/// else returns the result of the first
		/// non-passing expression.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.shell.find.Result apply(org.apache.hadoop.fs.shell.PathData
			 item, int depth)
		{
			org.apache.hadoop.fs.shell.find.Result result = org.apache.hadoop.fs.shell.find.Result
				.PASS;
			foreach (org.apache.hadoop.fs.shell.find.Expression child in getChildren())
			{
				org.apache.hadoop.fs.shell.find.Result childResult = child.apply(item, -1);
				result = result.combine(childResult);
				if (!result.isPass())
				{
					return result;
				}
			}
			return result;
		}

		public override bool isOperator()
		{
			return true;
		}

		public override int getPrecedence()
		{
			return 200;
		}

		public override void addChildren(java.util.Deque<org.apache.hadoop.fs.shell.find.Expression
			> expressions)
		{
			addChildren(expressions, 2);
		}
	}
}
