using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Hadoop.Common.Core.Fs.Shell;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
{
	public class Find : FsCommand
	{
		/// <summary>Register the names for the count command</summary>
		/// <param name="factory">the command factory that will instantiate this class</param>
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(Org.Apache.Hadoop.FS.Shell.Find.Find), "-find");
		}

		public const string Name = "find";

		public const string Usage = "<path> ... <expression> ...";

		public static readonly string Description;

		private static string[] Help = new string[] { "Finds all files that match the specified expression and"
			, "applies selected actions to them. If no <path> is specified", "then defaults to the current working directory. If no"
			, "expression is specified then defaults to -print." };

		private const string OptionFollowLink = "L";

		private const string OptionFollowArgLink = "H";

		/// <summary>List of expressions recognized by this command.</summary>
		private static readonly ICollection<Type> Expressions = new HashSet<Type>();

		private static void AddExpression(Type clazz)
		{
			Expressions.AddItem(clazz.AsSubclass<Expression>());
		}

		static Find()
		{
			// Initialize the static variables.
			// Operator Expressions
			AddExpression(typeof(And));
			// Action Expressions
			AddExpression(typeof(Print));
			// Navigation Expressions
			// Matcher Expressions
			AddExpression(typeof(Name));
			Description = BuildDescription(ExpressionFactory.GetExpressionFactory());
			// Register the expressions with the expression factory.
			RegisterExpressions(ExpressionFactory.GetExpressionFactory());
		}

		/// <summary>Options for use in this command</summary>
		private FindOptions options;

		/// <summary>Root expression for this instance of the command.</summary>
		private Expression rootExpression;

		/// <summary>
		/// Set of path items returning a
		/// <see cref="Result.Stop"/>
		/// result.
		/// </summary>
		private HashSet<Path> stopPaths = new HashSet<Path>();

		/// <summary>Register the expressions with the expression factory.</summary>
		private static void RegisterExpressions(ExpressionFactory factory)
		{
			foreach (Type exprClass in Expressions)
			{
				factory.RegisterExpression(exprClass);
			}
		}

		/// <summary>Build the description used by the help command.</summary>
		private static string BuildDescription(ExpressionFactory factory)
		{
			AList<Expression> operators = new AList<Expression>();
			AList<Expression> primaries = new AList<Expression>();
			foreach (Type exprClass in Expressions)
			{
				Expression expr = factory.CreateExpression(exprClass, null);
				if (expr.IsOperator())
				{
					operators.AddItem(expr);
				}
				else
				{
					primaries.AddItem(expr);
				}
			}
			operators.Sort(new _IComparer_120());
			primaries.Sort(new _IComparer_126());
			StringBuilder sb = new StringBuilder();
			foreach (string line in Help)
			{
				sb.Append(line).Append("\n");
			}
			sb.Append("\n");
			sb.Append("The following primary expressions are recognised:\n");
			foreach (Expression expr_1 in primaries)
			{
				foreach (string line_1 in expr_1.GetUsage())
				{
					sb.Append("  ").Append(line_1).Append("\n");
				}
				foreach (string line_2 in expr_1.GetHelp())
				{
					sb.Append("    ").Append(line_2).Append("\n");
				}
				sb.Append("\n");
			}
			sb.Append("The following operators are recognised:\n");
			foreach (Expression expr_2 in operators)
			{
				foreach (string line_1 in expr_2.GetUsage())
				{
					sb.Append("  ").Append(line_1).Append("\n");
				}
				foreach (string line_2 in expr_2.GetHelp())
				{
					sb.Append("    ").Append(line_2).Append("\n");
				}
				sb.Append("\n");
			}
			return sb.ToString();
		}

		private sealed class _IComparer_120 : IComparer<Expression>
		{
			public _IComparer_120()
			{
			}

			public int Compare(Expression arg0, Expression arg1)
			{
				return string.CompareOrdinal(arg0.GetType().FullName, arg1.GetType().FullName);
			}
		}

		private sealed class _IComparer_126 : IComparer<Expression>
		{
			public _IComparer_126()
			{
			}

			public int Compare(Expression arg0, Expression arg1)
			{
				return string.CompareOrdinal(arg0.GetType().FullName, arg1.GetType().FullName);
			}
		}

		/// <summary>Default constructor for the Find command.</summary>
		public Find()
		{
			SetRecursive(true);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessOptions(List<string> args)
		{
			CommandFormat cf = new CommandFormat(1, int.MaxValue, OptionFollowLink, OptionFollowArgLink
				, null);
			cf.Parse(args);
			if (cf.GetOpt(OptionFollowLink))
			{
				GetOptions().SetFollowLink(true);
			}
			else
			{
				if (cf.GetOpt(OptionFollowArgLink))
				{
					GetOptions().SetFollowArgLink(true);
				}
			}
			// search for first non-path argument (ie starts with a "-") and capture and
			// remove the remaining arguments as expressions
			List<string> expressionArgs = new List<string>();
			IEnumerator<string> it = args.GetEnumerator();
			bool isPath = true;
			while (it.HasNext())
			{
				string arg = it.Next();
				if (isPath)
				{
					if (arg.StartsWith("-"))
					{
						isPath = false;
					}
				}
				if (!isPath)
				{
					expressionArgs.AddItem(arg);
					it.Remove();
				}
			}
			if (args.IsEmpty())
			{
				args.AddItem(Path.CurDir);
			}
			Expression expression = ParseExpression(expressionArgs);
			if (!expression.IsAction())
			{
				Expression and = GetExpression(typeof(And));
				Deque<Expression> children = new List<Expression>();
				children.AddItem(GetExpression(typeof(Print)));
				children.AddItem(expression);
				and.AddChildren(children);
				expression = and;
			}
			SetRootExpression(expression);
		}

		/// <summary>Set the root expression for this find.</summary>
		/// <param name="expression"/>
		[InterfaceAudience.Private]
		internal virtual void SetRootExpression(Expression expression)
		{
			this.rootExpression = expression;
		}

		/// <summary>Return the root expression for this find.</summary>
		/// <returns>the root expression</returns>
		[InterfaceAudience.Private]
		internal virtual Expression GetRootExpression()
		{
			return this.rootExpression;
		}

		/// <summary>Returns the current find options, creating them if necessary.</summary>
		[InterfaceAudience.Private]
		internal virtual FindOptions GetOptions()
		{
			if (options == null)
			{
				options = CreateOptions();
			}
			return options;
		}

		/// <summary>Create a new set of find options.</summary>
		private FindOptions CreateOptions()
		{
			FindOptions options = new FindOptions();
			options.SetOut(@out);
			options.SetErr(err);
			options.SetIn(Runtime.@in);
			options.SetCommandFactory(GetCommandFactory());
			options.SetConfiguration(GetConf());
			return options;
		}

		/// <summary>
		/// Add the
		/// <see cref="Org.Apache.Hadoop.FS.Shell.PathData"/>
		/// item to the stop set.
		/// </summary>
		private void AddStop(PathData item)
		{
			stopPaths.AddItem(item.path);
		}

		/// <summary>
		/// Returns true if the
		/// <see cref="Org.Apache.Hadoop.FS.Shell.PathData"/>
		/// item is in the stop set.
		/// </summary>
		private bool IsStop(PathData item)
		{
			return stopPaths.Contains(item.path);
		}

		/// <summary>
		/// Parse a list of arguments to to extract the
		/// <see cref="Expression"/>
		/// elements.
		/// The input Deque will be modified to remove the used elements.
		/// </summary>
		/// <param name="args">arguments to be parsed</param>
		/// <returns>
		/// list of
		/// <see cref="Expression"/>
		/// elements applicable to this command
		/// </returns>
		/// <exception cref="System.IO.IOException">if list can not be parsed</exception>
		private Expression ParseExpression(Deque<string> args)
		{
			Deque<Expression> primaries = new List<Expression>();
			Deque<Expression> operators = new List<Expression>();
			Expression prevExpr = GetExpression(typeof(And));
			while (!args.IsEmpty())
			{
				string arg = args.Pop();
				if ("(".Equals(arg))
				{
					Expression expr = ParseExpression(args);
					primaries.AddItem(expr);
					prevExpr = new _BaseExpression_281();
				}
				else
				{
					// stub the previous expression to be a non-op
					if (")".Equals(arg))
					{
						break;
					}
					else
					{
						if (IsExpression(arg))
						{
							Expression expr = GetExpression(arg);
							expr.AddArguments(args);
							if (expr.IsOperator())
							{
								while (!operators.IsEmpty())
								{
									if (operators.Peek().GetPrecedence() >= expr.GetPrecedence())
									{
										Expression op = operators.Pop();
										op.AddChildren(primaries);
										primaries.Push(op);
									}
									else
									{
										break;
									}
								}
								operators.Push(expr);
							}
							else
							{
								if (!prevExpr.IsOperator())
								{
									Expression and = GetExpression(typeof(And));
									while (!operators.IsEmpty())
									{
										if (operators.Peek().GetPrecedence() >= and.GetPrecedence())
										{
											Expression op = operators.Pop();
											op.AddChildren(primaries);
											primaries.Push(op);
										}
										else
										{
											break;
										}
									}
									operators.Push(and);
								}
								primaries.Push(expr);
							}
							prevExpr = expr;
						}
						else
						{
							throw new IOException("Unexpected argument: " + arg);
						}
					}
				}
			}
			while (!operators.IsEmpty())
			{
				Expression @operator = operators.Pop();
				@operator.AddChildren(primaries);
				primaries.Push(@operator);
			}
			return primaries.IsEmpty() ? GetExpression(typeof(Print)) : primaries.Pop();
		}

		private sealed class _BaseExpression_281 : BaseExpression
		{
			public _BaseExpression_281()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override Result Apply(PathData item, int depth)
			{
				return Result.Pass;
			}
		}

		/// <summary>Returns true if the target is an ancestor of the source.</summary>
		private bool IsAncestor(PathData source, PathData target)
		{
			for (Path parent = source.path; (parent != null) && !parent.IsRoot(); parent = parent
				.GetParent())
			{
				if (parent.Equals(target.path))
				{
					return true;
				}
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void RecursePath(PathData item)
		{
			if (IsStop(item))
			{
				// this item returned a stop result so don't recurse any further
				return;
			}
			if (GetDepth() >= GetOptions().GetMaxDepth())
			{
				// reached the maximum depth so don't got any further.
				return;
			}
			if (item.stat.IsSymlink() && GetOptions().IsFollowLink())
			{
				PathData linkedItem = new PathData(item.stat.GetSymlink().ToString(), GetConf());
				if (IsAncestor(item, linkedItem))
				{
					GetOptions().GetErr().WriteLine("Infinite loop ignored: " + item.ToString() + " -> "
						 + linkedItem.ToString());
					return;
				}
				if (linkedItem.exists)
				{
					item = linkedItem;
				}
			}
			if (item.stat.IsDirectory())
			{
				base.RecursePath(item);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override bool IsPathRecursable(PathData item)
		{
			if (item.stat.IsDirectory())
			{
				return true;
			}
			if (item.stat.IsSymlink())
			{
				PathData linkedItem = new PathData(item.fs.ResolvePath(item.stat.GetSymlink()).ToString
					(), GetConf());
				if (linkedItem.stat.IsDirectory())
				{
					if (GetOptions().IsFollowLink())
					{
						return true;
					}
					if (GetOptions().IsFollowArgLink() && (GetDepth() == 0))
					{
						return true;
					}
				}
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPath(PathData item)
		{
			if (GetOptions().IsDepthFirst())
			{
				// depth first so leave until post processing
				return;
			}
			ApplyItem(item);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void PostProcessPath(PathData item)
		{
			if (!GetOptions().IsDepthFirst())
			{
				// not depth first so already processed
				return;
			}
			ApplyItem(item);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ApplyItem(PathData item)
		{
			if (GetDepth() >= GetOptions().GetMinDepth())
			{
				Result result = GetRootExpression().Apply(item, GetDepth());
				if (Result.Stop.Equals(result))
				{
					AddStop(item);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessArguments(List<PathData> args)
		{
			Expression expr = GetRootExpression();
			expr.SetOptions(GetOptions());
			expr.Prepare();
			base.ProcessArguments(args);
			expr.Finish();
		}

		/// <summary>Gets a named expression from the factory.</summary>
		private Expression GetExpression(string expressionName)
		{
			return ExpressionFactory.GetExpressionFactory().GetExpression(expressionName, GetConf
				());
		}

		/// <summary>Gets an instance of an expression from the factory.</summary>
		private Expression GetExpression(Type expressionClass)
		{
			return ExpressionFactory.GetExpressionFactory().CreateExpression(expressionClass, 
				GetConf());
		}

		/// <summary>Asks the factory whether an expression is recognized.</summary>
		private bool IsExpression(string expressionName)
		{
			return ExpressionFactory.GetExpressionFactory().IsExpression(expressionName);
		}
	}
}
